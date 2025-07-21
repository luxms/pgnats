#[cfg(any(test, feature = "pg_test"))]
#[pgrx::prelude::pg_schema]
mod tests {
    use std::sync::{
        mpsc::{Receiver, Sender},
        Arc, Mutex,
    };

    use pgrx::{pg_test, PgTryBuilder, Spi};

    use crate::{
        api,
        bgw::{SharedQueue, Worker, WorkerState},
        log,
        shared::WorkerMessage,
    };

    enum InternalMockMessage {
        Fetch,
        Insert(String, String),
        Delete(String, String),
        Call(String, Vec<u8>),
    }

    struct MockWorker {
        msg_bus: Sender<InternalMockMessage>,
        fetch_recv: Receiver<anyhow::Result<Vec<(String, String)>>>,
        quit_recv: Receiver<()>,
        state: Arc<Mutex<WorkerState>>,
    }

    impl MockWorker {
        pub fn new(
            msg_bus: Sender<InternalMockMessage>,
            fetch_recv: Receiver<anyhow::Result<Vec<(String, String)>>>,
            quit_recv: Receiver<()>,
            state: Arc<Mutex<WorkerState>>,
        ) -> Self {
            Self {
                msg_bus,
                fetch_recv,
                quit_recv,
                state,
            }
        }
    }

    impl Worker for MockWorker {
        fn transaction<F: FnOnce() -> R + std::panic::UnwindSafe + std::panic::RefUnwindSafe, R>(
            &self,
            body: F,
        ) -> R {
            body()
        }

        fn wait(&self, duration: std::time::Duration) -> bool {
            std::thread::sleep(duration);
            self.quit_recv.try_recv().is_err()
        }

        fn fetch_state(&self) -> WorkerState {
            *self.state.lock().unwrap()
        }

        fn fetch_subject_with_callbacks(&self) -> anyhow::Result<Vec<(String, String)>> {
            self.msg_bus.send(InternalMockMessage::Fetch).unwrap();
            self.fetch_recv.recv().unwrap()
        }

        fn insert_subject_callback(&self, subject: &str, callback: &str) -> anyhow::Result<()> {
            self.msg_bus
                .send(InternalMockMessage::Insert(
                    subject.to_string(),
                    callback.to_string(),
                ))
                .unwrap();
            Ok(())
        }

        fn delete_subject_callback(&self, subject: &str, callback: &str) -> anyhow::Result<()> {
            self.msg_bus
                .send(InternalMockMessage::Delete(
                    subject.to_string(),
                    callback.to_string(),
                ))
                .unwrap();
            Ok(())
        }

        fn call_function(&self, callback: &str, data: &[u8]) -> anyhow::Result<()> {
            self.msg_bus
                .send(InternalMockMessage::Call(
                    callback.to_string(),
                    data.to_vec(),
                ))
                .unwrap();
            Ok(())
        }
    }

    #[cfg(not(any(skip_pgnats_tests)))]
    #[pg_test]
    fn test_sub_unsub_call_pgnats_background_worker() {
        use std::sync::{mpsc::channel, RwLock};

        use pgrx::function_name;

        use crate::{bgw::run::run_worker, ring_queue::RingQueue};

        static SHARED_QUEUE: RwLock<RingQueue<65536>> = RwLock::new(RingQueue::new());

        // INIT
        let table_name = function_name!().split("::").last().unwrap();
        let subject = table_name;
        let fn_name = "example";
        let content = "Съешь ещё этих мягких французских булок, да выпей чаю";

        Spi::run(&format!(
            "CREATE TEMP TABLE {} (
            subject TEXT NOT NULL,
            callback TEXT NOT NULL,
            UNIQUE(subject, callback)
        );",
            table_name
        ))
        .unwrap();

        let state = Arc::new(Mutex::new(WorkerState::Master));
        let (msg_sdr, msg_recv) = channel();
        let (fetch_sdr, fetch_recv) = channel();
        let (quit_sdr, quit_recv) = channel();
        let worker = MockWorker::new(msg_sdr, fetch_recv, quit_recv, state.clone());

        let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));
        {
            match msg_recv.recv().expect("Failed to get fetch") {
                InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
                _ => {}
            };
            fetch_sdr
                .send(fetch_subject_with_callbacks(table_name))
                .unwrap();
        }

        pgnats_subscribe(subject.to_string(), fn_name.to_string(), &SHARED_QUEUE);

        // LOOP

        {
            match msg_recv.recv().expect("Failed to get fetch") {
                InternalMockMessage::Insert(sub, callback) => {
                    assert_eq!(sub, subject);
                    assert_eq!(callback, fn_name);

                    insert_subject_callback(table_name, &sub, &callback).unwrap();
                }
                InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Insert'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Insert'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Insert'"),
            }
        }

        let subs = fetch_subject_with_callbacks(table_name).unwrap();
        assert_eq!(subs[0].0, subject);
        assert_eq!(subs[0].1, fn_name);

        api::nats_publish_text(subject, content.to_string()).unwrap();

        {
            match msg_recv.recv().expect("Failed to get fetch") {
                InternalMockMessage::Call(callback, data) => {
                    assert_eq!(callback, fn_name);
                    assert_eq!(data, content.as_bytes());

                    assert!(call_function(&callback, &data).is_err());
                }
                InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Call'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Call'"),
                InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Call'"),
            }
        }

        pgnats_unsubscribe(subject.to_string(), fn_name.to_string(), &SHARED_QUEUE);

        {
            match msg_recv.recv().expect("Failed to get fetch") {
                InternalMockMessage::Delete(sub, callback) => {
                    assert_eq!(sub, subject);
                    assert_eq!(callback, fn_name);

                    delete_subject_callback(table_name, &sub, &callback).unwrap();
                }
                InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Delete'"),
                InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Delete'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Delete'"),
            }
        }

        let subs = fetch_subject_with_callbacks(table_name).unwrap();
        assert_eq!(subs.len(), 0);

        // FAKE SIGTERM

        quit_sdr.send(()).unwrap();
        assert!(handle.join().is_ok());
    }

    fn pgnats_subscribe<const N: usize, Q>(subject: String, fn_name: String, queue: &Q)
    where
        Q: SharedQueue<N>,
    {
        let msg = WorkerMessage::Subscribe { subject, fn_name };
        let buf = bincode::encode_to_vec(msg, bincode::config::standard()).unwrap();

        queue.unique().try_send(&buf).unwrap();
    }

    fn pgnats_unsubscribe<const N: usize, Q>(subject: String, fn_name: String, queue: &Q)
    where
        Q: SharedQueue<N>,
    {
        let msg = WorkerMessage::Unsubscribe { subject, fn_name };
        let buf = bincode::encode_to_vec(msg, bincode::config::standard()).unwrap();

        queue.unique().try_send(&buf).unwrap();
    }

    fn fetch_subject_with_callbacks(table_name: &str) -> anyhow::Result<Vec<(String, String)>> {
        PgTryBuilder::new(|| {
            Spi::connect_mut(|client| {
                let sql = format!("SELECT subject, callback FROM {}", table_name);
                let tuples = client.select(&sql, None, &[])?;
                let subject_callbacks: Vec<(String, String)> = tuples
                    .into_iter()
                    .filter_map(|tuple| {
                        let subject = tuple.get_by_name::<String, _>("subject");
                        let callback = tuple.get_by_name::<String, _>("callback");

                        match (subject, callback) {
                            (Ok(Some(subject)), Ok(Some(callback))) => Some((subject, callback)),
                            _ => None,
                        }
                    })
                    .collect();

                log!(
                    "Fetched {} registered subject callbacks",
                    subject_callbacks.len()
                );

                Ok(subject_callbacks)
            })
        })
        .catch_others(|e| match e {
            pgrx::pg_sys::panic::CaughtError::PostgresError(err) => Err(anyhow::anyhow!(
                "Code '{}': {}. ({:?})",
                err.sql_error_code(),
                err.message(),
                err.hint()
            )),
            _ => Err(anyhow::anyhow!("{:?}", e)),
        })
        .execute()
    }

    fn insert_subject_callback(
        table_name: &str,
        subject: &str,
        callback: &str,
    ) -> anyhow::Result<()> {
        PgTryBuilder::new(|| {
            Spi::connect_mut(|client| {
                let sql = format!("INSERT INTO {} VALUES ($1, $2)", table_name);
                let _ = client.update(&sql, None, &[subject.into(), callback.into()])?;

                log!(
                    "Inserted subject callback: subject='{}', callback='{}'",
                    subject,
                    callback
                );

                Ok(())
            })
        })
        .catch_others(|e| match e {
            pgrx::pg_sys::panic::CaughtError::PostgresError(err) => Err(anyhow::anyhow!(
                "Code '{}': {}. ({:?})",
                err.sql_error_code(),
                err.message(),
                err.hint()
            )),
            _ => Err(anyhow::anyhow!("{:?}", e)),
        })
        .execute()
    }

    fn delete_subject_callback(
        table_name: &str,
        subject: &str,
        callback: &str,
    ) -> anyhow::Result<()> {
        PgTryBuilder::new(|| {
            Spi::connect_mut(|client| {
                let sql = format!(
                    "DELETE FROM {} WHERE subject = $1 AND callback = $2",
                    table_name
                );
                let _ = client.update(&sql, None, &[subject.into(), callback.into()])?;

                log!(
                    "Deleted subject callback: subject='{}', callback='{}'",
                    subject,
                    callback
                );

                Ok(())
            })
        })
        .catch_others(|e| match e {
            pgrx::pg_sys::panic::CaughtError::PostgresError(err) => Err(anyhow::anyhow!(
                "Code '{}': {}. ({:?})",
                err.sql_error_code(),
                err.message(),
                err.hint()
            )),
            _ => Err(anyhow::anyhow!("{:?}", e)),
        })
        .execute()
    }

    fn call_function(callback: &str, data: &[u8]) -> anyhow::Result<()> {
        if !callback
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(anyhow::anyhow!("Invalid callback function name"));
        }

        PgTryBuilder::new(|| {
            Spi::connect_mut(|client| {
                let sql = format!("SELECT {}($1)", callback);
                let _ = client.update(&sql, None, &[data.into()])?;
                Ok(())
            })
        })
        .catch_others(|e| match e {
            pgrx::pg_sys::panic::CaughtError::PostgresError(err) => Err(anyhow::anyhow!(
                "Code '{}': {}. ({:?})",
                err.sql_error_code(),
                err.message(),
                err.hint()
            )),
            _ => Err(anyhow::anyhow!("{:?}", e)),
        })
        .execute()
    }
}
