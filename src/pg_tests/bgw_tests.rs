#[cfg(any(test, feature = "pg_test"))]
#[pgrx::prelude::pg_schema]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            mpsc::{Receiver, Sender},
        },
    };

    use pgrx::{PgTryBuilder, Spi, pg_test};

    use crate::{
        api,
        bgw::{SharedQueue, Worker, WorkerState},
        config::{Config, parse_config},
        connection::NatsConnectionOptions,
        log,
        notification::{PgInstanceNotification, PgInstanceTransition},
        worker_queue::WorkerMessage,
    };

    #[derive(Debug)]
    enum InternalMockMessage {
        Fetch,
        Insert(String, String),
        Delete(String, String),
        Call(String, Vec<u8>),
    }

    struct MockWorker {
        msg_bus: Sender<InternalMockMessage>,
        fetch_recv: Receiver<anyhow::Result<Vec<(String, String)>>>,
        notification_recv: Receiver<Option<PgInstanceNotification>>,
        quit_recv: Receiver<()>,
        state: Arc<Mutex<WorkerState>>,
        notify_subject: Option<String>,
    }

    impl MockWorker {
        pub fn new(
            msg_bus: Sender<InternalMockMessage>,
            fetch_recv: Receiver<anyhow::Result<Vec<(String, String)>>>,
            notification_recv: Receiver<Option<PgInstanceNotification>>,
            quit_recv: Receiver<()>,
            state: Arc<Mutex<WorkerState>>,
            notify_subject: Option<String>,
        ) -> Self {
            Self {
                msg_bus,
                fetch_recv,
                notification_recv,
                quit_recv,
                state,
                notify_subject,
            }
        }
    }

    impl Worker for MockWorker {
        fn wait(&self, duration: std::time::Duration) -> bool {
            std::thread::sleep(duration);
            self.quit_recv.try_recv().is_err()
        }

        fn fetch_state(&self) -> WorkerState {
            *self.state.lock().unwrap()
        }

        fn fetch_subject_with_callbacks(&self) -> anyhow::Result<Vec<(String, String)>> {
            self.msg_bus.send(InternalMockMessage::Fetch).unwrap();
            self.fetch_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
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

        fn fetch_config(&self) -> Config {
            if let Some(notify_subject) = &self.notify_subject {
                parse_config(&HashMap::from_iter([(
                    "notify_subject".into(),
                    notify_subject.into(),
                )]))
            } else {
                parse_config(&HashMap::new())
            }
        }

        fn make_notification(
            &self,
            _: PgInstanceTransition,
            _: Option<&str>,
        ) -> Option<PgInstanceNotification> {
            self.notification_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
        }
    }

    #[pg_test]
    fn test_background_worker_sub_call_unsub_call() {
        use std::sync::{RwLock, mpsc::channel};

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
        let (_, notification_recv) = channel();
        let (quit_sdr, quit_recv) = channel();
        let worker = MockWorker::new(
            msg_sdr,
            fetch_recv,
            notification_recv,
            quit_recv,
            state.clone(),
            None,
        );

        let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));
        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get fetch")
            {
                InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
                _ => {}
            };
            fetch_sdr
                .send(fetch_subject_with_callbacks(table_name))
                .unwrap();
        }

        // LOOP

        pgnats_subscribe(subject.to_string(), fn_name.to_string(), &SHARED_QUEUE);

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get insert")
            {
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
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get call")
            {
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
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get delete")
            {
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

        api::nats_publish_text(subject, content.to_string()).unwrap();

        {
            assert!(matches!(
                msg_recv.recv_timeout(std::time::Duration::from_secs(5)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ));
        }

        // FAKE SIGTERM

        quit_sdr.send(()).unwrap();
        assert!(handle.join().is_ok());
    }

    #[pg_test]
    fn test_background_worker_restore_after_restart() {
        use std::sync::{RwLock, mpsc::channel};

        use pgrx::function_name;

        use crate::{bgw::run::run_worker, ring_queue::RingQueue};

        static SHARED_QUEUE: RwLock<RingQueue<65536>> = RwLock::new(RingQueue::new());

        // INIT
        let table_name = function_name!().split("::").last().unwrap();
        let subject1 = format!("{}_1", table_name);
        let subject2 = format!("{}_2", table_name);
        let fn_name = "example";

        Spi::run(&format!(
            "CREATE TEMP TABLE {} (
            subject TEXT NOT NULL,
            callback TEXT NOT NULL,
            UNIQUE(subject, callback)
        );",
            table_name
        ))
        .unwrap();

        Spi::run(&format!(
            "INSERT INTO {} (subject, callback) VALUES
                ('{}', '{}'),
                ('{}', '{}');",
            table_name, subject1, fn_name, subject2, fn_name
        ))
        .unwrap();

        let state = Arc::new(Mutex::new(WorkerState::Master));
        let (msg_sdr, msg_recv) = channel();
        let (fetch_sdr, fetch_recv) = channel();
        let (_, notification_recv) = channel();
        let (quit_sdr, quit_recv) = channel();
        let worker = MockWorker::new(
            msg_sdr,
            fetch_recv,
            notification_recv,
            quit_recv,
            state.clone(),
            None,
        );

        let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get fetch")
            {
                InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
                _ => {}
            };

            let result = fetch_subject_with_callbacks(table_name).unwrap();

            assert_eq!(result.len(), 2);
            assert!(
                result
                    .iter()
                    .find(|(sub, call)| sub == &subject1 && call == fn_name)
                    .is_some()
            );
            assert!(
                result
                    .iter()
                    .find(|(sub, call)| sub == &subject2 && call == fn_name)
                    .is_some()
            );
            fetch_sdr.send(Ok(result)).unwrap();
        }

        // FAKE SIGTERM

        quit_sdr.send(()).unwrap();
        assert!(handle.join().is_ok());
    }

    #[pg_test]
    fn test_background_worker_changed_fdw_config() {
        use std::sync::{RwLock, mpsc::channel};

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
        let (_, notification_recv) = channel();
        let (quit_sdr, quit_recv) = channel();
        let worker = MockWorker::new(
            msg_sdr,
            fetch_recv,
            notification_recv,
            quit_recv,
            state.clone(),
            None,
        );

        let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get fetch")
            {
                InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
                _ => {}
            };

            fetch_sdr
                .send(fetch_subject_with_callbacks(table_name))
                .unwrap();
        }

        // LOOP

        pgnats_bgw_reload_conf(
            Config {
                nats_opt: NatsConnectionOptions {
                    host: "fake address".to_string(),
                    port: 1333,
                    capacity: 128,
                    tls: None,
                },
                notify_subject: None,
                patroni_url: Some("http://localhost:28008/patroni".to_string()),
            },
            &SHARED_QUEUE,
        );

        pgnats_subscribe(subject.to_string(), fn_name.to_string(), &SHARED_QUEUE);

        {
            assert!(matches!(
                msg_recv.recv_timeout(std::time::Duration::from_secs(5)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ));
        }

        pgnats_bgw_reload_conf(
            Config {
                nats_opt: NatsConnectionOptions {
                    host: "localhost".to_string(),
                    port: 4222,
                    capacity: 128,
                    tls: None,
                },
                notify_subject: None,
                patroni_url: Some("http://localhost:28008/patroni".to_string()),
            },
            &SHARED_QUEUE,
        );

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get fetch")
            {
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

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get insert")
            {
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
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get call")
            {
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

        // FAKE SIGTERM

        quit_sdr.send(()).unwrap();
        assert!(handle.join().is_ok());
    }

    #[pg_test]
    fn test_background_worker_different_subject_sub_call() {
        use std::sync::{RwLock, mpsc::channel};

        use pgrx::function_name;

        use crate::{bgw::run::run_worker, ring_queue::RingQueue};

        static SHARED_QUEUE: RwLock<RingQueue<65536>> = RwLock::new(RingQueue::new());

        // INIT
        let table_name = function_name!().split("::").last().unwrap();
        let subject1 = format!("{}_1", table_name);
        let subject2 = format!("{}_2", table_name);
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
        let (_, notification_recv) = channel();
        let (quit_sdr, quit_recv) = channel();
        let worker = MockWorker::new(
            msg_sdr,
            fetch_recv,
            notification_recv,
            quit_recv,
            state.clone(),
            None,
        );

        let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get fetch")
            {
                InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
                _ => {}
            };

            fetch_sdr
                .send(fetch_subject_with_callbacks(table_name))
                .unwrap();
        }

        pgnats_subscribe(subject1.to_string(), fn_name.to_string(), &SHARED_QUEUE);

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get insert")
            {
                InternalMockMessage::Insert(sub, callback) => {
                    assert_eq!(sub, subject1);
                    assert_eq!(callback, fn_name);

                    insert_subject_callback(table_name, &sub, &callback).unwrap();
                }
                InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Insert'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Insert'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Insert'"),
            }
        }

        pgnats_subscribe(subject2.to_string(), fn_name.to_string(), &SHARED_QUEUE);

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get insert")
            {
                InternalMockMessage::Insert(sub, callback) => {
                    assert_eq!(sub, subject2);
                    assert_eq!(callback, fn_name);

                    insert_subject_callback(table_name, &sub, &callback).unwrap();
                }
                InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Insert'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Insert'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Insert'"),
            }
        }

        let result = fetch_subject_with_callbacks(table_name).unwrap();

        assert_eq!(result.len(), 2);
        assert!(
            result
                .iter()
                .find(|(sub, call)| sub == &subject1 && call == fn_name)
                .is_some()
        );
        assert!(
            result
                .iter()
                .find(|(sub, call)| sub == &subject2 && call == fn_name)
                .is_some()
        );

        api::nats_publish_text(&subject1, content.to_string()).unwrap();

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get call")
            {
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

        // FAKE SIGTERM

        quit_sdr.send(()).unwrap();
        assert!(handle.join().is_ok());
    }

    #[pg_test]
    fn test_background_worker_same_subject_sub_call() {
        use std::sync::{RwLock, mpsc::channel};

        use pgrx::function_name;

        use crate::{bgw::run::run_worker, ring_queue::RingQueue};

        static SHARED_QUEUE: RwLock<RingQueue<65536>> = RwLock::new(RingQueue::new());

        // INIT
        let table_name = function_name!().split("::").last().unwrap();
        let subject = table_name;
        let fn_name1 = "foo";
        let fn_name2 = "bar";
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
        let (_, notification_recv) = channel();
        let (quit_sdr, quit_recv) = channel();
        let worker = MockWorker::new(
            msg_sdr,
            fetch_recv,
            notification_recv,
            quit_recv,
            state.clone(),
            None,
        );

        let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get fetch")
            {
                InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
                _ => {}
            };

            fetch_sdr
                .send(fetch_subject_with_callbacks(table_name))
                .unwrap();
        }

        pgnats_subscribe(subject.to_string(), fn_name1.to_string(), &SHARED_QUEUE);

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get insert")
            {
                InternalMockMessage::Insert(sub, callback) => {
                    assert_eq!(sub, subject);
                    assert_eq!(callback, fn_name1);

                    insert_subject_callback(table_name, &sub, &callback).unwrap();
                }
                InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Insert'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Insert'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Insert'"),
            }
        }

        pgnats_subscribe(subject.to_string(), fn_name2.to_string(), &SHARED_QUEUE);

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get insert")
            {
                InternalMockMessage::Insert(sub, callback) => {
                    assert_eq!(sub, subject);
                    assert_eq!(callback, fn_name2);

                    insert_subject_callback(table_name, &sub, &callback).unwrap();
                }
                InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Insert'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Insert'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Insert'"),
            }
        }

        let result = fetch_subject_with_callbacks(table_name).unwrap();

        assert_eq!(result.len(), 2);
        assert!(
            result
                .iter()
                .find(|(sub, call)| sub == &subject && call == fn_name1)
                .is_some()
        );
        assert!(
            result
                .iter()
                .find(|(sub, call)| sub == &subject && call == fn_name2)
                .is_some()
        );

        api::nats_publish_text(&subject, content.to_string()).unwrap();

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get call")
            {
                InternalMockMessage::Call(callback, data) => {
                    assert!(callback == fn_name1 || callback == fn_name2);
                    assert_eq!(data, content.as_bytes());

                    assert!(call_function(&callback, &data).is_err());
                }
                InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Call'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Call'"),
                InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Call'"),
            }
        }

        // FAKE SIGTERM

        quit_sdr.send(()).unwrap();
        assert!(handle.join().is_ok());
    }

    #[cfg(not(any(skip_pgnats_tests)))]
    #[pg_test]
    fn test_background_worker_sub_change_master_to_slave_try_call() {
        use std::sync::{RwLock, mpsc::channel};

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
        let (_, notification_recv) = channel();
        let (quit_sdr, quit_recv) = channel();
        let worker = MockWorker::new(
            msg_sdr,
            fetch_recv,
            notification_recv,
            quit_recv,
            state.clone(),
            None,
        );

        let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));
        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get fetch")
            {
                InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
                _ => {}
            };
            fetch_sdr
                .send(fetch_subject_with_callbacks(table_name))
                .unwrap();
        }

        // LOOP

        pgnats_subscribe(subject.to_string(), fn_name.to_string(), &SHARED_QUEUE);

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get insert")
            {
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

        {
            *state.lock().unwrap() = WorkerState::Replica;
        }

        std::thread::sleep(std::time::Duration::from_secs(5));

        api::nats_publish_text(subject, content.to_string()).unwrap();

        {
            assert!(matches!(
                msg_recv.recv_timeout(std::time::Duration::from_secs(5)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ));
        }

        // FAKE SIGTERM

        quit_sdr.send(()).unwrap();
        assert!(handle.join().is_ok());
    }

    #[pg_test]
    fn test_background_worker_try_sub_change_slave_to_master_call() {
        use std::sync::{RwLock, mpsc::channel};

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

        Spi::run(&format!(
            "INSERT INTO {} (subject, callback) VALUES
                ('{}', '{}');",
            table_name, subject, fn_name
        ))
        .unwrap();

        let state = Arc::new(Mutex::new(WorkerState::Replica));
        let (msg_sdr, msg_recv) = channel();
        let (fetch_sdr, fetch_recv) = channel();
        let (_, notification_recv) = channel();
        let (quit_sdr, quit_recv) = channel();
        let worker = MockWorker::new(
            msg_sdr,
            fetch_recv,
            notification_recv,
            quit_recv,
            state.clone(),
            None,
        );

        let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));

        {
            assert!(matches!(
                msg_recv.recv_timeout(std::time::Duration::from_secs(5)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ));
        }

        // LOOP

        pgnats_subscribe(subject.to_string(), fn_name.to_string(), &SHARED_QUEUE);

        {
            assert!(matches!(
                msg_recv.recv_timeout(std::time::Duration::from_secs(5)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ));
        }

        {
            *state.lock().unwrap() = WorkerState::Master;
        }

        std::thread::sleep(std::time::Duration::from_secs(5));

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get fetch")
            {
                InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
                _ => {}
            };

            let result = fetch_subject_with_callbacks(table_name).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].0, subject);
            assert_eq!(result[0].1, fn_name);

            fetch_sdr.send(Ok(result)).unwrap();
        }

        api::nats_publish_text(subject, content.to_string()).unwrap();

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get call")
            {
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

        // FAKE SIGTERM

        quit_sdr.send(()).unwrap();
        assert!(handle.join().is_ok());
    }

    #[pg_test]
    fn test_background_worker_m2r() {
        use std::sync::{RwLock, mpsc::channel};

        use pgrx::function_name;

        use crate::{bgw::run::run_worker, ring_queue::RingQueue};

        static SHARED_QUEUE: RwLock<RingQueue<65536>> = RwLock::new(RingQueue::new());

        // INIT
        let table_name = function_name!().split("::").last().unwrap();
        let notify_subject = table_name;
        let patroni_addr = "127.0.0.1:28008";

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
        let (notification_sdr, notification_recv) = channel();
        let (quit_sdr, quit_recv) = channel();
        let worker = MockWorker::new(
            msg_sdr,
            fetch_recv,
            notification_recv,
            quit_recv,
            state.clone(),
            Some(notify_subject.to_string()),
        );

        let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get fetch")
            {
                InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
                _ => {}
            };
            fetch_sdr
                .send(fetch_subject_with_callbacks(table_name))
                .unwrap();
        }

        let (sub_sdr, sub_recv) = channel();
        let (start_sub_sdr, start_sub_recv) = channel();
        let (start_server_sdr, start_server_recv) = channel();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to initialize Tokio runtime");

        let server_handle = rt.spawn(start_mock_patroni(
            table_name.to_string(),
            start_server_sdr,
            patroni_addr,
        ));

        let _ = start_server_recv
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();

        let sub_handle = rt.spawn(start_subscription(notify_subject, start_sub_sdr, sub_sdr));

        start_sub_recv
            .recv_timeout(std::time::Duration::from_secs(30))
            .unwrap();

        // LOOP

        {
            *state.lock().unwrap() = WorkerState::Replica;
            notification_sdr
                .send(PgInstanceNotification::new(
                    PgInstanceTransition::M2R,
                    Some(&format!("http://{patroni_addr}/patroni")),
                ))
                .unwrap();
        }

        {
            let message = sub_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
                .unwrap();

            assert_eq!(message.transition, PgInstanceTransition::M2R);

            assert!(message.listen_addresses.len() > 0);
            assert_eq!(message.listen_addresses[0], "localhost");

            #[cfg(feature = "pg13")]
            assert_eq!(message.port, 32213);
            #[cfg(feature = "pg14")]
            assert_eq!(message.port, 32214);
            #[cfg(feature = "pg15")]
            assert_eq!(message.port, 32215);
            #[cfg(feature = "pg16")]
            assert_eq!(message.port, 32216);
            #[cfg(feature = "pg17")]
            assert_eq!(message.port, 32217);
            #[cfg(feature = "pg18")]
            assert_eq!(message.port, 32218);

            assert_eq!(message.name.unwrap().as_str(), table_name);
        }

        // FAKE SIGTERM

        server_handle.abort();
        sub_handle.abort();
        quit_sdr.send(()).unwrap();
        assert!(handle.join().is_ok());
    }

    #[pg_test]
    fn test_background_worker_r2m() {
        use std::sync::{RwLock, mpsc::channel};

        use pgrx::function_name;

        use crate::{bgw::run::run_worker, ring_queue::RingQueue};

        static SHARED_QUEUE: RwLock<RingQueue<65536>> = RwLock::new(RingQueue::new());

        // INIT
        let table_name = function_name!().split("::").last().unwrap();
        let notify_subject = table_name;
        let patroni_addr = "127.0.0.1:28009";

        Spi::run(&format!(
            "CREATE TEMP TABLE {} (
            subject TEXT NOT NULL,
            callback TEXT NOT NULL,
            UNIQUE(subject, callback)
        );",
            table_name
        ))
        .unwrap();

        let state = Arc::new(Mutex::new(WorkerState::Replica));
        let (msg_sdr, msg_recv) = channel();
        let (fetch_sdr, fetch_recv) = channel();
        let (notification_sdr, notification_recv) = channel();
        let (quit_sdr, quit_recv) = channel();
        let worker = MockWorker::new(
            msg_sdr,
            fetch_recv,
            notification_recv,
            quit_recv,
            state.clone(),
            Some(notify_subject.to_string()),
        );

        let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));

        let (sub_sdr, sub_recv) = channel();
        let (start_sub_sdr, start_sub_recv) = channel();
        let (start_server_sdr, start_server_recv) = channel();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to initialize Tokio runtime");

        let server_handle = rt.spawn(start_mock_patroni(
            table_name.to_string(),
            start_server_sdr,
            patroni_addr,
        ));

        let _ = start_server_recv
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();

        let sub_handle = rt.spawn(start_subscription(notify_subject, start_sub_sdr, sub_sdr));

        start_sub_recv
            .recv_timeout(std::time::Duration::from_secs(30))
            .unwrap();

        // LOOP

        {
            *state.lock().unwrap() = WorkerState::Master;
            notification_sdr
                .send(PgInstanceNotification::new(
                    PgInstanceTransition::R2M,
                    Some(&format!("http://{patroni_addr}/patroni")),
                ))
                .unwrap();
        }

        {
            match msg_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get fetch")
            {
                InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
                InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
                InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
                _ => {}
            };
            fetch_sdr
                .send(fetch_subject_with_callbacks(table_name))
                .unwrap();
        }

        {
            let message = sub_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
                .unwrap();

            assert_eq!(message.transition, PgInstanceTransition::R2M);

            assert!(message.listen_addresses.len() > 0);
            assert_eq!(message.listen_addresses[0], "localhost");

            #[cfg(feature = "pg13")]
            assert_eq!(message.port, 32213);
            #[cfg(feature = "pg14")]
            assert_eq!(message.port, 32214);
            #[cfg(feature = "pg15")]
            assert_eq!(message.port, 32215);
            #[cfg(feature = "pg16")]
            assert_eq!(message.port, 32216);
            #[cfg(feature = "pg17")]
            assert_eq!(message.port, 32217);
            #[cfg(feature = "pg18")]
            assert_eq!(message.port, 32218);

            assert_eq!(message.name.unwrap().as_str(), table_name);
        }

        // FAKE SIGTERM

        server_handle.abort();
        sub_handle.abort();
        quit_sdr.send(()).unwrap();
        assert!(handle.join().is_ok());
    }

    fn pgnats_bgw_reload_conf<const N: usize, Q>(opt: Config, queue: &Q)
    where
        Q: SharedQueue<N>,
    {
        let msg = WorkerMessage::NewConfig(opt);
        let buf = bincode::encode_to_vec(msg, bincode::config::standard()).unwrap();

        queue.unique().try_send(&buf).unwrap();
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

    async fn start_subscription(
        notify_subject: &str,
        start_sub_sdr: Sender<()>,
        sub_sdr: Sender<anyhow::Result<PgInstanceNotification>>,
    ) {
        use crate::notification::PgInstanceNotification;

        async fn run(
            start_sub_sdr: Sender<()>,
            subject: String,
        ) -> anyhow::Result<PgInstanceNotification> {
            use futures::StreamExt;

            let client = async_nats::connect("127.0.0.1:4222").await?;

            let mut sub = client.subscribe(subject).await?;

            start_sub_sdr.send(()).unwrap();

            if let Some(msg) = sub.next().await {
                let value = serde_json::from_slice(&msg.payload)?;
                Ok(value)
            } else {
                Err(anyhow::anyhow!("Subscription is broken"))
            }
        }

        let _ = sub_sdr.send(run(start_sub_sdr, notify_subject.to_string()).await);
    }

    async fn start_mock_patroni(name: String, start_sdr: Sender<()>, patroni_addr: &str) {
        use tokio::net::TcpListener;
        use tokio_stream::wrappers::TcpListenerStream;
        use warp::Filter;

        let patroin_route = warp::path("patroni").and(warp::get()).map(move || {
            let response = serde_json::json!({
                "patroni": {
                    "name": name
                }
            });

            warp::reply::json(&response)
        });

        let listener = TcpListener::bind(patroni_addr).await.unwrap();

        start_sdr.send(()).unwrap();

        warp::serve(patroin_route)
            .run_incoming(TcpListenerStream::new(listener))
            .await;
    }
}
