#[cfg(any(test, feature = "pg_test"))]
pub fn init_test_shared_memory() {
    use pgrx::{PgSharedMemoryInitialization, pg_guard, pg_shmem_init, pg_sys};

    use crate::pg_tests::bgw_tests::tests_items::*;

    pg_shmem_init!(LAUNCHER_MESSAGE_BUS1);
    pg_shmem_init!(TEST_RESULT1);

    pg_shmem_init!(LAUNCHER_MESSAGE_BUS2);
    pg_shmem_init!(TEST_RESULT2);
}

#[cfg(any(test, feature = "pg_test"))]
mod tests_items {
    use crate::generate_test_background_worker;

    generate_test_background_worker!(1, c"l1", c"r1", "create_test_fdw_1",
        r#"
        CREATE TABLE test_subscription_table_1 (
            subject TEXT NOT NULL,
            callback TEXT NOT NULL,
            UNIQUE(subject, callback)
        );
        CREATE FOREIGN DATA WRAPPER pgnats_fdw_test_1;
        CREATE SERVER test_background_worker_sub_call_unsub_call FOREIGN DATA WRAPPER pgnats_fdw_test_1 OPTIONS (host 'localhost', port '4222');
        "#
    );

    generate_test_background_worker!(2, c"l2", c"r2", "create_test_fdw_2",
        r#"
        CREATE TABLE test_subscription_table_2 (
            subject TEXT NOT NULL,
            callback TEXT NOT NULL,
            UNIQUE(subject, callback)
        );

        INSERT INTO test_subscription_table_2 (subject, callback) VALUES
            ('test_background_worker_restore_after_restart', 'test_2_fn'),
            ('test_background_worker_restore_after_restart', 'example_fn_2')
        ;

        CREATE FOREIGN DATA WRAPPER pgnats_fdw_test_2;
        CREATE SERVER test_background_worker_restore_after_restart FOREIGN DATA WRAPPER pgnats_fdw_test_2 OPTIONS (host 'localhost', port '4222');
        "#
    );
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::prelude::pg_schema]
pub(super) mod tests {
    use std::hash::{DefaultHasher, Hasher};

    use pgrx::{bgworkers::BackgroundWorkerBuilder, pg_test, PgLwLock};

    use crate::{api, bgw::ring_queue::RingQueue, constants::EXTENSION_NAME, pg_tests::bgw_tests::tests_items::*};

    #[pg_test]
    fn test_background_worker_sub_call_unsub_call() {
        use pgrx::function_name;

        let table_name = function_name!().split("::").last().unwrap();
        let subject = table_name;
        let fn_name = "test_1_fn";
        let content = "Съешь ещё этих мягких французских булок, да выпей чаю";

        let worker = BackgroundWorkerBuilder::new("PGNats Background Worker Launcher 1")
            .set_function("background_worker_launcher_entry_point_test_1")
            .set_library(EXTENSION_NAME)
            .enable_spi_access()
            .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
            .load_dynamic()
            .unwrap();

        let _ = worker.wait_for_startup().unwrap();

        std::thread::sleep(std::time::Duration::from_secs(3));

        pgnats_subscribe(
            subject.to_string(),
            fn_name.to_string(),
            &LAUNCHER_MESSAGE_BUS1,
        );

        std::thread::sleep(std::time::Duration::from_secs(3));

        api::nats_publish_text(subject, content.to_string(), None, None).unwrap();

        std::thread::sleep(std::time::Duration::from_secs(3));

        let mut hasher = DefaultHasher::new();
        hasher.write(content.as_bytes());
        assert_eq!(*TEST_RESULT1.share(), hasher.finish());

        pgnats_unsubscribe(
            subject.to_string(),
            fn_name.to_string(),
            &LAUNCHER_MESSAGE_BUS1,
        );

        std::thread::sleep(std::time::Duration::from_secs(3));

        api::nats_publish_text(subject, content.to_string(), None, None).unwrap();

        std::thread::sleep(std::time::Duration::from_secs(3));

        let terminate = worker.terminate();
        terminate.wait_for_shutdown().unwrap();
    }

    #[pg_test]
    fn test_background_worker_restore_after_restart() {
        use pgrx::function_name;

        let table_name = function_name!().split("::").last().unwrap();
        let subject = table_name;
        let content = "Съешь ещё этих мягких французских булок, да выпей чаю";

        let worker = BackgroundWorkerBuilder::new("PGNats Background Worker Launcher 2")
            .set_function("background_worker_launcher_entry_point_test_2")
            .set_library(EXTENSION_NAME)
            .enable_spi_access()
            .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
            .load_dynamic()
            .unwrap();

        let _ = worker.wait_for_startup().unwrap();

        std::thread::sleep(std::time::Duration::from_secs(3));

        api::nats_publish_text(subject, content.to_string(), None, None).unwrap();

        std::thread::sleep(std::time::Duration::from_secs(3));

        let mut hasher = DefaultHasher::new();
        hasher.write(content.as_bytes());
        assert_eq!(*TEST_RESULT2.share(), hasher.finish());

        let terminate = worker.terminate();
        terminate.wait_for_shutdown().unwrap();
    }

    // #[pg_test]
    // fn test_background_worker_changed_fdw_config() {
    //     use std::sync::{RwLock, mpsc::channel};

    //     use pgrx::function_name;

    //     use crate::{bgw::run::run_worker, ring_queue::RingQueue};

    //     static SHARED_QUEUE: RwLock<RingQueue<65536>> = RwLock::new(RingQueue::new());

    //     // INIT
    //     let table_name = function_name!().split("::").last().unwrap();
    //     let subject = table_name;
    //     let fn_name = "example";
    //     let content = "Съешь ещё этих мягких французских булок, да выпей чаю";

    //     Spi::run(&format!(
    //         "CREATE TEMP TABLE {} (
    //         subject TEXT NOT NULL,
    //         callback TEXT NOT NULL,
    //         UNIQUE(subject, callback)
    //     );",
    //         table_name
    //     ))
    //     .unwrap();

    //     let state = Arc::new(Mutex::new(WorkerState::Master));
    //     let (msg_sdr, msg_recv) = channel();
    //     let (fetch_sdr, fetch_recv) = channel();
    //     let (_, notification_recv) = channel();
    //     let (quit_sdr, quit_recv) = channel();
    //     let worker = MockWorker::new(
    //         msg_sdr,
    //         fetch_recv,
    //         notification_recv,
    //         quit_recv,
    //         state.clone(),
    //         None,
    //     );

    //     let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get fetch")
    //         {
    //             InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
    //             InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
    //             _ => {}
    //         };

    //         fetch_sdr
    //             .send(fetch_subject_with_callbacks(table_name))
    //             .unwrap();
    //     }

    //     // LOOP

    //     pgnats_bgw_reload_conf(
    //         Config {
    //             nats_opt: NatsConnectionOptions {
    //                 host: "fake address".to_string(),
    //                 port: 1333,
    //                 capacity: 128,
    //                 tls: None,
    //             },
    //             notify_subject: None,
    //             patroni_url: Some("http://localhost:28008/patroni".to_string()),
    //         },
    //         &SHARED_QUEUE,
    //     );

    //     pgnats_subscribe(subject.to_string(), fn_name.to_string(), &SHARED_QUEUE);

    //     {
    //         assert!(matches!(
    //             msg_recv.recv_timeout(std::time::Duration::from_secs(5)),
    //             Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    //         ));
    //     }

    //     pgnats_bgw_reload_conf(
    //         Config {
    //             nats_opt: NatsConnectionOptions {
    //                 host: "localhost".to_string(),
    //                 port: 4222,
    //                 capacity: 128,
    //                 tls: None,
    //             },
    //             notify_subject: None,
    //             patroni_url: Some("http://localhost:28008/patroni".to_string()),
    //         },
    //         &SHARED_QUEUE,
    //     );

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get fetch")
    //         {
    //             InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
    //             InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
    //             _ => {}
    //         };

    //         fetch_sdr
    //             .send(fetch_subject_with_callbacks(table_name))
    //             .unwrap();
    //     }

    //     pgnats_subscribe(subject.to_string(), fn_name.to_string(), &SHARED_QUEUE);

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get insert")
    //         {
    //             InternalMockMessage::Insert(sub, callback) => {
    //                 assert_eq!(sub, subject);
    //                 assert_eq!(callback, fn_name);

    //                 insert_subject_callback(table_name, &sub, &callback).unwrap();
    //             }
    //             InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Insert'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Insert'"),
    //             InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Insert'"),
    //         }
    //     }

    //     let subs = fetch_subject_with_callbacks(table_name).unwrap();
    //     assert_eq!(subs[0].0, subject);
    //     assert_eq!(subs[0].1, fn_name);

    //     api::nats_publish_text(subject, content.to_string()).unwrap();

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get call")
    //         {
    //             InternalMockMessage::Call(callback, data) => {
    //                 assert_eq!(callback, fn_name);
    //                 assert_eq!(data, content.as_bytes());

    //                 assert!(call_function(&callback, &data).is_err());
    //             }
    //             InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Call'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Call'"),
    //             InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Call'"),
    //         }
    //     }

    //     // FAKE SIGTERM

    //     quit_sdr.send(()).unwrap();
    //     assert!(handle.join().is_ok());
    // }

    // #[pg_test]
    // fn test_background_worker_different_subject_sub_call() {
    //     use std::sync::{RwLock, mpsc::channel};

    //     use pgrx::function_name;

    //     use crate::{bgw::run::run_worker, ring_queue::RingQueue};

    //     static SHARED_QUEUE: RwLock<RingQueue<65536>> = RwLock::new(RingQueue::new());

    //     // INIT
    //     let table_name = function_name!().split("::").last().unwrap();
    //     let subject1 = format!("{}_1", table_name);
    //     let subject2 = format!("{}_2", table_name);
    //     let fn_name = "example";
    //     let content = "Съешь ещё этих мягких французских булок, да выпей чаю";

    //     Spi::run(&format!(
    //         "CREATE TEMP TABLE {} (
    //         subject TEXT NOT NULL,
    //         callback TEXT NOT NULL,
    //         UNIQUE(subject, callback)
    //     );",
    //         table_name
    //     ))
    //     .unwrap();

    //     let state = Arc::new(Mutex::new(WorkerState::Master));
    //     let (msg_sdr, msg_recv) = channel();
    //     let (fetch_sdr, fetch_recv) = channel();
    //     let (_, notification_recv) = channel();
    //     let (quit_sdr, quit_recv) = channel();
    //     let worker = MockWorker::new(
    //         msg_sdr,
    //         fetch_recv,
    //         notification_recv,
    //         quit_recv,
    //         state.clone(),
    //         None,
    //     );

    //     let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get fetch")
    //         {
    //             InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
    //             InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
    //             _ => {}
    //         };

    //         fetch_sdr
    //             .send(fetch_subject_with_callbacks(table_name))
    //             .unwrap();
    //     }

    //     pgnats_subscribe(subject1.to_string(), fn_name.to_string(), &SHARED_QUEUE);

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get insert")
    //         {
    //             InternalMockMessage::Insert(sub, callback) => {
    //                 assert_eq!(sub, subject1);
    //                 assert_eq!(callback, fn_name);

    //                 insert_subject_callback(table_name, &sub, &callback).unwrap();
    //             }
    //             InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Insert'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Insert'"),
    //             InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Insert'"),
    //         }
    //     }

    //     pgnats_subscribe(subject2.to_string(), fn_name.to_string(), &SHARED_QUEUE);

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get insert")
    //         {
    //             InternalMockMessage::Insert(sub, callback) => {
    //                 assert_eq!(sub, subject2);
    //                 assert_eq!(callback, fn_name);

    //                 insert_subject_callback(table_name, &sub, &callback).unwrap();
    //             }
    //             InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Insert'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Insert'"),
    //             InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Insert'"),
    //         }
    //     }

    //     let result = fetch_subject_with_callbacks(table_name).unwrap();

    //     assert_eq!(result.len(), 2);
    //     assert!(
    //         result
    //             .iter()
    //             .find(|(sub, call)| sub == &subject1 && call == fn_name)
    //             .is_some()
    //     );
    //     assert!(
    //         result
    //             .iter()
    //             .find(|(sub, call)| sub == &subject2 && call == fn_name)
    //             .is_some()
    //     );

    //     api::nats_publish_text(&subject1, content.to_string()).unwrap();

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get call")
    //         {
    //             InternalMockMessage::Call(callback, data) => {
    //                 assert_eq!(callback, fn_name);
    //                 assert_eq!(data, content.as_bytes());

    //                 assert!(call_function(&callback, &data).is_err());
    //             }
    //             InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Call'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Call'"),
    //             InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Call'"),
    //         }
    //     }

    //     // FAKE SIGTERM

    //     quit_sdr.send(()).unwrap();
    //     assert!(handle.join().is_ok());
    // }

    // #[pg_test]
    // fn test_background_worker_same_subject_sub_call() {
    //     use std::sync::{RwLock, mpsc::channel};

    //     use pgrx::function_name;

    //     use crate::{bgw::run::run_worker, ring_queue::RingQueue};

    //     static SHARED_QUEUE: RwLock<RingQueue<65536>> = RwLock::new(RingQueue::new());

    //     // INIT
    //     let table_name = function_name!().split("::").last().unwrap();
    //     let subject = table_name;
    //     let fn_name1 = "foo";
    //     let fn_name2 = "bar";
    //     let content = "Съешь ещё этих мягких французских булок, да выпей чаю";

    //     Spi::run(&format!(
    //         "CREATE TEMP TABLE {} (
    //         subject TEXT NOT NULL,
    //         callback TEXT NOT NULL,
    //         UNIQUE(subject, callback)
    //     );",
    //         table_name
    //     ))
    //     .unwrap();

    //     let state = Arc::new(Mutex::new(WorkerState::Master));
    //     let (msg_sdr, msg_recv) = channel();
    //     let (fetch_sdr, fetch_recv) = channel();
    //     let (_, notification_recv) = channel();
    //     let (quit_sdr, quit_recv) = channel();
    //     let worker = MockWorker::new(
    //         msg_sdr,
    //         fetch_recv,
    //         notification_recv,
    //         quit_recv,
    //         state.clone(),
    //         None,
    //     );

    //     let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get fetch")
    //         {
    //             InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
    //             InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
    //             _ => {}
    //         };

    //         fetch_sdr
    //             .send(fetch_subject_with_callbacks(table_name))
    //             .unwrap();
    //     }

    //     pgnats_subscribe(subject.to_string(), fn_name1.to_string(), &SHARED_QUEUE);

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get insert")
    //         {
    //             InternalMockMessage::Insert(sub, callback) => {
    //                 assert_eq!(sub, subject);
    //                 assert_eq!(callback, fn_name1);

    //                 insert_subject_callback(table_name, &sub, &callback).unwrap();
    //             }
    //             InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Insert'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Insert'"),
    //             InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Insert'"),
    //         }
    //     }

    //     pgnats_subscribe(subject.to_string(), fn_name2.to_string(), &SHARED_QUEUE);

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get insert")
    //         {
    //             InternalMockMessage::Insert(sub, callback) => {
    //                 assert_eq!(sub, subject);
    //                 assert_eq!(callback, fn_name2);

    //                 insert_subject_callback(table_name, &sub, &callback).unwrap();
    //             }
    //             InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Insert'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Insert'"),
    //             InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Insert'"),
    //         }
    //     }

    //     let result = fetch_subject_with_callbacks(table_name).unwrap();

    //     assert_eq!(result.len(), 2);
    //     assert!(
    //         result
    //             .iter()
    //             .find(|(sub, call)| sub == &subject && call == fn_name1)
    //             .is_some()
    //     );
    //     assert!(
    //         result
    //             .iter()
    //             .find(|(sub, call)| sub == &subject && call == fn_name2)
    //             .is_some()
    //     );

    //     api::nats_publish_text(&subject, content.to_string()).unwrap();

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get call")
    //         {
    //             InternalMockMessage::Call(callback, data) => {
    //                 assert!(callback == fn_name1 || callback == fn_name2);
    //                 assert_eq!(data, content.as_bytes());

    //                 assert!(call_function(&callback, &data).is_err());
    //             }
    //             InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Call'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Call'"),
    //             InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Call'"),
    //         }
    //     }

    //     // FAKE SIGTERM

    //     quit_sdr.send(()).unwrap();
    //     assert!(handle.join().is_ok());
    // }

    // #[cfg(not(any(skip_pgnats_tests)))]
    // #[pg_test]
    // fn test_background_worker_sub_change_master_to_slave_try_call() {
    //     use std::sync::{RwLock, mpsc::channel};

    //     use pgrx::function_name;

    //     use crate::{bgw::run::run_worker, ring_queue::RingQueue};

    //     static SHARED_QUEUE: RwLock<RingQueue<65536>> = RwLock::new(RingQueue::new());

    //     // INIT
    //     let table_name = function_name!().split("::").last().unwrap();
    //     let subject = table_name;
    //     let fn_name = "example";
    //     let content = "Съешь ещё этих мягких французских булок, да выпей чаю";

    //     Spi::run(&format!(
    //         "CREATE TEMP TABLE {} (
    //         subject TEXT NOT NULL,
    //         callback TEXT NOT NULL,
    //         UNIQUE(subject, callback)
    //     );",
    //         table_name
    //     ))
    //     .unwrap();

    //     let state = Arc::new(Mutex::new(WorkerState::Master));
    //     let (msg_sdr, msg_recv) = channel();
    //     let (fetch_sdr, fetch_recv) = channel();
    //     let (_, notification_recv) = channel();
    //     let (quit_sdr, quit_recv) = channel();
    //     let worker = MockWorker::new(
    //         msg_sdr,
    //         fetch_recv,
    //         notification_recv,
    //         quit_recv,
    //         state.clone(),
    //         None,
    //     );

    //     let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));
    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get fetch")
    //         {
    //             InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
    //             InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
    //             _ => {}
    //         };
    //         fetch_sdr
    //             .send(fetch_subject_with_callbacks(table_name))
    //             .unwrap();
    //     }

    //     // LOOP

    //     pgnats_subscribe(subject.to_string(), fn_name.to_string(), &SHARED_QUEUE);

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get insert")
    //         {
    //             InternalMockMessage::Insert(sub, callback) => {
    //                 assert_eq!(sub, subject);
    //                 assert_eq!(callback, fn_name);

    //                 insert_subject_callback(table_name, &sub, &callback).unwrap();
    //             }
    //             InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Insert'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Insert'"),
    //             InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Insert'"),
    //         }
    //     }

    //     let subs = fetch_subject_with_callbacks(table_name).unwrap();
    //     assert_eq!(subs[0].0, subject);
    //     assert_eq!(subs[0].1, fn_name);

    //     {
    //         *state.lock().unwrap() = WorkerState::Replica;
    //     }

    //     std::thread::sleep(std::time::Duration::from_secs(5));

    //     api::nats_publish_text(subject, content.to_string()).unwrap();

    //     {
    //         assert!(matches!(
    //             msg_recv.recv_timeout(std::time::Duration::from_secs(5)),
    //             Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    //         ));
    //     }

    //     // FAKE SIGTERM

    //     quit_sdr.send(()).unwrap();
    //     assert!(handle.join().is_ok());
    // }

    // #[pg_test]
    // fn test_background_worker_try_sub_change_slave_to_master_call() {
    //     use std::sync::{RwLock, mpsc::channel};

    //     use pgrx::function_name;

    //     use crate::{bgw::run::run_worker, ring_queue::RingQueue};

    //     static SHARED_QUEUE: RwLock<RingQueue<65536>> = RwLock::new(RingQueue::new());

    //     // INIT
    //     let table_name = function_name!().split("::").last().unwrap();
    //     let subject = table_name;
    //     let fn_name = "example";
    //     let content = "Съешь ещё этих мягких французских булок, да выпей чаю";

    //     Spi::run(&format!(
    //         "CREATE TEMP TABLE {} (
    //         subject TEXT NOT NULL,
    //         callback TEXT NOT NULL,
    //         UNIQUE(subject, callback)
    //     );",
    //         table_name
    //     ))
    //     .unwrap();

    //     Spi::run(&format!(
    //         "INSERT INTO {} (subject, callback) VALUES
    //             ('{}', '{}');",
    //         table_name, subject, fn_name
    //     ))
    //     .unwrap();

    //     let state = Arc::new(Mutex::new(WorkerState::Replica));
    //     let (msg_sdr, msg_recv) = channel();
    //     let (fetch_sdr, fetch_recv) = channel();
    //     let (_, notification_recv) = channel();
    //     let (quit_sdr, quit_recv) = channel();
    //     let worker = MockWorker::new(
    //         msg_sdr,
    //         fetch_recv,
    //         notification_recv,
    //         quit_recv,
    //         state.clone(),
    //         None,
    //     );

    //     let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));

    //     {
    //         assert!(matches!(
    //             msg_recv.recv_timeout(std::time::Duration::from_secs(5)),
    //             Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    //         ));
    //     }

    //     // LOOP

    //     pgnats_subscribe(subject.to_string(), fn_name.to_string(), &SHARED_QUEUE);

    //     {
    //         assert!(matches!(
    //             msg_recv.recv_timeout(std::time::Duration::from_secs(5)),
    //             Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    //         ));
    //     }

    //     {
    //         *state.lock().unwrap() = WorkerState::Master;
    //     }

    //     std::thread::sleep(std::time::Duration::from_secs(5));

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get fetch")
    //         {
    //             InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
    //             InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
    //             _ => {}
    //         };

    //         let result = fetch_subject_with_callbacks(table_name).unwrap();
    //         assert_eq!(result.len(), 1);
    //         assert_eq!(result[0].0, subject);
    //         assert_eq!(result[0].1, fn_name);

    //         fetch_sdr.send(Ok(result)).unwrap();
    //     }

    //     api::nats_publish_text(subject, content.to_string()).unwrap();

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get call")
    //         {
    //             InternalMockMessage::Call(callback, data) => {
    //                 assert_eq!(callback, fn_name);
    //                 assert_eq!(data, content.as_bytes());

    //                 assert!(call_function(&callback, &data).is_err());
    //             }
    //             InternalMockMessage::Fetch => panic!("Got 'Fetch' expected 'Call'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Call'"),
    //             InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Call'"),
    //         }
    //     }

    //     // FAKE SIGTERM

    //     quit_sdr.send(()).unwrap();
    //     assert!(handle.join().is_ok());
    // }

    // #[pg_test]
    // fn test_background_worker_m2r() {
    //     use std::sync::{RwLock, mpsc::channel};

    //     use pgrx::function_name;

    //     use crate::{bgw::run::run_worker, ring_queue::RingQueue};

    //     static SHARED_QUEUE: RwLock<RingQueue<65536>> = RwLock::new(RingQueue::new());

    //     // INIT
    //     let table_name = function_name!().split("::").last().unwrap();
    //     let notify_subject = table_name;
    //     let patroni_addr = "127.0.0.1:28008";

    //     Spi::run(&format!(
    //         "CREATE TEMP TABLE {} (
    //         subject TEXT NOT NULL,
    //         callback TEXT NOT NULL,
    //         UNIQUE(subject, callback)
    //     );",
    //         table_name
    //     ))
    //     .unwrap();

    //     let state = Arc::new(Mutex::new(WorkerState::Master));
    //     let (msg_sdr, msg_recv) = channel();
    //     let (fetch_sdr, fetch_recv) = channel();
    //     let (notification_sdr, notification_recv) = channel();
    //     let (quit_sdr, quit_recv) = channel();
    //     let worker = MockWorker::new(
    //         msg_sdr,
    //         fetch_recv,
    //         notification_recv,
    //         quit_recv,
    //         state.clone(),
    //         Some(notify_subject.to_string()),
    //     );

    //     let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get fetch")
    //         {
    //             InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
    //             InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
    //             _ => {}
    //         };
    //         fetch_sdr
    //             .send(fetch_subject_with_callbacks(table_name))
    //             .unwrap();
    //     }

    //     let (sub_sdr, sub_recv) = channel();
    //     let (start_sub_sdr, start_sub_recv) = channel();
    //     let (start_server_sdr, start_server_recv) = channel();
    //     let rt = tokio::runtime::Builder::new_multi_thread()
    //         .enable_all()
    //         .build()
    //         .expect("Failed to initialize Tokio runtime");

    //     let server_handle = rt.spawn(start_mock_patroni(
    //         table_name.to_string(),
    //         start_server_sdr,
    //         patroni_addr,
    //     ));

    //     let _ = start_server_recv
    //         .recv_timeout(std::time::Duration::from_secs(5))
    //         .unwrap();

    //     let sub_handle = rt.spawn(start_subscription(notify_subject, start_sub_sdr, sub_sdr));

    //     start_sub_recv
    //         .recv_timeout(std::time::Duration::from_secs(30))
    //         .unwrap();

    //     // LOOP

    //     {
    //         *state.lock().unwrap() = WorkerState::Replica;
    //         notification_sdr
    //             .send(PgInstanceNotification::new(
    //                 PgInstanceTransition::M2R,
    //                 Some(&format!("http://{patroni_addr}/patroni")),
    //             ))
    //             .unwrap();
    //     }

    //     {
    //         let message = sub_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .unwrap()
    //             .unwrap();

    //         assert_eq!(message.transition, PgInstanceTransition::M2R);

    //         assert!(message.listen_addresses.len() > 0);
    //         assert_eq!(message.listen_addresses[0], "localhost");

    //         #[cfg(feature = "pg13")]
    //         assert_eq!(message.port, 32213);
    //         #[cfg(feature = "pg14")]
    //         assert_eq!(message.port, 32214);
    //         #[cfg(feature = "pg15")]
    //         assert_eq!(message.port, 32215);
    //         #[cfg(feature = "pg16")]
    //         assert_eq!(message.port, 32216);
    //         #[cfg(feature = "pg17")]
    //         assert_eq!(message.port, 32217);
    //         #[cfg(feature = "pg18")]
    //         assert_eq!(message.port, 32218);

    //         assert_eq!(message.name.unwrap().as_str(), table_name);
    //     }

    //     // FAKE SIGTERM

    //     server_handle.abort();
    //     sub_handle.abort();
    //     quit_sdr.send(()).unwrap();
    //     assert!(handle.join().is_ok());
    // }

    // #[pg_test]
    // fn test_background_worker_r2m() {
    //     use std::sync::{RwLock, mpsc::channel};

    //     use pgrx::function_name;

    //     use crate::{bgw::run::run_worker, ring_queue::RingQueue};

    //     static SHARED_QUEUE: RwLock<RingQueue<65536>> = RwLock::new(RingQueue::new());

    //     // INIT
    //     let table_name = function_name!().split("::").last().unwrap();
    //     let notify_subject = table_name;
    //     let patroni_addr = "127.0.0.1:28009";

    //     Spi::run(&format!(
    //         "CREATE TEMP TABLE {} (
    //         subject TEXT NOT NULL,
    //         callback TEXT NOT NULL,
    //         UNIQUE(subject, callback)
    //     );",
    //         table_name
    //     ))
    //     .unwrap();

    //     let state = Arc::new(Mutex::new(WorkerState::Replica));
    //     let (msg_sdr, msg_recv) = channel();
    //     let (fetch_sdr, fetch_recv) = channel();
    //     let (notification_sdr, notification_recv) = channel();
    //     let (quit_sdr, quit_recv) = channel();
    //     let worker = MockWorker::new(
    //         msg_sdr,
    //         fetch_recv,
    //         notification_recv,
    //         quit_recv,
    //         state.clone(),
    //         Some(notify_subject.to_string()),
    //     );

    //     let handle = std::thread::spawn(move || run_worker(worker, &SHARED_QUEUE));

    //     let (sub_sdr, sub_recv) = channel();
    //     let (start_sub_sdr, start_sub_recv) = channel();
    //     let (start_server_sdr, start_server_recv) = channel();
    //     let rt = tokio::runtime::Builder::new_multi_thread()
    //         .enable_all()
    //         .build()
    //         .expect("Failed to initialize Tokio runtime");

    //     let server_handle = rt.spawn(start_mock_patroni(
    //         table_name.to_string(),
    //         start_server_sdr,
    //         patroni_addr,
    //     ));

    //     let _ = start_server_recv
    //         .recv_timeout(std::time::Duration::from_secs(5))
    //         .unwrap();

    //     let sub_handle = rt.spawn(start_subscription(notify_subject, start_sub_sdr, sub_sdr));

    //     start_sub_recv
    //         .recv_timeout(std::time::Duration::from_secs(30))
    //         .unwrap();

    //     // LOOP

    //     {
    //         *state.lock().unwrap() = WorkerState::Master;
    //         notification_sdr
    //             .send(PgInstanceNotification::new(
    //                 PgInstanceTransition::R2M,
    //                 Some(&format!("http://{patroni_addr}/patroni")),
    //             ))
    //             .unwrap();
    //     }

    //     {
    //         match msg_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .expect("Failed to get fetch")
    //         {
    //             InternalMockMessage::Insert(_, _) => panic!("Got 'Insert' expected 'Fetch'"),
    //             InternalMockMessage::Delete(_, _) => panic!("Got 'Delete' expected 'Fetch'"),
    //             InternalMockMessage::Call(_, _) => panic!("Got 'Call' expected 'Fetch'"),
    //             _ => {}
    //         };
    //         fetch_sdr
    //             .send(fetch_subject_with_callbacks(table_name))
    //             .unwrap();
    //     }

    //     {
    //         let message = sub_recv
    //             .recv_timeout(std::time::Duration::from_secs(5))
    //             .unwrap()
    //             .unwrap();

    //         assert_eq!(message.transition, PgInstanceTransition::R2M);

    //         assert!(message.listen_addresses.len() > 0);
    //         assert_eq!(message.listen_addresses[0], "localhost");

    //         #[cfg(feature = "pg13")]
    //         assert_eq!(message.port, 32213);
    //         #[cfg(feature = "pg14")]
    //         assert_eq!(message.port, 32214);
    //         #[cfg(feature = "pg15")]
    //         assert_eq!(message.port, 32215);
    //         #[cfg(feature = "pg16")]
    //         assert_eq!(message.port, 32216);
    //         #[cfg(feature = "pg17")]
    //         assert_eq!(message.port, 32217);
    //         #[cfg(feature = "pg18")]
    //         assert_eq!(message.port, 32218);

    //         assert_eq!(message.name.unwrap().as_str(), table_name);
    //     }

    //     // FAKE SIGTERM

    //     server_handle.abort();
    //     sub_handle.abort();
    //     quit_sdr.send(()).unwrap();
    //     assert!(handle.join().is_ok());
    // }

    // fn pgnats_bgw_reload_conf<const N: usize, Q>(opt: Config, queue: &Q)
    // where
    //     Q: SharedQueue<N>,
    // {
    //     let msg = WorkerMessage::NewConfig(opt);
    //     let buf = bincode::encode_to_vec(msg, bincode::config::standard()).unwrap();

    //     queue.unique().try_send(&buf).unwrap();
    // }

    fn pgnats_subscribe<const N: usize>(
        subject: String,
        fn_name: String,
        queue: &PgLwLock<RingQueue<N>>,
    ) {
        crate::bgw::launcher::send_message_to_launcher_with_retry(
            queue,
            crate::bgw::launcher::message::LauncherMessage::Subscribe {
                db_oid: unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32(),
                subject,
                fn_name,
            },
            5,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
    }

    fn pgnats_unsubscribe<const N: usize>(
        subject: String,
        fn_name: String,
        queue: &PgLwLock<RingQueue<N>>,
    ) {
        crate::bgw::launcher::send_message_to_launcher_with_retry(
            queue,
            crate::bgw::launcher::message::LauncherMessage::Unsubscribe {
                db_oid: unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32(),
                subject,
                fn_name,
            },
            5,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
    }

    // async fn start_subscription(
    //     notify_subject: &str,
    //     start_sub_sdr: Sender<()>,
    //     sub_sdr: Sender<anyhow::Result<PgInstanceNotification>>,
    // ) {
    //     use crate::notification::PgInstanceNotification;

    //     async fn run(
    //         start_sub_sdr: Sender<()>,
    //         subject: String,
    //     ) -> anyhow::Result<PgInstanceNotification> {
    //         use futures::StreamExt;

    //         let client = async_nats::connect("127.0.0.1:4222").await?;

    //         let mut sub = client.subscribe(subject).await?;

    //         start_sub_sdr.send(()).unwrap();

    //         if let Some(msg) = sub.next().await {
    //             let value = serde_json::from_slice(&msg.payload)?;
    //             Ok(value)
    //         } else {
    //             Err(anyhow::anyhow!("Subscription is broken"))
    //         }
    //     }

    //     let _ = sub_sdr.send(run(start_sub_sdr, notify_subject.to_string()).await);
    // }

    // async fn start_mock_patroni(name: String, start_sdr: Sender<()>, patroni_addr: &str) {
    //     use tokio::net::TcpListener;
    //     use tokio_stream::wrappers::TcpListenerStream;
    //     use warp::Filter;

    //     let patroin_route = warp::path("patroni").and(warp::get()).map(move || {
    //         let response = serde_json::json!({
    //             "patroni": {
    //                 "name": name
    //             }
    //         });

    //         warp::reply::json(&response)
    //     });

    //     let listener = TcpListener::bind(patroni_addr).await.unwrap();

    //     start_sdr.send(()).unwrap();

    //     warp::serve(patroin_route)
    //         .run_incoming(TcpListenerStream::new(listener))
    //         .await;
    // }
}
