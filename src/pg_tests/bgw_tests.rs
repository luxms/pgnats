#[cfg(any(test, feature = "pg_test"))]
pub fn init_test_shared_memory() {
    use pgrx::{PgSharedMemoryInitialization, pg_guard, pg_shmem_init, pg_sys};

    use crate::pg_tests::bgw_tests::tests_items::*;

    pg_shmem_init!(LAUNCHER_MESSAGE_BUS1);
    pg_shmem_init!(TEST_RESULT1);

    pg_shmem_init!(LAUNCHER_MESSAGE_BUS2);
    pg_shmem_init!(TEST_RESULT2);

    pg_shmem_init!(LAUNCHER_MESSAGE_BUS3);
    pg_shmem_init!(TEST_RESULT3);

    pg_shmem_init!(LAUNCHER_MESSAGE_BUS4);
    pg_shmem_init!(TEST_RESULT4);

    pg_shmem_init!(LAUNCHER_MESSAGE_BUS5);
    pg_shmem_init!(TEST_RESULT5);

    pg_shmem_init!(LAUNCHER_MESSAGE_BUS6);
    pg_shmem_init!(TEST_RESULT6);
}

#[cfg(any(test, feature = "pg_test"))]
mod tests_items {
    use crate::generate_test_background_worker;

    generate_test_background_worker!(
        1,                   // test number
        c"l1",               // launcher message bus shared memory name
        c"r1",               // result shared memory name
        "create_test_fdw_1", // name of extension_sql
        r#"
        CREATE TABLE test_subscription_table_1 (
            subject TEXT NOT NULL,
            fn_oid OID NOT NULL,
            UNIQUE(subject, fn_oid)
        );
        CREATE FOREIGN DATA WRAPPER pgnats_fdw_test_1;
        CREATE SERVER test_background_worker_sub_call_unsub_call FOREIGN DATA WRAPPER pgnats_fdw_test_1 OPTIONS (host 'localhost', port '4222');
        "#  // Extension SQL code
    );

    generate_test_background_worker!(
        2,
        c"l2",
        c"r2",
        "create_test_fdw_2",
        r#"
        CREATE TABLE test_subscription_table_2 (
            subject TEXT NOT NULL,
            fn_oid OID NOT NULL,
            UNIQUE(subject, fn_oid)
        );

        INSERT INTO test_subscription_table_2 (subject, fn_oid) VALUES
            ('test_background_worker_restore_after_restart', 'public.test_2_fn'::regproc)
        ;

        CREATE FOREIGN DATA WRAPPER pgnats_fdw_test_2;
        CREATE SERVER test_background_worker_restore_after_restart FOREIGN DATA WRAPPER pgnats_fdw_test_2 OPTIONS (host 'localhost', port '4222');
        "#
    );

    generate_test_background_worker!(
        3,
        c"l3",
        c"r3",
        "create_test_fdw_3",
        r#"
        CREATE TABLE test_subscription_table_3 (
            subject TEXT NOT NULL,
            fn_oid OID NOT NULL,
            UNIQUE(subject, fn_oid)
        );

        CREATE FOREIGN DATA WRAPPER pgnats_fdw_test_3 VALIDATOR pgnats_fdw_validator_test_3;
        CREATE SERVER test_background_worker_changed_fdw_config FOREIGN DATA WRAPPER pgnats_fdw_test_3 OPTIONS (host 'localhost', port '4222');
        "#
    );

    generate_test_background_worker!(
        4,
        c"l4",
        c"r4",
        "create_test_fdw_4",
        r#"
        CREATE TABLE test_subscription_table_4 (
            subject TEXT NOT NULL,
            fn_oid OID NOT NULL,
            UNIQUE(subject, fn_oid)
        );

        CREATE FOREIGN DATA WRAPPER pgnats_fdw_test_4 VALIDATOR pgnats_fdw_validator_test_4;
        CREATE SERVER test_background_worker_whoami FOREIGN DATA WRAPPER pgnats_fdw_test_4 OPTIONS (host 'localhost', port '4222', notify_subject 'test_background_worker_whoami', patroni_url 'http://localhost:28008/patroni/');
        "#
    );

    generate_test_background_worker!(
        5,
        c"l5",
        c"r5",
        "create_test_fdw_5",
        r#"
        CREATE TABLE test_subscription_table_5 (
            subject TEXT NOT NULL,
            fn_oid OID NOT NULL,
            UNIQUE(subject, fn_oid)
        );

        CREATE FOREIGN DATA WRAPPER pgnats_fdw_test_5 VALIDATOR pgnats_fdw_validator_test_5;
        CREATE SERVER test_background_worker_m2r FOREIGN DATA WRAPPER pgnats_fdw_test_5 OPTIONS (host 'localhost', port '4222');
        "#
    );

    generate_test_background_worker!(
        6,
        c"l6",
        c"r6",
        "create_test_fdw_6",
        r#"
        CREATE TABLE test_subscription_table_6 (
            subject TEXT NOT NULL,
            fn_oid OID NOT NULL,
            UNIQUE(subject, fn_oid)
        );

        CREATE FOREIGN DATA WRAPPER pgnats_fdw_test_6 VALIDATOR pgnats_fdw_validator_test_6;
        CREATE SERVER test_background_worker_r2m FOREIGN DATA WRAPPER pgnats_fdw_test_6 OPTIONS (host 'localhost', port '4222');
        "#
    );
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::prelude::pg_schema]
pub(super) mod tests {
    use std::{
        hash::{DefaultHasher, Hasher},
        sync::mpsc::Sender,
    };

    use pgrx::{PgLwLock, Spi, bgworkers::BackgroundWorkerBuilder, pg_test};

    use crate::{
        api,
        bgw::{
            notification::PgInstanceNotification, ring_queue::RingQueue,
            subscriber::pg_api::PgInstanceStatus,
        },
        constants::EXTENSION_NAME,
        pg_tests::{bgw_tests::tests_items::*, get_function_oid},
    };

    #[pg_test]
    fn test_background_worker_sub_call_unsub_call() {
        use pgrx::function_name;

        let table_name = function_name!().split("::").last().unwrap();
        let subject = table_name;
        let fn_name = "test_1_fn";
        let content1 = "Съешь ещё этих мягких французских булок, да выпей чаю";
        let content2 = "Hello, World";

        let worker = BackgroundWorkerBuilder::new("PGNats Background Worker Launcher 1")
            .set_function("background_worker_launcher_entry_point_test_1")
            .set_library(EXTENSION_NAME)
            .enable_spi_access()
            .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
            .load_dynamic()
            .unwrap();

        let _ = worker.wait_for_startup().unwrap();

        std::thread::sleep(std::time::Duration::from_secs(3));

        let fn_oid = get_function_oid(&format!("public.{fn_name}")).unwrap();

        pgnats_subscribe(
            subject.to_string(),
            fn_oid,
            fn_name.to_string(),
            &LAUNCHER_MESSAGE_BUS1,
        );
        std::thread::sleep(std::time::Duration::from_secs(3));

        api::nats_publish_text(subject, content1.to_string(), None, None).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        let mut hasher = DefaultHasher::new();
        hasher.write(content1.as_bytes());
        assert_eq!(*TEST_RESULT1.share(), hasher.finish());

        pgnats_unsubscribe(
            subject.to_string(),
            fn_oid,
            fn_name.to_string(),
            &LAUNCHER_MESSAGE_BUS1,
        );
        std::thread::sleep(std::time::Duration::from_secs(3));

        api::nats_publish_text(subject, content2.to_string(), None, None).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        let mut hasher = DefaultHasher::new();
        hasher.write(content2.as_bytes());
        assert_ne!(*TEST_RESULT1.share(), hasher.finish());

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

    #[pg_test]
    fn test_background_worker_changed_fdw_config() {
        use pgrx::function_name;

        let table_name = function_name!().split("::").last().unwrap();
        let subject = table_name;
        let fn_name = "test_3_fn";
        let content1 = "Съешь ещё этих мягких французских булок, да выпей чаю";
        let content2 = "Hello, World!";

        let worker = BackgroundWorkerBuilder::new("PGNats Background Worker Launcher 3")
            .set_function("background_worker_launcher_entry_point_test_3")
            .set_library(EXTENSION_NAME)
            .enable_spi_access()
            .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
            .load_dynamic()
            .unwrap();

        let _ = worker.wait_for_startup().unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        let fn_oid = get_function_oid(&format!("public.{fn_name}")).unwrap();

        pgnats_subscribe(
            subject.to_string(),
            fn_oid,
            fn_name.to_string(),
            &LAUNCHER_MESSAGE_BUS3,
        );
        std::thread::sleep(std::time::Duration::from_secs(3));

        // It won't happen again because the URL is incorrect
        Spi::run(
            "ALTER SERVER test_background_worker_changed_fdw_config OPTIONS (SET port '4224');",
        )
        .unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        api::nats_publish_text(subject, content1.to_string(), None, None).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        let mut hasher = DefaultHasher::new();
        hasher.write(content1.as_bytes());
        assert_eq!(*TEST_RESULT3.share(), hasher.finish());

        Spi::run(
            "ALTER SERVER test_background_worker_changed_fdw_config OPTIONS (SET port '4222');",
        )
        .unwrap();

        std::thread::sleep(std::time::Duration::from_secs(3));

        api::nats_publish_text(subject, content2.to_string(), None, None).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        let mut hasher = DefaultHasher::new();
        hasher.write(content2.as_bytes());
        assert_eq!(*TEST_RESULT3.share(), hasher.finish());

        let terminate = worker.terminate();
        terminate.wait_for_shutdown().unwrap();
    }

    #[pg_test]
    fn test_background_worker_whoami() {
        use pgrx::function_name;
        use std::sync::mpsc::channel;

        let table_name = function_name!().split("::").last().unwrap();
        let notify_subject = table_name;

        #[cfg(feature = "pg_test")]
        let patroni_addr = "127.0.0.1:28008";

        let (sub_sdr, sub_recv) = channel();
        let (start_sub_sdr, start_sub_recv) = channel();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to initialize Tokio runtime");

        #[cfg(feature = "pg_test")]
        let (start_server_sdr, start_server_recv) = channel();

        #[cfg(feature = "pg_test")]
        let server_handle = rt.spawn(start_mock_patroni(
            table_name.to_string(),
            start_server_sdr,
            patroni_addr,
        ));

        #[cfg(feature = "pg_test")]
        start_server_recv
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("Failed to start server");

        let sub_handle = rt.spawn(start_subscription(notify_subject, start_sub_sdr, sub_sdr));

        start_sub_recv
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("Failed to start sub");

        let worker = BackgroundWorkerBuilder::new("PGNats Background Worker Launcher 4")
            .set_function("background_worker_launcher_entry_point_test_4")
            .set_library(EXTENSION_NAME)
            .enable_spi_access()
            .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
            .load_dynamic()
            .unwrap();

        let _ = worker.wait_for_startup().unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        {
            let message = sub_recv
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Failed to get message from sub")
                .unwrap();

            assert_eq!(message.status, PgInstanceStatus::Master);

            assert!(!message.listen_addresses.is_empty());
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

        #[cfg(feature = "pg_test")]
        server_handle.abort();
        sub_handle.abort();

        let terminate = worker.terminate();
        terminate.wait_for_shutdown().unwrap();
    }

    #[pg_test]
    fn test_background_worker_m2r() {
        use pgrx::function_name;

        let table_name = function_name!().split("::").last().unwrap();
        let subject = table_name;
        let fn_name = "test_5_fn";
        let content1 = "Hello, World!";
        let content2 = "Привет, Мир!";

        let worker = BackgroundWorkerBuilder::new("PGNats Background Worker Launcher 5")
            .set_function("background_worker_launcher_entry_point_test_5")
            .set_library(EXTENSION_NAME)
            .enable_spi_access()
            .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
            .load_dynamic()
            .unwrap();

        let _ = worker.wait_for_startup().unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        let fn_oid = get_function_oid(&format!("public.{fn_name}")).unwrap();

        pgnats_subscribe(
            subject.to_string(),
            fn_oid,
            fn_name.to_string(),
            &LAUNCHER_MESSAGE_BUS5,
        );
        std::thread::sleep(std::time::Duration::from_secs(3));

        api::nats_publish_text(subject, content1.to_string(), None, None).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        let mut hasher = DefaultHasher::new();
        hasher.write(content1.as_bytes());
        assert_eq!(*TEST_RESULT5.share(), hasher.finish());

        __internal_change_state(false, &LAUNCHER_MESSAGE_BUS5);
        std::thread::sleep(std::time::Duration::from_secs(3));

        api::nats_publish_text(subject, content2.to_string(), None, None).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        let mut hasher = DefaultHasher::new();
        hasher.write(content2.as_bytes());
        assert_ne!(*TEST_RESULT5.share(), hasher.finish());

        let terminate = worker.terminate();
        terminate.wait_for_shutdown().unwrap();
    }

    #[pg_test]
    fn test_background_worker_r2m() {
        use pgrx::function_name;

        let table_name = function_name!().split("::").last().unwrap();
        let subject = table_name;
        let fn_name = "test_6_fn";
        let content1 = "Hello, World!";
        let content2 = "Привет, Мир!";

        let worker = BackgroundWorkerBuilder::new("PGNats Background Worker Launcher 6")
            .set_function("background_worker_launcher_entry_point_test_6")
            .set_library(EXTENSION_NAME)
            .enable_spi_access()
            .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
            .load_dynamic()
            .unwrap();

        let _ = worker.wait_for_startup().unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        let fn_oid = get_function_oid(&format!("public.{fn_name}")).unwrap();

        pgnats_subscribe(
            subject.to_string(),
            fn_oid,
            fn_name.to_string(),
            &LAUNCHER_MESSAGE_BUS6,
        );
        std::thread::sleep(std::time::Duration::from_secs(3));

        api::nats_publish_text(subject, content1.to_string(), None, None).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        let mut hasher = DefaultHasher::new();
        hasher.write(content1.as_bytes());
        assert_eq!(*TEST_RESULT6.share(), hasher.finish());

        __internal_change_state(false, &LAUNCHER_MESSAGE_BUS6);
        std::thread::sleep(std::time::Duration::from_secs(3));

        api::nats_publish_text(subject, content2.to_string(), None, None).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        let mut hasher = DefaultHasher::new();
        hasher.write(content2.as_bytes());
        assert_ne!(*TEST_RESULT6.share(), hasher.finish());

        __internal_change_state(true, &LAUNCHER_MESSAGE_BUS6);
        std::thread::sleep(std::time::Duration::from_secs(3));

        api::nats_publish_text(subject, content2.to_string(), None, None).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        let mut hasher = DefaultHasher::new();
        hasher.write(content2.as_bytes());
        assert_eq!(*TEST_RESULT6.share(), hasher.finish());

        let terminate = worker.terminate();
        terminate.wait_for_shutdown().unwrap();
    }

    fn pgnats_subscribe<const N: usize>(
        subject: String,
        fn_oid: u32,
        fn_name: String,
        queue: &PgLwLock<RingQueue<N>>,
    ) {
        crate::bgw::launcher::send_message_to_launcher_with_retry(
            queue,
            crate::bgw::launcher::message::LauncherMessage::Subscribe {
                db_oid: unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32(),
                subject,
                fn_oid,
                fn_name,
            },
            5,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
    }

    fn __internal_change_state<const N: usize>(master: bool, queue: &PgLwLock<RingQueue<N>>) {
        use crate::bgw::launcher::send_message_to_launcher_with_retry;

        send_message_to_launcher_with_retry(
            queue,
            crate::bgw::launcher::message::LauncherMessage::ChangeStatus {
                db_oid: unsafe { pgrx::pg_sys::MyDatabaseId.to_u32() },
                master,
            },
            5,
            std::time::Duration::from_secs(1),
        )
        .expect("Failed to send ChangeStatus message to launcher")
    }

    fn pgnats_unsubscribe<const N: usize>(
        subject: String,
        fn_oid: u32,
        fn_name: String,
        queue: &PgLwLock<RingQueue<N>>,
    ) {
        crate::bgw::launcher::send_message_to_launcher_with_retry(
            queue,
            crate::bgw::launcher::message::LauncherMessage::Unsubscribe {
                db_oid: unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32(),
                subject,
                fn_oid,
                fn_name,
            },
            5,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
    }

    async fn start_subscription(
        notify_subject: &str,
        start_sub_sdr: Sender<()>,
        sub_sdr: Sender<anyhow::Result<PgInstanceNotification>>,
    ) {
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

    #[cfg(feature = "pg_test")]
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
