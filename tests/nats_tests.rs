#[cfg(test)]
mod nats_tests {
    use std::{
        sync::{mpsc::channel, Arc},
        time::Duration,
    };

    use futures::StreamExt;
    use pgnats::connection::{NatsConnection, NatsConnectionOptions, NatsTlsOptions};

    use testcontainers::{
        core::{ContainerPort, Mount, WaitFor},
        runners::AsyncRunner,
        ContainerAsync, GenericImage, ImageExt,
    };
    use tokio::net::UdpSocket;

    const TEST_STORE: &str = "test-store";
    const TEST_FILE: &str = "file.txt";
    const TEST_CONTENT: &[u8] = b"Hello, PGNats!";

    async fn setup_nats_with_file(port: u16) -> NatsConnection {
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        nats.put_file(TEST_STORE, TEST_FILE, TEST_CONTENT.to_vec())
            .await
            .expect("put_file failed in setup");

        nats
    }

    #[must_use]
    async fn setup() -> (ContainerAsync<GenericImage>, u16) {
        let container = testcontainers::GenericImage::new("nats", "latest")
            .with_exposed_port(ContainerPort::Tcp(4222))
            .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
            .with_cmd(["-js"])
            .start()
            .await
            .expect("Failed to start NATS server");

        let host_port = container
            .get_host_port_ipv4(ContainerPort::Tcp(4222))
            .await
            .expect("Failed to get host port");

        (container, host_port)
    }

    #[must_use]
    async fn setup_with_tls() -> (ContainerAsync<GenericImage>, u16) {
        let certs_path = format!("{}/tests/certs", env!("CARGO_MANIFEST_DIR"));

        let container = testcontainers::GenericImage::new("nats", "latest")
            .with_exposed_port(ContainerPort::Tcp(4222))
            .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
            .with_cmd([
                "-js",
                "--tls",
                "--tlscert",
                "/certs/server.crt",
                "--tlskey",
                "/certs/server.key",
                "--tlscacert",
                "/certs/ca.crt",
            ])
            .with_mount(Mount::bind_mount(certs_path, "/certs"))
            .start()
            .await
            .expect("Failed to start NATS server");

        let host_port = container
            .get_host_port_ipv4(ContainerPort::Tcp(4222))
            .await
            .expect("Failed to get host port");

        (container, host_port)
    }

    #[tokio::test]
    async fn test_get_server_info_success() {
        let (_cont, port) = setup().await;
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let info_result = nats.get_server_info().await;

        assert!(
            info_result.is_ok(),
            "get_server_info failed: {:?}",
            info_result
        );

        let info = info_result.unwrap();

        assert!(!info.server_id.is_empty(), "server_id should not be empty");
        assert!(
            info.version.starts_with(|c: char| c.is_numeric()),
            "version should start with number, got: {}",
            info.version
        );
        assert!(
            info.host == "127.0.0.1" || info.host == "localhost" || info.host == "0.0.0.0",
            "unexpected host: {}",
            info.host
        );
    }

    #[tokio::test]
    async fn test_nats_publish() {
        let (_cont, port) = setup().await;
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let subject = "test.test_nats_publish";
        let message = "Hello, World! 🦀";

        let client = async_nats::connect(format!("127.0.0.1:{port}"))
            .await
            .expect("failed to connect to NATS server");
        let mut subscriber = client
            .subscribe(subject)
            .await
            .expect("failed to subscribe");

        let res = nats.publish(subject, message, None::<String>, None).await;

        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);
        assert_eq!(
            Some(message.as_bytes().to_vec()),
            subscriber.next().await.map(|m| m.payload.to_vec())
        );
    }

    #[tokio::test]
    async fn test_nats_request_text() {
        let (_cont, port) = setup().await;
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let subject = "test.test_nats_request_text";
        let request_msg = "Ping";
        let response_msg = "Pong";

        let client = async_nats::connect(format!("127.0.0.1:{port}"))
            .await
            .expect("failed to connect to NATS server");

        let subscriber = client
            .subscribe(subject)
            .await
            .expect("failed to subscribe");

        let handle = tokio::spawn(async move {
            let mut subscriber = subscriber;
            while let Some(message) = subscriber.next().await {
                if let Some(reply) = message.reply {
                    client
                        .publish(reply, response_msg.into())
                        .await
                        .expect("failed to send reply");
                }
            }
        });

        let res = nats.request(subject, request_msg, Some(1000)).await;

        assert!(res.is_ok(), "nats_request_text failed: {:?}", res);
        assert_eq!(response_msg.as_bytes().to_vec(), res.unwrap());

        handle.abort();
    }

    #[tokio::test]
    async fn test_nats_publish_stream() {
        let (_cont, port) = setup().await;
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let subject = "test.test_nats_publish_stream";
        let message = "Hello, World! 🦀";

        let client = async_nats::connect(format!("127.0.0.1:{port}"))
            .await
            .expect("failed to connect to NATS server");
        let mut subscriber = client
            .subscribe(subject)
            .await
            .expect("failed to subscribe");

        let res = nats.publish_stream(subject, message, None).await;

        assert!(res.is_ok(), "nats_publish_stream occurs error: {:?}", res);
        assert_eq!(
            Some(message.as_bytes().to_vec()),
            subscriber.next().await.map(|m| m.payload.to_vec())
        );
    }

    #[tokio::test]
    async fn test_nats_put_and_get_binary() {
        let (_cont, port) = setup().await;
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let bucket = "test_default".to_string();
        let key = "binary_key";
        let data = b"binary data";

        let put_res = nats.put_value(bucket.clone(), key, data.to_vec()).await;
        assert!(
            put_res.is_ok(),
            "nats_put_binary occurs error: {:?}",
            put_res
        );

        let get_res = nats.get_value::<Vec<u8>>(bucket.clone(), key).await;
        assert!(
            get_res.is_ok(),
            "nats_get_binary occurs error: {:?}",
            get_res
        );

        let value = get_res.unwrap();
        assert_eq!(Some(data.as_slice()), value.as_deref());
    }

    #[tokio::test]
    async fn test_nats_put_and_get_text() {
        let (_cont, port) = setup().await;
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let bucket = "test_default".to_string();
        let key = "text_key";
        let text = "Hello, text!";

        let put_res = nats.put_value(bucket.clone(), key, text).await;
        assert!(
            put_res.is_ok(),
            "nats_put_binary occurs error: {:?}",
            put_res
        );

        let get_res = nats.get_value::<String>(bucket.clone(), key).await;
        assert!(
            get_res.is_ok(),
            "nats_get_binary occurs error: {:?}",
            get_res
        );

        let value = get_res.unwrap();
        assert_eq!(Some(text), value.as_deref());
    }

    #[tokio::test]
    async fn test_nats_put_and_get_json() {
        let (_cont, port) = setup().await;
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let bucket = "test_default".to_string();
        let key = "json_key";
        let json_value = serde_json::json!({"key": "value"});

        let data = serde_json::to_string(&json_value).expect("failed to serialize");
        let put_res = nats.put_value(bucket.clone(), key, data).await;
        assert!(put_res.is_ok(), "nats_put_json occurs error: {:?}", put_res);

        let get_res = nats
            .get_value::<serde_json::Value>(bucket.clone(), key)
            .await;
        assert!(get_res.is_ok(), "nats_get_json occurs error: {:?}", get_res);

        let returned_json = get_res.unwrap();
        assert_eq!(Some(json_value), returned_json);
    }

    #[tokio::test]
    async fn test_nats_delete_value() {
        let (_cont, port) = setup().await;
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let bucket = "test_default".to_string();
        let key = "delete_key";
        let text = "to be deleted";

        let put_res = nats.put_value(bucket.clone(), key, text).await;
        assert!(put_res.is_ok(), "nats_put_text occurs error: {:?}", put_res);

        let del_res = nats.delete_value(bucket.clone(), key).await;
        assert!(
            del_res.is_ok(),
            "nats_delete_value occurs error: {:?}",
            del_res
        );

        let get_res = nats.get_value::<String>(bucket.clone(), key).await;
        assert!(get_res.is_ok(), "nats_get_text occurs error: {:?}", get_res);

        let value = get_res.unwrap();
        assert_eq!(None, value);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_nats_subscribe() {
        let (_cont, port) = setup().await;

        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let (msg_sender, msg_receiver) = channel();
        let mut worker_context = pgnats::bg_subscription::WorkerContext::new(msg_sender)
            .await
            .unwrap();

        let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

        let message = pgnats::ctx::WorkerMessage::Subscribe {
            opt: NatsConnectionOptions {
                host: "127.0.0.1".to_string(),
                port,
                capacity: 128,
                tls: None,
            },
            subject: "test.test_nats_subscribe".to_string(),
            fn_name: "test".to_string(),
        };
        let buf = bincode::encode_to_vec(message, bincode::config::standard()).unwrap();
        let _ = socket
            .send_to(&buf, format!("localhost:{}", worker_context.port))
            .await
            .unwrap();

        let message = msg_receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        let pgnats::bg_subscription::InternalWorkerMessage::Subscribe {
            opt,
            subject,
            fn_name,
            ..
        } = message
        else {
            panic!("wrong message")
        };

        assert_eq!(subject.as_str(), "test.test_nats_subscribe");
        assert_eq!(fn_name.as_str(), "test");
        worker_context
            .handle_subscribe(opt, Arc::from(subject), Arc::from(fn_name))
            .await;

        nats.publish(
            "test.test_nats_subscribe",
            "Hello, subscriber!",
            None::<String>,
            None,
        )
        .await
        .unwrap();

        let message = msg_receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        let pgnats::bg_subscription::InternalWorkerMessage::CallbackCall {
            client,
            subject,
            data,
        } = message
        else {
            panic!("wrong message")
        };

        assert_eq!(&*client, &format!("127.0.0.1:{}", port));
        assert_eq!(&*subject, "test.test_nats_subscribe");
        assert_eq!(&*data, b"Hello, subscriber!".as_slice());

        worker_context.handle_callback(&*client, &*subject, data, |fn_name, data| {
            assert_eq!(fn_name, "test");
            assert_eq!(data, b"Hello, subscriber!".as_slice());
        });

        let message = pgnats::ctx::WorkerMessage::Unsubscribe {
            opt: NatsConnectionOptions {
                host: "127.0.0.1".to_string(),
                port,
                capacity: 128,
                tls: None,
            },
            subject: "test.test_nats_subscribe".to_string(),
            fn_name: "test".to_string(),
        };
        let buf = bincode::encode_to_vec(message, bincode::config::standard()).unwrap();
        let _ = socket
            .send_to(&buf, format!("localhost:{}", worker_context.port))
            .await
            .unwrap();

        let message = msg_receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        let pgnats::bg_subscription::InternalWorkerMessage::Unsubscribe {
            opt,
            subject,
            fn_name,
            ..
        } = message
        else {
            panic!("wrong message")
        };

        assert_eq!(&*subject, "test.test_nats_subscribe");
        assert_eq!(&*fn_name, "test");
        worker_context.handle_unsubscribe(&opt, Arc::from(subject), &*fn_name);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_nats_subscribe_with_multiple_fn() {
        let (_cont, port) = setup().await;

        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let (msg_sender, msg_receiver) = channel();
        let mut worker_context = pgnats::bg_subscription::WorkerContext::new(msg_sender)
            .await
            .unwrap();

        let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

        let message = pgnats::ctx::WorkerMessage::Subscribe {
            opt: NatsConnectionOptions {
                host: "127.0.0.1".to_string(),
                port,
                capacity: 128,
                tls: None,
            },
            subject: "test.test_nats_subscribe".to_string(),
            fn_name: "test1".to_string(),
        };
        let buf = bincode::encode_to_vec(message, bincode::config::standard()).unwrap();
        let _ = socket
            .send_to(&buf, format!("localhost:{}", worker_context.port))
            .await
            .unwrap();

        let message = pgnats::ctx::WorkerMessage::Subscribe {
            opt: NatsConnectionOptions {
                host: "127.0.0.1".to_string(),
                port,
                capacity: 128,
                tls: None,
            },
            subject: "test.test_nats_subscribe".to_string(),
            fn_name: "test2".to_string(),
        };
        let buf = bincode::encode_to_vec(message, bincode::config::standard()).unwrap();
        let _ = socket
            .send_to(&buf, format!("localhost:{}", worker_context.port))
            .await
            .unwrap();

        let message = msg_receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        let pgnats::bg_subscription::InternalWorkerMessage::Subscribe {
            opt,
            subject,
            fn_name,
            ..
        } = message
        else {
            panic!("wrong message")
        };

        assert_eq!(subject.as_str(), "test.test_nats_subscribe");
        assert_eq!(fn_name.as_str(), "test1");
        worker_context
            .handle_subscribe(opt, Arc::from(subject), Arc::from(fn_name))
            .await;

        let message = msg_receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        let pgnats::bg_subscription::InternalWorkerMessage::Subscribe {
            opt,
            subject,
            fn_name,
            ..
        } = message
        else {
            panic!("wrong message")
        };

        assert_eq!(subject.as_str(), "test.test_nats_subscribe");
        assert_eq!(fn_name.as_str(), "test2");
        worker_context
            .handle_subscribe(opt, Arc::from(subject), Arc::from(fn_name))
            .await;

        nats.publish(
            "test.test_nats_subscribe",
            "Hello, subscriber!",
            None::<String>,
            None,
        )
        .await
        .unwrap();

        let message = msg_receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        let pgnats::bg_subscription::InternalWorkerMessage::CallbackCall {
            client,
            subject,
            data,
        } = message
        else {
            panic!("wrong message")
        };

        assert_eq!(&*client, &format!("127.0.0.1:{}", port));
        assert_eq!(&*subject, "test.test_nats_subscribe");
        assert_eq!(&*data, b"Hello, subscriber!".as_slice());

        worker_context.handle_callback(&*client, &*subject, data, |fn_name, data| {
            assert!(fn_name == "test1" || fn_name == "test2");
            assert_eq!(data, b"Hello, subscriber!".as_slice());
        });
    }

    #[tokio::test]
    async fn test_nats_publish_text_tls() {
        let (_cont, port) = setup_with_tls().await;

        // Настройка async_nats клиента с TLS
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "localhost".to_string(),
            port,
            capacity: 128,
            tls: Some(NatsTlsOptions::Tls {
                ca: "./tests/certs/ca.crt".into(),
            }),
        }));

        let subject = "test.test_nats_publish_text_tls";
        let message = "Hello, World! 🦀";

        let res = nats.publish(subject, message, None::<String>, None).await;

        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);
    }

    #[tokio::test]
    async fn test_nats_publish_with_reply() {
        let (_cont, port) = setup().await;
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let subject = "test.test_nats_publish_with_reply";
        let reply_to = "test.reply_to";
        let message = "Ping from 🦀";

        let client = async_nats::connect(format!("127.0.0.1:{port}"))
            .await
            .expect("failed to connect to NATS server");

        let mut subscriber = client
            .subscribe(subject)
            .await
            .expect("failed to subscribe to subject");

        let res = nats
            .publish(subject, message, Some(reply_to.to_string()), None)
            .await;

        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);

        if let Some(msg) = subscriber.next().await {
            assert_eq!(msg.payload, message.as_bytes());
            assert_eq!(msg.reply.as_deref(), Some(reply_to));
        } else {
            panic!("did not receive a message");
        }
    }

    #[tokio::test]
    async fn test_nats_publish_with_headers() {
        let (_cont, port) = setup().await;
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let subject = "test.test_nats_publish_with_headers";
        let message = "Hello with headers 🦀";

        let headers = serde_json::json!({
            "X-Custom": "123",
            "Content-Type": "text/plain"
        });

        let client = async_nats::connect(format!("127.0.0.1:{port}"))
            .await
            .expect("failed to connect to NATS server");

        let mut subscriber = client
            .subscribe(subject)
            .await
            .expect("failed to subscribe");

        let res = nats
            .publish(subject, message, None::<String>, Some(headers.clone()))
            .await;

        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);

        if let Some(msg) = subscriber.next().await {
            assert_eq!(msg.payload, message.as_bytes());

            if let Some(hdrs) = msg.headers {
                assert_eq!(hdrs.get("X-Custom").map(|v| v.as_str()), Some("123"));
                assert_eq!(
                    hdrs.get("Content-Type").map(|v| v.as_str()),
                    Some("text/plain")
                );
            } else {
                panic!("headers are missing from message");
            }
        } else {
            panic!("did not receive a message");
        }
    }

    #[tokio::test]
    async fn test_nats_publish_with_reply_and_headers() {
        let (_cont, port) = setup().await;
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let subject = "test.test_nats_publish_with_reply_and_headers";
        let reply_to = "test.reply_combined";
        let message = "Hello both 🦀";

        let headers = serde_json::json!({
            "User-Agent": "nats-test",
            "Accept": "application/json"
        });

        let client = async_nats::connect(format!("127.0.0.1:{port}"))
            .await
            .expect("failed to connect to NATS server");

        let mut subscriber = client
            .subscribe(subject)
            .await
            .expect("failed to subscribe");

        let res = nats
            .publish(
                subject,
                message,
                Some(reply_to.to_string()),
                Some(headers.clone()),
            )
            .await;

        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);

        if let Some(msg) = subscriber.next().await {
            assert_eq!(msg.payload, message.as_bytes());
            assert_eq!(msg.reply.as_deref(), Some(reply_to));

            if let Some(hdrs) = msg.headers {
                assert_eq!(
                    hdrs.get("User-Agent").map(|v| v.as_str()),
                    Some("nats-test")
                );
                assert_eq!(
                    hdrs.get("Accept").map(|v| v.as_str()),
                    Some("application/json")
                );
            } else {
                panic!("headers are missing from message");
            }
        } else {
            panic!("did not receive a message");
        }
    }

    #[tokio::test]
    async fn test_nats_publish_stream_with_headers() {
        let (_cont, port) = setup().await;
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let subject = "test.test_nats_publish_stream_with_headers";
        let message = "Streamed message 🦀";

        let headers = serde_json::json!({
            "X-Stream": "true",
            "X-Test-ID": "stream123"
        });

        let client = async_nats::connect(format!("127.0.0.1:{port}"))
            .await
            .expect("failed to connect to NATS server");

        let mut subscriber = client
            .subscribe(subject)
            .await
            .expect("failed to subscribe");

        let res = nats
            .publish_stream(subject, message, Some(headers.clone()))
            .await;

        assert!(res.is_ok(), "nats_publish_stream occurs error: {:?}", res);

        if let Some(msg) = subscriber.next().await {
            assert_eq!(msg.payload, message.as_bytes());

            if let Some(hdrs) = msg.headers {
                assert_eq!(hdrs.get("X-Stream").map(|v| v.as_str()), Some("true"));
                assert_eq!(hdrs.get("X-Test-ID").map(|v| v.as_str()), Some("stream123"));
            } else {
                panic!("headers are missing from stream message");
            }
        } else {
            panic!("did not receive a stream message");
        }
    }

    #[tokio::test]
    async fn test_nats_request_timeout() {
        let (_cont, port) = setup().await;
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let subject = "test.test_nats_request_timeout";
        let request_payload = "Will timeout ⏳";

        let result = nats.request(subject, request_payload, Some(500)).await;

        assert!(
            result.is_err(),
            "expected timeout error, got success: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_put_file() {
        let (_cont, port) = setup().await;
        let mut nats = NatsConnection::new(Some(NatsConnectionOptions {
            host: "127.0.0.1".to_string(),
            port,
            capacity: 128,
            tls: None,
        }));

        let res = nats
            .put_file(TEST_STORE, TEST_FILE, TEST_CONTENT.to_vec())
            .await;

        assert!(res.is_ok(), "put_file failed: {:?}", res);
    }

    #[tokio::test]
    async fn test_get_file() {
        let (_cont, port) = setup().await;
        let mut nats = setup_nats_with_file(port).await;

        let result = nats.get_file(TEST_STORE, TEST_FILE).await;
        assert!(result.is_ok(), "get_file failed: {:?}", result);

        let content = result.unwrap();
        assert_eq!(&content, TEST_CONTENT);
    }

    #[tokio::test]
    async fn test_get_file_info() {
        let (_cont, port) = setup().await;
        let mut nats = setup_nats_with_file(port).await;

        let result = nats.get_file_info(TEST_STORE, TEST_FILE).await;
        assert!(result.is_ok(), "get_file_info failed: {:?}", result);

        let info = result.unwrap();
        assert_eq!(info.name, TEST_FILE);
        assert_eq!(info.size, TEST_CONTENT.len());
    }

    #[tokio::test]
    async fn test_get_file_list() {
        let (_cont, port) = setup().await;
        let mut nats = setup_nats_with_file(port).await;

        let result = nats.get_file_list(TEST_STORE).await;
        assert!(result.is_ok(), "get_file_list failed: {:?}", result);

        let list = result.unwrap();
        assert!(
            list.iter().any(|f| f.name == TEST_FILE),
            "file not found in list"
        );
    }

    #[tokio::test]
    async fn test_delete_file() {
        let (_cont, port) = setup().await;
        let mut nats = setup_nats_with_file(port).await;

        let res = nats.delete_file(TEST_STORE, TEST_FILE).await;
        assert!(res.is_ok(), "delete_file failed: {:?}", res);

        let result = nats.get_file(TEST_STORE, TEST_FILE).await;
        assert!(result.is_err(), "file still exists after delete");
    }
}
