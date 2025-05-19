#[cfg(test)]
mod nats_tests {
    use futures::StreamExt;
    use pgnats::connection::{NatsConnectionOptions, NatsConnection};

    use testcontainers::{
        core::{ContainerPort, WaitFor},
        runners::AsyncRunner,
        ContainerAsync, GenericImage, ImageExt,
    };

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
}
