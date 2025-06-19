#[cfg(any(test, feature = "pg_test"))]
#[pgrx::prelude::pg_schema]
mod tests {
    use pgrx::{prelude::*, Json, JsonB};

    use crate::api;

    const NATS_HOST: &str = "127.0.0.1";
    const NATS_PORT: u16 = 4222;

    const NATS_HOST_CONF: &str = "nats.host";
    const NATS_PORT_CONF: &str = "nats.port";

    #[cfg(not(skip_pgnats_tests))]
    #[pg_test]
    fn test_hello_world() {
        assert_eq!(api::pgnats_version(), env!("CARGO_PKG_VERSION"));
    }

    #[cfg(not(skip_pgnats_tests))]
    #[pg_test]
    fn test_pgnats_publish() {
        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'"))
            .expect("Failed to set NATS host");
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).expect("Failed to set NATS host");

        let subject = "test.test_nats_publish";
        let message = "Hello, World! 🦀".to_string();

        let res = api::nats_publish_text(subject, message);
        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);

        let message = "Hello, World! 🦀".to_string().into_bytes();
        let res = api::nats_publish_binary(subject, message);
        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);

        let message = pgrx::Json(serde_json::json!({"key": "value"}));
        let res = api::nats_publish_json(subject, message);
        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);

        let message = pgrx::JsonB(serde_json::json!({"key": "value"}));
        let res = api::nats_publish_jsonb(subject, message);
        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);
    }

    #[cfg(not(any(skip_pgnats_tests, skip_pgnats_js_tests)))]
    #[pg_test]
    fn test_pgnats_publish_stream() {
        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'"))
            .expect("Failed to set NATS host");
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).expect("Failed to set NATS host");

        let subject = "test.test_nats_publish_stream";
        let message = "Hello, World! 🦀".to_string();

        let res = api::nats_publish_text_stream(subject, message);
        assert!(res.is_ok(), "nats_publish_stream occurs error: {:?}", res);

        let message = "Hello, World! 🦀".to_string().into_bytes();
        let res = api::nats_publish_binary(subject, message);
        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);

        let message = pgrx::Json(serde_json::json!({"key": "value"}));
        let res = api::nats_publish_json(subject, message);
        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);

        let message = pgrx::JsonB(serde_json::json!({"key": "value"}));
        let res = api::nats_publish_jsonb(subject, message);
        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);
    }

    #[cfg(not(skip_pgnats_tests))]
    #[pg_test]
    fn test_pgnats_publish_with_reply_and_headers() {
        use pgrx::Json;
        use serde_json::json;

        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'"))
            .expect("Failed to set NATS host");
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).expect("Failed to set NATS port");

        let subject = "test.test_nats_publish";
        let reply_to = "test.reply";
        let message = b"payload".to_vec();

        let res = api::nats_publish_binary_reply(subject, message.clone(), reply_to);
        assert!(res.is_ok(), "publish with reply failed: {:?}", res);

        let headers = json!({
            "x-id": ["123"],
            "x-type": ["unit-test"]
        });
        let res =
            api::nats_publish_binary_with_headers(subject, message.clone(), Json(headers.clone()));
        assert!(res.is_ok(), "publish with headers failed: {:?}", res);

        let res = api::nats_publish_binary_reply_with_headers(
            subject,
            message.clone(),
            reply_to,
            Json(headers),
        );
        assert!(
            res.is_ok(),
            "publish with reply + headers failed: {:?}",
            res
        );
    }

    #[cfg(not(any(skip_pgnats_tests, skip_pgnats_js_tests)))]
    #[pg_test]
    fn test_pgnats_request() {
        use std::sync::mpsc::channel;

        use futures::StreamExt;

        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'"))
            .expect("Failed to set NATS host");
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).expect("Failed to set NATS port");

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let (sdr, rcv) = channel();

        let handle = rt.spawn(async move {
            let client = async_nats::connect(format!("{NATS_HOST}:{NATS_PORT}"))
                .await
                .expect("failed to connect to NATS server");

            let mut subscriber = client
                .subscribe("test.test_nats_request".to_string())
                .await
                .expect("failed to subscribe");

            sdr.send(()).unwrap();

            while let Some(message) = subscriber.next().await {
                if let Some(reply) = message.reply {
                    client
                        .publish(reply, message.payload)
                        .await
                        .expect("failed to send reply");
                }
            }
        });

        rcv.recv().unwrap();

        let request_text = "Test request".to_string();
        let res = api::nats_request_text("test.test_nats_request", request_text.clone(), None);
        assert!(res.is_ok(), "nats_request_text failed: {:?}", res);
        assert_eq!(res.unwrap().as_slice(), request_text.as_bytes());

        let request_binary = b"Binary request".to_vec();
        let res = api::nats_request_binary("test.test_nats_request", request_binary.clone(), None);
        assert!(res.is_ok(), "nats_request_binary failed: {:?}", res);
        assert_eq!(res.unwrap(), request_binary);

        let request_json = pgrx::Json(serde_json::json!({"action": "ping"}));
        let res = api::nats_request_json("test.test_nats_request", request_json, None);
        assert!(res.is_ok(), "nats_request_json failed: {:?}", res);

        let request_jsonb = pgrx::JsonB(serde_json::json!({"action": "ping"}));
        let res = api::nats_request_jsonb("test.test_nats_request", request_jsonb, None);
        assert!(res.is_ok(), "nats_request_jsonb failed: {:?}", res);

        handle.abort();
    }

    #[cfg(not(any(skip_pgnats_tests, skip_pgnats_js_tests)))]
    #[pg_test]
    fn test_pgnats_put_and_get_binary() {
        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'"))
            .expect("Failed to set NATS host");
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).expect("Failed to set NATS host");

        let bucket = "test_default".to_string();
        let key = "binary_key";
        let data = b"binary data";

        let put_res = api::nats_put_binary(bucket.clone(), key, data.to_vec());
        assert!(
            put_res.is_ok(),
            "nats_put_binary occurs error: {:?}",
            put_res
        );

        let get_res = api::nats_get_binary(bucket.clone(), key);
        assert!(
            get_res.is_ok(),
            "nats_get_binary occurs error: {:?}",
            get_res
        );

        let value = get_res.unwrap();
        assert_eq!(Some(data.as_slice()), value.as_deref());
    }

    #[cfg(not(any(skip_pgnats_tests, skip_pgnats_js_tests)))]
    #[pg_test]
    fn test_pgnats_put_and_get_text() {
        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'"))
            .expect("Failed to set NATS host");
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).expect("Failed to set NATS host");

        let bucket = "test_default".to_string();
        let key = "text_key";
        let text = "Hello, text!";

        let put_res = api::nats_put_text(bucket.clone(), key, text);
        assert!(put_res.is_ok(), "nats_put_text occurs error: {:?}", put_res);

        let get_res = api::nats_get_text(bucket.clone(), key);
        assert!(get_res.is_ok(), "nats_get_text occurs error {:?}", get_res);

        let value = get_res.unwrap();
        assert_eq!(Some(text), value.as_deref());
    }

    #[cfg(not(any(skip_pgnats_tests, skip_pgnats_js_tests)))]
    #[pg_test]
    fn test_pgnats_put_and_get_json() {
        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'"))
            .expect("Failed to set NATS host");
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).expect("Failed to set NATS host");

        let bucket = "test_default".to_string();
        let key = "json_key";
        let json_value = serde_json::json!({"key": "value"});
        let json_input = Json(json_value.clone());

        let put_res = api::nats_put_json(bucket.clone(), key, json_input);
        assert!(put_res.is_ok(), "nats_put_json occurs error: {:?}", put_res);

        let get_res = api::nats_get_json(bucket.clone(), key);
        assert!(get_res.is_ok(), "nats_get_json occurs error: {:?}", get_res);

        let returned_json = get_res.unwrap();
        assert_eq!(Some(json_value), returned_json.map(|v| v.0));
    }

    #[cfg(not(any(skip_pgnats_tests, skip_pgnats_js_tests)))]
    #[pg_test]
    fn test_pgnats_put_and_get_jsonb() {
        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'"))
            .expect("Failed to set NATS host");
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).expect("Failed to set NATS host");

        let bucket = "test_default".to_string();
        let key = "jsonb_key";
        let json_value = serde_json::json!({"key": "value"});
        let jsonb_input = JsonB(json_value.clone());

        let put_res = api::nats_put_jsonb(bucket.clone(), key, jsonb_input);
        assert!(
            put_res.is_ok(),
            "nats_put_jsonb occurs error: {:?}",
            put_res
        );

        let get_res = api::nats_get_jsonb(bucket.clone(), key);
        assert!(
            get_res.is_ok(),
            "nats_get_jsonb occurs error: {:?}",
            get_res
        );

        let returned_json = get_res.unwrap();
        assert_eq!(Some(json_value), returned_json.map(|v| v.0));
    }

    #[cfg(not(any(skip_pgnats_tests, skip_pgnats_js_tests)))]
    #[pg_test]
    fn test_pgnats_delete_value() {
        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'"))
            .expect("Failed to set NATS host");
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).expect("Failed to set NATS host");

        let bucket = "test_default".to_string();
        let key = "delete_key";
        let text = "to be deleted";

        let put_res = api::nats_put_text(bucket.clone(), key, text);
        assert!(put_res.is_ok(), "nats_put_text occurs error: {:?}", put_res);

        let del_res = api::nats_delete_value(bucket.clone(), key);
        assert!(
            del_res.is_ok(),
            "nats_delete_value occurs error: {:?}",
            del_res
        );

        let get_res = api::nats_get_text(bucket.clone(), key);
        assert!(get_res.is_ok(), "nats_get_text occurs error: {:?}", get_res);

        let value = get_res.unwrap();
        assert_eq!(None, value);
    }

    #[cfg(not(any(skip_pgnats_tests, skip_pgnats_js_tests)))]
    #[pg_test]
    fn test_pgnats_put_and_get_file() {
        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'")).unwrap();
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).unwrap();

        let bucket = "test_file_io".to_string();
        let key = "test_file.txt";
        let content = b"file content for testing".to_vec();

        let put_res = api::nats_put_file(bucket.clone(), key, content.clone());
        assert!(put_res.is_ok(), "put_file failed: {:?}", put_res);

        let get_res = api::nats_get_file(bucket.clone(), key);
        assert!(get_res.is_ok(), "get_file failed: {:?}", get_res);

        let returned = get_res.unwrap();
        assert_eq!(content, returned);
    }

    #[cfg(not(any(skip_pgnats_tests, skip_pgnats_js_tests)))]
    #[pg_test]
    fn test_pgnats_file_info() {
        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'")).unwrap();
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).unwrap();

        let bucket = "test_file_info".to_string();
        let key = "info.txt";
        let content = b"12345".to_vec();

        api::nats_put_file(bucket.clone(), key, content.clone()).unwrap();

        let info_res = api::nats_get_file_info(bucket.clone(), key);
        assert!(info_res.is_ok(), "get_file_info failed",);

        let mut info = info_res.unwrap();
        let info = info.next().unwrap();
        assert_eq!(info.0, key);
        assert_eq!(info.5 as usize, content.len());
    }

    #[cfg(not(any(skip_pgnats_tests, skip_pgnats_js_tests)))]
    #[pg_test]
    fn test_pgnats_file_list() {
        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'")).unwrap();
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).unwrap();

        let bucket = "test_file_list".to_string();
        let files = vec![
            ("list1.txt", b"one".to_vec()),
            ("list2.txt", b"two".to_vec()),
        ];

        for (key, content) in &files {
            api::nats_put_file(bucket.clone(), key, content.clone()).unwrap();
        }

        let list_res = api::nats_get_file_list(bucket.clone());
        assert!(list_res.is_ok(), "get_file_list failed");

        let list = list_res.unwrap();
        let file_names: Vec<String> = list.into_iter().map(|info| info.0).collect();

        for (key, _) in files {
            assert!(file_names.contains(&key.to_string()));
        }
    }

    #[cfg(not(any(skip_pgnats_tests, skip_pgnats_js_tests)))]
    #[pg_test]
    fn test_pgnats_delete_file() {
        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'")).unwrap();
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).unwrap();

        let bucket = "test_file_delete".to_string();
        let key = "delete_me.txt";
        let content = b"goodbye".to_vec();

        api::nats_put_file(bucket.clone(), key, content).unwrap();
        api::nats_delete_file(bucket.clone(), key).unwrap();

        let get_res = api::nats_get_file(bucket.clone(), key);
        assert!(
            get_res.is_err(),
            "get_file after delete failed: {:?}",
            get_res
        );
    }

    #[cfg(not(skip_pgnats_tests))]
    #[pg_test]
    fn test_pgnats_reload_conf() {
        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'"))
            .expect("Failed to set NATS host");
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).expect("Failed to set NATS host");

        let res = api::nats_publish_text("test.test_pgnats_reload_conf", "test".to_string());
        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);

        api::pgnats_reload_conf();

        let res = api::nats_publish_text("test.test_pgnats_reload_conf", "test".to_string());
        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);

        Spi::run(&format!("SET {NATS_HOST_CONF} = 'localhost1'")).expect("Failed to set NATS host");
        let res = api::nats_publish_text("test.test_pgnats_reload_conf", "test".to_string());
        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);

        api::pgnats_reload_conf();

        assert!(
            api::nats_publish_text("test.test_pgnats_reload_conf", "test".to_string()).is_err()
        );

        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'"))
            .expect("Failed to set NATS host");
        api::pgnats_reload_conf_force();

        let res = api::nats_publish_text("test.test_pgnats_reload_conf", "test".to_string());
        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);
    }
}
