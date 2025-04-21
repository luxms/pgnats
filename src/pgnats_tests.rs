// ::pgrx::pg_module_magic!();

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::prelude::pg_schema]
mod tests {
    use pgrx::{prelude::*, Json, JsonB};

    use crate::api;

    const NATS_HOST: &str = "127.0.0.1";
    const NATS_PORT: u16 = 4222;

    const NATS_HOST_CONF: &str = "nats.host";
    const NATS_PORT_CONF: &str = "nats.port";

    #[pg_test]
    fn test_hello_pgnats() {
        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'"))
            .expect("Failed to set NATS host");
        Spi::run(&format!("SET {NATS_PORT_CONF} = {NATS_PORT}")).expect("Failed to set NATS host");

        assert_eq!("Hello, pgnats!", api::hello_pgnats());
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

        let put_res = api::nats_put_binary(bucket.clone(), key, data);
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
        assert_eq!(Some(data.as_slice()), value.as_ref().map(|v| v.as_slice()));
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
        assert_eq!(Some(text), value.as_ref().map(|v| v.as_str()));
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

        assert!(api::nats_publish_text("test.test_pgnats_reload_conf", "test".to_string()).is_err());

        Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'"))
            .expect("Failed to set NATS host");
        api::pgnats_reload_conf_force();

        let res = api::nats_publish_text("test.test_pgnats_reload_conf", "test".to_string());
        assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);
    }
}
