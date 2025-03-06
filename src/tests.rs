// ::pgrx::pg_module_magic!();

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::prelude::pg_schema]
mod tests {
  use pgrx::{prelude::*, Json, JsonB};
  use testcontainers::{
    core::{ContainerPort, WaitFor},
    runners::SyncRunner,
    Container, GenericImage, ImageExt,
  };

  use crate::api;

  const NATS_HOST: &str = "127.0.0.1";

  const NATS_HOST_CONF: &str = "nats.host";
  const NATS_PORT_CONF: &str = "nats.port";

  #[must_use]
  fn setup() -> (Container<GenericImage>, u16) {
    let container = testcontainers::GenericImage::new("nats", "latest")
      .with_exposed_port(ContainerPort::Tcp(4222))
      .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
      .with_cmd(["-js"])
      .start()
      .expect("Failed to start NATS server");

    let host_port = container
      .get_host_port_ipv4(ContainerPort::Tcp(4222))
      .expect("Failed to get host port");

    (container, host_port)
  }

  #[pg_test]
  fn test_hello_pgnats() {
    let (_container, host_port) = setup();
    Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'")).expect("Failed to set NATS host");
    Spi::run(&format!("SET {NATS_PORT_CONF} = {}", host_port)).expect("Failed to set NATS host");

    assert_eq!("Hello, pgnats!", api::hello_pgnats());
  }

  #[pg_test]
  fn test_nats_publish() {
    let (_container, host_port) = setup();
    Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'")).expect("Failed to set NATS host");
    Spi::run(&format!("SET {NATS_PORT_CONF} = {}", host_port)).expect("Failed to set NATS host");

    let subject = "test.test_nats_publish";
    let message = "Hello, World! 🦀";

    let res = api::nats_publish(subject, message);
    assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);
  }

  #[pg_test]
  fn test_nats_publish_stream() {
    let (_container, host_port) = setup();
    Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'")).expect("Failed to set NATS host");
    Spi::run(&format!("SET {NATS_PORT_CONF} = {}", host_port)).expect("Failed to set NATS host");

    let subject = "test.test_nats_publish_stream";
    let message = "Hello, World! 🦀";

    let res = api::nats_publish_stream(subject, message);
    assert!(res.is_ok(), "nats_publish_stream occurs error: {:?}", res);
  }

  #[pg_test]
  fn test_nats_put_and_get_binary() {
    let (_container, host_port) = setup();
    Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'")).expect("Failed to set NATS host");
    Spi::run(&format!("SET {NATS_PORT_CONF} = {}", host_port)).expect("Failed to set NATS host");

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

  #[pg_test]
  fn test_nats_put_and_get_text() {
    let (_container, host_port) = setup();
    Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'")).expect("Failed to set NATS host");
    Spi::run(&format!("SET {NATS_PORT_CONF} = {}", host_port)).expect("Failed to set NATS host");

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

  #[pg_test]
  fn test_nats_put_and_get_json() {
    let (_container, host_port) = setup();
    Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'")).expect("Failed to set NATS host");
    Spi::run(&format!("SET {NATS_PORT_CONF} = {}", host_port)).expect("Failed to set NATS host");

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

  #[pg_test]
  fn test_nats_put_and_get_jsonb() {
    let (_container, host_port) = setup();
    Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'")).expect("Failed to set NATS host");
    Spi::run(&format!("SET {NATS_PORT_CONF} = {}", host_port)).expect("Failed to set NATS host");

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

  #[pg_test]
  fn test_nats_delete_value() {
    let (_container, host_port) = setup();
    Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'")).expect("Failed to set NATS host");
    Spi::run(&format!("SET {NATS_PORT_CONF} = {}", host_port)).expect("Failed to set NATS host");

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

  #[pg_test]
  fn test_pgnats_reload_conf() {
    let (_container, host_port) = setup();
    Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'")).expect("Failed to set NATS host");
    Spi::run(&format!("SET {NATS_PORT_CONF} = {}", host_port)).expect("Failed to set NATS host");

    let res = api::nats_publish("test.test_pgnats_reload_conf", "test");
    assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);

    api::pgnats_reload_conf();

    let res = api::nats_publish("test.test_pgnats_reload_conf", "test");
    assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);

    Spi::run(&format!("SET {NATS_HOST_CONF} = 'localhost1'")).expect("Failed to set NATS host");
    let res = api::nats_publish("test.test_pgnats_reload_conf", "test");
    assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);

    api::pgnats_reload_conf();

    assert!(api::nats_publish("test.test_pgnats_reload_conf", "test").is_err());

    Spi::run(&format!("SET {NATS_HOST_CONF} = '{NATS_HOST}'")).expect("Failed to set NATS host");
    api::pgnats_reload_conf_force();

    let res = api::nats_publish("test.test_pgnats_reload_conf", "test");
    assert!(res.is_ok(), "nats_publish occurs error: {:?}", res);
  }
}
