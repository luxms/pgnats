use pgrx::prelude::*;

use crate::config::get_nats_connection;

pub fn get_message(message_text: String) -> String {
  return format!("PGNATS: {}", message_text);
}


#[pg_extern]
pub fn hello_pgnats() -> &'static str {
    "Hello, pgnats!"
}


#[pg_extern]
fn nats_publish(publish_text: String) {
  get_nats_connection()
    .unwrap()
    .publish("luxmsbi.cdc.audit.events", publish_text)
    .expect(&get_message("Exception on publishing message at NATS!".to_owned()));
}


#[pg_extern]
fn nats_init() {
  // Auto run at first call any function at extencion
  // initialize_configuration();
}
