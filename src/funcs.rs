use pgrx::prelude::*;

use crate::connection::NATS_CONNECTION;

pub fn get_message(message_text: impl AsRef<str>) -> String {
  format!("PGNATS: {}", message_text.as_ref())
}

pub fn do_panic_with_message(message_text: impl AsRef<str>) -> ! {
  panic!("PGNATS: {}", message_text.as_ref())
}

#[pg_extern]
pub fn hello_pgnats() -> &'static str {
  "Hello, pgnats!"
}

#[pg_extern]
fn nats_publish(publish_text: &str, subject: &str) -> Result<(), String> {
  NATS_CONNECTION
    .publish(publish_text, subject)
    .map_err(|err| get_message(err.to_string()))
}

#[pg_extern]
fn nats_publish_stream(publish_text: &str, subject: &str) -> Result<(), String> {
  NATS_CONNECTION
    .publish_stream(publish_text, subject)
    .map_err(|err| get_message(err.to_string()))
}

#[pg_extern]
fn nats_init() {
  // Auto run at first call any function at extencion
  // initialize_configuration();
}
