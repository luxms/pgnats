use pgrx::prelude::*;

use crate::{connection::NATS_CONNECTION, errors::PgNatsError};

pub fn get_message(message_text: String) -> String {
  return format!("PGNATS: {}", message_text);
}

#[pg_extern]
pub fn hello_pgnats() -> &'static str {
  "Hello, pgnats!"
}

#[pg_extern]
fn nats_publish(publish_text: String, subject: String) -> Result<(), PgNatsError> {
  NATS_CONNECTION.publish(publish_text, subject)
}

#[pg_extern]
fn nats_publish_stream(publish_text: String, subject: String) -> Result<(), PgNatsError> {
  NATS_CONNECTION.publish_stream(publish_text, subject)
}

#[pg_extern]
fn nats_init() {
  // Auto run at first call any function at extencion
  // initialize_configuration();
}
