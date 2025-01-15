use pgrx::pg_extern;

use crate::{connection::NATS_CONNECTION, utils::format_message};

#[pg_extern]
fn nats_publish(publish_text: &str, subject: &str) -> Result<(), String> {
  NATS_CONNECTION
    .publish(publish_text, subject)
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_publish_stream(publish_text: &str, subject: &str) -> Result<(), String> {
  NATS_CONNECTION
    .publish_stream(publish_text, subject)
    .map_err(|err| format_message(err.to_string()))
}
