use pgrx::pg_extern;

use crate::{ctx::CTX, utils::format_message};

#[pg_extern]
fn nats_publish(publish_text: &str, subject: &str) -> Result<(), String> {
  CTX
    .rt()
    .block_on(CTX.nats().publish(publish_text, subject))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_publish_stream(publish_text: &str, subject: &str) -> Result<(), String> {
  CTX
    .rt()
    .block_on(CTX.nats().publish_stream(publish_text, subject))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_put_value(key: &str, data: &[u8]) -> Result<(), String> {
  CTX
    .rt()
    .block_on(CTX.nats().put_value(key, data))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_get_value(key: &str) -> Result<Option<Vec<u8>>, String> {
  CTX
    .rt()
    .block_on(CTX.nats().get_value(key))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_delete_value(key: &str) -> Result<(), String> {
  CTX
    .rt()
    .block_on(CTX.nats().delete_value(key))
    .map_err(|err| format_message(err.to_string()))
}
