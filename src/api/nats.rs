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
