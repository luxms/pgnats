use pgrx::pg_extern;

use crate::{ctx::CTX, errors::PgNatsError, utils::format_message};

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
fn nats_put_binary(key: &str, data: &[u8]) -> Result<(), String> {
  CTX
    .rt()
    .block_on(CTX.nats().put_value(key, data))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_put_text(key: &str, data: &str) -> Result<(), String> {
  CTX
    .rt()
    .block_on(CTX.nats().put_value(key, data))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_put_jsonb(key: &str, data: pgrx::JsonB) -> Result<(), String> {
  CTX
    .rt()
    .block_on(async {
      let data = serde_json::to_string(&data.0).map_err(PgNatsError::Serialize)?;

      CTX.nats().put_value(key, data).await
    })
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_put_json(key: &str, data: pgrx::Json) -> Result<(), String> {
  CTX
    .rt()
    .block_on(async {
      let data = serde_json::to_string(&data.0).map_err(PgNatsError::Serialize)?;

      CTX.nats().put_value(key, data).await
    })
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_get_binary(key: &str) -> Result<Option<Vec<u8>>, String> {
  CTX
    .rt()
    .block_on(CTX.nats().get_value(key))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_get_text(key: &str) -> Result<Option<String>, String> {
  CTX
    .rt()
    .block_on(CTX.nats().get_value(key))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_get_json(key: &str) -> Result<Option<pgrx::Json>, String> {
  CTX
    .rt()
    .block_on(CTX.nats().get_value::<serde_json::Value>(key))
    .map(|v| v.map(|v| pgrx::Json(v)))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_get_jsonb(key: &str) -> Result<Option<pgrx::JsonB>, String> {
  CTX
    .rt()
    .block_on(CTX.nats().get_value::<serde_json::Value>(key))
    .map(|v| v.map(|v| pgrx::JsonB(v)))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_delete_value(key: &str) -> Result<(), String> {
  CTX
    .rt()
    .block_on(CTX.nats().delete_value(key))
    .map_err(|err| format_message(err.to_string()))
}
