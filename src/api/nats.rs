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
fn nats_put_binary(bucket: String, key: &str, data: &[u8]) -> Result<(), String> {
  CTX
    .rt()
    .block_on(CTX.nats().put_value(bucket, key, data))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_put_text(bucket: String, key: &str, data: &str) -> Result<(), String> {
  CTX
    .rt()
    .block_on(CTX.nats().put_value(bucket, key, data))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_put_jsonb(bucket: String, key: &str, data: pgrx::JsonB) -> Result<(), String> {
  CTX
    .rt()
    .block_on(async {
      let data = serde_json::to_string(&data.0).map_err(PgNatsError::Serialize)?;

      CTX.nats().put_value(bucket, key, data).await
    })
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_put_json(bucket: String, key: &str, data: pgrx::Json) -> Result<(), String> {
  CTX
    .rt()
    .block_on(async {
      let data = serde_json::to_string(&data.0).map_err(PgNatsError::Serialize)?;

      CTX.nats().put_value(bucket, key, data).await
    })
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_get_binary(bucket: String, key: &str) -> Result<Option<Vec<u8>>, String> {
  CTX
    .rt()
    .block_on(CTX.nats().get_value(bucket, key))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_get_text(bucket: String, key: &str) -> Result<Option<String>, String> {
  CTX
    .rt()
    .block_on(CTX.nats().get_value(bucket, key))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_get_json(bucket: String, key: &str) -> Result<Option<pgrx::Json>, String> {
  CTX
    .rt()
    .block_on(CTX.nats().get_value::<serde_json::Value>(bucket, key))
    .map(|v| v.map(pgrx::Json))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_get_jsonb(bucket: String, key: &str) -> Result<Option<pgrx::JsonB>, String> {
  CTX
    .rt()
    .block_on(CTX.nats().get_value::<serde_json::Value>(bucket, key))
    .map(|v| v.map(pgrx::JsonB))
    .map_err(|err| format_message(err.to_string()))
}

#[pg_extern]
fn nats_delete_value(bucket: String, key: &str) -> Result<(), String> {
  CTX
    .rt()
    .block_on(CTX.nats().delete_value(bucket, key))
    .map_err(|err| format_message(err.to_string()))
}
