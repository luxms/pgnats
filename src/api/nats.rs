use pgrx::pg_extern;

use crate::{ctx::CTX, errors::PgNatsError};

#[pg_extern]
pub fn nats_publish(subject: &str, publish_text: &str) -> Result<(), PgNatsError> {
  CTX.rt().block_on(CTX.nats().publish(subject, publish_text))
}

#[pg_extern]
pub fn nats_publish_stream(subject: &str, publish_text: &str) -> Result<(), PgNatsError> {
  CTX
    .rt()
    .block_on(CTX.nats().publish_stream(subject, publish_text))
}

#[pg_extern]
pub fn nats_put_binary(bucket: String, key: &str, data: &[u8]) -> Result<(), PgNatsError> {
  CTX.rt().block_on(CTX.nats().put_value(bucket, key, data))
}

#[pg_extern]
pub fn nats_put_text(bucket: String, key: &str, data: &str) -> Result<(), PgNatsError> {
  CTX.rt().block_on(CTX.nats().put_value(bucket, key, data))
}

#[pg_extern]
pub fn nats_put_jsonb(bucket: String, key: &str, data: pgrx::JsonB) -> Result<(), PgNatsError> {
  CTX.rt().block_on(async {
    let data = serde_json::to_string(&data.0).map_err(PgNatsError::Serialize)?;

    CTX.nats().put_value(bucket, key, data).await
  })
}

#[pg_extern]
pub fn nats_put_json(bucket: String, key: &str, data: pgrx::Json) -> Result<(), PgNatsError> {
  CTX.rt().block_on(async {
    let data = serde_json::to_string(&data.0).map_err(PgNatsError::Serialize)?;

    CTX.nats().put_value(bucket, key, data).await
  })
}

#[pg_extern]
pub fn nats_get_binary(bucket: String, key: &str) -> Result<Option<Vec<u8>>, PgNatsError> {
  CTX.rt().block_on(CTX.nats().get_value(bucket, key))
}

#[pg_extern]
pub fn nats_get_text(bucket: String, key: &str) -> Result<Option<String>, PgNatsError> {
  CTX.rt().block_on(CTX.nats().get_value(bucket, key))
}

#[pg_extern]
pub fn nats_get_json(bucket: String, key: &str) -> Result<Option<pgrx::Json>, PgNatsError> {
  CTX
    .rt()
    .block_on(CTX.nats().get_value::<serde_json::Value>(bucket, key))
    .map(|v| v.map(pgrx::Json))
}

#[pg_extern]
pub fn nats_get_jsonb(bucket: String, key: &str) -> Result<Option<pgrx::JsonB>, PgNatsError> {
  CTX
    .rt()
    .block_on(CTX.nats().get_value::<serde_json::Value>(bucket, key))
    .map(|v| v.map(pgrx::JsonB))
}

#[pg_extern]
pub fn nats_delete_value(bucket: String, key: &str) -> Result<(), PgNatsError> {
  CTX.rt().block_on(CTX.nats().delete_value(bucket, key))
}
