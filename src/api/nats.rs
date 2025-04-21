use pgrx::pg_extern;

use crate::{ctx::CTX, errors::PgNatsError};

#[pg_extern]
pub fn nats_publish_bytes(subject: &str, bytes: Vec<u8>) -> Result<(), PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.publish(subject, bytes))
    })
}

#[pg_extern]
pub fn nats_publish_bytes_stream(subject: &str, bytes: Vec<u8>) -> Result<(), PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set.block_on(
            &ctx.rt,
            ctx.nats_connection.publish_stream(subject, bytes),
        )
    })
}

#[pg_extern]
pub fn nats_publish_text(subject: &str, publish_text: String) -> Result<(), PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.publish(subject, publish_text))
    })
}

#[pg_extern]
pub fn nats_publish_text_stream(subject: &str, publish_text: String) -> Result<(), PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set.block_on(
            &ctx.rt,
            ctx.nats_connection.publish_stream(subject, publish_text),
        )
    })
}

#[pg_extern]
pub fn nats_publish_json(subject: &str, json: pgrx::Json) -> Result<(), PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.publish(subject, json.0))
    })
}

#[pg_extern]
pub fn nats_publish_json_stream(subject: &str, json: pgrx::Json) -> Result<(), PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set.block_on(
            &ctx.rt,
            ctx.nats_connection.publish_stream(subject, json.0),
        )
    })
}

#[pg_extern]
pub fn nats_publish_jsonb(subject: &str, json: pgrx::JsonB) -> Result<(), PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.publish(subject, json.0))
    })
}

#[pg_extern]
pub fn nats_publish_jsonb_stream(subject: &str, json: pgrx::JsonB) -> Result<(), PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set.block_on(
            &ctx.rt,
            ctx.nats_connection.publish_stream(subject, json.0),
        )
    })
}

#[pg_extern]
pub fn nats_put_binary(bucket: String, key: &str, data: &[u8]) -> Result<(), PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.put_value(bucket, key, data))
    })
}

#[pg_extern]
pub fn nats_put_text(bucket: String, key: &str, data: &str) -> Result<(), PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.put_value(bucket, key, data))
    })
}

#[pg_extern]
pub fn nats_put_jsonb(bucket: String, key: &str, data: pgrx::JsonB) -> Result<(), PgNatsError> {
    let data = serde_json::to_string(&data.0).map_err(PgNatsError::Serialize)?;
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.put_value(bucket, key, data))
    })
}

#[pg_extern]
pub fn nats_put_json(bucket: String, key: &str, data: pgrx::Json) -> Result<(), PgNatsError> {
    let data = serde_json::to_string(&data.0).map_err(PgNatsError::Serialize)?;
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.put_value(bucket, key, data))
    })
}

#[pg_extern]
pub fn nats_get_binary(bucket: String, key: &str) -> Result<Option<Vec<u8>>, PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.get_value(bucket, key))
    })
}

#[pg_extern]
pub fn nats_get_text(bucket: String, key: &str) -> Result<Option<String>, PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.get_value(bucket, key))
    })
}

#[pg_extern]
pub fn nats_get_json(bucket: String, key: &str) -> Result<Option<pgrx::Json>, PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.get_value(bucket, key))
    })
    .map(|v| v.map(pgrx::Json))
}

#[pg_extern]
pub fn nats_get_jsonb(bucket: String, key: &str) -> Result<Option<pgrx::JsonB>, PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.get_value(bucket, key))
    })
    .map(|v| v.map(pgrx::JsonB))
}

#[pg_extern]
pub fn nats_delete_value(bucket: String, key: &str) -> Result<(), PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.delete_value(bucket, key))
    })
}
