use pgrx::pg_extern;

use crate::{ctx::CTX, errors::PgNatsError, impl_nats_get, impl_nats_publish, impl_nats_put};

impl_nats_publish!(binary, Vec<u8>);
impl_nats_publish!(text, String);
impl_nats_publish!(json, pgrx::Json);
impl_nats_publish!(jsonb, pgrx::JsonB);

impl_nats_put!(binary, Vec<u8>);
impl_nats_put!(text, &str);
impl_nats_put!(json, pgrx::Json);
impl_nats_put!(jsonb, pgrx::JsonB);

impl_nats_get!(binary, Vec<u8>);
impl_nats_get!(text, String);
impl_nats_get!(json, pgrx::Json);
impl_nats_get!(jsonb, pgrx::JsonB);

#[pg_extern]
pub fn nats_delete_value(bucket: String, key: &str) -> Result<(), PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.delete_value(bucket, key))
    })
}
