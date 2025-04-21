#[macro_export]
macro_rules! impl_nats_publish {
    ($suffix:ident, $ty:ty) => {
        paste::paste! {
            #[pg_extern]
            pub fn [<nats_publish_ $suffix>](subject: &str, payload: $ty) -> Result<(), PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.local_set.block_on(&ctx.rt,
                        ctx.nats_connection.publish(subject, payload)
                    )
                })
            }

            #[pg_extern]
            pub fn [<nats_publish_ $suffix _stream>](subject: &str, payload: $ty) -> Result<(), PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.local_set.block_on(&ctx.rt,
                        ctx.nats_connection.publish_stream(subject, payload)
                    )
                })
            }
        }
    };
}

#[macro_export]
macro_rules! impl_nats_put {
    ($suffix:ident, $ty:ty) => {
        paste::paste! {
            #[pg_extern]
            pub fn [<nats_put_ $suffix>](bucket: String, key: &str, data: $ty) -> Result<(), PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.local_set.block_on(&ctx.rt,
                        ctx.nats_connection.put_value(bucket, key, data)
                    )
                })
            }
        }
    };
}

#[macro_export]
macro_rules! impl_nats_get {
    ($suffix:ident, $ret:ty) => {
        paste::paste! {
            #[pg_extern]
            pub fn [<nats_get_ $suffix>](bucket: String, key: &str) -> Result<Option<$ret>, PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.local_set.block_on(&ctx.rt,
                        ctx.nats_connection.get_value(bucket, key)
                    )
                })
            }
        }
    };
}