#[macro_export]
#[doc(hidden)]
macro_rules! impl_nats_publish {
    ($(#[$attr:meta])* $suffix:ident, $ty:ty) => {
        paste::paste! {
            #[pg_extern]
            $(#[$attr])*
            pub fn [<nats_publish_ $suffix>](subject: &str, payload: $ty) -> Result<(), PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.local_set.block_on(&ctx.rt,
                        ctx.nats_connection.publish(subject, payload)
                    )
                })
            }

            #[pg_extern]
            #[doc = concat!("JetStream version of [`nats_publish_", stringify!($suffix), "`]", " but with JetStream delivery guarantees.")]
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
#[doc(hidden)]
macro_rules! impl_nats_request {
    ($(#[$attr:meta])* $suffix:ident, $ty:ty) => {
        paste::paste! {
            #[pg_extern]
            $(#[$attr])*
                pub fn [<nats_request_ $suffix>](subject: &str, payload: $ty, timeout: Option<i32>) -> Result<Vec<u8>, PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.local_set.block_on(&ctx.rt,
                        ctx.nats_connection.request(subject, payload, timeout.and_then(|x| x.try_into().ok()))
                    )
                })
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! impl_nats_put {
    ($(#[$attr:meta])* $suffix:ident, $ty:ty) => {
        paste::paste! {
            #[pg_extern]
            $(#[$attr])*
                pub fn [<nats_put_ $suffix>](bucket: String, key: &str, data: $ty) -> Result<i64, PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.local_set.block_on(&ctx.rt,
                        ctx.nats_connection.put_value(bucket, key, data)
                    ).map(|v| v.try_into().unwrap_or(i64::MAX))
                })
            }
        }
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! impl_nats_get {
    ($(#[$attr:meta])* $suffix:ident, $ret:ty) => {
        paste::paste! {
            #[pg_extern]
            $(#[$attr])*
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
