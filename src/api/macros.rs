#[macro_export]
#[doc(hidden)]
macro_rules! impl_nats_publish {
    ($(#[$attr:meta])* $suffix:ident, $ty:ty) => {
        paste::paste! {
            #[pgrx::pg_extern]
            $(#[$attr])*
            pub fn [<nats_publish_ $suffix>](subject: &str, payload: $ty, reply: ::pgrx::default!(Option<&str>, "NULL"), headers: ::pgrx::default!(Option<pgrx::JsonB>, "NULL")) -> anyhow::Result<()> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.rt.block_on(async {
                        let res = ctx.nats_connection.publish(subject, payload, reply, headers.map(|h| h.0)).await;
                        tokio::task::yield_now().await;
                        res
                    })
                })
            }

            #[pgrx::pg_extern]
            #[doc = concat!("JetStream version of [`nats_publish_", stringify!($suffix), "`].")]
            pub fn [<nats_publish_ $suffix _stream>](subject: &str, payload: $ty, headers: ::pgrx::default!(Option<pgrx::JsonB>, "NULL")) -> anyhow::Result<()> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.rt.block_on(async {
                        let res = ctx.nats_connection.publish_stream(subject, payload, headers.map(|h| h.0)).await;
                        tokio::task::yield_now().await;
                        res
                    })
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
            #[pgrx::pg_extern]
            $(#[$attr])*
                pub fn [<nats_request_ $suffix>](subject: &str, payload: $ty, timeout: Option<i32>) -> anyhow::Result<Vec<u8>> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.rt.block_on(async {
                        let res = ctx.nats_connection.request(subject, payload, timeout.and_then(|x| x.try_into().ok())).await;
                        tokio::task::yield_now().await;
                        res
                    })
                })
            }
        }
    };
}

#[cfg(feature = "kv")]
#[macro_export]
#[doc(hidden)]
macro_rules! impl_nats_put {
    ($(#[$attr:meta])* $suffix:ident, $ty:ty) => {
        paste::paste! {
            #[pgrx::pg_extern]
            $(#[$attr])*
                pub fn [<nats_put_ $suffix>](bucket: String, key: &str, data: $ty) -> anyhow::Result<i64> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.rt.block_on(async {
                        let res = ctx.nats_connection.put_value(bucket, key, data).await;
                        tokio::task::yield_now().await;
                        res
                    })
                    .map(|v| v.try_into().unwrap_or(i64::MAX))
                })
            }
        }
    };
}

#[cfg(feature = "kv")]
#[macro_export]
#[doc(hidden)]
macro_rules! impl_nats_get {
    ($(#[$attr:meta])* $suffix:ident, $ret:ty) => {
        paste::paste! {
            #[pgrx::pg_extern]
            $(#[$attr])*
            pub fn [<nats_get_ $suffix>](bucket: String, key: &str) -> anyhow::Result<Option<$ret>> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.rt.block_on(async {
                        let res = ctx.nats_connection.get_value(bucket, key).await;
                        tokio::task::yield_now().await;
                        res
                    })
                })
            }
        }
    };
}
