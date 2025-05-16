#[macro_export]
#[doc(hidden)]
macro_rules! impl_nats_publish {
    ($(#[$attr:meta])* $suffix:ident, $ty:ty) => {
        paste::paste! {
            #[pgrx::pg_extern]
            $(#[$attr])*
            pub fn [<nats_publish_ $suffix>](subject: &str, payload: $ty) -> Result<(), PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.rt.block_on(
                        ctx.nats_connection.publish(subject, payload, None::<String>, None)
                    )
                })
            }


            #[pgrx::pg_extern]
            #[doc = concat!("Equivalent to [`nats_publish_", stringify!($suffix), "`], but includes support for headers.")]
            /// # Arguments
            ///
            /// * `headers` - A JSON object representing the message headers. This should be a dictionary where each key and value is a string.
            pub fn [<nats_publish_ $suffix _with_headers>](subject: &str, payload: $ty, headers: pgrx::Json) -> Result<(), PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.rt.block_on(
                        ctx.nats_connection.publish(subject, payload, None::<String>, Some(headers.0))
                    )
                })
            }

            #[pgrx::pg_extern]
            #[doc = concat!("Equivalent to [`nats_publish_", stringify!($suffix), "`], but includes a reply subject.")]
            /// # Arguments
            ///
            /// * `reply` - The NATS subject where the response should be sent. This sets the `reply-to` field in the message, enabling request-reply semantics.
            pub fn [<nats_publish_ $suffix _reply>](subject: &str, payload: $ty, reply: &str) -> Result<(), PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.rt.block_on(
                        ctx.nats_connection.publish(subject, payload, Some(reply), None)
                    )
                })
            }

            #[pgrx::pg_extern]
            #[doc = concat!("Equivalent to [`nats_publish_", stringify!($suffix), "`], but includes both a reply subject and headers.")]
            /// # Arguments
            ///
            /// * `reply` - The NATS subject where the response should be sent. This sets the `reply-to` field in the message, enabling request-reply semantics.
            /// * `headers` - A JSON object representing the message headers. This should be a dictionary where each key and value is a string.
            pub fn [<nats_publish_ $suffix _reply_with_headers>](subject: &str, payload: $ty, reply: &str,  headers: pgrx::Json) -> Result<(), PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.rt.block_on(
                        ctx.nats_connection.publish(subject, payload, Some(reply), Some(headers.0))
                    )
                })
            }

            #[pgrx::pg_extern]
            #[doc = concat!("JetStream version of [`nats_publish_", stringify!($suffix), "`].")]
            ///
            /// # Alternative
            /// For additional functionality, consider the following variants:
            #[doc = concat!("- [`nats_publish_", stringify!($suffix), "_stream_with_headers`] - Publishes a message to JetStream with headers.")]
            pub fn [<nats_publish_ $suffix _stream>](subject: &str, payload: $ty) -> Result<(), PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.rt.block_on(
                        ctx.nats_connection.publish_stream(subject, payload, None)
                    )
                })
            }

            #[pgrx::pg_extern]
            #[doc = concat!("JetStream version of [`nats_publish_", stringify!($suffix), "`] but with headers.")]
            /// # Arguments
            ///
            /// * `headers` - A JSON object representing the message headers. This should be a dictionary where each key and value is a string.
            pub fn [<nats_publish_ $suffix _stream_with_headers>](subject: &str, payload: $ty) -> Result<(), PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.rt.block_on(
                        ctx.nats_connection.publish_stream(subject, payload, None)
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
            #[pgrx::pg_extern]
            $(#[$attr])*
                pub fn [<nats_request_ $suffix>](subject: &str, payload: $ty, timeout: Option<i32>) -> Result<Vec<u8>, PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.rt.block_on(
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
            #[pgrx::pg_extern]
            $(#[$attr])*
                pub fn [<nats_put_ $suffix>](bucket: String, key: &str, data: $ty) -> Result<i64, PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.rt.block_on(
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
            #[pgrx::pg_extern]
            $(#[$attr])*
            pub fn [<nats_get_ $suffix>](bucket: String, key: &str) -> Result<Option<$ret>, PgNatsError> {
                CTX.with_borrow_mut(|ctx| {
                    ctx.rt.block_on(
                        ctx.nats_connection.get_value(bucket, key)
                    )
                })
            }
        }
    };
}
