use pgrx::pg_extern;

use crate::{
    api::types::ServerInfo, ctx::CTX, errors::PgNatsError, impl_nats_get, impl_nats_publish,
    impl_nats_put, impl_nats_request,
};

impl_nats_publish! {
    /// Publishes a raw binary message to the specified NATS subject.
    ///
    /// # Arguments
    /// * `subject` - NATS subject to publish to
    /// * `payload` - Binary data to publish as `Vec<u8>`
    ///
    /// # Returns
    /// * `Ok(())` - On successful publish
    /// * `Err(PgNatsError)` - If publish failed
    ///
    /// # SQL Usage
    /// ```sql
    /// SELECT nats_publish_binary('events.raw', E'\\xDEADBEEF'::bytea);
    /// ```
    ///
    /// # JetStream Version
    /// The stream version [`nats_publish_binary_stream`] provides JetStream
    /// persistence and delivery guarantees.
    ///
    /// # Alternative
    /// For additional functionality, consider the following variants:
    /// - [`nats_publish_binary_reply`] – Publishes a message with a reply subject.
    /// - [`nats_publish_binary_with_headers`] – Publishes a message with headers.
    /// - [`nats_publish_binary_reply_with_headers`] – Publishes a message with both a reply subject and headers.
    binary, Vec<u8>
}
impl_nats_publish! {
    /// Publishes a UTF-8 text message to the specified NATS subject.
    ///
    /// # Arguments
    /// * `subject` - NATS subject to publish to
    /// * `payload` - Text message to publish as `String`
    ///
    /// # Returns
    /// * `Ok(())` - On successful publish
    /// * `Err(PgNatsError)` - If publish failed
    ///
    /// # SQL Usage
    /// ```sql
    /// SELECT nats_publish_text('alerts', 'System temperature critical');
    /// ```
    ///
    /// # JetStream Version
    /// The stream version [`nats_publish_text_stream`] provides JetStream
    /// persistence and delivery guarantees.
    ///
    /// # Alternative
    /// For additional functionality, consider the following variants:
    /// - [`nats_publish_text_reply`] – Publishes a message with a reply subject.
    /// - [`nats_publish_text_with_headers`] – Publishes a message with headers.
    /// - [`nats_publish_text_reply_with_headers`] – Publishes a message with both a reply subject and headers.
    text, String
}

impl_nats_publish! {
    /// Publishes a JSON payload to the specified NATS subject.
    ///
    /// # Arguments
    /// * `subject` - NATS subject to publish to
    /// * `payload` - JSON data to publish as `pgrx::Json`
    ///
    /// # Returns
    /// * `Ok(())` - On successful publish
    /// * `Err(PgNatsError)` - If publish failed
    ///
    /// # SQL Usage
    /// ```sql
    /// SELECT nats_publish_json('events', '{"type": "login", "user": "admin"}'::json);
    /// ```
    ///
    /// # JetStream Version
    /// The stream version [`nats_publish_json_stream`] provides JetStream
    /// persistence and delivery guarantees.
    ///
    /// # Alternative
    /// For additional functionality, consider the following variants:
    /// - [`nats_publish_json_reply`] – Publishes a message with a reply subject.
    /// - [`nats_publish_json_with_headers`] – Publishes a message with headers.
    /// - [`nats_publish_json_reply_with_headers`] – Publishes a message with both a reply subject and headers.
    json, pgrx::Json
}

impl_nats_publish! {
    /// Publishes a binary-encoded JSON (JSONB) payload to the specified NATS subject.
    ///
    /// # Arguments
    /// * `subject` - NATS subject to publish to
    /// * `payload` - Binary JSON data to publish as `pgrx::JsonB`
    ///
    /// # Returns
    /// * `Ok(())` - On successful publish
    /// * `Err(PgNatsError)` - If publish failed
    ///
    /// # SQL Usage
    /// ```sql
    /// SELECT nats_publish_jsonb('large.events', '{"data": "...", "meta": {...}}'::jsonb);
    /// ```
    ///
    /// # JetStream Version
    /// The stream version [`nats_publish_jsonb_stream`] provides JetStream
    /// persistence and delivery guarantees.
    ///
    /// # Alternative
    /// For additional functionality, consider the following variants:
    /// - [`nats_publish_jsonb_reply`] – Publishes a message with a reply subject.
    /// - [`nats_publish_jsonb_with_headers`] – Publishes a message with headers.
    /// - [`nats_publish_jsonb_reply_with_headers`] – Publishes a message with both a reply subject and headers.
    jsonb, pgrx::JsonB
}

impl_nats_request! {
    /// Performs a binary request/response operation with NATS
    ///
    /// # Arguments
    /// * `subject` - NATS subject to send request to
    /// * `payload` - Binary request data as `Vec<u8>`
    /// * `timeout` - Optional maximum duration to wait for response in ms
    ///
    /// # Returns
    /// * `Ok(Vec<u8>)` - Binary response data on success
    /// * `Err(PgNatsError)` - If request fails or times out
    ///
    /// # SQL Usage
    /// ```sql
    /// -- Simple binary request
    /// SELECT nats_request_binary('service.call', E'\\x01'::bytea, '5s');
    /// ```
    binary, Vec<u8>
}

impl_nats_request! {
    /// Performs a text request/response operation with NATS
    ///
    /// # Arguments
    /// * `subject` - NATS subject to send request to
    /// * `payload` - Text request data as `String`
    /// * `timeout` - Optional maximum duration to wait for response in ms
    ///
    /// # Returns
    /// * `Ok(String)` - Text response on success
    /// * `Err(PgNatsError)` - If request fails or times out
    ///
    /// # SQL Usage
    /// ```sql
    /// -- Simple text request
    /// SELECT nats_request_text('api.get', '{"id":42}', '1s');
    /// ```
    text, String
}

impl_nats_request! {
    /// Performs a JSON request/response operation with NATS
    ///
    /// # Arguments
    /// * `subject` - NATS subject to send request to
    /// * `payload` - JSON request data as `pgrx::Json`
    /// * `timeout` - Optional maximum duration to wait for response in ms
    ///
    /// # Returns
    /// * `Ok(pgrx::Json)` - JSON response on success
    /// * `Err(PgNatsError)` - If request fails or times out
    ///
    /// # SQL Usage
    /// ```sql
    /// -- Basic JSON request
    /// SELECT nats_request_json('api.users', '{"action":"get"}'::json, '2s');
    /// ```
    json, pgrx::Json
}

impl_nats_request! {
    /// Performs a binary JSON (JSONB) request/response operation with NATS
    ///
    /// # Arguments
    /// * `subject` - NATS subject to send request to
    /// * `payload` - Binary JSON request data as `pgrx::JsonB`
    /// * `timeout` - Optional maximum duration to wait for response in ms
    ///
    /// # Returns
    /// * `Ok(pgrx::JsonB)` - Binary JSON response on success
    /// * `Err(PgNatsError)` - If request fails or times out
    ///
    /// # SQL Usage
    /// ```sql
    /// -- Simple JSONB request
    /// SELECT nats_request_jsonb('data.export', '{"format":"parquet"}'::jsonb, '10s');
    /// ```
    jsonb, pgrx::JsonB
}

impl_nats_put! {
    /// Stores a raw binary value in the KV bucket under the specified key.
    ///
    /// # Arguments
    /// * `bucket` - Name of the KV bucket
    /// * `key` - Key to store the value under
    /// * `data` - Binary data to store as `Vec<u8>`
    ///
    /// # Returns
    /// * `Ok(i64)` - The revision number of the stored value on success.
    /// * `Err(PgNatsError)` - If operation failed
    ///
    /// # SQL Usage
    /// ```sql
    /// SELECT nats_put_binary('config_files', 'server_cert', E'\\xDEADBEEF'::bytea);
    /// ```
    binary, Vec<u8>
}

impl_nats_put! {
    /// Stores a UTF-8 text value in the KV bucket under the specified key.
    ///
    /// # Arguments
    /// * `bucket` - Name of the KV bucket
    /// * `key` - Key to store the value under
    /// * `data` - Text data to store as `&str`
    ///
    /// # Returns
    /// * `Ok(i64)` - The revision number of the stored value on success.
    /// * `Err(PgNatsError)` - If operation failed
    ///
    /// # SQL Usage
    /// ```sql
    /// SELECT nats_put_text('templates', 'welcome_email', `Hello, ${name}`);
    /// ```
    text, &str
}

impl_nats_put! {
    /// Stores a JSON value in the KV bucket under the specified key.
    ///
    /// # Arguments
    /// * `bucket` - Name of the KV bucket
    /// * `key` - Key to store the value under
    /// * `data` - JSON data to store as `pgrx::Json`
    ///
    /// # Returns
    /// * `Ok(i64)` - The revision number of the stored value on success.
    /// * `Err(PgNatsError)` - If operation failed
    ///
    /// # SQL Usage
    /// ```sql
    /// SELECT nats_put_json('user_profiles', 'user123', '{"prefs": {...}}'::json);
    /// ```
    json, pgrx::Json
}

impl_nats_put! {
    /// Stores a binary-encoded JSON (JSONB) value in the KV bucket under the specified key.
    ///
    /// # Arguments
    /// * `bucket` - Name of the KV bucket
    /// * `key` - Key to store the value under
    /// * `data` - Binary JSON data to store as `pgrx::JsonB`
    ///
    /// # Returns
    /// * `Ok(i64)` - The revision number of the stored value on success.
    /// * `Err(PgNatsError)` - If operation failed
    ///
    /// # SQL Usage
    /// ```sql
    /// SELECT nats_put_jsonb('large_docs', 'spec_v2', large_document::jsonb);
    /// ```
    jsonb, pgrx::JsonB
}

impl_nats_get! {
    /// Retrieves a raw binary value from the KV bucket by the specified key.
    ///
    /// # Arguments
    /// * `bucket` - Name of the KV bucket
    /// * `key` - Key to retrieve the value from
    ///
    /// # Returns
    /// * `Ok(Some(Vec<u8>))` - If value exists
    /// * `Ok(None)` - If key doesn't exist
    /// * `Err(PgNatsError)` - If operation failed
    ///
    /// # SQL Usage
    /// ```sql
    /// SELECT nats_get_binary('config_files', 'server_cert');
    /// ```
    binary, Vec<u8>
}

impl_nats_get! {
    /// Retrieves a UTF-8 text value from the KV bucket by the specified key.
    ///
    /// # Arguments
    /// * `bucket` - Name of the KV bucket
    /// * `key` - Key to retrieve the value from
    ///
    /// # Returns
    /// * `Ok(Some(String))` - If value exists
    /// * `Ok(None)` - If key doesn't exist
    /// * `Err(PgNatsError)` - If operation failed
    ///
    /// # SQL Usage
    /// ```sql
    /// SELECT nats_get_text('templates', 'welcome_email') AS template;
    /// ```
    text, String
}

impl_nats_get! {
    /// Retrieves a JSON value from the KV bucket by the specified key.
    ///
    /// # Arguments
    /// * `bucket` - Name of the KV bucket
    /// * `key` - Key to retrieve the value from
    ///
    /// # Returns
    /// * `Ok(Some(pgrx::Json))` - If value exists
    /// * `Ok(None)` - If key doesn't exist
    /// * `Err(PgNatsError)` - If operation failed
    ///
    /// # SQL Usage
    /// ```sql
    /// SELECT nats_get_json('user_profiles', 'user123');
    /// ```
    json, pgrx::Json
}

impl_nats_get! {
    /// Retrieves a binary-encoded JSON (JSONB) value from the KV bucket by the specified key.
    ///
    /// # Arguments
    /// * `bucket` - Name of the KV bucket
    /// * `key` - Key to retrieve the value from
    ///
    /// # Returns
    /// * `Ok(Some(pgrx::JsonB))` - If value exists
    /// * `Ok(None)` - If key doesn't exist
    /// * `Err(PgNatsError)` - If operation failed
    ///
    /// # SQL Usage
    /// ```sql
    /// SELECT nats_get_jsonb('large_docs', 'spec_v2');
    /// ```
    jsonb, pgrx::JsonB
}

/// Deletes a value from the NATS KV bucket by the specified key.
///
/// # Arguments
/// * `bucket` - The name of the KV bucket
/// * `key` - The key associated with the value to be deleted
///
/// # Returns
/// * `Ok(())` - If the deletion was successful
/// * `Err(PgNatsError)` - If an error occurred during deletion
///
/// # SQL Usage
/// ```sql
/// SELECT nats_delete_value('user_profiles', 'inactive_user_123');
/// ```
#[pg_extern]
pub fn nats_delete_value(bucket: String, key: &str) -> Result<(), PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.delete_value(bucket, key))
    })
}

/// Retrieves information about the NATS server connection.
///
/// # Returns
/// * `Ok(ServerInfo)` - Contains details about the NATS server if successful
/// * `Err(PgNatsError)` - If an error occurred while fetching server information
///
/// # SQL Usage
/// ```sql
/// SELECT nats_get_server_info();
/// ```
#[pg_extern]
pub fn nats_get_server_info() -> Result<ServerInfo, PgNatsError> {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.get_server_info())
            .map(|v| v.into())
    })
}
