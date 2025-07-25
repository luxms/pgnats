use async_nats::{ServerInfo, jetstream::object_store::ObjectInfo};
use pgrx::name;

#[allow(clippy::type_complexity)]
pub fn map_server_info(
    v: impl IntoIterator<Item = ServerInfo> + 'static,
) -> pgrx::iter::TableIterator<
    'static,
    (
        name!(server_id, String),
        name!(server_name, String),
        name!(host, String),
        name!(port, i32),
        name!(version, String),
        name!(auth_required, bool),
        name!(tls_requiered, bool),
        name!(max_payload, i64),
        name!(proto, i8),
        name!(client_id, i64),
        name!(go, String),
        name!(nonce, String),
        name!(connect_urls, pgrx::Json),
        name!(client_ip, String),
        name!(headers, bool),
        name!(lame_duck_mode, bool),
    ),
> {
    pgrx::iter::TableIterator::new(v.into_iter().map(|v| {
        (
            v.server_id,
            v.server_name,
            v.host,
            v.port as _,
            v.version,
            v.auth_required,
            v.tls_required,
            v.max_payload as _,
            v.proto,
            v.client_id as _,
            v.go,
            v.nonce,
            pgrx::Json(serde_json::to_value(v.connect_urls).expect("Must generate value")),
            v.client_ip,
            v.headers,
            v.lame_duck_mode,
        )
    }))
}

#[allow(clippy::type_complexity)]
pub fn map_object_info(
    v: impl IntoIterator<Item = ObjectInfo> + 'static,
) -> pgrx::iter::TableIterator<
    'static,
    (
        name!(name, String),
        name!(description, Option<String>),
        name!(metadata, pgrx::Json),
        name!(bucket, String),
        name!(nuid, String),
        name!(size, i64),
        name!(chunks, i64),
        name!(modified, Option<String>),
        name!(digest, Option<String>),
        name!(delete, bool),
    ),
> {
    pgrx::iter::TableIterator::new(v.into_iter().map(|v| {
        (
            v.name,
            v.description,
            pgrx::Json(serde_json::to_value(v.metadata).expect("Must generate value")),
            v.bucket,
            v.nuid,
            v.size as i64,
            v.chunks as i64,
            v.modified.map(|v| v.to_string()),
            v.digest,
            v.deleted,
        )
    }))
}
