use async_nats::jetstream::object_store::ObjectInfo;
use pgrx::{name, PostgresType};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, PostgresType)]
pub struct ServerInfo {
    pub server_id: String,
    pub server_name: String,
    pub host: String,
    pub port: u16,
    pub version: String,
    pub auth_required: bool,
    pub tls_required: bool,
    pub max_payload: usize,
    pub proto: i8,
    pub client_id: u64,
    pub go: String,
    pub nonce: String,
    pub connect_urls: Vec<String>,
    pub client_ip: String,
    pub headers: bool,
    pub lame_duck_mode: bool,
}

impl From<async_nats::ServerInfo> for ServerInfo {
    fn from(value: async_nats::ServerInfo) -> Self {
        Self {
            server_id: value.server_id,
            server_name: value.server_name,
            host: value.host,
            port: value.port,
            version: value.version,
            auth_required: value.auth_required,
            tls_required: value.tls_required,
            max_payload: value.max_payload,
            proto: value.proto,
            client_id: value.client_id,
            go: value.go,
            nonce: value.nonce,
            connect_urls: value.connect_urls,
            client_ip: value.client_ip,
            headers: value.headers,
            lame_duck_mode: value.lame_duck_mode,
        }
    }
}

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
