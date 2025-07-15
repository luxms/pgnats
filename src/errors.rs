use async_nats::{
    client::{FlushErrorKind, PublishErrorKind},
    error::Error,
    jetstream, ConnectErrorKind, RequestErrorKind,
};
use thiserror::Error as TError;

use crate::log::MSG_PREFIX;

/// This error type encapsulates various failure scenarios that can occur when
/// interacting with NATS and JetStream from PostgreSQL.
#[derive(TError, Debug)]
pub enum PgNatsError {
    /// Failed to publish message to core NATS
    PublishIo(#[from] Error<PublishErrorKind>),

    /// Failed to publish message via JetStream
    JetStreamPublishIo(#[from] Error<jetstream::context::PublishErrorKind>),

    Request(#[from] Error<RequestErrorKind>),

    /// Failed to establish connection to NATS server
    ///
    /// # Fields
    /// - `host`: The server host that was attempted
    /// - `port`: The port used for connection
    /// - `io_error`: Detailed connection failure reason
    Connection {
        host: String,
        port: u16,
        io_error: Error<ConnectErrorKind>,
    },

    /// Failed to update/create JetStream stream configuration
    UpdateStream(#[from] Error<jetstream::context::CreateStreamErrorKind>),

    /// Failed to flush messages to NATS server
    Flush(#[from] Error<FlushErrorKind>),

    /// Failed to retrieve stream information
    StreamInfo(#[from] Error<jetstream::context::RequestErrorKind>),

    /// Failed to create JetStream Key-Value bucket
    CreateBucket(#[from] Error<jetstream::context::CreateKeyValueErrorKind>),

    /// Failed to store value in Key-Value bucket
    PutValue(#[from] Error<jetstream::kv::PutErrorKind>),

    /// Failed to retrieve value from Key-Value bucket
    GetValue(#[from] Error<jetstream::kv::EntryErrorKind>),

    /// Failed to delete value from Key-Value bucket
    DeleteValue(#[from] Error<jetstream::kv::DeleteErrorKind>),

    /// Failed to serialize data to JSON
    Serialize(serde_json::Error),

    /// Failed to deserialize data from JSON
    Deserialize(String),

    /// Failed to retrieve an object from JetStream object store
    GetError(#[from] Error<jetstream::object_store::GetErrorKind>),

    /// Failed to store an object in JetStream object store
    PutError(#[from] Error<jetstream::object_store::PutErrorKind>),

    /// Failed to delete an object from JetStream object store
    DeleteError(#[from] Error<jetstream::object_store::DeleteErrorKind>),

    /// Failed to retrieve metadata about an object
    InfoError(#[from] Error<jetstream::object_store::InfoErrorKind>),

    /// Failed to subscribe to watch changes in an object store
    WatchError(#[from] Error<jetstream::object_store::WatchErrorKind>),

    /// General IO error
    IoError(#[from] tokio::io::Error),

    /// No connection options
    NoConnectionOptions,

    /// Encoding
    EncodingError,

    /// Shared Queue is full
    SharedQueueIsFull,
}

impl std::fmt::Display for PgNatsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PgNatsError::PublishIo(error) => write!(f, "{MSG_PREFIX}: publish error {error}"),
            PgNatsError::JetStreamPublishIo(error) => {
                write!(f, "{MSG_PREFIX}: jetsteam publish error {error}")
            }
            PgNatsError::Request(error) => {
                write!(f, "{MSG_PREFIX}: jetsteam request error {error}")
            }
            PgNatsError::Connection {
                host,
                port,
                io_error,
            } => write!(
                f,
                "{MSG_PREFIX}: failed to connect to nats server {host}:{port}. {io_error}"
            ),
            PgNatsError::UpdateStream(error) => {
                write!(f, "{MSG_PREFIX}: update stream info {error}")
            }
            PgNatsError::Flush(error) => write!(f, "{MSG_PREFIX}: nats buffer flush error {error}"),
            PgNatsError::StreamInfo(error) => {
                write!(f, "{MSG_PREFIX}: failed to get stream info {error}")
            }
            PgNatsError::CreateBucket(error) => {
                write!(f, "{MSG_PREFIX}: failed to create bucket {error}")
            }
            PgNatsError::PutValue(error) => {
                write!(f, "{MSG_PREFIX}: failed to put value in bucket {error}")
            }
            PgNatsError::GetValue(error) => {
                write!(f, "{MSG_PREFIX}: failed to get value in bucket {error}")
            }
            PgNatsError::DeleteValue(error) => {
                write!(f, "{MSG_PREFIX}: failed to delete value in bucket {error}")
            }
            PgNatsError::Serialize(error) => {
                write!(f, "{MSG_PREFIX}: failed to serialize json {error}")
            }
            PgNatsError::Deserialize(error) => {
                write!(f, "{MSG_PREFIX}: failed to deserialize json {error}")
            }
            PgNatsError::GetError(error) => {
                write!(
                    f,
                    "{MSG_PREFIX}: failed to get object from object store {error}"
                )
            }
            PgNatsError::PutError(error) => {
                write!(
                    f,
                    "{MSG_PREFIX}: failed to put object to object store {error}"
                )
            }
            PgNatsError::DeleteError(error) => {
                write!(
                    f,
                    "{MSG_PREFIX}: failed to delete object from object store {error}"
                )
            }
            PgNatsError::InfoError(error) => {
                write!(
                    f,
                    "{MSG_PREFIX}: failed to retrieve object metadata {error}"
                )
            }
            PgNatsError::WatchError(error) => {
                write!(
                    f,
                    "{MSG_PREFIX}: failed to watch object store changes {error}"
                )
            }
            PgNatsError::IoError(error) => {
                write!(f, "{MSG_PREFIX}: IO error {error}")
            }
            PgNatsError::NoConnectionOptions => {
                write!(f, "{MSG_PREFIX}: Failed to get connection options")
            }
            PgNatsError::EncodingError => {
                write!(f, "{MSG_PREFIX}: Failed to encode structure")
            }
            PgNatsError::SharedQueueIsFull => {
                write!(f, "{MSG_PREFIX}: Shared queue is full")
            }
        }
    }
}
