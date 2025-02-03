use async_nats::{
  client::{FlushErrorKind, PublishErrorKind},
  error::Error,
  jetstream, ConnectErrorKind,
};
use thiserror::Error as TError;

use crate::utils::MSG_PREFIX;

#[derive(TError, Debug)]
pub enum PgNatsError {
  PublishIo(#[from] Error<PublishErrorKind>),
  JetStreamPublishIo(#[from] Error<jetstream::context::PublishErrorKind>),
  Connection {
    host: String,
    port: u16,
    io_error: Error<ConnectErrorKind>,
  },
  UpdateStream(#[from] Error<jetstream::context::CreateStreamErrorKind>),
  Flush(#[from] Error<FlushErrorKind>),
  StreamInfo(#[from] Error<jetstream::context::RequestErrorKind>),
  CreateBucket(#[from] Error<jetstream::context::CreateKeyValueErrorKind>),
  PutValue(#[from] Error<jetstream::kv::PutErrorKind>),
  GetValue(#[from] Error<jetstream::kv::EntryErrorKind>),
  DeleteValue(#[from] Error<jetstream::kv::DeleteErrorKind>),
  Serialize(serde_json::Error),
  Deserialize(String),
}

impl std::fmt::Display for PgNatsError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      PgNatsError::PublishIo(error) => write!(f, "{MSG_PREFIX}: publish error {error}"),
      PgNatsError::JetStreamPublishIo(error) => {
        write!(f, "{MSG_PREFIX}: jetsteam publish error {error}")
      }
      PgNatsError::Connection {
        host,
        port,
        io_error,
      } => write!(
        f,
        "{MSG_PREFIX}: failed to connect to nats server {host}:{port}. {io_error}"
      ),
      PgNatsError::UpdateStream(error) => write!(f, "{MSG_PREFIX}: update stream info {error}"),
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
      PgNatsError::Serialize(error) => write!(f, "{MSG_PREFIX}: failed to serialize json {error}"),
      PgNatsError::Deserialize(error) => {
        write!(f, "{MSG_PREFIX}: failed to deserialize json {error}")
      }
    }
  }
}
