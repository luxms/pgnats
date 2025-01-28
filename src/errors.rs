use async_nats::{
  client::{FlushErrorKind, PublishErrorKind},
  error::Error,
  jetstream, ConnectErrorKind,
};
use thiserror::Error as TError;

#[derive(TError, Debug)]
pub enum PgNatsError {
  #[error("publish error {0}")]
  PublishIo(#[from] Error<PublishErrorKind>),

  #[error("jetsteam publish error {0}")]
  JetStreamPublishIo(#[from] Error<jetstream::context::PublishErrorKind>),

  #[error("failed to connect to nats server {host}:{port}. {io_error}")]
  Connection {
    host: String,
    port: u16,
    io_error: Error<ConnectErrorKind>,
  },

  #[error("update stream info {0}")]
  UpdateStream(#[from] Error<jetstream::context::CreateStreamErrorKind>),

  #[error("nats buffer flush error {0}")]
  Flush(#[from] Error<FlushErrorKind>),

  #[error("failed to get stream info {0}")]
  StreamInfo(#[from] Error<jetstream::context::RequestErrorKind>),

  #[error("failed to create bucket {0}")]
  CreateBucket(#[from] Error<jetstream::context::CreateKeyValueErrorKind>),

  #[error("failed to put value in bucket {0}")]
  PutValue(#[from] Error<jetstream::kv::PutErrorKind>),

  #[error("failed to get value in bucket {0}")]
  GetValue(#[from] Error<jetstream::kv::EntryErrorKind>),

  #[error("failed to delete value in bucket {0}")]
  DeleteValue(#[from] Error<jetstream::kv::DeleteErrorKind>),
}
