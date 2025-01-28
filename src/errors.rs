use async_nats::{
  client::{FlushErrorKind, PublishErrorKind},
  error::Error,
  jetstream,
};
use thiserror::Error as TError;

#[derive(TError, Debug)]
pub enum PgNatsError {
  #[error("publish error {0}")]
  PublishIoError(#[from] Error<PublishErrorKind>),

  #[error("jetsteam publish error {0}")]
  JetStreamPublishIoError(#[from] Error<jetstream::context::PublishErrorKind>),

  #[error("failed to connect to nats server {host}:{port}. {io_error}")]
  ConnectionError {
    host: String,
    port: u16,
    io_error: String,
  },

  #[error("update stream info {0}")]
  UpdateStreamError(#[from] Error<jetstream::context::CreateStreamErrorKind>),

  #[error("nats buffer flush error {0}")]
  FlushError(#[from] Error<FlushErrorKind>),

  #[error("failed to get stream info {0}")]
  StreamInfoError(#[from] Error<jetstream::context::RequestErrorKind>),
}
