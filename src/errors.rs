use async_nats::{
  client::{FlushErrorKind, PublishErrorKind},
  error::Error,
  jetstream,
};
use thiserror::Error as TError;

#[derive(TError, Debug)]
pub enum PgNatsError {
  #[error("publish error {0}")]
  PublishIoError(Error<PublishErrorKind>),

  #[error("jetsteam publish error {0}")]
  JetStreamPublishIoError(Error<jetstream::context::PublishErrorKind>),

  #[error("failed to connect to nats server {host}:{port}. {io_error}")]
  ConnectionError {
    host: String,
    port: u16,
    io_error: String,
  },

  #[error("update stream info {0}")]
  UpdateStreamError(Error<jetstream::context::CreateStreamErrorKind>),

  #[error("nats buffer flush error {0}")]
  FlushError(Error<FlushErrorKind>),

  #[error("failed to get stream info {0}")]
  StreamInfoError(Error<jetstream::context::RequestErrorKind>),
}

impl From<Error<PublishErrorKind>> for PgNatsError {
  fn from(value: Error<PublishErrorKind>) -> Self {
    PgNatsError::PublishIoError(value)
  }
}

impl From<Error<FlushErrorKind>> for PgNatsError {
  fn from(value: Error<FlushErrorKind>) -> Self {
    PgNatsError::FlushError(value)
  }
}

impl From<Error<jetstream::context::PublishErrorKind>> for PgNatsError {
  fn from(value: Error<jetstream::context::PublishErrorKind>) -> Self {
    PgNatsError::JetStreamPublishIoError(value)
  }
}

impl From<Error<jetstream::context::RequestErrorKind>> for PgNatsError {
  fn from(value: Error<jetstream::context::RequestErrorKind>) -> Self {
    PgNatsError::StreamInfoError(value)
  }
}

impl From<Error<jetstream::context::CreateStreamErrorKind>> for PgNatsError {
  fn from(value: Error<jetstream::context::CreateStreamErrorKind>) -> Self {
    PgNatsError::UpdateStreamError(value)
  }
}
