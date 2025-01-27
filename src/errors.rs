use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgNatsError {
  #[error("publish error {0}")]
  PublishIoError(String),

  #[error("failed to connect to nats server {host}:{port}. {io_error}")]
  ConnectionError {
    host: String,
    port: u16,
    io_error: String,
  },

  #[error("update stream info {0}")]
  UpdateStreamError(String),

  #[error("nats buffer flush error")]
  FlushError,

  #[error("failed to get stream info")]
  StreamInfoError,
}
