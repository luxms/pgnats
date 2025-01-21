use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgNatsError {
  #[error("publish error: {0}")]
  PublishIo(std::io::Error),

  #[error("failed to connect to nats server {host}:{port}. {io_error}")]
  Connection {
    host: String,
    port: u16,
    io_error: std::io::Error,
  },

  #[error("update stream info: {0}")]
  UpdateStream(std::io::Error),
}
