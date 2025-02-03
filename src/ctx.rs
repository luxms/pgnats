use std::sync::{Arc, LazyLock};

use crate::connection::NatsConnection;

pub static CTX: LazyLock<Context> = LazyLock::new(|| Context {
  nats_connection: Default::default(),
  rt: tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .expect("Failed to initialize Tokio runtime"),
});

pub struct Context {
  nats_connection: Arc<NatsConnection>,
  rt: tokio::runtime::Runtime,
}

impl Context {
  pub fn nats(&self) -> Arc<NatsConnection> {
    Arc::clone(&self.nats_connection)
  }

  pub fn rt(&self) -> &tokio::runtime::Runtime {
    &self.rt
  }
}
