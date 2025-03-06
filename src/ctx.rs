use std::{cell::LazyCell, sync::Arc};

use crate::connection::NatsConnection;

thread_local! {
    pub static CTX: LazyCell<Context> = LazyCell::new(|| Context {
        nats_connection: Default::default(),
        rt: tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to initialize Tokio runtime"),
    })
}

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

impl Drop for Context {
  fn drop(&mut self) {
    self
      .rt
      .block_on(self.nats_connection.invalidate_connection());
  }
}
