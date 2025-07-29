use std::cell::RefCell;

use crate::nats_client::NatsClient;

thread_local! {
    pub static CTX: RefCell<Context> = RefCell::new(create_context());
}

pub struct Context {
    pub nats_connection: NatsClient,
    pub rt: tokio::runtime::Runtime,
}

fn create_context() -> Context {
    Context {
        nats_connection: Default::default(),
        rt: tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to initialize Tokio runtime"),
    }
}
