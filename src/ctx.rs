use std::cell::RefCell;

use crate::{config::fetch_config, constants::FDW_EXTENSION_NAME, nats_client::NatsClient};

thread_local! {
    pub static CTX: RefCell<Context> = RefCell::new(create_context());
}

pub struct Context {
    pub nats_connection: NatsClient,
    pub rt: tokio::runtime::Runtime,
}

fn create_context() -> Context {
    Context {
        nats_connection: NatsClient::new(None, || fetch_config(FDW_EXTENSION_NAME)),
        rt: tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to initialize Tokio runtime"),
    }
}
