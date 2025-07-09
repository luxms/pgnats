use std::cell::RefCell;

use bincode::{Decode, Encode};

use crate::connection::{NatsConnection, NatsConnectionOptions};

thread_local! {
    pub static CTX: RefCell<Context> = RefCell::new(create_context())
}

pub struct Context {
    pub nats_connection: NatsConnection,
    pub rt: tokio::runtime::Runtime,
}

#[derive(Decode, Encode)]
pub enum WorkerMessage {
    Subscribe {
        opt: NatsConnectionOptions,
        subject: String,
        fn_name: String,
    },
    Unsubscribe {
        subject: String,
        fn_name: String,
    },
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
