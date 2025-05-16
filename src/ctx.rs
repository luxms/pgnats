use std::cell::RefCell;

use bincode::{Decode, Encode};

use crate::connection::{ConnectionOptions, NatsConnection};

thread_local! {
    pub static CTX: RefCell<Context> = RefCell::new(create_context())
}

#[derive(Decode, Encode)]
pub enum BgMessage {
    Subscribe {
        opt: ConnectionOptions,
        subject: String,
        fn_name: String,
    },
    Unsubscribe {
        opt: ConnectionOptions,
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

pub struct Context {
    pub nats_connection: NatsConnection,
    pub rt: tokio::runtime::Runtime,
}
