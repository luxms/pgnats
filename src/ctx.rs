use std::cell::RefCell;

use crate::connection::NatsConnection;

thread_local! {
    pub static CTX: RefCell<Context> = RefCell::new(Context {
        nats_connection: Default::default(),
        rt: tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to initialize Tokio runtime"),
        local_set: tokio::task::LocalSet::new(),
    })
}

pub struct Context {
    pub nats_connection: NatsConnection,
    pub rt: tokio::runtime::Runtime,
    pub local_set: tokio::task::LocalSet,
}

impl Drop for Context {
    fn drop(&mut self) {
        self.local_set
            .block_on(&self.rt, self.nats_connection.invalidate_connection());
    }
}
