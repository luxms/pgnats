use std::{
    cell::RefCell,
    sync::{mpsc::channel, LazyLock, Mutex},
};

use crate::connection::NatsConnection;

pub static SUBSCRIBTION_BRIDGE: LazyLock<SubscribtionBridge> = LazyLock::new(|| {
    let (sdr, recv) = channel();
    SubscribtionBridge {
        sender: sdr,
        recv: Mutex::new(Some(recv)),
    }
});

thread_local! {
    pub static CTX: RefCell<Context> = RefCell::new(create_context())
}

fn create_context() -> Context {
    Context {
        nats_connection: Default::default(),
        rt: tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to initialize Tokio runtime"),
        local_set: tokio::task::LocalSet::new(),
    }
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

pub struct SubscribtionBridge {
    pub sender: std::sync::mpsc::Sender<(String, Vec<u8>)>,
    pub recv: std::sync::Mutex<Option<std::sync::mpsc::Receiver<(String, Vec<u8>)>>>,
}
