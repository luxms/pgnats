use bincode::{Decode, Encode};
use pgrx::PgLwLock;

use crate::{connection::NatsConnectionOptions, shared_queue::SharedRingQueue};

pub static WORKER_MESSAGE_QUEUE: PgLwLock<SharedRingQueue<65536>> =
    PgLwLock::new(c"shared_worker_queue");

#[derive(Debug, Decode, Encode)]
pub enum WorkerMessage {
    NewConnectionConfig(NatsConnectionOptions),
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
