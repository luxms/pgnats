use bincode::{Decode, Encode};
use pgrx::PgLwLock;

use crate::{config::Config, ring_queue::RingQueue};

pub static WORKER_MESSAGE_QUEUE: PgLwLock<RingQueue<65536>> = PgLwLock::new(c"shared_worker_queue");

#[derive(Debug, Decode, Encode)]
pub enum WorkerMessage {
    NewConfig(Config),
    Subscribe { subject: String, fn_name: String },
    Unsubscribe { subject: String, fn_name: String },
}
