use bincode::{Decode, Encode};
use pgrx::{
    PgLwLock, PgSharedMemoryInitialization, bgworkers::BackgroundWorkerBuilder, pg_shmem_init,
    prelude::*,
};

use crate::{bgw::ring_queue::RingQueue, config::Config, constants::EXTENSION_NAME};

mod fdw;
mod launcher;
mod ring_queue;

pub mod notification;
pub mod pgrx_wrappers;

pub const SUBSCRIPTIONS_TABLE_NAME: &str = "pgnats.subscriptions";
pub const LAUNCHER_ENTRY_POINT: &str = "background_worker_launcher";

extension_sql!(
    r#"
    CREATE SCHEMA IF NOT EXISTS pgnats;

    CREATE TABLE IF NOT EXISTS pgnats.subscriptions (
        subject TEXT NOT NULL,
        callback TEXT NOT NULL,
        UNIQUE(subject, callback)
    );
    "#,
    name = "create_subscriptions_table",
);

pub static LAUNCHER_MESSAGE_BUS: PgLwLock<RingQueue<65536>> = PgLwLock::new(c"shared_worker_queue");

#[derive(Debug, Encode, Decode)]
pub enum WorkerMessage {
    NewConfig { db_oid: u32, config: Config },
    Subscribe { subject: String, fn_name: String },
    Unsubscribe { subject: String, fn_name: String },
}

pub fn init_background_worker_launcher() {
    pg_shmem_init!(LAUNCHER_MESSAGE_BUS);

    BackgroundWorkerBuilder::new("Background Worker Launcher")
        .set_function(LAUNCHER_ENTRY_POINT)
        .set_library(EXTENSION_NAME)
        .enable_spi_access()
        .load();
}
