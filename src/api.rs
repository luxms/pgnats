mod nats;
mod types;

#[macro_use]
mod macros;

use pgrx::pg_extern;

pub use nats::*;

use crate::{
    ctx::CTX,
    log,
    shm::{WorkerMessage, WORKER_MESSAGE_QUEUE},
};

/// Reloads NATS connection if configuration has changed
///
/// # SQL Usage
/// ```sql
/// -- Reload connection if config changed
/// SELECT pgnats_reload_conf();
///
/// -- Typical usage after configuration changes
/// SET nats.host = 'new.nats.server:4222';
/// SELECT pgnats_reload_conf();
/// ```
#[pg_extern]
pub fn pgnats_reload_conf() {
    CTX.with_borrow_mut(|ctx| {
        ctx.rt.block_on(async {
            let res = ctx.nats_connection.check_and_invalidate_connection().await;
            tokio::task::yield_now().await;
            res
        })
    })
}

/// Returns the current crate version as declared in Cargo.toml
///
/// # SQL Usage
/// ```sql
/// -- Get the current extension version
/// SELECT pgnats_version();
/// ```
#[pg_extern]
pub fn pgnats_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Forces immediate NATS connection reinitialization
///
/// # SQL Usage
/// ```sql
/// -- Force reconnect immediately
/// SELECT pgnats_reload_conf_force();
/// ```
#[pg_extern]
pub fn pgnats_reload_conf_force() {
    CTX.with_borrow_mut(|ctx| {
        ctx.rt.block_on(async {
            let res = ctx.nats_connection.invalidate_connection().await;
            tokio::task::yield_now().await;
            res
        })
    })
}

#[pg_extern]
pub fn send_options_change(name: String) {
    let msg = WorkerMessage::Config { name };

    if let Ok(buf) = bincode::encode_to_vec(msg, bincode::config::standard()) {
        if WORKER_MESSAGE_QUEUE.exclusive().try_send(&buf).is_err() {
            log!("Shared queue is full");
        }
    }
}
