mod worker_entry;

pub mod context;
pub mod message;
pub mod pg_api;

use pgrx::{PgLwLock, bgworkers::BackgroundWorker, pg_sys as sys};

use crate::{
    bgw::{
        LAUNCHER_MESSAGE_BUS, SUBSCRIBER_ENTRY_POINT,
        launcher::{
            context::LauncherContext,
            message::{ExtensionStatus, LauncherMessage},
            pg_api::fetch_database_oids,
        },
        ring_queue::RingQueue,
    },
    constants::{EXTENSION_NAME, FDW_EXTENSION_NAME},
    debug, log, warn,
};

pub(super) const LAUNCHER_CTX: &str = "LAUNCHER";

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_launcher_entry_point(_arg: pgrx::pg_sys::Datum) {
    if let Err(err) = background_worker_launcher_main(&LAUNCHER_MESSAGE_BUS, SUBSCRIBER_ENTRY_POINT)
    {
        warn!("Launcher worker error: {}", err);
    }
}

pub fn background_worker_launcher_main<const N: usize>(
    launcher_bus: &PgLwLock<RingQueue<N>>,
    subscriber_entry_point: &str,
) -> anyhow::Result<()> {
    let mut ctx = LauncherContext::new();

    let database_oids = BackgroundWorker::transaction(fetch_database_oids);

    log!(
        context = LAUNCHER_CTX,
        "Found {} databases",
        database_oids.len()
    );

    add_subscribe_workers(&mut ctx, database_oids, subscriber_entry_point);

    while BackgroundWorker::wait_latch(Some(std::time::Duration::from_secs(1))) {
        process_launcher_bus(launcher_bus, subscriber_entry_point, &mut ctx);
    }

    shutdown_workers(&mut ctx);

    log!(context = LAUNCHER_CTX, "Launcher worker stopped gracefully");

    Ok(())
}

pub fn process_launcher_bus<const N: usize>(
    queue: &PgLwLock<RingQueue<N>>,
    entry_point: &str,
    ctx: &mut LauncherContext,
) {
    let mut guard = queue.exclusive();

    while let Some(buf) = guard.try_recv() {
        debug!(
            context = LAUNCHER_CTX,
            "Received message from shared queue: {:?}",
            String::from_utf8_lossy(&buf)
        );

        let parse_result: Result<(LauncherMessage, _), _> =
            bincode::decode_from_slice(&buf[..], bincode::config::standard());

        let msg = match parse_result {
            Ok((msg, _)) => msg,
            Err(err) => {
                warn!("Failed to decode launcher message: {}", err);
                continue;
            }
        };

        match msg {
            LauncherMessage::DbExtensionStatus { db_oid, status } => match status {
                ExtensionStatus::Exist => {
                    if let Some(worker) = ctx.get_worker(db_oid) {
                        log!(
                            context = LAUNCHER_CTX,
                            "Database '{}' has extension '{}'",
                            worker.db_name,
                            EXTENSION_NAME
                        );
                    }
                }
                ExtensionStatus::NoExtension => match ctx.shutdown_worker(db_oid) {
                    Ok(entry) => {
                        log!(
                            context = LAUNCHER_CTX,
                            "Database '{}' has NOT extension '{}'",
                            entry.db_name,
                            EXTENSION_NAME
                        );
                    }
                    Err(err) => warn!("Got error: {}", err),
                },
                ExtensionStatus::NoForeignServer => match ctx.shutdown_worker(db_oid) {
                    Ok(entry) => {
                        log!(
                            context = LAUNCHER_CTX,
                            "Database '{}' has NOT foreign server extension '{}'",
                            entry.db_name,
                            FDW_EXTENSION_NAME
                        );
                    }
                    Err(err) => warn!("Got error: {}", err),
                },
            },
            LauncherMessage::NewConfig { db_oid, config } => {
                match ctx.handle_new_config_message(db_oid, config, entry_point) {
                    Ok(Some(db_name)) => {
                        log!("Registered new db: {}", db_name);
                    }
                    Ok(None) => {}
                    Err(err) => {
                        warn!("Got error: {}", err);
                    }
                }
            }
            LauncherMessage::Subscribe {
                db_oid,
                subject,
                fn_name,
            } => {
                if let Err(err) = ctx.handle_subscribe_message(db_oid, subject, fn_name) {
                    warn!("Got error: {}", err);
                }
            }
            LauncherMessage::Unsubscribe {
                db_oid,
                subject,
                fn_name,
            } => {
                if let Err(err) = ctx.handle_unsubscribe_message(db_oid, subject, fn_name) {
                    warn!("Got error: {}", err);
                }
            }
            LauncherMessage::SubscriberExit { db_oid } => {
                if let Err(err) = ctx.handle_subscriber_exit_message(db_oid) {
                    warn!("Got error: {}", err);
                }
            }
        }
    }
}

fn add_subscribe_workers(
    ctx: &mut LauncherContext,
    oids: impl IntoIterator<Item = sys::Oid>,
    entry_point: &str,
) {
    for oid in oids {
        match ctx.start_subscribe_worker(oid.to_u32(), entry_point) {
            Ok(entry) => {
                log!(
                    context = LAUNCHER_CTX,
                    "Trying to start background worker subscriber for '{}'",
                    entry.db_name
                );
                if let Err(err) = ctx.add_subscribe_worker(oid.to_u32(), entry) {
                    warn!(
                        context = LAUNCHER_CTX,
                        "Got error for {:?} oid: {}", oid, err
                    );
                }
            }
            Err(err) => {
                warn!(
                    context = LAUNCHER_CTX,
                    "Got error for {:?} oid: {}", oid, err
                );
            }
        }
    }
}

fn shutdown_workers(ctx: &mut LauncherContext) {
    for entry in ctx.drain_workers() {
        if let Err(err) = LauncherContext::shutdown_worker_entry(entry) {
            warn!(context = LAUNCHER_CTX, "{}", err);
        }
    }
}
