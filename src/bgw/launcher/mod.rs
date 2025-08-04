mod worker_entry;

pub mod context;
pub mod message;
pub mod pg_api;

use pgrx::{
    PgLwLock,
    bgworkers::{BackgroundWorker, SignalWakeFlags},
    pg_sys as sys,
};

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
        warn!(
            context = LAUNCHER_CTX,
            "Launcher worker exited with error: {}", err
        );
    }
}

pub fn background_worker_launcher_main<const N: usize>(
    launcher_bus: &PgLwLock<RingQueue<N>>,
    subscriber_entry_point: &str,
) -> anyhow::Result<()> {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(None, None);

    let mut ctx = LauncherContext::default();

    let database_oids = BackgroundWorker::transaction(fetch_database_oids);

    log!(
        context = LAUNCHER_CTX,
        "Launcher started. Found {} databases.",
        database_oids.len(),
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
            "Received raw message from shared queue: {:?}",
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

        log!("MSG: {:?}", msg);

        match msg {
            LauncherMessage::DbExtensionStatus { db_oid, status } => match status {
                ExtensionStatus::Exist => {
                    if let Some(worker) = ctx.get_worker(db_oid) {
                        log!(
                            context = LAUNCHER_CTX,
                            "Extension '{}' is present in database '{}' (OID: {})",
                            EXTENSION_NAME,
                            worker.db_name,
                            db_oid
                        );
                    }
                }
                ExtensionStatus::NoExtension => match ctx.shutdown_worker(db_oid) {
                    Ok(Some(entry)) => {
                        log!(
                            context = LAUNCHER_CTX,
                            "Extension '{}' not found in database '{}'; worker shut down",
                            EXTENSION_NAME,
                            entry.db_name
                        );
                    }
                    Ok(None) => { /* ignore */ }
                    Err(err) => warn!(
                        context = LAUNCHER_CTX,
                        "Failed to shutdown worker for db_oid {} (no extension): {}", db_oid, err
                    ),
                },
                ExtensionStatus::NoForeignServer => match ctx.shutdown_worker(db_oid) {
                    Ok(Some(entry)) => {
                        log!(
                            context = LAUNCHER_CTX,
                            "Foreign server '{}' not found in database '{}'; worker shut down",
                            FDW_EXTENSION_NAME,
                            entry.db_name
                        );
                    }
                    Ok(None) => { /* ignore */ }
                    Err(err) => warn!(
                        context = LAUNCHER_CTX,
                        "Failed to shutdown worker for db_oid {} (no foreign server): {}",
                        db_oid,
                        err
                    ),
                },
            },
            LauncherMessage::NewConfig { db_oid, config } => {
                match ctx.handle_new_config_message(db_oid, config, entry_point) {
                    Ok(Some(db_name)) => {
                        log!(
                            context = LAUNCHER_CTX,
                            "Trying to start background worker subscriber for '{}'",
                            db_name
                        );
                    }
                    Ok(None) => {
                        debug!(
                            context = LAUNCHER_CTX,
                            "Updated configuration for existing database worker (OID: {})", db_oid
                        );
                    }
                    Err(err) => {
                        warn!(
                            context = LAUNCHER_CTX,
                            "Failed to apply config for db_oid {}: {}", db_oid, err
                        );
                    }
                }
            }
            LauncherMessage::Subscribe {
                db_oid,
                subject,
                fn_name,
            } => {
                if let Err(err) = ctx.handle_subscribe_message(db_oid, subject, fn_name) {
                    warn!(
                        context = LAUNCHER_CTX,
                        "Failed to process subscription (db_oid: {}): {}", db_oid, err
                    );
                } else {
                    debug!(
                        context = LAUNCHER_CTX,
                        "Registered subscription: db_oid={}", db_oid
                    );
                }
            }
            LauncherMessage::Unsubscribe {
                db_oid,
                subject,
                fn_name,
            } => {
                if let Err(err) = ctx.handle_unsubscribe_message(db_oid, subject, fn_name) {
                    warn!(
                        context = LAUNCHER_CTX,
                        "Failed to process unsubscription (db_oid: {}): {}", db_oid, err
                    );
                } else {
                    debug!(
                        context = LAUNCHER_CTX,
                        "Removed subscription: db_oid={}", db_oid
                    );
                }
            }
            LauncherMessage::SubscriberExit { db_oid, reason } => {
                match reason {
                    Ok(()) => {
                        debug!(
                            context = LAUNCHER_CTX,
                            "Subscriber for db_oid {} exited normally (SIGTERM)", db_oid
                        );
                    }
                    Err(msg) => {
                        debug!(
                            context = LAUNCHER_CTX,
                            "Subscriber for db_oid {} exited with error: {}", db_oid, msg
                        );
                    }
                }

                match ctx.handle_subscriber_exit_message(db_oid) {
                    Ok(Some(we)) => {
                        debug!(
                            context = LAUNCHER_CTX,
                            "Subscriber for database '{}' (OID {}) exited and cleaned up",
                            we.db_name,
                            db_oid
                        );
                    }
                    Ok(None) => { /* ignore */ }
                    Err(err) => {
                        warn!(
                            context = LAUNCHER_CTX,
                            "Subscriber exit handling failed for db_oid {}: {}", db_oid, err
                        );
                    }
                }
            }
            LauncherMessage::ForeignServerDropped { db_oid } => {
                match ctx.handle_foreign_server_dropped(db_oid) {
                    Ok(Some(we)) => {
                        debug!(
                            context = LAUNCHER_CTX,
                            "Foreign server for database '{}' (OID: {}) was dropped — subscriber worker terminated and cleaned up successfully",
                            we.db_name,
                            db_oid
                        );
                    }
                    Ok(None) => { /* ignore */ }
                    Err(err) => {
                        warn!(
                            context = LAUNCHER_CTX,
                            "Failed to clean up subscriber after foreign server drop for db_oid {}: {}",
                            db_oid,
                            err
                        );
                    }
                }
            }
        }
    }
}

pub fn send_message_to_launcher<const N: usize>(
    bus: &PgLwLock<RingQueue<N>>,
    msg: LauncherMessage,
) -> anyhow::Result<()> {
    let data = bincode::encode_to_vec(msg, bincode::config::standard())?;

    bus.exclusive()
        .try_send(&data)
        .map_err(|_| anyhow::anyhow!("Failed to send to launcher message"))?;

    Ok(())
}

pub fn send_message_to_launcher_with_retry<const N: usize>(
    bus: &PgLwLock<RingQueue<N>>,
    msg: LauncherMessage,
    tries: usize,
    interval: std::time::Duration,
) -> anyhow::Result<()> {
    let data = bincode::encode_to_vec(msg, bincode::config::standard())?;
    let mut n = 0;

    while n < tries {
        if bus.exclusive().try_send(&data).is_ok() {
            return Ok(());
        }

        n += 1;
        std::thread::sleep(interval);
    }

    Err(anyhow::anyhow!(
        "Failed to send launcher message after {} tries",
        tries
    ))
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
