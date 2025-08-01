mod context;

pub mod message;
pub mod pg_api;

use std::sync::{Arc, mpsc::channel};

use pgrx::{
    FromDatum, PgLwLock,
    bgworkers::{BackgroundWorker, SignalWakeFlags},
    pg_sys as sys,
};

use crate::{
    bgw::{
        LAUNCHER_MESSAGE_BUS, SUBSCRIPTIONS_TABLE_NAME,
        launcher::message::{ExtensionStatus, LauncherMessage},
        pgrx_wrappers::{
            dsm::{DsmHandle, DynamicSharedMemory},
            shm_mq::ShmMqReceiver,
        },
        ring_queue::RingQueue,
        subscriber::{
            context::SubscriberContext,
            message::{InternalWorkerMessage, SubscriberMessage},
        },
    },
    config::{fetch_config, fetch_fdw_server_name},
    constants::{EXTENSION_NAME, FDW_EXTENSION_NAME},
    debug, log,
    utils::{get_database_name, is_extension_installed, unpack_i64_to_oid_dsmh},
    warn,
};

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_subscriber_entry_point(arg: sys::Datum) {
    let arg = unsafe {
        i64::from_polymorphic_datum(arg, false, sys::INT8OID)
            .expect("Failed to extract argument from Datum")
    };

    let (db_oid, dsmh) = unpack_i64_to_oid_dsmh(arg);

    if let Err(err) = background_worker_subscriber_main(
        &LAUNCHER_MESSAGE_BUS,
        SUBSCRIPTIONS_TABLE_NAME,
        db_oid,
        dsmh,
    ) {
        warn!(
            context = format!("Database OID {db_oid}"),
            "Subscriber worker exited with error: {}", err
        );
    }
}

pub fn background_worker_subscriber_main<const N: usize>(
    launcher_bus: &PgLwLock<RingQueue<N>>,
    sub_table_name: &str,
    db_oid: sys::Oid,
    dsmh: DsmHandle,
) -> anyhow::Result<()> {
    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGCHLD,
    );

    unsafe {
        sys::BackgroundWorkerInitializeConnectionByOid(db_oid, sys::InvalidOid, 0);
    }

    let db_name = BackgroundWorker::transaction(|| get_database_name(db_oid))
        .ok_or_else(|| anyhow::anyhow!("Failed to fetch database name for oid: {}", db_oid))?;

    let result = background_worker_subscriber_main_internal(
        launcher_bus,
        sub_table_name,
        db_oid.to_u32(),
        &db_name,
        dsmh,
    );

    let msg = LauncherMessage::SubscriberExit {
        db_oid: db_oid.to_u32(),
    };

    let data = bincode::encode_to_vec(msg, bincode::config::standard())?;

    launcher_bus
        .exclusive()
        .try_send(&data)
        .map_err(|_| anyhow::anyhow!("Failed to send to launcher SubscriberExit message"))?;

    log!(context = db_name, "Subscriber worker stopped gracefully");
    result
}

fn background_worker_subscriber_main_internal<const N: usize>(
    launcher_bus: &PgLwLock<RingQueue<N>>,
    sub_table_name: &str,
    db_oid: u32,
    db_name: &str,
    dsmh: DsmHandle,
) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| anyhow::anyhow!("Failed to create tokio runtime: {err}"))?;

    let (msg_sender, msg_receiver) = channel();

    let mut ctx = SubscriberContext::new(rt, msg_sender.clone());

    {
        let is_installed = BackgroundWorker::transaction(|| is_extension_installed(EXTENSION_NAME));

        let status = if is_installed {
            if BackgroundWorker::transaction(|| fetch_fdw_server_name(FDW_EXTENSION_NAME)).is_some()
            {
                ExtensionStatus::Exist
            } else {
                ExtensionStatus::NoForeignServer
            }
        } else {
            ExtensionStatus::NoExtension
        };

        let msg = LauncherMessage::DbExtensionStatus { db_oid, status };

        let data =
            bincode::encode_to_vec(msg, bincode::config::standard()).expect("failed to encode");

        launcher_bus
            .exclusive()
            .try_send(&data)
            .expect("failed to send");

        if status != ExtensionStatus::Exist {
            log!(context = db_name, "Background worker exiting...");
            return Ok(());
        }
    }

    let dsm = DynamicSharedMemory::attach(dsmh).expect("Failed to attach to DSM");
    let mut recv = ShmMqReceiver::attach(&dsm).expect("Failed to get mq receiver");

    if ctx.is_master() {
        log!("Restoring NATS state");

        let opt = BackgroundWorker::transaction(|| fetch_config());

        if let Err(error) = ctx.restore_state(opt, sub_table_name) {
            warn!("Error restoring state: {}", error);
        }
    }

    while BackgroundWorker::wait_latch(Some(std::time::Duration::from_secs(1))) {
        ctx.check_migration(sub_table_name);

        {
            while let Ok(Some(buf)) = recv.try_recv() {
                let parse_result: Result<(SubscriberMessage, _), _> =
                    bincode::decode_from_slice(&buf[..], bincode::config::standard());
                let msg = match parse_result {
                    Ok((msg, _)) => msg,
                    Err(err) => {
                        warn!(
                            context = db_name,
                            "Failed to decode subscriber message: {}", err
                        );
                        continue;
                    }
                };

                match msg {
                    SubscriberMessage::NewConfig { config } => {
                        log!("Handling NewConnectionConfig update");

                        if let Err(err) = ctx.restore_state(config, sub_table_name) {
                            log!("Error during restoring state: {}", err);
                        }
                    }
                    SubscriberMessage::Subscribe { subject, fn_name } => {
                        debug!(
                            "Handling Subscribe for subject '{}', fn '{}'",
                            subject, fn_name
                        );

                        msg_sender
                            .send(InternalWorkerMessage::Subscribe {
                                register: true,
                                subject: subject.to_string(),
                                fn_name: fn_name.to_string(),
                            })
                            .unwrap();
                    }
                    SubscriberMessage::Unsubscribe { subject, fn_name } => {
                        debug!(
                            "Handling Unsubscribe for subject '{}', fn '{}'",
                            subject, fn_name
                        );

                        msg_sender
                            .send(InternalWorkerMessage::Unsubscribe {
                                reason: None,
                                subject: Arc::from(subject.as_str()),
                                fn_name: Arc::from(fn_name.as_str()),
                            })
                            .unwrap();
                    }
                }
            }
        }

        while let Ok(message) = msg_receiver.try_recv() {
            if ctx.is_replica() {
                debug!("Received internal message on replica. Ignoring.");
                continue;
            }

            ctx.handle_internal_message(message, sub_table_name);
        }
    }

    Ok(())
}
