mod context;
mod nats;

pub mod message;
pub mod pg_api;

use std::sync::{
    Arc,
    mpsc::{Sender, channel},
};

use pgrx::{
    FromDatum, PgLwLock,
    bgworkers::{BackgroundWorker, SignalWakeFlags},
    pg_sys as sys,
};

use crate::{
    bgw::{
        LAUNCHER_MESSAGE_BUS, SUBSCRIPTIONS_TABLE_NAME,
        launcher::{
            message::{ExtensionStatus, LauncherMessage},
            send_message_to_launcher,
        },
        pgrx_wrappers::{
            dsm::{DsmHandle, DynamicSharedMemory},
            shm_mq::ShmMqReceiver,
        },
        ring_queue::RingQueue,
        subscriber::{
            context::SubscriberContext,
            message::{InternalWorkerMessage, SubscriberMessage},
            nats::NatsConnectionState,
            pg_api::{call_function, delete_subject_callback, insert_subject_callback},
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
            .expect("Subscriber: failed to extract i64 argument from Datum")
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

    let db_name = BackgroundWorker::transaction(|| get_database_name(db_oid)).ok_or_else(|| {
        anyhow::anyhow!(
            "Subscriber: failed to resolve database name for OID {}",
            db_oid
        )
    })?;

    let result = background_worker_subscriber_main_internal(
        launcher_bus,
        sub_table_name,
        db_oid.to_u32(),
        &db_name,
        dsmh,
    );

    send_message_to_launcher(
        launcher_bus,
        LauncherMessage::SubscriberExit {
            db_oid: db_oid.to_u32(),
            reason: result.as_ref().map(|v| *v).map_err(|err| err.to_string()),
        },
    )?;

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
    let status = check_extension_status()?;
    send_message_to_launcher(
        launcher_bus,
        LauncherMessage::DbExtensionStatus { db_oid, status },
    )?;

    if status != ExtensionStatus::Exist {
        log!(
            context = db_name,
            "Extension is not fully installed (status: {:?}). Subscriber background worker is exiting.",
            status
        );
        return Ok(());
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| {
            anyhow::anyhow!("Failed to initialize Tokio runtime in subscriber: {}", err)
        })?;

    let (msg_sender, msg_receiver) = channel();

    let config = BackgroundWorker::transaction(fetch_config);
    let nats = rt.block_on(NatsConnectionState::new(&config.nats_opt))?;

    let mut ctx = SubscriberContext::new(rt, msg_sender.clone(), nats, config);
    if let Err(err) = ctx.send_notification() {
        warn!(
            context = db_name,
            "Failed to send initial Postgres instance notification: {}", err
        );
    }

    let dsm = DynamicSharedMemory::attach(dsmh)?;
    let mut recv = ShmMqReceiver::attach(&dsm)?;

    if ctx.is_master() {
        log!(context = db_name, "Restoring previous subscription state");

        if let Err(error) = ctx.restore_state(sub_table_name) {
            warn!(
                context = db_name,
                "Failed to restore subscription state: {}", error
            );
        }
    }

    'bg_loop: while BackgroundWorker::wait_latch(Some(std::time::Duration::from_secs(1))) {
        if let Err(err) = ctx.check_migration(sub_table_name) {
            warn!(context = db_name, "Migration check failed: {}", err);
        }

        loop {
            match recv.try_recv() {
                Ok(Some(buf)) => {
                    handle_message_from_shared_queue(&buf, &msg_sender, &mut ctx, db_name)
                }
                Ok(None) => break,
                Err(err) => {
                    warn!(
                        context = db_name,
                        "Error reading message from shared memory queue: {}", err
                    );

                    break 'bg_loop;
                }
            }
        }

        while let Ok(message) = msg_receiver.try_recv() {
            if ctx.is_replica() {
                debug!("Received internal message on replica. Ignoring.");
                continue;
            }

            handle_internal_message(&mut ctx, message, sub_table_name, db_name);
        }
    }

    Ok(())
}

fn handle_message_from_shared_queue(
    buf: &[u8],
    sender: &Sender<InternalWorkerMessage>,
    ctx: &mut SubscriberContext,
    db_name: &str,
) {
    let parse_result: Result<(SubscriberMessage, _), _> =
        bincode::decode_from_slice(buf, bincode::config::standard());
    let msg = match parse_result {
        Ok((msg, _)) => msg,
        Err(err) => {
            warn!(
                context = db_name,
                "Failed to decode message from launcher: {}", err
            );
            return;
        }
    };

    match msg {
        SubscriberMessage::NewConfig { config } => {
            debug!("Handling NewConnectionConfig update");

            if let Err(err) = ctx.apply_config(config) {
                warn!("Error during restoring state: {}", err);
            }
        }
        SubscriberMessage::Subscribe { subject, fn_name } => {
            debug!(
                "Handling Subscribe for subject '{}', fn '{}'",
                subject, fn_name
            );

            let _ = sender.send(InternalWorkerMessage::Subscribe {
                register: true,
                subject: subject.to_string(),
                fn_name: fn_name.to_string(),
            });
        }
        SubscriberMessage::Unsubscribe { subject, fn_name } => {
            debug!(
                "Handling Unsubscribe for subject '{}', fn '{}'",
                subject, fn_name
            );

            let _ = sender.send(InternalWorkerMessage::Unsubscribe {
                subject: Arc::from(subject.as_str()),
                fn_name: Arc::from(fn_name.as_str()),
            });
        }
    }
}

fn handle_internal_message(
    ctx: &mut SubscriberContext,
    msg: InternalWorkerMessage,
    subscriptions_table_name: &str,
    db_name: &str,
) {
    match msg {
        InternalWorkerMessage::Subscribe {
            register,
            subject,
            fn_name,
        } => {
            debug!(
                "Received subscription request: subject='{}', fn='{}'",
                subject, fn_name
            );

            if register {
                if let Err(error) = BackgroundWorker::transaction(|| {
                    insert_subject_callback(subscriptions_table_name, &subject, &fn_name)
                }) {
                    warn!(
                        context = db_name,
                        "Failed to register subscription in catalog: subject='{}', callback='{}': {}",
                        subject,
                        fn_name,
                        error
                    );
                } else {
                    debug!(
                        "Inserted subject callback: subject='{}', callback='{}'",
                        subject, fn_name
                    );
                }
            }

            ctx.handle_subscribe(Arc::from(subject), Arc::from(fn_name));
        }
        InternalWorkerMessage::Unsubscribe { subject, fn_name } => {
            debug!(
                "Received unsubscription request: subject='{}', fn='{}'",
                subject, fn_name,
            );

            if let Err(error) = BackgroundWorker::transaction(|| {
                delete_subject_callback(subscriptions_table_name, &subject, &fn_name)
            }) {
                warn!(
                    context = db_name,
                    "Failed to remove subscription from catalog: subject='{}', callback='{}': {}",
                    subject,
                    fn_name,
                    error
                );
            } else {
                debug!(
                    "Deleted subject callback: subject='{}', callback='{}'",
                    subject, fn_name
                );
            }

            ctx.handle_unsubscribe(subject, fn_name);
        }
        InternalWorkerMessage::CallbackCall { subject, data } => {
            debug!("Dispatching callbacks for subject '{}'", subject);

            ctx.handle_callback(&subject, data, |callback, data| {
                if let Err(err) = BackgroundWorker::transaction(|| call_function(callback, data)) {
                    warn!(
                        context = db_name,
                        "Error while calling subscriber function '{}': {:?}", callback, err
                    );
                }
            });
        }
        InternalWorkerMessage::UnsubscribeSubject { subject, reason } => {
            warn!(
                context = db_name,
                "Unsubscribing subject due to: {}", reason
            );
            ctx.handle_unsubscribe_subject(&subject)
        }
    }
}

fn check_extension_status() -> anyhow::Result<ExtensionStatus> {
    let is_installed = BackgroundWorker::transaction(|| is_extension_installed(EXTENSION_NAME));

    let status = if is_installed {
        if BackgroundWorker::transaction(|| fetch_fdw_server_name(FDW_EXTENSION_NAME)).is_some() {
            ExtensionStatus::Exist
        } else {
            ExtensionStatus::NoForeignServer
        }
    } else {
        ExtensionStatus::NoExtension
    };

    Ok(status)
}
