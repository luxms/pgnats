mod worker_entry;

pub mod message;

use std::{
    collections::{HashMap, hash_map::Entry},
    ptr::null_mut,
};

use pgrx::{
    bgworkers::{BackgroundWorker, SignalWakeFlags},
    pg_sys as sys,
};

use crate::{
    bgw::{
        DSM_SIZE, LAUNCHER_MESSAGE_BUS, SUBSCRIBER_ENTRY_POINT,
        launcher::{message::LauncherMessage, worker_entry::WorkerEntry},
        subscriber::message::SubscriberMessage,
    },
    constants::EXTENSION_NAME,
    debug, info, log, warn,
};

pub(super) const LAUNCHER_CTX: &str = "LAUNCHER";

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_launcher_main(_arg: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(None, None);

    let database_oids = fetch_database_oids();

    log!(
        context = LAUNCHER_CTX,
        "Found {} databases",
        database_oids.len()
    );

    let mut workers: HashMap<u32, WorkerEntry> = database_oids
        .into_iter()
        .enumerate()
        .map(|(idx, oid)| {
            (
                oid,
                WorkerEntry::start(
                    oid,
                    &format!("PGNats Background Worker Subscriber"),
                    &format!("pgnats_bgw_subscriber_{}", idx),
                    SUBSCRIBER_ENTRY_POINT,
                    DSM_SIZE,
                ),
            )
        })
        .filter_map(|(oid, res)| match res {
            Ok(entry) => Some((entry.oid.to_u32(), entry)),
            Err(err) => {
                warn!(
                    context = LAUNCHER_CTX,
                    "Got error for {:?} oid: {}", oid, err
                );
                None
            }
        })
        .inspect(|(_, entry)| {
            log!(
                context = LAUNCHER_CTX,
                "Trying to start background worker subscriber for '{}'",
                entry.db_name
            )
        })
        .collect();

    while BackgroundWorker::wait_latch(Some(std::time::Duration::from_secs(1))) {
        {
            while let Some(buf) = LAUNCHER_MESSAGE_BUS.exclusive().try_recv() {
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
                    LauncherMessage::DbExtensionStatus { db_oid, contains } => {
                        if contains && let Some(worker) = workers.get(&db_oid) {
                            log!(
                                context = LAUNCHER_CTX,
                                "'{}' has extension '{}'",
                                worker.db_name,
                                EXTENSION_NAME
                            );
                        } else if let Some(worker) = workers.remove(&db_oid) {
                            log!(
                                context = LAUNCHER_CTX,
                                "'{}' has NOT extension '{}'",
                                worker.db_name,
                                EXTENSION_NAME
                            );
                        }
                    }
                    LauncherMessage::NewConfig { db_oid, config } => match workers.entry(db_oid) {
                        Entry::Occupied(mut worker) => {
                            let data = bincode::encode_to_vec(
                                SubscriberMessage::NewConfig { config },
                                bincode::config::standard(),
                            )
                            .expect("failed to encode");
                            worker.get_mut().sender.send(&data).unwrap();
                        }
                        Entry::Vacant(worker) => {
                            if let Ok(we) = WorkerEntry::start(
                                sys::Oid::from_u32(db_oid),
                                &format!("PGNats Background Worker Subscriber"),
                                &format!("pgnats_bgw_subscriber_{}", 0),
                                SUBSCRIBER_ENTRY_POINT,
                                DSM_SIZE,
                            ) {
                                info!(
                                    context = LAUNCHER_CTX,
                                    "Registered new worker for database `{}`", we.db_name
                                );
                                let _ = worker.insert(we);
                            } else {
                                warn!(
                                    context = LAUNCHER_CTX,
                                    "Failed to initialized worker for db oid: {db_oid}"
                                );
                            }
                        }
                    },
                    LauncherMessage::Subscribe {
                        db_oid,
                        subject,
                        fn_name,
                    } => {
                        if let Some(worker) = workers.get_mut(&db_oid) {
                            let data = bincode::encode_to_vec(
                                SubscriberMessage::Subscribe { subject, fn_name },
                                bincode::config::standard(),
                            )
                            .expect("failed to encode");
                            worker.sender.send(&data).unwrap();
                        }
                    }
                    LauncherMessage::Unsubscribe {
                        db_oid,
                        subject,
                        fn_name,
                    } => {
                        if let Some(worker) = workers.get_mut(&db_oid) {
                            let data = bincode::encode_to_vec(
                                SubscriberMessage::Unsubscribe { subject, fn_name },
                                bincode::config::standard(),
                            )
                            .expect("failed to encode");
                            worker.sender.send(&data).unwrap();
                        }
                    }
                }
            }
        }
    }
}

fn fetch_database_oids() -> Vec<sys::Oid> {
    BackgroundWorker::transaction(|| unsafe {
        let mut workers = vec![];

        let rel = sys::table_open(sys::DatabaseRelationId, sys::AccessShareLock as _);

        let scan = sys::table_beginscan_catalog(rel, 0, null_mut());

        let mut tup = sys::heap_getnext(scan, sys::ScanDirection::ForwardScanDirection);

        while !tup.is_null() {
            let pgdb = &*(sys::GETSTRUCT(tup) as sys::Form_pg_database);

            if pgdb.datallowconn && !pgdb.datistemplate {
                workers.push(pgdb.oid);
            }

            tup = sys::heap_getnext(scan, sys::ScanDirection::ForwardScanDirection);
        }

        sys::table_endscan(scan);
        sys::table_close(rel, sys::AccessShareLock as _);

        workers
    })
}
