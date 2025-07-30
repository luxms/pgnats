pub mod message;
mod worker_entry;

use std::collections::HashMap;
use std::ptr::null_mut;

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys as sys;

use crate::bgw::launcher::message::LauncherMessage;
use crate::bgw::launcher::worker_entry::WorkerEntry;
use crate::bgw::{DSM_SIZE, LAUNCHER_MESSAGE_BUS, SUBSCRIBER_ENTRY_POINT};
use crate::constants::EXTENSION_NAME;
use crate::{debug, log, warn};

pub(super) const LAUNCHER_CTX: &str = "LAUNCHER";

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_launcher_main(_arg: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGCHLD,
    );

    BackgroundWorker::connect_worker_to_spi(None, None);

    let database_oids = BackgroundWorker::transaction(fetch_database_oids);

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
                            log!("'{}' has extension '{}'", worker.db_name, EXTENSION_NAME);
                        } else if let Some(worker) = workers.remove(&db_oid) {
                            log!(
                                "'{}' has NOT extension '{}'",
                                worker.db_name,
                                EXTENSION_NAME
                            );
                        }
                    }
                    LauncherMessage::NewConfig { db_oid, config } => todo!(),
                    LauncherMessage::Subscribe {
                        db_oid,
                        subject,
                        fn_name,
                    } => todo!(),
                    LauncherMessage::Unsubscribe {
                        db_oid,
                        subject,
                        fn_name,
                    } => todo!(),
                }
            }
        }
    }
}

fn fetch_database_oids() -> Vec<sys::Oid> {
    unsafe {
        let mut workers = vec![];

        let rel = pgrx::pg_sys::table_open(
            pgrx::pg_sys::DatabaseRelationId,
            pgrx::pg_sys::AccessShareLock as _,
        );

        let scan = pgrx::pg_sys::table_beginscan_catalog(rel, 0, null_mut());

        let mut tup =
            pgrx::pg_sys::heap_getnext(scan, pgrx::pg_sys::ScanDirection::ForwardScanDirection);

        while !tup.is_null() {
            let pgdb = &*(pgrx::pg_sys::GETSTRUCT(tup) as pgrx::pg_sys::Form_pg_database);

            if pgdb.datallowconn && !pgdb.datistemplate {
                workers.push(pgdb.oid);
            }

            tup =
                pgrx::pg_sys::heap_getnext(scan, pgrx::pg_sys::ScanDirection::ForwardScanDirection);
        }

        pgrx::pg_sys::table_endscan(scan);
        pgrx::pg_sys::table_close(rel, pgrx::pg_sys::AccessShareLock as _);

        workers
    }
}
