use std::ptr::null_mut;

use pgrx::{
    FromDatum, IntoDatum,
    bgworkers::{BackgroundWorker, BackgroundWorkerBuilder},
};

use crate::{
    bgw::{posgres::PostgresWorker, run::run_worker},
    error, log,
    worker_queue::WORKER_MESSAGE_QUEUE,
};

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_subscriber(arg: pgrx::pg_sys::Datum) {
    log!("Starting background worker: subscriber");

    let db_oid =
        unsafe { pgrx::pg_sys::Oid::from_polymorphic_datum(arg, false, pgrx::pg_sys::OIDOID) }
            .unwrap();

    let worker = match PostgresWorker::new(db_oid) {
        Ok(worker) => worker,
        Err(err) => {
            error!("Failed to initialize subscriber worker: {}", err);
            return;
        }
    };

    log!(
        "Subscriber worker connected to database '{}'",
        worker.connected_db_name()
    );

    if let Err(err) = run_worker(worker, &WORKER_MESSAGE_QUEUE) {
        error!("Error while running subscriber worker: {}", err);
        return;
    }

    log!("Subscriber worker stopped gracefully");
}

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_launcher(_arg: pgrx::pg_sys::Datum) {
    //let mut servers = HashMap::new();

    BackgroundWorker::connect_worker_to_spi(None, None);

    BackgroundWorker::transaction(|| unsafe {
        let rel = pgrx::pg_sys::table_open(
            pgrx::pg_sys::DatabaseRelationId,
            pgrx::pg_sys::AccessShareLock as _,
        );

        let scan = pgrx::pg_sys::table_beginscan_catalog(rel, 0, null_mut());

        let mut tup =
            pgrx::pg_sys::heap_getnext(scan, pgrx::pg_sys::ScanDirection::ForwardScanDirection);

        while !tup.is_null() {
            let pgdb = pgrx::pg_sys::GETSTRUCT(tup) as pgrx::pg_sys::Form_pg_database;

            if !(*pgdb).datallowconn || (*pgdb).datistemplate {
                continue;
            }

            let name = pgrx::pg_sys::get_database_name((*pgdb).oid);

            if !name.is_null() {
                let worker = BackgroundWorkerBuilder::new("Example")
                    .set_library("pgnats")
                    .set_function("background_worker_subscriber")
                    .enable_spi_access()
                    .set_argument((*pgdb).oid.into_datum())
                    .set_notify_pid(pgrx::pg_sys::MyProcPid)
                    .load_dynamic()
                    .expect("Failed to create");

                //let pid = worker.wait_for_startup().unwrap();
                log!("Worker started");
                //servers.insert((*pgdb).oid, worker);
            }

            tup =
                pgrx::pg_sys::heap_getnext(scan, pgrx::pg_sys::ScanDirection::ForwardScanDirection);
        }

        pgrx::pg_sys::table_endscan(scan);
        pgrx::pg_sys::table_close(rel, pgrx::pg_sys::AccessShareLock as _);
    });
}
