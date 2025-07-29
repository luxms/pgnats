use std::{ffi::CStr, ptr::null_mut};

use pgrx::{
    FromDatum, IntoDatum,
    bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags},
};

use crate::{
    bgw::{posgres::PostgresWorker, run::run_worker},
    dsm::{DsmSegment, MessageQueue},
    error, log,
    utils::{pack_u32_to_i64, unpack_i64_to_u32},
    worker_queue::WORKER_MESSAGE_QUEUE,
};

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_subscriber(arg: pgrx::pg_sys::Datum) {
    // log!("Starting background worker: subscriber");

    // let db_oid =
    //     unsafe { pgrx::pg_sys::Oid::from_polymorphic_datum(arg, false, pgrx::pg_sys::OIDOID) }
    //         .unwrap();

    // let worker = match PostgresWorker::new(db_oid) {
    //     Ok(worker) => worker,
    //     Err(err) => {
    //         error!("Failed to initialize subscriber worker: {}", err);
    //         return;
    //     }
    // };

    // log!(
    //     "Subscriber worker connected to database '{}'",
    //     worker.connected_db_name()
    // );

    // if let Err(err) = run_worker(worker, &WORKER_MESSAGE_QUEUE) {
    //     error!("Error while running subscriber worker: {}", err);
    //     return;
    // }

    // log!("Subscriber worker stopped gracefully");
    //
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGHUP);

    let packed = unsafe { i64::from_polymorphic_datum(arg, false, pgrx::pg_sys::INT8OID) }.unwrap();

    let (oid, dsm_handle) = unpack_i64_to_u32(packed);
    let oid = pgrx::pg_sys::Oid::from_u32(oid);

    log!("DATABASE OID: {}", oid);
    log!("HANDLE: {}", dsm_handle);

    let dsm = DsmSegment::attach(dsm_handle).unwrap();
    let mut mq = MessageQueue::open_in(&dsm);

    let result = mq.receive_blocking().unwrap();

    assert_eq!(result, b"Hello, World");

    log!("FIRST MSG: {}", String::from_utf8_lossy(&result));

    let result = mq.receive_blocking().unwrap();

    assert_eq!(result, b"Hello, Rust");

    log!("SECOND MSG: {}", String::from_utf8_lossy(&result));
}

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_launcher(_arg: pgrx::pg_sys::Datum) {
    //let mut servers = HashMap::new();

    BackgroundWorker::connect_worker_to_spi(None, None);

    let workers = BackgroundWorker::transaction(|| unsafe {
        let mut workers = vec![];

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

            let dsm = DsmSegment::create(1024).unwrap();
            let mut mq = MessageQueue::create_in(&dsm, 1024);
            mq.send(b"Hello, World").unwrap();
            mq.send(b"Hello, Rust").unwrap();

            log!("HANDLE TO PASS: {}", dsm.handle());

            let packed = pack_u32_to_i64((*pgdb).oid.to_u32(), dsm.handle());

            let worker = BackgroundWorkerBuilder::new("Example")
                .set_library("pgnats")
                .set_function("background_worker_subscriber")
                .enable_spi_access()
                //.set_argument((*pgdb).oid.into_datum())
                .set_argument(packed.into_datum())
                .set_notify_pid(pgrx::pg_sys::MyProcPid)
                .load_dynamic()
                .expect("Failed to create");

            workers.push(worker);
            log!(
                "Worker started: {:?}",
                CStr::from_ptr((*pgdb).datname.data.as_ptr())
            );

            tup =
                pgrx::pg_sys::heap_getnext(scan, pgrx::pg_sys::ScanDirection::ForwardScanDirection);
        }

        pgrx::pg_sys::table_endscan(scan);
        pgrx::pg_sys::table_close(rel, pgrx::pg_sys::AccessShareLock as _);

        workers
    });

    for worker in workers {
        worker.wait_for_startup().unwrap();
        let status = worker.terminate();
        status.wait_for_shutdown().unwrap();
    }
}
