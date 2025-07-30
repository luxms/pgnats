use pgrx::{
    FromDatum,
    bgworkers::{BackgroundWorker, SignalWakeFlags},
    pg_sys as sys,
};

use crate::{
    bgw::{LAUNCHER_MESSAGE_BUS, launcher::message::LauncherMessage},
    constants::EXTENSION_NAME,
    log,
    utils::{get_database_name, is_extension_installed, unpack_i64_to_oid_dsmh},
};

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_subscriber_main(arg: pgrx::pg_sys::Datum) {
    let arg = unsafe {
        i64::from_polymorphic_datum(arg, false, sys::INT8OID).expect("Failed to get the argument")
    };

    let (db_oid, dsmh) = unpack_i64_to_oid_dsmh(arg);

    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGCHLD,
    );

    unsafe {
        sys::BackgroundWorkerInitializeConnectionByOid(db_oid, sys::InvalidOid, 0);
    }

    let db_name = BackgroundWorker::transaction(|| get_database_name(db_oid)).unwrap();

    {
        let msg = LauncherMessage::DbExtensionStatus {
            db_oid: db_oid.to_u32(),
            contains: BackgroundWorker::transaction(|| is_extension_installed(EXTENSION_NAME)),
        };

        let data =
            bincode::encode_to_vec(msg, bincode::config::standard()).expect("failed to encode");

        LAUNCHER_MESSAGE_BUS
            .exclusive()
            .try_send(&data)
            .expect("failed to send");
    }
}
