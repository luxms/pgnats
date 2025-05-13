use crate::{
    config::initialize_configuration,
    ctx::{CTX, SUBSCRIBTION_BRIDGE},
};
use pgrx::{
    bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags},
    prelude::*,
};

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    initialize_configuration();

    /*BackgroundWorkerBuilder::new("Background Worker Subscribtion Listener")
    .set_function("background_worker_listener")
    .set_library("bgworker")
    .enable_spi_access()
    .load();*/

    unsafe {
        pg_sys::on_proc_exit(Some(extension_exit_callback), pg_sys::Datum::from(0));
    }
}

#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn background_worker_listener(_arg: pgrx::pg_sys::Datum) {
    info!("Starting background worker listener");

    let recv = SUBSCRIBTION_BRIDGE
        .recv
        .lock()
        .unwrap()
        .take()
        .expect("failed to get receiver");

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(None, None);

    while let Ok((fn_name, data)) = recv.recv() {
        info!("Got message: {fn_name}");
        Spi::connect(|client| {
            let result = client.select(&format!("SELECT {fn_name}($1)"), None, &[data.into()]);

            if let Err(err) = result {
                info!("Got an error in Background Worker: {err:?}");
            }
        });
    }

    info!("Stopping background worker listener");
}

#[pg_guard]
pub extern "C-unwind" fn _PG_fini() {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.invalidate_connection());
    });
}

unsafe extern "C-unwind" fn extension_exit_callback(_: i32, _: pg_sys::Datum) {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.invalidate_connection());
    });
}
