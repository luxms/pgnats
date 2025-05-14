use std::net::UdpSocket;

use pgrx::bgworkers::*;
use pgrx::prelude::*;

use crate::ctx::BgMessage;
use crate::{config::initialize_configuration, ctx::CTX};

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    initialize_configuration();

    BackgroundWorkerBuilder::new("Background Worker Subscribtion Listener")
        .set_function("background_worker_listener")
        .set_library("pgnats")
        .enable_spi_access()
        .load();

    unsafe {
        pg_sys::on_proc_exit(Some(extension_exit_callback), pg_sys::Datum::from(0));
    }
}

#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn background_worker_listener(_arg: pgrx::pg_sys::Datum) {
    log!("Starting background worker listener");

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    let Ok(udp) = UdpSocket::bind("0.0.0.0:52525") else {
        log!("Failed to create UDP socket");
        return;
    };
    if udp.set_nonblocking(true).is_err() {
        log!("Failed to set nonblocking");
    }

    while BackgroundWorker::wait_latch(Some(std::time::Duration::from_millis(250))) {
        let result = std::panic::catch_unwind(|| {
            let mut buf = [0u8; 2048];
            match udp.recv(&mut buf) {
                Ok(size) => {
                    let result: Result<(BgMessage, _), _> =
                        bincode::decode_from_slice(&buf[0..size], bincode::config::standard());

                    match result {
                        Ok((msg, _)) => {
                            let result: Result<(), pgrx::spi::Error> =
                                BackgroundWorker::transaction(|| {
                                    Spi::connect(|client| {
                                        let _ = client.select(
                                            &format!("SELECT {}($1)", msg.name),
                                            None,
                                            &[msg.data.to_vec().into()],
                                        )?;

                                        Ok(())
                                    })
                                });

                            if let Err(err) = result {
                                log!("Got an error in Background Worker: {err:?}");
                            }
                        }
                        Err(e) => log!("Got parse error: {}", e),
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => {
                    log!("Got UDP error: {}", e);
                }
            }
        });

        if let Err(err) = result {
            log!("Got panic: {:?}", err);
        }
    }

    log!("Stopping background worker listener");
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
