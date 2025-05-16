use std::net::UdpSocket;

use pgrx::bgworkers::*;
use pgrx::prelude::*;

use crate::config::GUC_SUB_DB_NAME;
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
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_listener(_arg: pgrx::pg_sys::Datum) {
    log!("Starting background worker listener");

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(
        GUC_SUB_DB_NAME.get().and_then(|v| v.to_str().ok()),
        None,
    );

    let udp = match UdpSocket::bind("0.0.0.0:52525") {
        Ok(sock) => sock,
        Err(e) => {
            log!("Failed to bind UDP socket: {}", e);
            return;
        }
    };

    if let Err(e) = udp.set_nonblocking(true) {
        log!("Failed to set UDP socket to non-blocking: {}", e);
    }

    while BackgroundWorker::wait_latch(Some(std::time::Duration::from_millis(250))) {
        let mut buf = [0u8; 2048];

        match udp.recv(&mut buf) {
            Ok(size) => {
                let parse_result: Result<(BgMessage, _), _> =
                    bincode::decode_from_slice(&buf[..size], bincode::config::standard());

                match parse_result {
                    Ok((msg, _)) => {
                        let transaction_result: Result<spi::Result<()>, _> =
                            std::panic::catch_unwind(|| {
                                BackgroundWorker::transaction(|| {
                                    Spi::connect(|client| {
                                        let sql = format!("SELECT {}($1)", msg.name);
                                        let _ = client.select(
                                            &sql,
                                            None,
                                            &[msg.data.to_vec().into()],
                                        )?;
                                        Ok(())
                                    })
                                })
                            });

                        match transaction_result {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                log!("SPI transaction failed for message '{}': {:?}", msg.name, e);
                            }
                            Err(panic) => {
                                log!(
                                    "Panic occurred during transaction execution function '{}': {:?}",
                                    msg.name,
                                    panic // Message handling
                                );
                            }
                        }
                    }
                    Err(e) => {
                        log!("Failed to parse incoming message: {}", e);
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No data; continue waiting
            }
            Err(e) => {
                log!("Error receiving UDP packet: {}", e);
            }
        }
    }

    log!("Stopping background worker listener");
}

#[pg_guard]
pub extern "C-unwind" fn _PG_fini() {
    CTX.with_borrow_mut(|ctx| {
        ctx.rt.block_on(async {
            let res = ctx.nats_connection.invalidate_connection().await;
            tokio::task::yield_now().await;
            res
        })
    })
}

unsafe extern "C-unwind" fn extension_exit_callback(_: i32, _: pg_sys::Datum) {
    CTX.with_borrow_mut(|ctx| {
        ctx.rt.block_on(async {
            let res = ctx.nats_connection.invalidate_connection().await;
            tokio::task::yield_now().await;
            res
        })
    })
}
