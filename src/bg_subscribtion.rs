use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::mpsc::channel;

use futures::StreamExt;
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

use crate::config::GUC_SUB_DB_NAME;
use crate::connection::ConnectionOptions;
use crate::connection::TlsOptions;
use crate::ctx::BgMessage;

enum BgInternalMessage {
    Subscribe {
        opt: ConnectionOptions,
        subject: String,
        fn_name: String,
    },
    Unsubscribe {
        opt: ConnectionOptions,
        subject: String,
        fn_name: String,
    },
    CallbackCall {
        fn_name: String,
        data: Vec<u8>,
    },
}

struct Connection {
    connection: async_nats::Client,
    suscribtions: HashMap<String, Subscribtion>,
}

struct Subscribtion {
    connected_funcs: HashMap<String, JoinHandle<()>>,
}

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_subscriber(_arg: pgrx::pg_sys::Datum) {
    log!("Starting background worker subscriber");

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(
        GUC_SUB_DB_NAME.get().and_then(|v| v.to_str().ok()),
        None,
    );

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            log!("Failed to create tokio multi-threaded runtime: {}", e);
            return;
        }
    };

    let udp = match rt.block_on(UdpSocket::bind("127.0.0.1:52525")) {
        Ok(sock) => sock,
        Err(e) => {
            log!("Failed to bind UDP socket: {}", e);
            return;
        }
    };

    let mut subs: HashMap<String, Connection> = HashMap::new();
    let (msg_sender, msg_receiver) = channel();

    let sender = msg_sender.clone();
    let udp_thread = rt.spawn(async move {
        let mut buf = [0u8; 2048];
        loop {
            match udp.recv(&mut buf).await {
                Ok(size) => {
                    let parse_result: Result<(BgMessage, _), _> =
                        bincode::decode_from_slice(&buf[..size], bincode::config::standard());
                    let msg = match parse_result {
                        Ok((msg, _)) => msg,
                        Err(e) => {
                            log!("Failed to parse incoming message: {}", e);
                            continue;
                        }
                    };

                    match msg {
                        BgMessage::Subscribe {
                            opt,
                            subject,
                            fn_name,
                        } => {
                            if sender
                                .send(BgInternalMessage::Subscribe {
                                    opt,
                                    subject,
                                    fn_name,
                                })
                                .is_err()
                            {
                                return;
                            }
                        }
                        BgMessage::Unsubscribe {
                            opt,
                            subject,
                            fn_name,
                        } => {
                            if sender
                                .send(BgInternalMessage::Unsubscribe {
                                    opt,
                                    subject,
                                    fn_name,
                                })
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
                Err(e) => {
                    log!("Error receiving UDP packet: {}", e);
                }
            }
        }
    });

    while BackgroundWorker::wait_latch(Some(std::time::Duration::from_millis(250))) {
        while let Ok(message) = msg_receiver.try_recv() {
            match message {
                BgInternalMessage::Subscribe {
                    opt,
                    subject,
                    fn_name,
                } => {
                    let source = format!("{}:{}", opt.host, opt.port);

                    match subs.entry(source) {
                        Entry::Occupied(mut e) => {
                            let connection = e.get_mut();
                            let client = connection.connection.clone();

                            match connection.suscribtions.entry(subject.clone()) {
                                Entry::Occupied(mut e) => {
                                    let sender = msg_sender.clone();

                                    let func = fn_name.clone();
                                    let handle = rt.spawn(async move {
                                        let Ok(mut sub) = client.subscribe(subject.clone()).await
                                        else {
                                            let _ = sender.send(BgInternalMessage::Unsubscribe {
                                                opt,
                                                subject,
                                                fn_name: func,
                                            });
                                            return;
                                        };

                                        while let Some(message) = sub.next().await {
                                            let _ = sender.send(BgInternalMessage::CallbackCall {
                                                fn_name: func.clone(),
                                                data: message.payload.to_vec(),
                                            });
                                        }
                                    });

                                    if let Some(thread) =
                                        e.get_mut().connected_funcs.insert(fn_name, handle)
                                    {
                                        thread.abort();
                                    }
                                }
                                Entry::Vacant(e) => {
                                    let sender = msg_sender.clone();

                                    let func = fn_name.clone();
                                    let handle = rt.spawn(async move {
                                        let Ok(mut sub) = client.subscribe(subject.clone()).await
                                        else {
                                            let _ = sender.send(BgInternalMessage::Unsubscribe {
                                                opt,
                                                subject,
                                                fn_name: func,
                                            });
                                            return;
                                        };

                                        while let Some(message) = sub.next().await {
                                            let _ = sender.send(BgInternalMessage::CallbackCall {
                                                fn_name: func.clone(),
                                                data: message.payload.to_vec(),
                                            });
                                        }
                                    });

                                    let _ = e.insert(Subscribtion {
                                        connected_funcs: HashMap::from([(fn_name, handle)]),
                                    });
                                }
                            }
                        }
                        Entry::Vacant(e) => {
                            let mut opts =
                                async_nats::ConnectOptions::new().client_capacity(opt.capacity);

                            if let Some(tls) = &opt.tls {
                                if let Ok(root) = std::env::current_dir() {
                                    match tls {
                                        TlsOptions::Tls { ca } => {
                                            info!(
                                                "Trying to find CA cert in '{:?}'",
                                                root.join(ca)
                                            );
                                            opts = opts
                                                .require_tls(true)
                                                .add_root_certificates(root.join(ca))
                                        }
                                        TlsOptions::MutualTls { ca, cert, key } => {
                                            info!(
                                                "Trying to find CA cert in '{:?}', cert in '{:?}' and key in '{:?}'",
                                                root.join(ca),
                                                root.join(cert),
                                                root.join(key)
                                            );
                                            opts = opts
                                                .require_tls(true)
                                                .add_root_certificates(root.join(ca))
                                                .add_client_certificate(
                                                    root.join(cert),
                                                    root.join(key),
                                                );
                                        }
                                    }
                                }
                            }

                            let Ok(connection) = rt.block_on(opts.connect(e.key())) else {
                                continue;
                            };
                            let sender = msg_sender.clone();

                            let func = fn_name.clone();
                            let client = connection.clone();
                            let sub = subject.clone();
                            let handle = rt.spawn(async move {
                                let Ok(mut sub) = client.subscribe(sub.clone()).await else {
                                    let _ = sender.send(BgInternalMessage::Unsubscribe {
                                        opt,
                                        subject: sub.clone(),
                                        fn_name: func,
                                    });
                                    return;
                                };

                                while let Some(message) = sub.next().await {
                                    let _ = sender.send(BgInternalMessage::CallbackCall {
                                        fn_name: func.clone(),
                                        data: message.payload.to_vec(),
                                    });
                                }
                            });

                            let suscribtions = Subscribtion {
                                connected_funcs: HashMap::from([(fn_name, handle)]),
                            };
                            let connection = Connection {
                                connection,
                                suscribtions: HashMap::from([(subject, suscribtions)]),
                            };

                            let _ = e.insert(connection);
                        }
                    }
                }
                BgInternalMessage::Unsubscribe {
                    opt,
                    subject,
                    fn_name,
                } => {
                    let source = format!("{}:{}", opt.host, opt.port);
                    if let Some(connection) = subs.get_mut(&source) {
                        if let Some(subscrition) = connection.suscribtions.get_mut(&subject) {
                            if let Some(thread) = subscrition.connected_funcs.remove(&fn_name) {
                                thread.abort();
                            }
                        }
                    }
                }
                BgInternalMessage::CallbackCall { fn_name, data } => {
                    let transaction_result: Result<spi::Result<()>, _> =
                        std::panic::catch_unwind(|| {
                            BackgroundWorker::transaction(|| {
                                Spi::connect(|client| {
                                    let sql = format!("SELECT {}($1)", fn_name);
                                    let _ = client.select(&sql, None, &[data.to_vec().into()])?;
                                    Ok(())
                                })
                            })
                        });

                    match transaction_result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            log!("SPI transaction failed for message '{}': {:?}", fn_name, e);
                        }
                        Err(panic) => {
                            log!(
                                "Panic occurred during transaction execution function '{}': {:?}",
                                fn_name,
                                panic // Message handling
                            );
                        }
                    }
                }
            }
        }
    }

    udp_thread.abort();

    for (_, connection) in subs {
        for (_, sub) in connection.suscribtions {
            for (_, func) in sub.connected_funcs {
                func.abort();
            }
        }
    }

    log!("Stopping background worker listener");
}
