use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;

use futures::StreamExt;
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use tokio::net::UdpSocket;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use crate::config::GUC_SUB_DB_NAME;
use crate::connection::NatsConnectionOptions;
use crate::connection::NatsTlsOptions;
use crate::ctx::WorkerMessage;
use crate::log;

enum InternalWorkerMessage {
    Subscribe {
        opt: NatsConnectionOptions,
        subject: String,
        fn_name: String,
    },
    Unsubscribe {
        opt: NatsConnectionOptions,
        subject: String,
        fn_name: String,
    },
    CallbackCall {
        fn_name: String,
        data: Vec<u8>,
    },
}

struct NatsConnectionState {
    client: async_nats::Client,
    subscriptions: HashMap<String, NatsSubscription>,
}

struct NatsSubscription {
    handlers: HashMap<String, JoinHandle<()>>,
}

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_subscriber(_arg: pgrx::pg_sys::Datum) {
    log!("Starting background worker subscriber");

    let db_name = GUC_SUB_DB_NAME.get().and_then(|v| v.to_str().ok());
    log!("Background worker connected to {:?} database", db_name);

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(db_name, None);

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

    let mut subscriptions: HashMap<String, NatsConnectionState> = HashMap::new();
    let (msg_sender, msg_receiver) = channel();

    let udp_thread = spawn_udp_listener(&rt, udp, msg_sender.clone());

    while BackgroundWorker::wait_latch(Some(std::time::Duration::from_millis(250))) {
        while let Ok(message) = msg_receiver.try_recv() {
            match message {
                InternalWorkerMessage::Subscribe {
                    opt,
                    subject,
                    fn_name,
                } => {
                    handle_subscribe(&rt, &mut subscriptions, &msg_sender, opt, subject, fn_name);
                }
                InternalWorkerMessage::Unsubscribe {
                    opt,
                    subject,
                    fn_name,
                } => {
                    handle_unsubscribe(&mut subscriptions, &opt, &subject, &fn_name);
                }
                InternalWorkerMessage::CallbackCall { fn_name, data } => {
                    handle_callback(fn_name, data);
                }
            }
        }
    }

    udp_thread.abort();

    for (_, connection) in subscriptions {
        for (_, sub) in connection.subscriptions {
            for (_, func) in sub.handlers {
                func.abort();
            }
        }
    }

    log!("Stopping background worker listener");
}

fn handle_subscribe(
    rt: &Runtime,
    subs: &mut HashMap<String, NatsConnectionState>,
    msg_sender: &Sender<InternalWorkerMessage>,
    opt: NatsConnectionOptions,
    subject: String,
    fn_name: String,
) {
    let source = format!("{}:{}", opt.host, opt.port);

    match subs.entry(source.clone()) {
        // Reuse existing NATS connection
        Entry::Occupied(mut e) => {
            let connection = e.get_mut();
            let client = connection.client.clone();
            let func = fn_name.clone();

            // Spawn a new handler task for the function
            let handle = spawn_subscription_task(
                rt,
                client.clone(),
                subject.clone(),
                func.clone(),
                msg_sender.clone(),
                opt.clone(),
            );

            match connection.subscriptions.entry(subject.clone()) {
                // Subject already exists; update or add the function handler
                Entry::Occupied(mut se) => {
                    if let Some(old) = se.get_mut().handlers.insert(fn_name, handle) {
                        old.abort();
                    }
                }
                // First time subscribing to this subject
                Entry::Vacant(se) => {
                    let _ = se.insert(NatsSubscription {
                        handlers: HashMap::from([(fn_name, handle)]),
                    });
                }
            }
        }
        // Establish new NATS connection
        Entry::Vacant(e) => {
            let mut opts = async_nats::ConnectOptions::new().client_capacity(opt.capacity);

            if let Some(tls) = &opt.tls {
                if let Ok(root) = std::env::current_dir() {
                    match tls {
                        NatsTlsOptions::Tls { ca } => {
                            opts = opts.require_tls(true).add_root_certificates(root.join(ca));
                        }
                        NatsTlsOptions::MutualTls { ca, cert, key } => {
                            opts = opts
                                .require_tls(true)
                                .add_root_certificates(root.join(ca))
                                .add_client_certificate(root.join(cert), root.join(key));
                        }
                    }
                }
            }

            match rt.block_on(opts.connect(&source)) {
                Ok(connection) => {
                    let handle = spawn_subscription_task(
                        rt,
                        connection.clone(),
                        subject.clone(),
                        fn_name.clone(),
                        msg_sender.clone(),
                        opt.clone(),
                    );

                    let sub = NatsSubscription {
                        handlers: HashMap::from([(fn_name, handle)]),
                    };

                    let _ = e.insert(NatsConnectionState {
                        client: connection,
                        subscriptions: HashMap::from([(subject, sub)]),
                    });
                }
                Err(err) => {
                    log!("Failed to connect to NATS server at {}: {}", source, err);
                }
            }
        }
    }
}

fn handle_unsubscribe(
    subs: &mut HashMap<String, NatsConnectionState>,
    opt: &NatsConnectionOptions,
    subject: &str,
    fn_name: &str,
) {
    let source = format!("{}:{}", opt.host, opt.port);
    if let Some(connection) = subs.get_mut(&source) {
        if let Some(sub) = connection.subscriptions.get_mut(subject) {
            if let Some(handle) = sub.handlers.remove(fn_name) {
                handle.abort();
            }
        }
    }
}

fn handle_callback(fn_name: String, data: Vec<u8>) {
    let result: Result<spi::Result<()>, _> = std::panic::catch_unwind(|| {
        BackgroundWorker::transaction(|| {
            Spi::connect(|client| {
                let sql = format!("SELECT {}($1)", fn_name);
                let _ = client.select(&sql, None, &[data.into()])?;
                Ok(())
            })
        })
    });

    if let Err(panic) = result {
        log!("Panic during function '{}': {:?}", fn_name, panic);
    } else if let Ok(Err(e)) = result {
        log!("Error in SPI call '{}': {:?}", fn_name, e);
    }
}

fn spawn_subscription_task(
    rt: &Runtime,
    client: async_nats::Client,
    subject: String,
    fn_name: String,
    sender: Sender<InternalWorkerMessage>,
    opt: NatsConnectionOptions,
) -> JoinHandle<()> {
    rt.spawn(async move {
        match client.subscribe(subject.clone()).await {
            Ok(mut sub) => {
                while let Some(msg) = sub.next().await {
                    let _ = sender.send(InternalWorkerMessage::CallbackCall {
                        fn_name: fn_name.clone(),
                        data: msg.payload.to_vec(),
                    });
                }
            }
            Err(_) => {
                let _ = sender.send(InternalWorkerMessage::Unsubscribe {
                    opt,
                    subject,
                    fn_name,
                });
            }
        }
    })
}

fn spawn_udp_listener(
    rt: &Runtime,
    udp: UdpSocket,
    sender: Sender<InternalWorkerMessage>,
) -> JoinHandle<()> {
    rt.spawn(async move {
        let mut buf = [0u8; 2048];
        loop {
            match udp.recv(&mut buf).await {
                Ok(size) => {
                    let parse_result: Result<(WorkerMessage, _), _> =
                        bincode::decode_from_slice(&buf[..size], bincode::config::standard());
                    let msg = match parse_result {
                        Ok((msg, _)) => msg,
                        Err(_) => {
                            continue;
                        }
                    };

                    match msg {
                        WorkerMessage::Subscribe {
                            opt,
                            subject,
                            fn_name,
                        } => {
                            if sender
                                .send(InternalWorkerMessage::Subscribe {
                                    opt,
                                    subject,
                                    fn_name,
                                })
                                .is_err()
                            {
                                return;
                            }
                        }
                        WorkerMessage::Unsubscribe {
                            opt,
                            subject,
                            fn_name,
                        } => {
                            if sender
                                .send(InternalWorkerMessage::Unsubscribe {
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
                Err(_) => {}
            }
        }
    })
}
