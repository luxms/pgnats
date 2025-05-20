use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use futures::StreamExt;
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use tokio::net::UdpSocket;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use crate::connection::NatsConnectionOptions;
use crate::connection::NatsTlsOptions;
use crate::ctx::WorkerMessage;
use crate::log;

enum InternalWorkerMessage {
    Subscribe {
        dbname: Option<String>,
        opt: NatsConnectionOptions,
        subject: String,
        fn_name: String,
    },
    Unsubscribe {
        dbname: Option<String>,
        opt: NatsConnectionOptions,
        subject: Arc<str>,
        fn_name: Arc<str>,
    },
    CallbackCall {
        client: Arc<str>,
        subject: Arc<str>,
        data: Arc<[u8]>,
    },
}

struct NatsConnectionState {
    client: async_nats::Client,
    subscriptions: HashMap<Arc<str>, NatsSubscription>,
}

struct NatsSubscription {
    handler: JoinHandle<()>,
    funcs: HashSet<Arc<str>>,
}

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_subscriber(_arg: pgrx::pg_sys::Datum) {
    log!("Starting background worker subscriber");

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

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

    log!("Tokio runtime initialized");

    let udp = match rt.block_on(UdpSocket::bind("127.0.0.1:52525")) {
        Ok(sock) => sock,
        Err(e) => {
            log!("Failed to bind UDP socket: {}", e);
            return;
        }
    };

    log!("UDP socket bound to 127.0.0.1:52525");

    let mut subscriptions = HashMap::new();
    let (msg_sender, msg_receiver) = channel();

    let udp_thread = spawn_udp_listener(&rt, udp, msg_sender.clone());

    let mut db_name = None;

    while BackgroundWorker::wait_latch(Some(std::time::Duration::from_millis(250))) {
        while let Ok(message) = msg_receiver.try_recv() {
            match message {
                InternalWorkerMessage::Subscribe {
                    dbname,
                    opt,
                    subject,
                    fn_name,
                } => {
                    log!(
                        "Received subscription: subject='{}', fn='{}'",
                        subject,
                        fn_name
                    );
                    if let Some(dbname) = dbname {
                        connect_to_database(&mut db_name, dbname);
                    }
                    handle_subscribe(
                        &rt,
                        &mut subscriptions,
                        &msg_sender,
                        opt,
                        Arc::from(subject),
                        Arc::from(fn_name),
                    );
                }
                InternalWorkerMessage::Unsubscribe {
                    dbname,
                    opt,
                    subject,
                    fn_name,
                } => {
                    log!(
                        "Received unsubscription: subject='{}', fn='{}'",
                        subject,
                        fn_name
                    );
                    if let Some(dbname) = dbname {
                        connect_to_database(&mut db_name, dbname);
                    }
                    handle_unsubscribe(&mut subscriptions, &opt, subject.clone(), &fn_name);
                }
                InternalWorkerMessage::CallbackCall {
                    client,
                    subject,
                    data,
                } => {
                    log!(
                        "Received callback for subject '{}' (client='{}')",
                        subject,
                        client
                    );
                    handle_callback(&subscriptions, &client, &subject, data);
                }
            }
        }
    }

    udp_thread.abort();

    for (_, connection) in subscriptions {
        for (_, sub) in connection.subscriptions {
            sub.handler.abort();
        }
    }

    log!("Stopping background worker listener");
}

#[cold]
fn connect_to_database(src: &mut Option<String>, dst: String) {
    if src.is_some() {
        return;
    }

    BackgroundWorker::connect_worker_to_spi(Some(&dst), None);
    log!("Background worker connected to '{}' database", dst);
    *src = Some(dst);
}

fn handle_subscribe(
    rt: &Runtime,
    subs: &mut HashMap<Arc<str>, NatsConnectionState>,
    msg_sender: &Sender<InternalWorkerMessage>,
    opt: NatsConnectionOptions,
    subject: Arc<str>,
    fn_name: Arc<str>,
) {
    let source: Arc<str> = Arc::from(format!("{}:{}", opt.host, opt.port));

    match subs.entry(source.clone()) {
        // Reuse existing NATS connection
        Entry::Occupied(mut e) => {
            let connection = e.get_mut();
            let client = connection.client.clone();

            match connection.subscriptions.entry(subject.clone()) {
                // Subject already exists; update or add the function handler
                Entry::Occupied(mut s) => {
                    log!(
                        "Adding function '{}' to existing subject '{}'",
                        fn_name,
                        subject
                    );
                    let _ = s.get_mut().funcs.insert(fn_name);
                }
                // First time subscribing to this subject
                Entry::Vacant(se) => {
                    log!(
                        "Subscribing new subject '{}' with function `{}`",
                        subject,
                        fn_name
                    );
                    let func = fn_name.clone();
                    // Spawn a new handler task for the function
                    let handler = spawn_subscription_task(
                        rt,
                        client.clone(),
                        source,
                        subject.clone(),
                        func,
                        msg_sender.clone(),
                        opt,
                    );

                    let _ = se.insert(NatsSubscription {
                        handler,
                        funcs: HashSet::from([fn_name]),
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

            match rt.block_on(opts.connect(&**e.key())) {
                Ok(connection) => {
                    log!("Connected to NATS server '{}'", e.key());
                    log!(
                        "Subscribing new subject '{}' with function '{}'`",
                        subject,
                        fn_name
                    );

                    let handler = spawn_subscription_task(
                        rt,
                        connection.clone(),
                        source,
                        subject.clone(),
                        fn_name.clone(),
                        msg_sender.clone(),
                        opt,
                    );

                    let sub = NatsSubscription {
                        handler,
                        funcs: HashSet::from([fn_name]),
                    };

                    let _ = e.insert(NatsConnectionState {
                        client: connection,
                        subscriptions: HashMap::from([(subject, sub)]),
                    });
                }
                Err(err) => {
                    log!("Failed to connect to NATS server at {}: {}", e.key(), err);
                }
            }
        }
    }
}

fn handle_unsubscribe(
    subs: &mut HashMap<Arc<str>, NatsConnectionState>,
    opt: &NatsConnectionOptions,
    subject: Arc<str>,
    fn_name: &str,
) {
    let source = Arc::from(format!("{}:{}", opt.host, opt.port));
    if let Some(connection) = subs.get_mut(&source) {
        if let Entry::Occupied(mut e) = connection.subscriptions.entry(subject.clone()) {
            let _ = e.get_mut().funcs.remove(fn_name);

            if e.get().funcs.is_empty() {
                log!(
                    "No functions left; aborting handler for subject '{}'",
                    subject
                );
                let sub = e.remove();
                sub.handler.abort();
            }
        }
    }
}

fn handle_callback(
    subs: &HashMap<Arc<str>, NatsConnectionState>,
    client_key: &str,
    subject: &str,
    data: Arc<[u8]>,
) {
    if let Some(subjects) = subs.get(client_key) {
        if let Some(subject) = subjects.subscriptions.get(subject) {
            for callback in subject.funcs.iter() {
                let result = PgTryBuilder::new(|| {
                    BackgroundWorker::transaction(|| {
                        Spi::connect(|client| {
                            let sql = format!("SELECT {}($1)", callback);
                            let data = &*data;
                            let _ = client
                                .select(&sql, None, &[data.into()])
                                .map_err(|e| e.to_string())?;
                            Ok(())
                        })
                    })
                })
                .catch_others(|e| match e {
                    pg_sys::panic::CaughtError::PostgresError(err) => Err(format!(
                        "Code '{}': {}. ({:?})",
                        err.sql_error_code(),
                        err.message(),
                        err.hint()
                    )),
                    _ => Err(format!("{:?}", e)),
                })
                .execute();

                if let Err(err) = result {
                    log!("Error in SPI call '{}': {:?}", callback, err);
                }
            }
        }
    }
}

fn spawn_subscription_task(
    rt: &Runtime,
    client: async_nats::Client,
    client_key: Arc<str>,
    subject: Arc<str>,
    fn_name: Arc<str>,
    sender: Sender<InternalWorkerMessage>,
    opt: NatsConnectionOptions,
) -> JoinHandle<()> {
    rt.spawn(async move {
        match client.subscribe(subject.to_string()).await {
            Ok(mut sub) => {
                while let Some(msg) = sub.next().await {
                    let _ = sender.send(InternalWorkerMessage::CallbackCall {
                        client: client_key.clone(),
                        subject: subject.clone(),
                        data: Arc::from(msg.payload.to_vec()),
                    });
                }
            }
            Err(_) => {
                let _ = sender.send(InternalWorkerMessage::Unsubscribe {
                    dbname: None,
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
            if let Ok(size) = udp.recv(&mut buf).await {
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
                        dbname,
                        opt,
                        subject,
                        fn_name,
                    } => {
                        if sender
                            .send(InternalWorkerMessage::Subscribe {
                                dbname: Some(dbname),
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
                        dbname,
                        opt,
                        subject,
                        fn_name,
                    } => {
                        if sender
                            .send(InternalWorkerMessage::Unsubscribe {
                                dbname: Some(dbname),
                                opt,
                                subject: Arc::from(subject),
                                fn_name: Arc::from(fn_name),
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            }
        }
    })
}
