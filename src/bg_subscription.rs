use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::{
        mpsc::{channel, Sender},
        Arc,
    },
};

use futures::StreamExt;
use pgrx::{bgworkers::*, pg_sys as sys, PgTryBuilder, Spi};
use tokio::task::JoinHandle;

use crate::{
    config::{fetch_connection_options, GUC_SUB_DB_NAME},
    connection::{NatsConnectionOptions, NatsTlsOptions},
    init::SUBSCRIPTIONS_TABLE_NAME,
    log,
    shared::{WorkerMessage, WORKER_MESSAGE_QUEUE},
};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WorkerState {
    Master,
    Slave,
}

pub enum InternalWorkerMessage {
    Subscribe {
        register: bool,
        subject: String,
        fn_name: String,
    },
    Unsubscribe {
        subject: Arc<str>,
        fn_name: Arc<str>,
    },
    CallbackCall {
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

pub struct WorkerContext {
    sender: Sender<InternalWorkerMessage>,
    nats_state: Option<NatsConnectionState>,
    state: WorkerState,
}

impl WorkerContext {
    pub fn new(sender: Sender<InternalWorkerMessage>, state: WorkerState) -> Result<Self, String> {
        Ok(Self {
            sender,
            nats_state: None,
            state,
        })
    }

    pub fn handle_subscribe(
        &mut self,
        rt: &tokio::runtime::Runtime,
        subject: Arc<str>,
        fn_name: Arc<str>,
    ) {
        if let Some(connection) = &mut self.nats_state {
            match connection.subscriptions.entry(subject.clone()) {
                // Subject already exists; update or add the function handler
                Entry::Occupied(mut s) => {
                    let _ = s.get_mut().funcs.insert(fn_name);
                }
                // First time subscribing to this subject
                Entry::Vacant(se) => {
                    let func = fn_name.clone();
                    // Spawn a new handler task for the function
                    let handler = Self::spawn_subscription_task(
                        rt,
                        connection.client.clone(),
                        subject.clone(),
                        func,
                        self.sender.clone(),
                    );

                    let _ = se.insert(NatsSubscription {
                        handler,
                        funcs: HashSet::from([fn_name]),
                    });
                }
            }
        } else {
            log!("Can not subscribe, because connection is not established");
        }
    }

    pub fn handle_unsubscribe(&mut self, subject: Arc<str>, fn_name: &str) {
        if let Some(nats_state) = &mut self.nats_state {
            if let Entry::Occupied(mut e) = nats_state.subscriptions.entry(subject.clone()) {
                let _ = e.get_mut().funcs.remove(fn_name);

                if e.get().funcs.is_empty() {
                    let sub = e.remove();
                    sub.handler.abort();
                }
            }
        }
    }

    pub fn handle_callback(&self, subject: &str, data: Arc<[u8]>, callback: impl Fn(&str, &[u8])) {
        if let Some(nats_state) = &self.nats_state {
            if let Some(subject) = nats_state.subscriptions.get(subject) {
                for fnname in subject.funcs.iter() {
                    callback(fnname, &data);
                }
            }
        }
    }
}

impl WorkerContext {
    fn spawn_subscription_task(
        rt: &tokio::runtime::Runtime,
        client: async_nats::Client,
        subject: Arc<str>,
        fn_name: Arc<str>,
        sender: Sender<InternalWorkerMessage>,
    ) -> JoinHandle<()> {
        rt.spawn(async move {
            match client.subscribe(subject.to_string()).await {
                Ok(mut sub) => {
                    while let Some(msg) = sub.next().await {
                        let _ = sender.send(InternalWorkerMessage::CallbackCall {
                            subject: subject.clone(),
                            data: Arc::from(msg.payload.to_vec()),
                        });
                    }
                }
                Err(_) => {
                    let _ = sender.send(InternalWorkerMessage::Unsubscribe { subject, fn_name });
                }
            }
        })
    }
}

impl Drop for NatsConnectionState {
    fn drop(&mut self) {
        for (_, sub) in std::mem::take(&mut self.subscriptions) {
            sub.handler.abort();
        }
    }
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

    let (msg_sender, msg_receiver) = channel();
    let mut worker_context = match WorkerContext::new(
        msg_sender.clone(),
        if unsafe { sys::RecoveryInProgress() } {
            WorkerState::Master
        } else {
            WorkerState::Slave
        },
    ) {
        Ok(sock) => sock,
        Err(e) => {
            log!("{}", e);
            return;
        }
    };

    let Some(db_name) = GUC_SUB_DB_NAME.get() else {
        log!("nats.sub_dbname is NULL");
        return;
    };
    let Ok(db_name) = db_name.into_string() else {
        log!("nats.sub_dbname contains non character symbols");
        return;
    };

    BackgroundWorker::connect_worker_to_spi(Some(&db_name), None);
    log!("Background worker connected to '{}' database", db_name);

    if worker_context.state == WorkerState::Master {
        let opt = BackgroundWorker::transaction(fetch_connection_options);
        restore_state(&rt, msg_sender.clone(), &mut worker_context, opt);
    }

    while BackgroundWorker::wait_latch(Some(std::time::Duration::from_secs(1))) {
        // Cache result
        let is_slave = unsafe { sys::RecoveryInProgress() };

        match (is_slave, worker_context.state) {
            (true, WorkerState::Master) => {
                worker_context.state = WorkerState::Slave;
                let _ = worker_context.nats_state.take();
            }
            (false, WorkerState::Slave) => {
                let opt = BackgroundWorker::transaction(fetch_connection_options);
                restore_state(&rt, msg_sender.clone(), &mut worker_context, opt);
            }
            _ => { /* NO OP */ }
        }

        {
            let mut deq = WORKER_MESSAGE_QUEUE.exclusive();

            while let Some(buf) = deq.try_recv() {
                log!(
                    "Got msg from shared queue: {:?}",
                    String::from_utf8_lossy(&buf)
                );
                let parse_result: Result<(WorkerMessage, _), _> =
                    bincode::decode_from_slice(&buf[..], bincode::config::standard());
                let msg = match parse_result {
                    Ok((msg, _)) => msg,
                    Err(_) => {
                        continue;
                    }
                };

                match msg {
                    WorkerMessage::Subscribe { subject, fn_name } => {
                        if msg_sender
                            .send(InternalWorkerMessage::Subscribe {
                                register: true,
                                subject: subject.to_string(),
                                fn_name: fn_name.to_string(),
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                    WorkerMessage::Unsubscribe { subject, fn_name } => {
                        if msg_sender
                            .send(InternalWorkerMessage::Unsubscribe {
                                subject: Arc::from(subject.as_str()),
                                fn_name: Arc::from(fn_name.as_str()),
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                    WorkerMessage::NewConnectionConfig(opt) => {
                        restore_state(&rt, msg_sender.clone(), &mut worker_context, opt);
                    }
                }
            }
        }

        while let Ok(message) = msg_receiver.try_recv() {
            if is_slave {
                continue;
            }

            match message {
                InternalWorkerMessage::Subscribe {
                    register,
                    subject,
                    fn_name,
                } => {
                    log!(
                        "Received subscription: subject='{}', fn='{}'",
                        subject,
                        fn_name
                    );

                    if register {
                        if let Err(error) = insert_subject_callback(&subject, &fn_name) {
                            log!(
                                "Got an error while subscribing from subject '{}' and callback '{}': {}",
                                subject,
                                fn_name,
                                error,
                            );
                        }
                    }

                    worker_context.handle_subscribe(&rt, Arc::from(subject), Arc::from(fn_name));
                }
                InternalWorkerMessage::Unsubscribe { subject, fn_name } => {
                    log!(
                        "Received unsubscription: subject='{}', fn='{}'",
                        subject,
                        fn_name
                    );

                    if let Err(error) = delete_subject_callback(&subject, &fn_name) {
                        log!(
                            "Got an error while unsubscribing from subject '{}' and callback '{}': {}",
                            subject,
                            fn_name,
                            error,
                        );
                    }

                    worker_context.handle_unsubscribe(subject.clone(), &fn_name);
                }
                InternalWorkerMessage::CallbackCall { subject, data } => {
                    log!("Received callback for subject '{}", subject,);
                    worker_context.handle_callback(&subject, data, |callback, data| {
                        let result = BackgroundWorker::transaction(|| {
                            PgTryBuilder::new(|| {
                                Spi::connect_mut(|client| {
                                    let sql = format!("SELECT {}($1)", callback);
                                    let _ = client
                                        .update(&sql, None, &[data.into()])
                                        .map_err(|e| e.to_string())?;
                                    Ok(())
                                })
                            })
                            .catch_others(|e| match e {
                                sys::panic::CaughtError::PostgresError(err) => Err(format!(
                                    "Code '{}': {}. ({:?})",
                                    err.sql_error_code(),
                                    err.message(),
                                    err.hint()
                                )),
                                _ => Err(format!("{:?}", e)),
                            })
                            .execute()
                        });

                        if let Err(err) = result {
                            log!("Error in SPI call '{}': {:?}", callback, err);
                        }
                    });
                }
            }
        }
    }

    log!("Stopping background worker listener");
}

fn fetch_subject_with_callbacks() -> Result<Vec<(String, String)>, String> {
    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            Spi::connect_mut(|client| {
                let sql = format!("SELECT subject, callback FROM {}", SUBSCRIPTIONS_TABLE_NAME);
                let tuples = client.select(&sql, None, &[]).map_err(|e| e.to_string())?;
                let subject_callbacks = tuples
                    .into_iter()
                    .filter_map(|tuple| {
                        let subject = tuple.get_by_name::<String, _>("subject");
                        let callback = tuple.get_by_name::<String, _>("callback");

                        match (subject, callback) {
                            (Ok(Some(subject)), Ok(Some(callback))) => Some((subject, callback)),
                            _ => None,
                        }
                    })
                    .collect();
                Ok(subject_callbacks)
            })
        })
        .catch_others(|e| match e {
            sys::panic::CaughtError::PostgresError(err) => Err(format!(
                "Code '{}': {}. ({:?})",
                err.sql_error_code(),
                err.message(),
                err.hint()
            )),
            _ => Err(format!("{:?}", e)),
        })
        .execute()
    })
}

fn insert_subject_callback(subject: &str, callback: &str) -> Result<(), String> {
    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            Spi::connect_mut(|client| {
                let sql = format!("INSERT INTO {} VALUES ($1, $2)", SUBSCRIPTIONS_TABLE_NAME);
                let _ = client
                    .update(&sql, None, &[subject.into(), callback.into()])
                    .map_err(|e| e.to_string())?;
                Ok(())
            })
        })
        .catch_others(|e| match e {
            sys::panic::CaughtError::PostgresError(err) => Err(format!(
                "Code '{}': {}. ({:?})",
                err.sql_error_code(),
                err.message(),
                err.hint()
            )),
            _ => Err(format!("{:?}", e)),
        })
        .execute()
    })
}

fn delete_subject_callback(subject: &str, callback: &str) -> Result<(), String> {
    BackgroundWorker::transaction(|| {
        PgTryBuilder::new(|| {
            Spi::connect_mut(|client| {
                let sql = format!(
                    "DELETE FROM {} WHERE subject = $1 AND callback = $2",
                    SUBSCRIPTIONS_TABLE_NAME
                );
                let _ = client
                    .update(&sql, None, &[subject.into(), callback.into()])
                    .map_err(|e| e.to_string())?;
                Ok(())
            })
        })
        .catch_others(|e| match e {
            sys::panic::CaughtError::PostgresError(err) => Err(format!(
                "Code '{}': {}. ({:?})",
                err.sql_error_code(),
                err.message(),
                err.hint()
            )),
            _ => Err(format!("{:?}", e)),
        })
        .execute()
    })
}

async fn connect_nats(opt: &NatsConnectionOptions) -> Option<async_nats::Client> {
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

    opts.connect(format!("{}:{}", opt.host, opt.port))
        .await
        .ok()
}

fn restore_state(
    rt: &tokio::runtime::Runtime,
    sender: Sender<InternalWorkerMessage>,
    worker_context: &mut WorkerContext,
    opt: NatsConnectionOptions,
) {
    if let Some(client) = rt.block_on(connect_nats(&opt)) {
        worker_context.nats_state = Some(NatsConnectionState {
            client,
            subscriptions: HashMap::new(),
        });

        match fetch_subject_with_callbacks() {
            Ok(subscriptions) => {
                log!("Registered {} callbacks", subscriptions.len());

                for (subject, fn_name) in subscriptions {
                    let _ = sender.send(InternalWorkerMessage::Subscribe {
                        register: false,
                        subject,
                        fn_name,
                    });
                }
            }
            Err(err) => {
                log!("Failed to fetch subscriptions: {err}");
            }
        }
    } else {
        log!("Failed to connect to NATS, options: {:?}", opt);
    }
}
