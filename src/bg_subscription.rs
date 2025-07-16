use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use futures::StreamExt;
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use tokio::task::JoinHandle;

use crate::connection::NatsConnectionOptions;
use crate::connection::NatsTlsOptions;
use crate::init::SUBSCRIPTIONS_TABLE_NAME;
use crate::log;
use crate::shared::WorkerMessage;
use crate::shared::WORKER_MESSAGE_QUEUE;

pub enum InternalWorkerMessage {
    Subscribe {
        opt: NatsConnectionOptions,
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
}

impl WorkerContext {
    pub fn new(sender: Sender<InternalWorkerMessage>) -> Result<Self, String> {
        Ok(Self {
            sender,
            nats_state: None,
        })
    }

    pub async fn handle_subscribe(
        &mut self,
        opt: NatsConnectionOptions,
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
                        connection.client.clone(),
                        subject.clone(),
                        func,
                        self.sender.clone(),
                    )
                    .await;

                    let _ = se.insert(NatsSubscription {
                        handler,
                        funcs: HashSet::from([fn_name]),
                    });
                }
            }
        } else {
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

            if let Ok(connection) = opts.connect(format!("{}:{}", opt.host, opt.port)).await {
                let handler = Self::spawn_subscription_task(
                    connection.clone(),
                    subject.clone(),
                    fn_name.clone(),
                    self.sender.clone(),
                )
                .await;

                let sub = NatsSubscription {
                    handler,
                    funcs: HashSet::from([fn_name]),
                };

                self.nats_state = Some(NatsConnectionState {
                    client: connection,
                    subscriptions: HashMap::from([(subject, sub)]),
                });
            }
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
    async fn spawn_subscription_task(
        client: async_nats::Client,
        subject: Arc<str>,
        fn_name: Arc<str>,
        sender: Sender<InternalWorkerMessage>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
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

impl Drop for WorkerContext {
    fn drop(&mut self) {
        if let Some(mut nats_state) = self.nats_state.take() {
            for (_, sub) in std::mem::take(&mut nats_state.subscriptions) {
                sub.handler.abort();
            }
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
    let mut worker_context = match WorkerContext::new(msg_sender.clone()) {
        Ok(sock) => sock,
        Err(e) => {
            log!("{}", e);
            return;
        }
    };

    let db_name = std::env::var("PGNATS_SUB_DBNAME").unwrap_or("mi".to_string());
    BackgroundWorker::connect_worker_to_spi(Some(&db_name), None);
    log!("Background worker connected to '{}' database", db_name);

    while BackgroundWorker::wait_latch(Some(std::time::Duration::from_secs(1))) {
        // Cache result
        let is_slave = unsafe { pg_sys::RecoveryInProgress() };

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
                    WorkerMessage::Subscribe {
                        opt,
                        subject,
                        fn_name,
                    } => {
                        if msg_sender
                            .send(InternalWorkerMessage::Subscribe {
                                opt,
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
                    WorkerMessage::NewConnectionConfig(cfg) => {
                        log!("Got new connection config: {:?}", cfg)
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
                    opt,
                    subject,
                    fn_name,
                } => {
                    log!(
                        "Received subscription: subject='{}', fn='{}'",
                        subject,
                        fn_name
                    );

                    if let Err(error) = insert_subject_callback(&subject, &fn_name) {
                        log!(
                            "Got an error while subscribing from subject '{}' and callback '{}': {}",
                            subject,
                            fn_name,
                            error,
                        );
                    }

                    rt.block_on(worker_context.handle_subscribe(
                        opt,
                        Arc::from(subject),
                        Arc::from(fn_name),
                    ));
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
                        let result = PgTryBuilder::new(|| {
                            BackgroundWorker::transaction(|| {
                                Spi::connect_mut(|client| {
                                    let sql = format!("SELECT {}($1)", callback);
                                    let _ = client
                                        .update(&sql, None, &[data.into()])
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
                    });
                }
            }
        }
    }

    log!("Stopping background worker listener");
}

fn _fetch_subject_with_callbacks() -> Result<Vec<(String, String)>, String> {
    PgTryBuilder::new(|| {
        BackgroundWorker::transaction(|| {
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
    .execute()
}

fn insert_subject_callback(subject: &str, callback: &str) -> Result<(), String> {
    PgTryBuilder::new(|| {
        BackgroundWorker::transaction(|| {
            Spi::connect_mut(|client| {
                let sql = format!("INSERT INTO {} VALUES ($1, $2)", SUBSCRIPTIONS_TABLE_NAME);
                let _ = client
                    .update(&sql, None, &[subject.into(), callback.into()])
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
    .execute()
}

fn delete_subject_callback(subject: &str, callback: &str) -> Result<(), String> {
    PgTryBuilder::new(|| {
        BackgroundWorker::transaction(|| {
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
    .execute()
}
