use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use futures::StreamExt;
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use pgrx::PgLwLock;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

use crate::connection::NatsConnectionOptions;
use crate::connection::NatsTlsOptions;
use crate::ctx::WorkerMessage;
use crate::log;

pub static BG_SOCKET_PORT: PgLwLock<u16> = PgLwLock::new(c"shmem_bg_scoket_port");

pub enum InternalWorkerMessage {
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

pub struct WorkerContext {
    pub udp_thread: JoinHandle<()>,
    pub port: u16,
    sender: Sender<InternalWorkerMessage>,
    subscriptions: HashMap<Arc<str>, NatsConnectionState>,
}

impl WorkerContext {
    pub async fn new(sender: Sender<InternalWorkerMessage>) -> Result<Self, String> {
        let udp = match UdpSocket::bind("localhost:0").await {
            Ok(sock) => sock,
            Err(e) => {
                return Err(format!("Failed to bind UDP socket: {}", e));
            }
        };

        let port = udp.local_addr().expect("failed to get port").port();

        log!("UDP socket bound to localhost:{}", port);

        let udp_thread = Self::spawn_udp_listener(udp, sender.clone()).await;

        *BG_SOCKET_PORT.exclusive() = port;

        Ok(Self {
            udp_thread,
            port,
            sender,
            subscriptions: HashMap::new(),
        })
    }

    pub async fn handle_subscribe(
        &mut self,
        opt: NatsConnectionOptions,
        subject: Arc<str>,
        fn_name: Arc<str>,
    ) {
        let source: Arc<str> = Arc::from(format!("{}:{}", opt.host, opt.port));

        match self.subscriptions.entry(source.clone()) {
            // Reuse existing NATS connection
            Entry::Occupied(mut e) => {
                let connection = e.get_mut();
                let client = connection.client.clone();

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
                            client.clone(),
                            source,
                            subject.clone(),
                            func,
                            self.sender.clone(),
                            opt,
                        )
                        .await;

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

                if let Ok(connection) = opts.connect(&**e.key()).await {
                    let handler = Self::spawn_subscription_task(
                        connection.clone(),
                        source,
                        subject.clone(),
                        fn_name.clone(),
                        self.sender.clone(),
                        opt,
                    )
                    .await;

                    let sub = NatsSubscription {
                        handler,
                        funcs: HashSet::from([fn_name]),
                    };

                    let _ = e.insert(NatsConnectionState {
                        client: connection,
                        subscriptions: HashMap::from([(subject, sub)]),
                    });
                }
            }
        }
    }

    pub fn handle_unsubscribe(
        &mut self,
        opt: &NatsConnectionOptions,
        subject: Arc<str>,
        fn_name: &str,
    ) {
        let source = Arc::from(format!("{}:{}", opt.host, opt.port));
        if let Some(connection) = self.subscriptions.get_mut(&source) {
            if let Entry::Occupied(mut e) = connection.subscriptions.entry(subject.clone()) {
                let _ = e.get_mut().funcs.remove(fn_name);

                if e.get().funcs.is_empty() {
                    let sub = e.remove();
                    sub.handler.abort();
                }
            }
        }
    }

    pub fn handle_callback(
        &self,
        client_key: &str,
        subject: &str,
        data: Arc<[u8]>,
        callback: impl Fn(&str, &[u8]),
    ) {
        if let Some(subjects) = self.subscriptions.get(client_key) {
            if let Some(subject) = subjects.subscriptions.get(subject) {
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
        client_key: Arc<str>,
        subject: Arc<str>,
        fn_name: Arc<str>,
        sender: Sender<InternalWorkerMessage>,
        opt: NatsConnectionOptions,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
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

    async fn spawn_udp_listener(
        udp: UdpSocket,
        sender: Sender<InternalWorkerMessage>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
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
}

impl Drop for WorkerContext {
    fn drop(&mut self) {
        self.udp_thread.abort();

        for (_, connection) in std::mem::take(&mut self.subscriptions) {
            for (_, sub) in connection.subscriptions {
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
    let mut worker_context = match rt.block_on(WorkerContext::new(msg_sender)) {
        Ok(sock) => sock,
        Err(e) => {
            log!("{}", e);
            return;
        }
    };

    let mut db_name = None;

    while BackgroundWorker::wait_latch(Some(std::time::Duration::from_millis(250))) {
        // Cache result
        let is_slave = unsafe { pg_sys::RecoveryInProgress() };

        while let Ok(message) = msg_receiver.try_recv() {
            if is_slave {
                continue;
            }

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
                    rt.block_on(worker_context.handle_subscribe(
                        opt,
                        Arc::from(subject),
                        Arc::from(fn_name),
                    ));
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
                    worker_context.handle_unsubscribe(&opt, subject.clone(), &fn_name);
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
                    worker_context.handle_callback(&client, &subject, data, |callback, data| {
                        let result = PgTryBuilder::new(|| {
                            BackgroundWorker::transaction(|| {
                                Spi::connect(|client| {
                                    let sql = format!("SELECT {}($1)", callback);
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
                    });
                }
            }
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
