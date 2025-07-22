use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::{Arc, mpsc::Sender},
};

use futures::StreamExt;
use tokio::task::JoinHandle;

use crate::{
    bgw::{Worker, WorkerState},
    config::{Config, fetch_connection_options},
    connection::{NatsConnectionOptions, NatsTlsOptions},
    debug, log,
    notification::{PgInstanceNotification, PgInstanceTransition},
    warn,
};

pub(super) enum InternalWorkerMessage {
    Subscribe {
        register: bool,
        subject: String,
        fn_name: String,
    },
    Unsubscribe {
        reason: Option<String>,
        subject: Arc<str>,
        fn_name: Arc<str>,
    },
    CallbackCall {
        subject: Arc<str>,
        data: Arc<[u8]>,
    },
}

struct NatsSubscription {
    handler: JoinHandle<()>,
    funcs: HashSet<Arc<str>>,
}

struct NatsConnectionState {
    client: async_nats::Client,
    subscriptions: HashMap<Arc<str>, NatsSubscription>,
}

pub struct WorkerContext<T> {
    rt: tokio::runtime::Runtime,
    sender: Sender<InternalWorkerMessage>,
    config: Option<Config>,
    nats_state: Option<NatsConnectionState>,
    state: WorkerState,
    worker: T,
}

impl<T: Worker> WorkerContext<T> {
    pub fn new(
        rt: tokio::runtime::Runtime,
        sender: Sender<InternalWorkerMessage>,
        worker: T,
    ) -> Self {
        Self {
            rt,
            sender,
            nats_state: None,
            config: None,
            state: worker.fetch_state(),
            worker,
        }
    }

    pub fn handle_internal_message(&mut self, msg: InternalWorkerMessage) {
        match msg {
            InternalWorkerMessage::Subscribe {
                register,
                subject,
                fn_name,
            } => {
                log!(
                    "Received subscription request: subject='{}', fn='{}'",
                    subject,
                    fn_name
                );

                if self.nats_state.is_none() {
                    warn!("Connection not established!");
                    return;
                }

                if register {
                    if let Err(error) = self.worker.insert_subject_callback(&subject, &fn_name) {
                        warn!(
                            "Error subscribing: subject='{}', callback='{}': {}",
                            subject, fn_name, error,
                        );
                    }
                }

                self.handle_subscribe(Arc::from(subject), Arc::from(fn_name));
            }
            InternalWorkerMessage::Unsubscribe {
                reason,
                subject,
                fn_name,
            } => {
                log!(
                    "Received unsubscription request: subject='{}', fn='{}', reason='{}'",
                    subject,
                    fn_name,
                    reason.unwrap_or_else(|| "Requested".to_string())
                );

                if self.nats_state.is_none() {
                    warn!("Connection not established!");
                    return;
                }

                if let Err(error) = self.worker.delete_subject_callback(&subject, &fn_name) {
                    warn!(
                        "Error unsubscribing: subject='{}', callback='{}': {}",
                        subject, fn_name, error
                    );
                }

                self.handle_unsubscribe(subject.clone(), &fn_name);
            }
            InternalWorkerMessage::CallbackCall { subject, data } => {
                log!("Dispatching callbacks for subject '{}'", subject);

                self.handle_callback(&subject, data, |callback, data| {
                    if let Err(err) = self.worker.call_function(callback, data) {
                        warn!("Error invoking callback '{}': {:?}", callback, err);
                    }
                });
            }
        }
    }

    pub fn check_migration(&mut self) {
        let state = self.worker.fetch_state();

        match (self.state, state) {
            (WorkerState::Master, WorkerState::Slave) => {
                if let Some(nats) = self.nats_state.take() {
                    self.send_notification(&nats, PgInstanceTransition::M2R);
                }

                self.state = WorkerState::Slave;
            }
            (WorkerState::Slave, WorkerState::Master) => {
                if let Some(nats) = &self.nats_state {
                    self.send_notification(nats, PgInstanceTransition::R2M);
                }

                let opt = self.worker.transaction(fetch_connection_options);
                if let Err(err) = self.restore_state(opt) {
                    warn!("Error during restoring state: {}", err);
                }
                self.state = WorkerState::Master;
            }
            _ => {}
        }
    }

    pub fn worker(&self) -> &T {
        &self.worker
    }

    pub fn restore_state(&mut self, config: Config) -> anyhow::Result<()> {
        if self.config.as_ref().map(|s| &s.nats_opt) != Some(&config.nats_opt) {
            let _ = self.nats_state.take();

            let client = self.connect_nats(&config.nats_opt)?;

            self.nats_state = Some(NatsConnectionState {
                client,
                subscriptions: HashMap::new(),
            });

            self.config = Some(config);
        }

        let subs = self.worker.fetch_subject_with_callbacks()?;

        for (subject, fn_name) in subs {
            let _ = self.sender.send(InternalWorkerMessage::Subscribe {
                register: false,
                subject,
                fn_name,
            });
        }

        Ok(())
    }

    fn handle_subscribe(&mut self, subject: Arc<str>, fn_name: Arc<str>) {
        if let Some(mut connection) = self.nats_state.take() {
            match connection.subscriptions.entry(subject.clone()) {
                // Subject already exists; update or add the function handler
                Entry::Occupied(mut s) => {
                    let _ = s.get_mut().funcs.insert(fn_name);
                }
                // First time subscribing to this subject
                Entry::Vacant(se) => {
                    let func = fn_name.clone();
                    // Spawn a new handler task for the function
                    let handler = self.spawn_subscription_task(
                        connection.client.clone(),
                        subject.clone(),
                        func,
                    );

                    let _ = se.insert(NatsSubscription {
                        handler,
                        funcs: HashSet::from([fn_name]),
                    });
                }
            }

            self.nats_state = Some(connection);
        } else {
            warn!(
                "Cannot subscribe to subject '{}': NATS connection not established",
                subject
            );
        }
    }

    fn handle_unsubscribe(&mut self, subject: Arc<str>, fn_name: &str) {
        if let Some(nats_state) = &mut self.nats_state {
            if let Entry::Occupied(mut e) = nats_state.subscriptions.entry(subject.clone()) {
                let _ = e.get_mut().funcs.remove(fn_name);

                if e.get().funcs.is_empty() {
                    debug!(
                        "All callbacks removed for subject '{}'. Stopping subscription.",
                        subject
                    );

                    let sub = e.remove();
                    sub.handler.abort();
                }
            }
        } else {
            warn!(
                "Cannot subscribe to subject '{}': NATS connection not established",
                subject
            );
        }
    }

    fn handle_callback(&self, subject: &str, data: Arc<[u8]>, callback: impl Fn(&str, &[u8])) {
        if let Some(nats_state) = &self.nats_state {
            if let Some(subject) = nats_state.subscriptions.get(subject) {
                for fnname in subject.funcs.iter() {
                    callback(fnname, &data);
                }
            }
        } else {
            warn!(
                "Cannot subscribe to subject '{}': NATS connection not established",
                subject
            );
        }
    }

    fn send_notification(&self, nats: &NatsConnectionState, transition: PgInstanceTransition) {
        if let Some(config) = &self.config
            && let Some(notify_subject) = &config.notify_subject
            && let Some(notification) = PgInstanceNotification::new(transition)
            && let Ok(notification) = serde_json::to_vec(&notification)
        {
            let subject = notify_subject.clone();
            let notification = notification.into();
            let result = self.rt.block_on(nats.client.publish(subject, notification));

            if let Err(err) = result {
                warn!("Notification publish error: {}", err);
            }
        } else {
            warn!("Failed to send notification about Postgres instance");
        }
    }
}

impl<T> WorkerContext<T> {
    pub fn is_master(&self) -> bool {
        self.state == WorkerState::Master
    }

    pub fn is_slave(&self) -> bool {
        self.state == WorkerState::Slave
    }

    fn spawn_subscription_task(
        &self,
        client: async_nats::Client,
        subject: Arc<str>,
        fn_name: Arc<str>,
    ) -> JoinHandle<()> {
        let sender = self.sender.clone();
        self.rt.spawn(async move {
            match client.subscribe(subject.to_string()).await {
                Ok(mut sub) => {
                    while let Some(msg) = sub.next().await {
                        let _ = sender.send(InternalWorkerMessage::CallbackCall {
                            subject: subject.clone(),
                            data: Arc::from(msg.payload.to_vec()),
                        });
                    }
                }
                Err(err) => {
                    let _ = sender.send(InternalWorkerMessage::Unsubscribe {
                        reason: Some(err.to_string()),
                        subject,
                        fn_name,
                    });
                }
            }
        })
    }

    fn connect_nats(&self, opt: &NatsConnectionOptions) -> anyhow::Result<async_nats::Client> {
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

        Ok(self
            .rt
            .block_on(opts.connect(format!("{}:{}", opt.host, opt.port)))?)
    }
}

impl Drop for NatsConnectionState {
    fn drop(&mut self) {
        for (_, sub) in std::mem::take(&mut self.subscriptions) {
            sub.handler.abort();
        }
    }
}
