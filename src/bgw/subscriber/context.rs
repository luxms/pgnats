use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::{Arc, mpsc::Sender},
};

use pgrx::bgworkers::BackgroundWorker;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use crate::{
    bgw::{
        notification::PgInstanceNotification,
        subscriber::{
            InternalWorkerMessage,
            pg_api::{
                PgInstanceStatus, call_function, delete_subject_callback, fetch_status,
                fetch_subject_with_callbacks, insert_subject_callback,
            },
        },
    },
    config::{Config, NatsConnectionOptions, NatsTlsOptions, fetch_config},
    debug, log, warn,
};

struct NatsSubscription {
    handler: JoinHandle<()>,
    funcs: HashSet<Arc<str>>,
}

struct NatsConnectionState {
    client: async_nats::Client,
    subscriptions: HashMap<Arc<str>, NatsSubscription>,
}

impl Drop for NatsConnectionState {
    fn drop(&mut self) {
        for (_, sub) in std::mem::take(&mut self.subscriptions) {
            sub.handler.abort();
        }
    }
}

pub struct SubscriberContext {
    rt: tokio::runtime::Runtime,
    sender: Sender<InternalWorkerMessage>,
    config: Option<Config>,
    nats_state: Option<NatsConnectionState>,
    status: PgInstanceStatus,
}

impl SubscriberContext {
    pub fn new(rt: tokio::runtime::Runtime, sender: Sender<InternalWorkerMessage>) -> Self {
        Self {
            rt,
            sender,
            nats_state: None,
            config: None,
            status: BackgroundWorker::transaction(fetch_status),
        }
    }

    pub fn handle_internal_message(
        &mut self,
        msg: InternalWorkerMessage,
        subscriptions_table_name: &str,
    ) {
        match msg {
            InternalWorkerMessage::Subscribe {
                register,
                subject,
                fn_name,
            } => {
                debug!(
                    "Received subscription request: subject='{}', fn='{}'",
                    subject, fn_name
                );

                if self.nats_state.is_none() {
                    debug!("Connection not established!");
                    return;
                }

                if register {
                    if let Err(error) = BackgroundWorker::transaction(|| {
                        insert_subject_callback(subscriptions_table_name, &subject, &fn_name)
                    }) {
                        warn!(
                            "Error subscribing: subject='{}', callback='{}': {}",
                            subject, fn_name, error,
                        );
                    } else {
                        debug!(
                            "Inserted subject callback: subject='{}', callback='{}'",
                            subject, fn_name
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
                debug!(
                    "Received unsubscription request: subject='{}', fn='{}', reason='{}'",
                    subject,
                    fn_name,
                    reason.unwrap_or_else(|| "Requested".to_string())
                );

                if self.nats_state.is_none() {
                    warn!("Connection not established!");
                    return;
                }

                if let Err(error) = BackgroundWorker::transaction(|| {
                    delete_subject_callback(subscriptions_table_name, &subject, &fn_name)
                }) {
                    warn!(
                        "Error unsubscribing: subject='{}', callback='{}': {}",
                        subject, fn_name, error
                    );
                } else {
                    debug!(
                        "Deleted subject callback: subject='{}', callback='{}'",
                        subject, fn_name
                    );
                }

                self.handle_unsubscribe(subject.clone(), &fn_name);
            }
            InternalWorkerMessage::CallbackCall { subject, data } => {
                debug!("Dispatching callbacks for subject '{}'", subject);

                self.handle_callback(&subject, data, |callback, data| {
                    if let Err(err) =
                        BackgroundWorker::transaction(|| call_function(callback, data))
                    {
                        warn!("Error invoking callback '{}': {:?}", callback, err);
                    }
                });
            }
        }
    }

    pub fn check_migration(&mut self, subscriptions_table_name: &str) {
        let state = BackgroundWorker::transaction(fetch_status);

        match (self.status, state) {
            (PgInstanceStatus::Master, PgInstanceStatus::Replica) => {
                if let Some(nats) = self.nats_state.take() {
                    self.send_notification(&nats, PgInstanceStatus::Replica);
                }

                self.status = PgInstanceStatus::Replica;
            }
            (PgInstanceStatus::Replica, PgInstanceStatus::Master) => {
                log!("Restoring NATS state");
                let opt = BackgroundWorker::transaction(fetch_config);
                if let Err(err) = self.restore_state(opt, subscriptions_table_name) {
                    warn!("Error during restoring state: {}", err);
                }

                if let Some(nats) = &self.nats_state {
                    self.send_notification(nats, PgInstanceStatus::Master);
                }

                self.status = PgInstanceStatus::Master;
            }
            _ => {}
        }
    }

    pub fn restore_state(
        &mut self,
        config: Config,
        subscriptions_table_name: &str,
    ) -> anyhow::Result<()> {
        if self.config.as_ref().map(|s| &s.nats_opt) != Some(&config.nats_opt) {
            let _ = self.nats_state.take();

            let client = self.connect_nats(&config.nats_opt)?;

            self.nats_state = Some(NatsConnectionState {
                client,
                subscriptions: HashMap::new(),
            });

            self.config = Some(config);
        }

        let subs = BackgroundWorker::transaction(|| {
            fetch_subject_with_callbacks(subscriptions_table_name)
        })?;

        for (subject, fn_name) in subs {
            let _ = self.sender.send(InternalWorkerMessage::Subscribe {
                register: false,
                subject,
                fn_name,
            });
        }

        Ok(())
    }

    pub fn is_master(&self) -> bool {
        self.status == PgInstanceStatus::Master
    }

    pub fn is_replica(&self) -> bool {
        self.status == PgInstanceStatus::Replica
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
                "Cannot unsubscribe to subject '{}': NATS connection not established",
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
                "Cannot call callbacks for subject '{}': NATS connection not established",
                subject
            );
        }
    }

    fn send_notification(&self, nats: &NatsConnectionState, status: PgInstanceStatus) {
        if let Some(config) = &self.config
            && let Some(notify_subject) = &config.notify_subject
            && let Some(notification) = BackgroundWorker::transaction(|| {
                PgInstanceNotification::new(status, config.patroni_url.as_deref())
            })
            && let Ok(notification) = serde_json::to_vec(&notification)
        {
            let subject = notify_subject.clone();
            let notification = notification.into();

            let result: anyhow::Result<()> = self.rt.block_on(async {
                nats.client.publish(subject, notification).await?;
                nats.client.flush().await?;

                Ok(())
            });

            if let Err(err) = result {
                warn!("Notification publish error: {}", err);
            }
        } else {
            warn!("Failed to send notification about Postgres instance");
        }
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

impl Drop for SubscriberContext {
    fn drop(&mut self) {
        if let Some(nats) = self.nats_state.take() {
            let _ = self.rt.block_on(nats.client.drain());
        }
    }
}
