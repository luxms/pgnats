use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::{Arc, mpsc::Sender},
};

use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use crate::{
    bgw::subscriber::InternalWorkerMessage,
    config::{NatsConnectionOptions, NatsTlsOptions},
};

pub(super) struct NatsSubscription {
    handler: JoinHandle<()>,
    funcs: HashSet<Arc<str>>,
}

pub(super) struct NatsConnectionState {
    client: async_nats::Client,
    subscriptions: HashMap<Arc<str>, NatsSubscription>,
}

impl NatsConnectionState {
    pub(super) async fn new(config: &NatsConnectionOptions) -> anyhow::Result<Self> {
        let client = Self::connect_nats(config).await?;
        Ok(Self {
            client,
            subscriptions: HashMap::new(),
        })
    }

    pub(super) fn subscribe(
        &mut self,
        subject: Arc<str>,
        fn_name: Arc<str>,
        rt: &tokio::runtime::Runtime,
        sender: Sender<InternalWorkerMessage>,
    ) {
        match self.subscriptions.entry(subject.clone()) {
            // Subject already exists; update or add the function handler
            Entry::Occupied(mut s) => {
                let _ = s.get_mut().funcs.insert(fn_name);
            }
            // First time subscribing to this subject
            Entry::Vacant(se) => {
                // Spawn a new handler task for the function
                let handler =
                    Self::spawn_subscription_task(self.client.clone(), rt, sender, subject.clone());

                let _ = se.insert(NatsSubscription {
                    handler,
                    funcs: HashSet::from([fn_name]),
                });
            }
        }
    }

    pub(super) fn unsubscribe(&mut self, subject: Arc<str>, fn_name: Arc<str>) {
        if let Entry::Occupied(mut e) = self.subscriptions.entry(subject.clone()) {
            let _ = e.get_mut().funcs.remove(&fn_name);

            if e.get().funcs.is_empty() {
                let sub = e.remove();
                sub.handler.abort();
            }
        }
    }

    pub(super) fn unsubscribe_subject(&mut self, subject: &str) {
        if let Some(sub) = self.subscriptions.remove(subject) {
            sub.handler.abort();
        }
    }

    pub(super) fn unsubscribe_all(&mut self) -> HashMap<Arc<str>, NatsSubscription> {
        let subs = std::mem::take(&mut self.subscriptions);
        for sub in subs.values() {
            sub.handler.abort();
        }

        subs
    }

    pub(super) fn run_callbacks(
        &self,
        subject: &str,
        data: Arc<[u8]>,
        callback: impl Fn(&str, &[u8]),
    ) {
        if let Some(subject) = self.subscriptions.get(subject) {
            for fnname in subject.funcs.iter() {
                callback(fnname, &data);
            }
        }
    }

    pub(super) fn reconnect_nats(
        &mut self,
        config: &NatsConnectionOptions,
        rt: &tokio::runtime::Runtime,
        sender: Sender<InternalWorkerMessage>,
    ) -> anyhow::Result<()> {
        let client = rt.block_on(Self::connect_nats(config))?;

        let mut subs = self.unsubscribe_all();

        for (subject, sub) in &mut subs {
            sub.handler =
                Self::spawn_subscription_task(client.clone(), rt, sender.clone(), subject.clone());
        }

        self.client = client;
        self.subscriptions = subs;

        Ok(())
    }

    pub(super) async fn publish(&self, subject: String, body: Vec<u8>) -> anyhow::Result<()> {
        self.client.publish(subject, body.into()).await?;
        self.client.flush().await?;

        Ok(())
    }
    pub(super) async fn drain(&self) -> anyhow::Result<()> {
        self.client.drain().await?;

        Ok(())
    }

    async fn connect_nats(config: &NatsConnectionOptions) -> anyhow::Result<async_nats::Client> {
        let mut opts = async_nats::ConnectOptions::new().client_capacity(config.capacity);

        if let Some(tls) = &config.tls {
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

        Ok(opts
            .connect(format!("{}:{}", config.host, config.port))
            .await?)
    }

    fn spawn_subscription_task(
        client: async_nats::Client,
        rt: &tokio::runtime::Runtime,
        sender: Sender<InternalWorkerMessage>,
        subject: Arc<str>,
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
                Err(err) => {
                    let _ = sender.send(InternalWorkerMessage::UnsubscribeSubject {
                        subject: subject.clone(),
                        reason: err.to_string(),
                    });
                }
            }
        })
    }
}

impl Drop for NatsConnectionState {
    fn drop(&mut self) {
        let _ = self.unsubscribe_all();
    }
}
