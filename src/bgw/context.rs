use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::{mpsc::Sender, Arc},
};

use futures::StreamExt;
use tokio::task::JoinHandle;

use crate::{
    bgw::{Worker, WorkerState},
    connection::{NatsConnectionOptions, NatsTlsOptions},
};

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
    rt: tokio::runtime::Runtime,
    sender: Sender<InternalWorkerMessage>,
    nats_state: Option<NatsConnectionState>,
    state: WorkerState,
}

#[allow(dead_code)]
impl WorkerContext {
    pub fn new(
        rt: tokio::runtime::Runtime,
        sender: Sender<InternalWorkerMessage>,
        state: WorkerState,
    ) -> Self {
        Self {
            rt,
            sender,
            nats_state: None,
            state,
        }
    }

    pub fn handle_subscribe(&mut self, subject: Arc<str>, fn_name: Arc<str>) {
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
            //log!("Can not subscribe, because connection is not established");
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

    pub fn is_master(&self) -> bool {
        self.state == WorkerState::Master
    }

    pub fn is_slave(&self) -> bool {
        self.state == WorkerState::Slave
    }

    pub fn state(&self) -> WorkerState {
        self.state
    }

    pub fn clear_state(&mut self) {
        let _ = self.nats_state.take();
    }

    pub fn restore_state(
        &mut self,
        opt: NatsConnectionOptions,
        worker: &impl Worker,
    ) -> anyhow::Result<()> {
        let client = self.connect_nats(&opt)?;

        self.nats_state = Some(NatsConnectionState {
            client,
            subscriptions: HashMap::new(),
        });

        let subs = worker.fetch_subject_with_callbacks()?;

        for (subject, fn_name) in subs {
            let _ = self.sender.send(InternalWorkerMessage::Subscribe {
                register: false,
                subject,
                fn_name,
            });
        }

        Ok(())
    }
}

impl WorkerContext {
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
                Err(_) => {
                    let _ = sender.send(InternalWorkerMessage::Unsubscribe { subject, fn_name });
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
