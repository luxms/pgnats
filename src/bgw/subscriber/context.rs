use std::sync::{Arc, mpsc::Sender};

use pgrx::bgworkers::BackgroundWorker;

use crate::{
    bgw::{
        notification::PgInstanceNotification,
        subscriber::{
            InternalWorkerMessage, NatsConnectionState,
            pg_api::{
                PgInstanceStatus, call_function, delete_subject_callback, fetch_status,
                fetch_subject_with_callbacks, insert_subject_callback,
            },
        },
    },
    config::Config,
    debug, warn,
};

pub struct SubscriberContext {
    pub sender: Sender<InternalWorkerMessage>,

    rt: tokio::runtime::Runtime,
    config: Config,
    nats: NatsConnectionState,
    status: PgInstanceStatus,
}

impl SubscriberContext {
    pub fn new(
        rt: tokio::runtime::Runtime,
        sender: Sender<InternalWorkerMessage>,
        nats: NatsConnectionState,
        config: Config,
    ) -> Self {
        Self {
            rt,
            sender,
            nats,
            config,
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
            InternalWorkerMessage::Unsubscribe { subject, fn_name } => {
                debug!(
                    "Received unsubscription request: subject='{}', fn='{}'",
                    subject, fn_name,
                );

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

                self.handle_unsubscribe(subject, fn_name);
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
            InternalWorkerMessage::UnsubscribeSubject { subject, reason } => {
                warn!("LOG: {reason}");
                self.handle_unsubscribe_subject(&subject)
            }
        }
    }

    pub fn check_migration(&mut self, subscriptions_table_name: &str) {
        let state = BackgroundWorker::transaction(fetch_status);

        match (self.status, state) {
            (PgInstanceStatus::Master, PgInstanceStatus::Replica) => {
                self.status = PgInstanceStatus::Replica;
                self.send_notification();

                let _ = self.nats.unsubscribe_all();
            }
            (PgInstanceStatus::Replica, PgInstanceStatus::Master) => {
                self.status = PgInstanceStatus::Master;
                self.send_notification();

                if let Err(err) = self.restore_state(subscriptions_table_name) {
                    warn!("Error during restoring state: {}", err);
                }
            }
            _ => {}
        }
    }

    pub fn set_new_nats_config(&mut self, config: Config) -> anyhow::Result<()> {
        if self.config.nats_opt != config.nats_opt {
            self.nats
                .set_new_connection(&config.nats_opt, &self.rt, self.sender.clone())?;
        }
        self.config = config;
        Ok(())
    }

    pub fn restore_state(&mut self, subscriptions_table_name: &str) -> anyhow::Result<()> {
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
        self.nats
            .subscribe(subject, fn_name, &self.rt, self.sender.clone());
    }

    fn handle_unsubscribe(&mut self, subject: Arc<str>, fn_name: Arc<str>) {
        self.nats.unsubscribe(subject, fn_name);
    }

    fn handle_unsubscribe_subject(&mut self, subject: &str) {
        self.nats.unsubscribe_subject(subject);
    }

    fn handle_callback(&self, subject: &str, data: Arc<[u8]>, callback: impl Fn(&str, &[u8])) {
        self.nats.run_callbacks(subject, data, callback);
    }

    fn send_notification(&self) {
        let config = &self.config;
        let status = self.status;
        if let Some(notify_subject) = &config.notify_subject
            && let Some(notification) = BackgroundWorker::transaction(|| {
                PgInstanceNotification::new(status, config.patroni_url.as_deref())
            })
            && let Ok(notification) = serde_json::to_vec(&notification)
        {
            let subject = notify_subject.clone();

            let result: anyhow::Result<()> =
                self.rt.block_on(self.nats.publish(subject, notification));

            if let Err(err) = result {
                warn!("Notification publish error: {}", err);
            }
        } else {
            warn!("Failed to send notification about Postgres instance");
        }
    }
}

impl Drop for SubscriberContext {
    fn drop(&mut self) {
        let _ = self.rt.block_on(self.nats.drain());
    }
}
