use std::sync::{Arc, mpsc::Sender};

use pgrx::bgworkers::BackgroundWorker;

use crate::{
    bgw::{
        notification::PgInstanceNotification,
        subscriber::{
            InternalWorkerMessage, NatsConnectionState,
            pg_api::{PgInstanceStatus, fetch_status, fetch_subject_with_callbacks},
        },
    },
    config::Config,
};

pub struct SubscriberContext {
    sender: Sender<InternalWorkerMessage>,

    rt: tokio::runtime::Runtime,
    config: Config,
    nats: NatsConnectionState,
    status: PgInstanceStatus,

    #[cfg(any(test, feature = "pg_test"))]
    pub(super) fetch_status: PgInstanceStatus,
}

impl SubscriberContext {
    pub(super) fn new(
        rt: tokio::runtime::Runtime,
        sender: Sender<InternalWorkerMessage>,
        nats: NatsConnectionState,
        config: Config,
    ) -> Self {
        let status = BackgroundWorker::transaction(fetch_status);

        Self {
            rt,
            sender,
            nats,
            config,
            status,
            #[cfg(any(test, feature = "pg_test"))]
            fetch_status: status,
        }
    }

    pub fn check_migration(&mut self, subscriptions_table_name: &str) -> anyhow::Result<()> {
        #[cfg(not(feature = "pg_test"))]
        let state = BackgroundWorker::transaction(fetch_status);

        #[cfg(feature = "pg_test")]
        let state = self.fetch_status;

        match (self.status, state) {
            (PgInstanceStatus::Master, PgInstanceStatus::Replica) => {
                self.status = PgInstanceStatus::Replica;
                let _ = self.nats.unsubscribe_all();

                self.send_notification()?;
            }
            (PgInstanceStatus::Replica, PgInstanceStatus::Master) => {
                self.status = PgInstanceStatus::Master;
                self.restore_state(subscriptions_table_name)?;

                self.send_notification()?;
            }
            _ => {}
        }

        Ok(())
    }

    pub fn apply_config(&mut self, config: Config) -> anyhow::Result<()> {
        if self.config.nats_opt != config.nats_opt {
            self.nats
                .reconnect_nats(&config.nats_opt, &self.rt, self.sender.clone())?;
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

    pub fn handle_subscribe(&mut self, subject: Arc<str>, fn_name: Arc<str>) {
        self.nats
            .subscribe(subject, fn_name, &self.rt, self.sender.clone());
    }

    pub fn handle_unsubscribe(&mut self, subject: Arc<str>, fn_name: Arc<str>) {
        self.nats.unsubscribe(subject, fn_name);
    }

    pub fn handle_unsubscribe_subject(&mut self, subject: &str) {
        self.nats.unsubscribe_subject(subject);
    }

    pub fn handle_callback(&self, subject: &str, data: Arc<[u8]>, callback: impl Fn(&str, &[u8])) {
        self.nats.run_callbacks(subject, data, callback);
    }

    pub fn send_notification(&self) -> anyhow::Result<()> {
        let config = &self.config;
        let status = self.status;

        let notify_subject = config
            .notify_subject
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("notify_subject is not set in config"))?;

        let notification = BackgroundWorker::transaction(|| {
            PgInstanceNotification::new(status, config.patroni_url.as_deref())
        })
        .ok_or_else(|| anyhow::anyhow!("Failed to construct PgInstanceNotification: missing or invalid listen_addresses or port GUC"))?;

        let notification = serde_json::to_vec(&notification).map_err(|err| {
            anyhow::anyhow!("Failed to serialize PgInstanceNotification: {}", err)
        })?;

        self.rt
            .block_on(self.nats.publish(notify_subject.clone(), notification))
            .map_err(|err| anyhow::anyhow!("Failed to publish notification to NATS: {}", err))
    }
}

impl Drop for SubscriberContext {
    fn drop(&mut self) {
        let _ = self.rt.block_on(self.nats.drain());
    }
}
