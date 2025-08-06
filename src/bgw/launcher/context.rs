use std::collections::HashMap;

use pgrx::pg_sys as sys;

use crate::{
    bgw::{
        DSM_SIZE, launcher::worker_entry::WorkerEntry, pgrx_wrappers::shm_mq::ShmMqSender,
        subscriber::message::SubscriberMessage,
    },
    config::Config,
};

#[derive(Default)]
pub struct LauncherContext {
    workers: HashMap<u32, WorkerEntry>,
    counter: usize,
}

impl LauncherContext {
    pub fn handle_new_config_message(
        &mut self,
        db_oid: u32,
        config: Config,
        entry_point: &str,
    ) -> anyhow::Result<Option<String>> {
        if let Some(entry) = self.workers.get_mut(&db_oid) {
            send_subscriber_message(&mut entry.sender, SubscriberMessage::NewConfig { config })?;

            Ok(None)
        } else {
            let we = self.start_subscribe_worker(db_oid, entry_point)?;
            let db_name = we.db_name.clone();
            let _ = self.add_subscribe_worker(db_oid, we)?;

            Ok(Some(db_name))
        }
    }

    pub fn handle_subscribe_message(
        &mut self,
        db_oid: u32,
        subject: String,
        fn_name: String,
    ) -> anyhow::Result<()> {
        if let Some(entry) = self.workers.get_mut(&db_oid) {
            send_subscriber_message(
                &mut entry.sender,
                SubscriberMessage::Subscribe { subject, fn_name },
            )?;
        }

        Ok(())
    }

    pub fn handle_unsubscribe_message(
        &mut self,
        db_oid: u32,
        subject: String,
        fn_name: String,
    ) -> anyhow::Result<()> {
        if let Some(entry) = self.workers.get_mut(&db_oid) {
            send_subscriber_message(
                &mut entry.sender,
                SubscriberMessage::Unsubscribe { subject, fn_name },
            )?;
        }

        Ok(())
    }

    pub fn handle_subscriber_exit_message(
        &mut self,
        db_oid: u32,
    ) -> anyhow::Result<Option<WorkerEntry>> {
        self.shutdown_worker(db_oid)
    }

    pub fn handle_foreign_server_dropped(
        &mut self,
        db_oid: u32,
    ) -> anyhow::Result<Option<WorkerEntry>> {
        self.shutdown_worker(db_oid)
    }

    #[cfg(any(test, feature = "pg_test"))]
    pub fn handle_change_status(&mut self, db_oid: u32, is_master: bool) -> anyhow::Result<()> {
        if let Some(entry) = self.workers.get_mut(&db_oid) {
            send_subscriber_message(
                &mut entry.sender,
                SubscriberMessage::ChangeStatus { is_master },
            )?;
        }

        Ok(())
    }

    pub fn start_subscribe_worker(
        &mut self,
        oid: u32,
        entry_point: &str,
    ) -> anyhow::Result<WorkerEntry> {
        let entry = WorkerEntry::start(
            sys::Oid::from_u32(oid),
            &format!("PGNats Background Worker Subscriber {}", self.counter),
            &format!("pgnats_bgw_subscriber_{}", self.counter),
            entry_point,
            DSM_SIZE,
        )?;
        self.counter += 1;

        Ok(entry)
    }

    pub fn add_subscribe_worker(
        &mut self,
        oid: u32,
        entry: WorkerEntry,
    ) -> anyhow::Result<Option<WorkerEntry>> {
        if let Some(prev_entry) = self.workers.insert(oid, entry) {
            let we = Self::shutdown_worker_entry(prev_entry)?;
            return Ok(Some(we));
        }

        Ok(None)
    }

    pub fn shutdown_worker(&mut self, db_oid: u32) -> anyhow::Result<Option<WorkerEntry>> {
        let Some(entry) = self.workers.remove(&db_oid) else {
            return Ok(None);
        };

        Self::shutdown_worker_entry(entry).map(Some)
    }

    pub fn shutdown_worker_entry(mut entry: WorkerEntry) -> anyhow::Result<WorkerEntry> {
        if let Some(worker) = entry.worker.take() {
            let terminate = worker.terminate();

            terminate.wait_for_shutdown().map_err(|err| {
                anyhow::anyhow!(
                    "Failed to gracefully shutdown background worker for database '{}': {err:?}",
                    entry.db_name
                )
            })?;
        }

        Ok(entry)
    }

    pub fn get_worker(&self, db_oid: u32) -> Option<&WorkerEntry> {
        self.workers.get(&db_oid)
    }

    pub fn drain_workers(&mut self) -> impl Iterator<Item = WorkerEntry> {
        self.workers.drain().map(|(_, v)| v)
    }
}

fn send_subscriber_message(sender: &mut ShmMqSender, msg: SubscriberMessage) -> anyhow::Result<()> {
    let data = bincode::encode_to_vec(msg, bincode::config::standard())?;

    sender.send(&data)
}
