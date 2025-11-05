use std::collections::HashMap;

use pgrx::pg_sys as sys;

use crate::{
    bgw::{
        DSM_SIZE,
        launcher::worker_entry::{RunningState, TerminatedState, WorkerEntry},
        pgrx_wrappers::shm_mq::ShmMqSender,
        subscriber::message::SubscriberMessage,
    },
    config::Config,
};

#[derive(Default)]
pub struct LauncherContext {
    pending_workers: HashMap<u32, WorkerEntry<RunningState>>,
    workers: HashMap<u32, WorkerEntry<RunningState>>,
    terminated_workers: HashMap<u32, WorkerEntry<TerminatedState>>,
    counter: usize,
}

impl LauncherContext {
    pub fn process_terminated_workers(&mut self) {
        for (_, v) in self.terminated_workers.drain() {
            let _ = v.wait_for_shutdown(); // ignore error
        }
    }

    pub fn register_worker(&mut self, db_oid: u32) {
        if let Some(worker) = self.pending_workers.remove(&db_oid) {
            let _ = self.workers.insert(db_oid, worker);
        }
    }

    pub fn handle_new_config_message(
        &mut self,
        db_oid: u32,
        config: Config,
        entry_point: &str,
    ) -> anyhow::Result<Option<String>> {
        if let Some(entry) = self.workers.get_mut(&db_oid) {
            send_subscriber_message(&mut entry.sender, SubscriberMessage::NewConfig { config })?;

            Ok(None)
        } else if !self.pending_workers.contains_key(&db_oid) {
            let db_name = self.start_subscribe_worker(db_oid, entry_point)?;

            Ok(Some(db_name))
        } else {
            Ok(None)
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

    pub fn handle_subscriber_exit_message(&mut self, db_oid: u32) {
        self.shutdown_worker(db_oid);
    }

    pub fn handle_foreign_server_dropped(&mut self, db_oid: u32) {
        self.shutdown_worker(db_oid);
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
    ) -> anyhow::Result<String> {
        let entry = WorkerEntry::start(
            sys::Oid::from_u32(oid),
            &format!("PGNats Background Worker Subscriber {}", self.counter),
            &format!("pgnats_bgw_subscriber_{}", self.counter),
            entry_point,
            DSM_SIZE,
        )?;
        self.counter += 1;
        let db_name = entry.db_name.clone();
        let _ = self.pending_workers.insert(oid, entry);

        Ok(db_name)
    }

    pub fn shutdown_worker(&mut self, db_oid: u32) {
        let Some(entry) = self
            .workers
            .remove(&db_oid)
            .or_else(|| self.pending_workers.remove(&db_oid))
        else {
            return;
        };

        self.shutdown_worker_entry(entry);
    }

    pub fn shutdown_all_workers(&mut self) {
        for (_, v) in std::mem::take(&mut self.workers) {
            self.shutdown_worker_entry(v);
        }

        for (_, v) in std::mem::take(&mut self.pending_workers) {
            self.shutdown_worker_entry(v);
        }
    }

    pub fn shutdown_worker_entry(&mut self, entry: WorkerEntry<RunningState>) {
        let entry = entry.terminate();
        let _ = self.terminated_workers.insert(entry.oid.to_u32(), entry);
    }

    pub fn get_worker(&self, db_oid: u32) -> Option<&WorkerEntry<RunningState>> {
        self.workers.get(&db_oid)
    }
}

fn send_subscriber_message(sender: &mut ShmMqSender, msg: SubscriberMessage) -> anyhow::Result<()> {
    let data = bincode::encode_to_vec(msg, bincode::config::standard())?;

    sender.send(&data)
}
