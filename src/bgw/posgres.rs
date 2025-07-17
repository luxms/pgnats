use anyhow::bail;
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};

use crate::{
    bgw::{SharedQueue, Worker, WorkerState},
    config::GUC_SUB_DB_NAME,
    ring_queue::RingQueue,
};

pub struct PostgresWorker {
    db_name: String,
}

impl PostgresWorker {
    pub fn new() -> anyhow::Result<Self> {
        BackgroundWorker::attach_signal_handlers(
            SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
        );

        let Some(db_name) = GUC_SUB_DB_NAME.get() else {
            bail!("nats.sub_dbname is NULL");
        };

        let db_name = db_name.into_string()?;
        BackgroundWorker::connect_worker_to_spi(Some(&db_name), None);

        Ok(Self { db_name })
    }

    pub fn connected_db_name(&self) -> &str {
        &self.db_name
    }
}

impl Worker for PostgresWorker {
    fn transaction<F: FnOnce() -> R + std::panic::UnwindSafe + std::panic::RefUnwindSafe, R>(
        &self,
        body: F,
    ) -> R {
        BackgroundWorker::transaction(body)
    }

    fn wait(&self, duration: std::time::Duration) -> bool {
        BackgroundWorker::wait_latch(Some(duration))
    }

    fn fetch_state(&self) -> WorkerState {
        if unsafe { pgrx::pg_sys::RecoveryInProgress() } {
            WorkerState::Slave
        } else {
            WorkerState::Master
        }
    }
}

impl<const N: usize> SharedQueue<N> for pgrx::PgLwLock<RingQueue<N>> {
    type Unqiue<'a> = pgrx::PgLwLockExclusiveGuard<'a, RingQueue<N>>;
    type Shared<'a> = pgrx::PgLwLockShareGuard<'a, RingQueue<N>>;

    fn shared(&self) -> Self::Shared<'_> {
        self.share()
    }

    fn unique(&self) -> Self::Unqiue<'_> {
        self.exclusive()
    }
}
