use anyhow::bail;
use pgrx::{
    PgTryBuilder, Spi,
    bgworkers::{BackgroundWorker, SignalWakeFlags},
};

use crate::{
    bgw::{SharedQueue, Worker, WorkerState},
    config::{Config, GUC_SUB_DB_NAME, fetch_config},
    init::SUBSCRIPTIONS_TABLE_NAME,
    log,
    notification::{PgInstanceNotification, PgInstanceTransition},
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

    fn fetch_subject_with_callbacks(&self) -> anyhow::Result<Vec<(String, String)>> {
        BackgroundWorker::transaction(|| {
            PgTryBuilder::new(|| {
                Spi::connect_mut(|client| {
                    let sql = format!("SELECT subject, callback FROM {SUBSCRIPTIONS_TABLE_NAME}");
                    let tuples = client.select(&sql, None, &[])?;
                    let subject_callbacks: Vec<(String, String)> = tuples
                        .into_iter()
                        .filter_map(|tuple| {
                            let subject = tuple.get_by_name::<String, _>("subject");
                            let callback = tuple.get_by_name::<String, _>("callback");

                            match (subject, callback) {
                                (Ok(Some(subject)), Ok(Some(callback))) => {
                                    Some((subject, callback))
                                }
                                _ => None,
                            }
                        })
                        .collect();

                    log!(
                        "Fetched {} registered subject callbacks",
                        subject_callbacks.len()
                    );

                    Ok(subject_callbacks)
                })
            })
            .catch_others(|e| match e {
                pgrx::pg_sys::panic::CaughtError::PostgresError(err) => Err(anyhow::anyhow!(
                    "Code '{}': {}. ({:?})",
                    err.sql_error_code(),
                    err.message(),
                    err.hint()
                )),
                _ => Err(anyhow::anyhow!("{:?}", e)),
            })
            .execute()
        })
    }

    fn insert_subject_callback(&self, subject: &str, callback: &str) -> anyhow::Result<()> {
        BackgroundWorker::transaction(|| {
            PgTryBuilder::new(|| {
                Spi::connect_mut(|client| {
                    let sql = format!("INSERT INTO {SUBSCRIPTIONS_TABLE_NAME} VALUES ($1, $2)");
                    let _ = client.update(&sql, None, &[subject.into(), callback.into()])?;

                    log!(
                        "Inserted subject callback: subject='{}', callback='{}'",
                        subject,
                        callback
                    );

                    Ok(())
                })
            })
            .catch_others(|e| match e {
                pgrx::pg_sys::panic::CaughtError::PostgresError(err) => Err(anyhow::anyhow!(
                    "Code '{}': {}. ({:?})",
                    err.sql_error_code(),
                    err.message(),
                    err.hint()
                )),
                _ => Err(anyhow::anyhow!("{:?}", e)),
            })
            .execute()
        })
    }

    fn delete_subject_callback(&self, subject: &str, callback: &str) -> anyhow::Result<()> {
        BackgroundWorker::transaction(|| {
            PgTryBuilder::new(|| {
                Spi::connect_mut(|client| {
                    let sql = format!(
                        "DELETE FROM {SUBSCRIPTIONS_TABLE_NAME} WHERE subject = $1 AND callback = $2",

                    );
                    let _ = client.update(&sql, None, &[subject.into(), callback.into()])?;

                    log!(
                        "Deleted subject callback: subject='{}', callback='{}'",
                        subject,
                        callback
                    );

                    Ok(())
                })
            })
            .catch_others(|e| match e {
                pgrx::pg_sys::panic::CaughtError::PostgresError(err) => Err(anyhow::anyhow!(
                    "Code '{}': {}. ({:?})",
                    err.sql_error_code(),
                    err.message(),
                    err.hint()
                )),
                _ => Err(anyhow::anyhow!("{:?}", e)),
            })
            .execute()
        })
    }

    fn call_function(&self, callback: &str, data: &[u8]) -> anyhow::Result<()> {
        if !callback
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(anyhow::anyhow!("Invalid callback function name"));
        }

        BackgroundWorker::transaction(|| {
            PgTryBuilder::new(|| {
                Spi::connect_mut(|client| {
                    let sql = format!("SELECT {callback}($1)");
                    let _ = client.update(&sql, None, &[data.into()])?;
                    Ok(())
                })
            })
            .catch_others(|e| match e {
                pgrx::pg_sys::panic::CaughtError::PostgresError(err) => Err(anyhow::anyhow!(
                    "Code '{}': {}. ({:?})",
                    err.sql_error_code(),
                    err.message(),
                    err.hint()
                )),
                _ => Err(anyhow::anyhow!("{:?}", e)),
            })
            .execute()
        })
    }

    fn fetch_config(&self) -> Config {
        BackgroundWorker::transaction(|| fetch_config())
    }

    fn make_notification(
        &self,
        transition: PgInstanceTransition,
    ) -> Option<PgInstanceNotification> {
        PgInstanceNotification::new(transition)
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
