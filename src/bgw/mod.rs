use std::ops::{Deref, DerefMut};

use pgrx::{PgTryBuilder, Spi};

use crate::{init::SUBSCRIPTIONS_TABLE_NAME, log, ring_queue::RingQueue};

mod context;

pub mod export;
pub mod native;
pub mod posgres;
pub mod run;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum WorkerState {
    Master,
    Slave,
}

pub trait Worker {
    fn transaction<F: FnOnce() -> R + std::panic::UnwindSafe + std::panic::RefUnwindSafe, R>(
        &self,
        body: F,
    ) -> R;

    fn wait(&self, duration: std::time::Duration) -> bool;

    fn fetch_state(&self) -> WorkerState;

    fn fetch_subject_with_callbacks(&self) -> anyhow::Result<Vec<(String, String)>> {
        self.transaction(|| {
            PgTryBuilder::new(|| {
                Spi::connect_mut(|client| {
                    let sql = format!("SELECT subject, callback FROM {}", SUBSCRIPTIONS_TABLE_NAME);
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
        self.transaction(|| {
            PgTryBuilder::new(|| {
                Spi::connect_mut(|client| {
                    let sql = format!("INSERT INTO {} VALUES ($1, $2)", SUBSCRIPTIONS_TABLE_NAME);
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
        self.transaction(|| {
            PgTryBuilder::new(|| {
                Spi::connect_mut(|client| {
                    let sql = format!(
                        "DELETE FROM {} WHERE subject = $1 AND callback = $2",
                        SUBSCRIPTIONS_TABLE_NAME
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

        self.transaction(|| {
            PgTryBuilder::new(|| {
                Spi::connect_mut(|client| {
                    let sql = format!("SELECT {}($1)", callback);
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
}

pub trait SharedQueue<const N: usize> {
    type Unqiue<'a>: Deref<Target = RingQueue<N>> + DerefMut
    where
        Self: 'a;

    type Shared<'a>: Deref<Target = RingQueue<N>>
    where
        Self: 'a;

    fn shared(&self) -> Self::Shared<'_>;
    fn unique(&self) -> Self::Unqiue<'_>;
}
