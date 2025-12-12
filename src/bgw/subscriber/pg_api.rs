use pgrx::{PgTryBuilder, Spi};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PgInstanceStatus {
    Master,
    Replica,
}

pub fn fetch_status() -> PgInstanceStatus {
    if unsafe { pgrx::pg_sys::RecoveryInProgress() } {
        PgInstanceStatus::Replica
    } else {
        PgInstanceStatus::Master
    }
}

pub fn fetch_subject_with_callbacks(table_name: &str) -> anyhow::Result<Vec<(String, String)>> {
    PgTryBuilder::new(|| {
        Spi::connect_mut(|client| {
            let sql = format!("SELECT subject, callback FROM {table_name}");
            let tuples = client.select(&sql, None, &[])?;
            let subject_callbacks: Vec<(String, String)> = tuples
                .into_iter()
                .filter_map(|tuple| {
                    let subject = tuple.get_by_name::<String, _>("subject");
                    let callback = tuple.get_by_name::<String, _>("callback");

                    match (subject, callback) {
                        (Ok(Some(subject)), Ok(Some(callback))) => Some((subject, callback)),
                        _ => None,
                    }
                })
                .collect();

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
}

pub fn insert_subject_callback(
    table_name: &str,
    subject: &str,
    callback: &str,
) -> anyhow::Result<()> {
    PgTryBuilder::new(|| {
        Spi::connect_mut(|client| {
            let sql = format!("INSERT INTO {table_name} VALUES ($1, $2)");
            let _ = client.update(&sql, None, &[subject.into(), callback.into()])?;

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
}

pub fn delete_subject_callback(
    table_name: &str,
    subject: &str,
    callback: &str,
) -> anyhow::Result<()> {
    PgTryBuilder::new(|| {
        Spi::connect_mut(|client| {
            let sql = format!("DELETE FROM {table_name} WHERE subject = $1 AND callback = $2",);
            let _ = client.update(&sql, None, &[subject.into(), callback.into()])?;

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
}

pub fn call_function(callback: &str, data: &[u8]) -> anyhow::Result<()> {
    if !callback
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return Err(anyhow::anyhow!("Invalid callback function name"));
    }

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
}
