use pgrx::{PgSqlErrorCode, PgTryBuilder, Spi};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PgInstanceStatus {
    Master,
    Replica,
}

#[derive(Debug)]
pub enum CallError {
    NotFound,
    Other(anyhow::Error),
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
                    let fn_oid = tuple.get_by_name::<String, _>("callback");

                    match (subject, fn_oid) {
                        (Ok(Some(subject)), Ok(Some(fn_oid))) => Some((subject, fn_oid)),
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
    fn_name: &str,
) -> anyhow::Result<()> {
    PgTryBuilder::new(|| {
        Spi::connect_mut(|client| {
            let sql = format!("INSERT INTO {table_name} VALUES ($1, $2)");
            let _ = client.update(&sql, None, &[subject.into(), fn_name.into()])?;

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

pub fn call_function(callback: &str, data: &[u8]) -> Result<(), CallError> {
    if !callback
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return Err(CallError::Other(anyhow::anyhow!(
            "Invalid callback function name"
        )));
    }

    PgTryBuilder::new(|| {
        Spi::connect_mut(|client| {
            let sql = format!("SELECT {callback}($1)");
            let _ = client
                .update(&sql, None, &[data.into()])
                .map_err(|err| CallError::Other(err.into()))?;
            Ok(())
        })
    })
    .catch_others(|e| match e {
        pgrx::pg_sys::panic::CaughtError::PostgresError(err) => {
            if err.sql_error_code() == PgSqlErrorCode::ERRCODE_UNDEFINED_FUNCTION {
                Err(CallError::NotFound)
            } else {
                Err(CallError::Other(anyhow::anyhow!(
                    "Code '{}': {}. ({:?})",
                    err.sql_error_code(),
                    err.message(),
                    err.hint()
                )))
            }
        }
        _ => Err(CallError::Other(anyhow::anyhow!("{:?}", e))),
    })
    .execute()
}
