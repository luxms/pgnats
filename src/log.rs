pub const MSG_PREFIX: &str = "[PGNATS]";

#[macro_export]
macro_rules! info {
    ($($msg:tt)*) => {
        $crate::log!(
            pgrx::PgLogLevel::INFO,
            $($msg)*
        )
    };
}

#[macro_export]
macro_rules! warn {
    ($($msg:tt)*) => {
        log!(
            pgrx::PgLogLevel::WARNING,
            $($msg)*
        )
    };
}

#[macro_export]
macro_rules! error {
    ($($msg:tt)*) => {
        log!(
            pgrx::PgLogLevel;:ERROR,
            $($msg)*
        )
    };
}

#[cfg(not(feature = "pg_test"))]
#[macro_export]
macro_rules! log {
    ($level:expr, $($msg:tt)*) => {
        pgrx::ereport!(
            $level,
            pgrx::PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            &format!("{}: {}", $crate::log::MSG_PREFIX, format!($($msg)*))
        )
    };
}

#[cfg(feature = "pg_test")]
#[macro_export]
macro_rules! log {
    ($level:expr, $($msg:tt)*) => {
        /* NO OP */
    };
}
