pub const MSG_PREFIX: &str = "[PGNATS]";

#[macro_export]
macro_rules! info {
    ($($msg:tt)*) => {
        $crate::report!(
            pgrx::PgLogLevel::INFO,
            $($msg)*
        )
    };
}

#[macro_export]
macro_rules! log {
    ($($msg:tt)*) => {
        $crate::report!(
            pgrx::PgLogLevel::LOG,
            $($msg)*
        )
    };
}

#[macro_export]
macro_rules! warn {
    ($($msg:tt)*) => {
        $crate::report!(
            pgrx::PgLogLevel::WARNING,
            $($msg)*
        )
    };
}

#[macro_export]
macro_rules! error {
    ($($msg:tt)*) => {
        $crate::report!(
            pgrx::PgLogLevel;:ERROR,
            $($msg)*
        )
    };
}

#[cfg(not(feature = "pg_test"))]
#[macro_export]
macro_rules! report {
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
macro_rules! report {
    ($level:expr, $($msg:tt)*) => {
        /* NO OP */
    };
}
