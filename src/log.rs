#[macro_export]
macro_rules! info {
    (context = $ctx:expr, $($msg:tt)*) => {
        $crate::report!(
            context = $ctx,
            pgrx::PgLogLevel::INFO,
            $($msg)*
        )
    };

    ($($msg:tt)*) => {
        $crate::report!(
            pgrx::PgLogLevel::INFO,
            $($msg)*
        )
    };
}

#[macro_export]
macro_rules! log {
    (context = $ctx:expr, $($msg:tt)*) => {
        $crate::report!(
            context = $ctx,
            pgrx::PgLogLevel::LOG,
            $($msg)*
        )
    };

    ($($msg:tt)*) => {
        $crate::report!(
            pgrx::PgLogLevel::LOG,
            $($msg)*
        )
    };
}

#[macro_export]
macro_rules! warn {
    (context = $ctx:expr, $($msg:tt)*) => {
        $crate::report!(
            context = $ctx,
            pgrx::PgLogLevel::WARNING,
            $($msg)*
        )
    };

    ($($msg:tt)*) => {
        $crate::report!(
            pgrx::PgLogLevel::WARNING,
            $($msg)*
        )
    };
}

#[macro_export]
macro_rules! error {
    (context = $ctx:expr, $($msg:tt)*) => {
        $crate::report!(
            context = $ctx,
            pgrx::PgLogLevel::ERROR,
            $($msg)*
        )
    };

    ($($msg:tt)*) => {
        $crate::report!(
            pgrx::PgLogLevel::ERROR,
            $($msg)*
        )
    };
}

#[macro_export]
macro_rules! debug {
    (context = $ctx:expr, $($msg:tt)*) => {
        $crate::report!(
            context = $ctx,
            pgrx::PgLogLevel::DEBUG2,
            $($msg)*
        )
    };

    ($($msg:tt)*) => {
        $crate::report!(
            pgrx::PgLogLevel::DEBUG2,
            $($msg)*
        )
    };
}

#[cfg(not(feature = "pg_test"))]
#[macro_export]
macro_rules! report {
    (context = $ctx:expr, $level:expr, $($msg:tt)*) => {
        pgrx::ereport!(
            $level,
            pgrx::PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            &format!("[PGNATS({})]: {}", $ctx, format!($($msg)*))
        )
    };
    ($level:expr, $($msg:tt)*) => {
        pgrx::ereport!(
            $level,
            pgrx::PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            &format!("[PGNATS]: {}", format!($($msg)*))
        )
    };
}

#[cfg(feature = "pg_test")]
#[macro_export]
macro_rules! report {
    (context = $ctx:expr, $level:expr, $($msg:tt)*) => {
        eprintln!("[PGNATS({})]: {}", $ctx, format!($($msg)*))
    };
    ($level:expr, $($msg:tt)*) => {
        eprintln!("[PGNATS]: {}", format!($($msg)*))
    };
}
