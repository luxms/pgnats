::pgrx::pg_module_magic!();

mod config;
mod init;
mod log;
mod pgnats_tests;
mod shared_queue;
mod utils;

/// Main public API for PostgreSQL extensions
///
/// Contains all NATS operations exported to PostgreSQL including:
/// - Message publishing (core NATS and JetStream)
/// - Key-Value store operations
pub mod api;

#[doc(hidden)]
pub mod connection;

#[doc(hidden)]
pub mod ctx;

#[doc(hidden)]
pub mod bg_subscription;

#[doc(hidden)]
pub mod shm;

/// Error handling types
///
/// Defines all error types used in the crate including:
/// - Connection errors
/// - Publishing errors
/// - JetStream errors
/// - KV store operation errors
pub mod errors;

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
