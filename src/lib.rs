//! # PGNats
//!
//! Provides seamless integration between PostgreSQL and NATS messaging system,
//! enabling:
//! Provides one-way integration from PostgreSQL to NATS, supporting:
//! - Message publishing to core NATS subjects from SQL
//! - Subscriptions to NATS subjects that invoke PostgreSQL functions on incoming messages
//! - JetStream persistent message streams
//! - Key-Value storage operations from SQL
//! - Object Store operations (uploading, downloading, deleting files) from SQL
//! - Works on Postgres Cluster
//!
//! ### Feature Flags
//!
//! This extension uses a set of feature flags to enable optional functionality while keeping dependencies manageable. By default, **all features are enabled**, providing full integration with NATS and support for HTTP-based communication with [Patroni](https://github.com/zalando/patroni).
//!
//! Below is a list of available feature flags and what they enable:
//! - **kv** - Enables integration with the NATS key-value store. Useful for distributed configuration and metadata storage.
//! - **object_store** - Enables support for the NATS object store, allowing storage and retrieval of large binary blobs.
//! - **sub** - Enables NATS subscription handling.
//!
//! If you need a minimal setup, you can disable default features and enable only the ones required.

::pgrx::pg_module_magic!();

mod pg_tests;

#[cfg(feature = "sub")]
mod fdw;

#[cfg(feature = "sub")]
mod notification;

#[cfg(feature = "sub")]
mod ring_queue;

mod init;
mod log;
mod utils;

pub mod api;

#[doc(hidden)]
#[cfg(feature = "sub")]
pub mod bgw;

#[doc(hidden)]
pub mod config;

#[doc(hidden)]
pub mod nats_client;

#[doc(hidden)]
pub mod constants;

#[doc(hidden)]
pub mod ctx;

#[doc(hidden)]
#[cfg(feature = "sub")]
pub mod worker_queue;

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
