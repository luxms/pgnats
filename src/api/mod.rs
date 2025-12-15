mod conv;
mod nats;

#[macro_use]
mod macros;

pub use nats::*;
use pgrx::{name, pg_extern};

use crate::{config::fetch_config, constants::FDW_EXTENSION_NAME, ctx::CTX};

shadow_rs::shadow!(build);

/// Reloads NATS connection if configuration has changed
///
/// # SQL Usage
/// ```sql
/// -- Reload connection if config changed
/// SELECT pgnats_reload_conf();
///
/// -- Typical usage after configuration changes
/// SET nats.host = 'new.nats.server:4222';
/// SELECT pgnats_reload_conf();
/// ```
#[pg_extern]
pub fn pgnats_reload_conf() {
    let config = fetch_config(FDW_EXTENSION_NAME);
    CTX.with_borrow_mut(|ctx| {
        ctx.rt.block_on(async {
            let res = ctx
                .nats_connection
                .check_and_invalidate_connection(config)
                .await;
            tokio::task::yield_now().await;
            res
        })
    })
}

/// Forces immediate NATS connection reinitialization
///
/// # SQL Usage
/// ```sql
/// -- Force reconnect immediately
/// SELECT pgnats_reload_conf_force();
/// ```
#[pg_extern]
pub fn pgnats_reload_conf_force() {
    CTX.with_borrow_mut(|ctx| {
        ctx.rt.block_on(async {
            let res = ctx.nats_connection.invalidate_connection().await;
            tokio::task::yield_now().await;
            res
        })
    })
}

/// Returns the current crate version, commit date, short commit, branch, and last tag
///
/// # SQL Usage
/// ```sql
/// -- Get info about the extension version
/// SELECT * FROM pgnats_version();
/// ```
#[pg_extern]
pub fn pgnats_version() -> pgrx::iter::TableIterator<
    'static,
    (
        name!(version, String),
        name!(commit_date, String),
        name!(short_commit, String),
        name!(branch, String),
        name!(last_tag, String),
    ),
> {
    pgrx::iter::TableIterator::new([(
        build::PKG_VERSION.to_string(),
        build::COMMIT_DATE.to_string(),
        build::SHORT_COMMIT.to_string(),
        build::BRANCH.to_string(),
        build::LAST_TAG.to_string(),
    )])
}
