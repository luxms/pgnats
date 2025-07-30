use pgrx::prelude::*;

use crate::{config::init_guc, ctx::CTX};

#[cfg(feature = "sub")]
pub const SUBSCRIPTIONS_TABLE_NAME: &str = "pgnats.subscriptions";

#[cfg(feature = "sub")]
extension_sql!(
    r#"
    CREATE SCHEMA IF NOT EXISTS pgnats;

    CREATE TABLE IF NOT EXISTS pgnats.subscriptions (
        subject TEXT NOT NULL,
        callback TEXT NOT NULL,
        UNIQUE(subject, callback)
    );
    "#,
    name = "create_subscriptions_table",
);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    init_guc();

    #[cfg(feature = "sub")]
    init_background_worker();

    unsafe {
        pg_sys::on_proc_exit(Some(extension_exit_callback), pg_sys::Datum::from(0));
    }
}

#[pg_guard]
pub extern "C-unwind" fn _PG_fini() {
    CTX.with_borrow_mut(|ctx| {
        ctx.rt.block_on(async {
            let res = ctx.nats_connection.invalidate_connection().await;
            tokio::task::yield_now().await;
            res
        })
    })
}

unsafe extern "C-unwind" fn extension_exit_callback(_: i32, _: pg_sys::Datum) {
    CTX.with_borrow_mut(|ctx| {
        ctx.rt.block_on(async {
            let res = ctx.nats_connection.invalidate_connection().await;
            tokio::task::yield_now().await;
            res
        })
    })
}

#[cfg(feature = "sub")]
fn init_background_worker() {
    use pgrx::{bgworkers::*, pg_shmem_init, shmem::*};

    pg_shmem_init!(crate::worker_queue::WORKER_MESSAGE_QUEUE);

    BackgroundWorkerBuilder::new("Background Worker Subscribtion")
        .set_function("background_worker_subscriber")
        .set_library("pgnats")
        .set_restart_time(Some(std::time::Duration::from_secs(1)))
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::ConsistentState)
        .load();
}
