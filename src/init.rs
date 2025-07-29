use pgrx::prelude::*;

use crate::ctx::CTX;

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    #[cfg(feature = "sub")]
    crate::bgw::init_background_worker_launcher();

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
