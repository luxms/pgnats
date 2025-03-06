use crate::{config::initialize_configuration, ctx::CTX};
use pgrx::prelude::*;

#[pg_guard]
pub extern "C" fn _PG_init() {
  initialize_configuration();

  unsafe {
    pg_sys::on_proc_exit(Some(extension_exit_callback), pg_sys::Datum::from(0));
  }
}

#[pg_guard]
pub extern "C" fn _PG_fini() {
  CTX.with_borrow_mut(|ctx| {
    ctx
      .local_set
      .block_on(&ctx.rt, ctx.nats_connection.invalidate_connection());
  });
}

unsafe extern "C" fn extension_exit_callback(_: i32, _: pg_sys::Datum) {
  CTX.with_borrow_mut(|ctx| {
    ctx
      .local_set
      .block_on(&ctx.rt, ctx.nats_connection.invalidate_connection());
  });
}
