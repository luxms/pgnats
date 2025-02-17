use crate::{config::initialize_configuration, ctx::CTX};
use pgrx::prelude::*;

#[pg_guard]
pub extern "C" fn _PG_init() {
  initialize_configuration();

  unsafe {
    pg_sys::before_shmem_exit(Some(extension_exit_callback), pg_sys::Datum::from(0));
  }
}

#[pg_guard]
pub extern "C" fn _PG_fini() {
  CTX.rt().block_on(CTX.nats().invalidate_connection());
}

unsafe extern "C" fn extension_exit_callback(_: i32, _: pg_sys::Datum) {
  CTX.rt().block_on(CTX.nats().invalidate_connection());
}
