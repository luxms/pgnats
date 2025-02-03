use crate::{config::initialize_configuration, ctx::CTX};
use pgrx::prelude::*;

#[pg_guard]
pub extern "C" fn _PG_init() {
  initialize_configuration();
}

#[pg_guard]
pub extern "C" fn _PG_fini() {
  CTX.rt().block_on(CTX.nats().invalidate_connection());
}
