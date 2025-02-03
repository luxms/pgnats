mod nats;

use pgrx::pg_extern;

pub use nats::*;

use crate::ctx::CTX;

#[pg_extern]
pub fn hello_pgnats() -> &'static str {
  "Hello, pgnats!"
}

#[pg_extern]
pub fn pgnats_reload_conf() {
  CTX.rt().block_on(CTX.nats().invalidate_connection());
}
