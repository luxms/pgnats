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
  CTX.with(|ctx| {
    ctx
      .rt()
      .block_on(ctx.nats().check_and_invalidate_connection());
  });
}

#[pg_extern]
pub fn pgnats_reload_conf_force() {
  CTX.with(|ctx| {
    ctx.rt().block_on(ctx.nats().invalidate_connection());
  });
}
