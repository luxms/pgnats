mod nats;

#[macro_use]
mod macros;

use pgrx::pg_extern;

pub use nats::*;

use crate::ctx::CTX;

#[pg_extern]
pub fn hello_pgnats() -> &'static str {
    "Hello, pgnats!"
}

#[pg_extern]
pub fn pgnats_reload_conf() {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set.block_on(
            &ctx.rt,
            ctx.nats_connection.check_and_invalidate_connection(),
        );
    });
}

#[pg_extern]
pub fn pgnats_reload_conf_force() {
    CTX.with_borrow_mut(|ctx| {
        ctx.local_set
            .block_on(&ctx.rt, ctx.nats_connection.invalidate_connection());
    });
}
