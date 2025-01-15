mod config;
mod nats;

use pgrx::pg_extern;

pub use config::*;
pub use nats::*;

#[pg_extern]
pub fn hello_pgnats() -> &'static str {
  "Hello, pgnats!"
}

#[pg_extern]
fn nats_init() {
  // Auto run at first call any function at extencion
  // initialize_configuration();
}
