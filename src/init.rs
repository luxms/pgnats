use crate::config::initialize_configuration;
use pgrx::prelude::*;

#[pg_guard]
pub extern "C" fn _PG_init() {
  initialize_configuration();
}
