use pgrx::prelude::*;
use crate::config::initialize_configuration;


#[pg_guard]
pub extern "C" fn _PG_init() {
  initialize_configuration();
}
