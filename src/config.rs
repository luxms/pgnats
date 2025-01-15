use core::ffi::CStr;

use pgrx::guc::*;
use pgrx::prelude::*;

use crate::connection::NATS_CONNECTION;
use crate::funcs::get_message;

// configs names
static CONFIG_HOST: &str = "nats.host";
static CONFIG_PORT: &str = "nats.port";

// configs values
pub static GUC_HOST: GucSetting<Option<&'static CStr>> =
  GucSetting::<Option<&'static CStr>>::new(Some(c"127.0.0.1"));
pub static GUC_PORT: GucSetting<i32> = GucSetting::<i32>::new(4222);

pub fn initialize_configuration() {
  // initialization of postgres userdef configs
  GucRegistry::define_string_guc(
    CONFIG_HOST,
    "address of rust service",
    "address of rust service",
    &GUC_HOST,
    GucContext::Userset,
    GucFlags::default(),
  );
  GucRegistry::define_int_guc(
    CONFIG_PORT,
    "port of rust service",
    "port of rust service",
    &GUC_PORT,
    0,
    999999,
    GucContext::Userset,
    GucFlags::default(),
  );
  ereport!(
    PgLogLevel::INFO,
    PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
    get_message(format!("initialize_configuration success!"))
  );
}

#[pg_extern]
fn get_config(config_name: &str) -> Option<String> {
  Spi::connect(|client| {
    client
      .select(
        &format!("SELECT current_setting('{0}', true);", config_name),
        None,
        None,
      )?
      .first()
      .get_one()
  })
  .ok()?
}

#[pg_extern]
fn set_config(config_name: &str, config_value: &str) {
  Spi::run(&format!("SET {} = {}", config_name, config_value)).expect(&get_message(format!(
    "Set configuration failed: <{}> -> <{}>",
    config_name, config_value
  )));

  if config_name.to_lowercase().contains("nats.") {
    NATS_CONNECTION.invalidate();
  }
}

#[pg_extern]
fn set_config_string(config_name: &str, config_value: &str) {
  set_config(
    config_name,
    &format!("'{}'", config_value.replace("'", "''")),
  );
}

#[pg_extern]
fn reset_config(config_name: &str) {
  set_config(config_name, "DEFAULT");
}
