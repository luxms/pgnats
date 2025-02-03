use core::ffi::CStr;

use pgrx::guc::*;
use pgrx::prelude::*;

use crate::utils::format_message;

// configs names
pub const CONFIG_HOST: &str = "nats.host";
pub const CONFIG_PORT: &str = "nats.port";
pub const CONFIG_BUCKET_NAME: &str = "nats.bucket";

// configs values
pub static GUC_HOST: GucSetting<Option<&'static CStr>> =
  GucSetting::<Option<&'static CStr>>::new(Some(c"127.0.0.1"));
pub static GUC_PORT: GucSetting<i32> = GucSetting::<i32>::new(4222);
pub static GUC_BUCKET_NAME: GucSetting<Option<&'static CStr>> =
  GucSetting::<Option<&'static CStr>>::new(Some(c"default"));

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
    1024,
    0xFFFF,
    GucContext::Userset,
    GucFlags::default(),
  );
  

  GucRegistry::define_string_guc(
    CONFIG_BUCKET_NAME,
    "current bucket of rust service",
    "current bucket of rust service",
    &GUC_BUCKET_NAME,
    GucContext::Userset,
    GucFlags::default(),
  );

  ereport!(
    PgLogLevel::INFO,
    PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
    format_message("initialize_configuration success!")
  );
}
