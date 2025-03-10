use core::ffi::CStr;

use pgrx::guc::*;
use pgrx::prelude::*;

use crate::connection::ConnectionOptions;
use crate::utils::format_message;

// configs names
pub const CONFIG_HOST: &str = "nats.host";
pub const CONFIG_PORT: &str = "nats.port";
pub const CONFIG_CAPACITY: &str = "nats.capacity";

// configs values
pub static GUC_HOST: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"127.0.0.1"));
pub static GUC_PORT: GucSetting<i32> = GucSetting::<i32>::new(4222);
pub static GUC_CAPACITY: GucSetting<i32> = GucSetting::<i32>::new(128);

pub fn initialize_configuration() {
    // initialization of postgres userdef configs
    GucRegistry::define_string_guc(
        CONFIG_HOST,
        "address of NATS Server",
        "address of NATS Server",
        &GUC_HOST,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        CONFIG_PORT,
        "port of NATS Server",
        "port of NATS Server",
        &GUC_PORT,
        1024,
        0xFFFF,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        CONFIG_CAPACITY,
        "Buffer capacity of NATS Client",
        "Buffer capacity of NATS Client",
        &GUC_CAPACITY,
        1,
        0xFFFF,
        GucContext::Userset,
        GucFlags::default(),
    );

    ereport!(
        PgLogLevel::INFO,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        format_message("PGNats has been successfully initialized!")
    );
}

pub fn fetch_connection_options() -> ConnectionOptions {
    ConnectionOptions {
        host: GUC_HOST
            .get()
            .map(|host| host.to_string_lossy().to_string())
            .unwrap_or_default(),
        port: GUC_PORT.get() as u16,
        capacity: GUC_CAPACITY.get() as usize,
    }
}
