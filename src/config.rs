use core::ffi::CStr;
use std::path::PathBuf;

use pgrx::guc::*;

use crate::connection::ConnectionOptions;
use crate::connection::TlsOptions;
use crate::info;

// configs names
pub const CONFIG_HOST: &str = "nats.host";
pub const CONFIG_PORT: &str = "nats.port";
pub const CONFIG_CAPACITY: &str = "nats.capacity";
pub const CONFIG_TLS_CA_PATH: &str = "nats.tls.ca";
pub const CONFIG_TLS_CERT_PATH: &str = "nats.tls.cert";
pub const CONFIG_TLS_KEY_PATH: &str = "nats.tls.key";
pub const CONFIG_SUB_DB_NAME: &str = "nats.sub.dbname";

// configs values
pub static GUC_HOST: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"127.0.0.1"));
pub static GUC_PORT: GucSetting<i32> = GucSetting::<i32>::new(4222);
pub static GUC_CAPACITY: GucSetting<i32> = GucSetting::<i32>::new(128);

pub static GUC_TLS_CA_PATH: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static GUC_TLS_CERT_PATH: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static GUC_TLS_KEY_PATH: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub static GUC_SUB_DB_NAME: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"postgres"));

pub fn initialize_configuration() {
    // initialization of postgres userdef configs
    GucRegistry::define_string_guc(
        CONFIG_HOST,
        "Address of NATS Server",
        "Address of NATS Server",
        &GUC_HOST,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        CONFIG_PORT,
        "Port of NATS Server",
        "Port of NATS Server",
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

    GucRegistry::define_string_guc(
        CONFIG_TLS_CA_PATH,
        "Path to TLS CA certificate",
        "Path to TLS CA certificate",
        &GUC_TLS_CA_PATH,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        CONFIG_TLS_CERT_PATH,
        "Path to TLS certificate",
        "Path to TLS certificate",
        &GUC_TLS_CERT_PATH,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        CONFIG_TLS_KEY_PATH,
        "Path to TLS key",
        "Path to TLS key",
        &GUC_TLS_KEY_PATH,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        CONFIG_SUB_DB_NAME,
        "A database to which all queries from subscriptions will be directed",
        "A database to which all queries from subscriptions will be directed",
        &GUC_SUB_DB_NAME,
        GucContext::Userset,
        GucFlags::default(),
    );

    info!("PGNats has been successfully initialized!");
}

fn fetch_tls_options() -> Option<TlsOptions> {
    let ca = GUC_TLS_CA_PATH.get().and_then(|path| path.to_str().ok())?;

    match (
        GUC_TLS_CERT_PATH.get().and_then(|c| c.to_str().ok()),
        GUC_TLS_KEY_PATH.get().and_then(|c| c.to_str().ok()),
    ) {
        (Some(cert), Some(key)) => Some(TlsOptions::MutualTls {
            ca: PathBuf::from(ca),
            cert: PathBuf::from(cert),
            key: PathBuf::from(key),
        }),
        _ => Some(TlsOptions::Tls {
            ca: PathBuf::from(ca),
        }),
    }
}

pub fn fetch_connection_options() -> ConnectionOptions {
    let tls = fetch_tls_options();

    ConnectionOptions {
        host: GUC_HOST
            .get()
            .map(|host| host.to_string_lossy().to_string())
            .unwrap_or("127.0.0.1".to_string()),
        port: GUC_PORT.get() as u16,
        capacity: GUC_CAPACITY.get() as usize,
        tls,
    }
}
