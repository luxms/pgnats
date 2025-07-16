use core::ffi::CStr;
use std::collections::HashMap;
use std::ffi::CString;
use std::path::PathBuf;

use pgrx::guc::*;

use crate::connection::NatsConnectionOptions;
use crate::connection::NatsTlsOptions;
use crate::info;

// configs names
pub const CONFIG_HOST: &CStr = c"nats.host";
pub const CONFIG_PORT: &CStr = c"nats.port";
pub const CONFIG_CAPACITY: &CStr = c"nats.capacity";
pub const CONFIG_TLS_CA_PATH: &CStr = c"nats.tls.ca";
pub const CONFIG_TLS_CERT_PATH: &CStr = c"nats.tls.cert";
pub const CONFIG_TLS_KEY_PATH: &CStr = c"nats.tls.key";

// configs values
pub static GUC_HOST: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"127.0.0.1"));
pub static GUC_PORT: GucSetting<i32> = GucSetting::<i32>::new(4222);
pub static GUC_CAPACITY: GucSetting<i32> = GucSetting::<i32>::new(128);

pub static GUC_TLS_CA_PATH: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
pub static GUC_TLS_CERT_PATH: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub static GUC_TLS_KEY_PATH: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

pub fn initialize_configuration() {
    // initialization of postgres userdef configs
    GucRegistry::define_string_guc(
        CONFIG_HOST,
        c"Address of NATS Server",
        c"Address of NATS Server",
        &GUC_HOST,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        CONFIG_PORT,
        c"Port of NATS Server",
        c"Port of NATS Server",
        &GUC_PORT,
        1024,
        0xFFFF,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        CONFIG_CAPACITY,
        c"Buffer capacity of NATS Client",
        c"Buffer capacity of NATS Client",
        &GUC_CAPACITY,
        1,
        0xFFFF,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        CONFIG_TLS_CA_PATH,
        c"Path to TLS CA certificate",
        c"Path to TLS CA certificate",
        &GUC_TLS_CA_PATH,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        CONFIG_TLS_CERT_PATH,
        c"Path to TLS certificate",
        c"Path to TLS certificate",
        &GUC_TLS_CERT_PATH,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        CONFIG_TLS_KEY_PATH,
        c"Path to TLS key",
        c"Path to TLS key",
        &GUC_TLS_KEY_PATH,
        GucContext::Userset,
        GucFlags::default(),
    );

    info!("PGNats has been successfully initialized!");
}

fn fetch_tls_options() -> Option<NatsTlsOptions> {
    let ca = GUC_TLS_CA_PATH
        .get()
        .and_then(|path| path.into_string().ok())?;

    match (
        GUC_TLS_CERT_PATH.get().and_then(|c| c.into_string().ok()),
        GUC_TLS_KEY_PATH.get().and_then(|c| c.into_string().ok()),
    ) {
        (Some(cert), Some(key)) => Some(NatsTlsOptions::MutualTls {
            ca: PathBuf::from(ca),
            cert: PathBuf::from(cert),
            key: PathBuf::from(key),
        }),
        _ => Some(NatsTlsOptions::Tls {
            ca: PathBuf::from(ca),
        }),
    }
}

pub fn fetch_connection_options() -> NatsConnectionOptions {
    let tls = fetch_tls_options();

    NatsConnectionOptions {
        host: GUC_HOST
            .get()
            .map(|host| host.to_string_lossy().to_string())
            .unwrap_or("127.0.0.1".to_string()),
        port: GUC_PORT.get() as u16,
        capacity: GUC_CAPACITY.get() as usize,
        tls,
    }
}

pub fn parse_connection_options(options: &[String]) -> NatsConnectionOptions {
    let options = options
        .iter()
        .filter_map(|opt| opt.split_once('='))
        .collect::<HashMap<_, _>>();

    let host = options
        .get("host")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string());

    let port = options
        .get("port")
        .and_then(|port| port.parse::<u16>().ok())
        .unwrap_or(4222);

    let capacity = options
        .get("capacity")
        .and_then(|c| c.parse::<usize>().ok())
        .unwrap_or(128);

    let tls = if let Some(ca) = options.get("tls_ca_path") {
        let tls_cert_part = options.get("tls_cert_path");
        let tls_key_path = options.get("tls_key_path");

        match (tls_cert_part, tls_key_path) {
            (Some(cert), Some(key)) => Some(NatsTlsOptions::MutualTls {
                ca: PathBuf::from(ca),
                cert: PathBuf::from(cert),
                key: PathBuf::from(key),
            }),
            _ => Some(NatsTlsOptions::Tls {
                ca: PathBuf::from(ca),
            }),
        }
    } else {
        None
    };

    NatsConnectionOptions {
        host,
        port,
        capacity,
        tls,
    }
}
