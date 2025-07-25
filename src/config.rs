use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::{CStr, CString},
    path::PathBuf,
};

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, PgTryBuilder, Spi};

use crate::connection::{NatsConnectionOptions, NatsTlsOptions};

pub const CONFIG_SUB_DB_NAME: &CStr = c"pgnats.sub_dbname";
pub static GUC_SUB_DB_NAME: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"pgnats"));

pub const FDW_EXTENSION_NAME: &str = "pgnats_fdw";

const DEFAULT_NATS_HOST: &str = "127.0.0.1";
const DEFAULT_NATS_PORT: u16 = 4222;
const DEFAULT_NATS_CAPACITY: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "sub", derive(bincode::Encode, bincode::Decode))]
pub struct Config {
    pub nats_opt: NatsConnectionOptions,
    pub notify_subject: Option<String>,
    pub patroni_url: Option<String>,
}

pub fn init_guc() {
    GucRegistry::define_string_guc(
        CONFIG_SUB_DB_NAME,
        c"A database to which all queries from subscriptions will be directed",
        c"A database to which all queries from subscriptions will be directed",
        &GUC_SUB_DB_NAME,
        GucContext::Userset,
        GucFlags::default(),
    );
}

#[cfg(not(feature = "pg_test"))]
pub fn fetch_config() -> Config {
    use std::str::FromStr;

    let mut options = HashMap::new();

    let Some(fdw_server_name) = fetch_fdw_server_name(FDW_EXTENSION_NAME) else {
        crate::warn!("Failed to get FDW server name");
        return parse_config(&options);
    };

    let Ok(fdw_server_name) = CString::from_str(&fdw_server_name) else {
        crate::warn!("Failed to parse FDW server name");
        return parse_config(&options);
    };

    unsafe {
        let server = pgrx::pg_sys::GetForeignServerByName(fdw_server_name.as_ptr(), true);

        if server.is_null() {
            return parse_config(&options);
        }

        let options_list = (*server).options;
        if !options_list.is_null() {
            let list: pgrx::PgList<pgrx::pg_sys::DefElem> = pgrx::PgList::from_pg(options_list);

            for def_elem in list.iter_ptr() {
                let key = std::ffi::CStr::from_ptr((*def_elem).defname)
                    .to_string_lossy()
                    .to_string();

                let value = if !(*def_elem).arg.is_null() {
                    let node = (*def_elem).arg;

                    if (*node).type_ == pgrx::pg_sys::NodeTag::T_String {
                        #[cfg(any(feature = "pg13", feature = "pg14"))]
                        let val = (*(node as *mut pgrx::pg_sys::Value)).val.str_;

                        #[cfg(not(any(feature = "pg13", feature = "pg14")))]
                        let val = (*(node as *mut pgrx::pg_sys::String)).sval;

                        std::ffi::CStr::from_ptr(val).to_string_lossy().to_string()
                    } else {
                        continue;
                    }
                } else {
                    continue;
                };

                let _ = options.insert(key.into(), value.into());
            }
        }
    };

    parse_config(&options)
}

#[cfg(feature = "pg_test")]
pub fn fetch_config() -> Config {
    parse_config(&HashMap::new())
}

pub fn parse_config(options: &HashMap<Cow<'_, str>, Cow<'_, str>>) -> Config {
    let host = options
        .get("host")
        .map(|v| v.to_string())
        .unwrap_or_else(|| DEFAULT_NATS_HOST.to_string());

    let port = options
        .get("port")
        .and_then(|port| port.parse::<u16>().ok())
        .unwrap_or(DEFAULT_NATS_PORT);

    let capacity = options
        .get("capacity")
        .and_then(|c| c.parse::<usize>().ok())
        .unwrap_or(DEFAULT_NATS_CAPACITY);

    let tls = if let Some(ca) = options.get("tls_ca_path") {
        let tls_cert_part = options.get("tls_cert_path");
        let tls_key_path = options.get("tls_key_path");

        match (tls_cert_part, tls_key_path) {
            (Some(cert), Some(key)) => Some(NatsTlsOptions::MutualTls {
                ca: PathBuf::from(ca.as_ref()),
                cert: PathBuf::from(cert.as_ref()),
                key: PathBuf::from(key.as_ref()),
            }),
            _ => Some(NatsTlsOptions::Tls {
                ca: PathBuf::from(ca.as_ref()),
            }),
        }
    } else {
        None
    };

    let notify_subject = options.get("notify_subject").map(|v| v.to_string());

    let patroni_url = options.get("patroni_url").map(|v| v.to_string());

    Config {
        nats_opt: NatsConnectionOptions {
            host,
            port,
            capacity,
            tls,
        },
        notify_subject,
        patroni_url,
    }
}

pub fn fetch_fdw_server_name(fdw_name: &str) -> Option<String> {
    PgTryBuilder::new(|| {
        Spi::connect(|conn| {
            let Ok(result) = conn.select(
                "SELECT srv.srvname::text FROM pg_foreign_server srv JOIN pg_foreign_data_wrapper fdw ON srv.srvfdw = fdw.oid WHERE fdw.fdwname = $1;",
                None,
                &[fdw_name.into()],
            ) else {
                return None;
            };

            result.into_iter().filter_map(|tuple| {
                tuple.get_by_name::<String, _>("srvname").ok().flatten()
            }).next()
        })
    })
    .catch_others(|_| None)
    .execute()
}
