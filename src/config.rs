use std::{borrow::Cow, collections::HashMap, path::PathBuf};

use pgrx::{PgTryBuilder, Spi};

use crate::constants::{DEFAULT_NATS_CAPACITY, DEFAULT_NATS_HOST, DEFAULT_NATS_PORT};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NatsTlsOptions {
    Tls {
        ca: PathBuf,
    },
    MutualTls {
        ca: PathBuf,
        cert: PathBuf,
        key: PathBuf,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NatsConnectionOptions {
    pub host: String,
    pub port: u16,
    pub capacity: usize,
    pub tls: Option<NatsTlsOptions>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub nats_opt: NatsConnectionOptions,
    pub notify_subject: Option<String>,
    pub patroni_url: Option<String>,
}

pub fn fetch_config(fdw_extension_name: &str) -> Config {
    use std::str::FromStr;

    let mut options = HashMap::new();

    let Some(fdw_server_name) = fetch_fdw_server_name(fdw_extension_name) else {
        crate::warn!("Failed to get FDW server name for {fdw_extension_name}");
        return parse_config(&options);
    };

    let Ok(fdw_server_name) = std::ffi::CString::from_str(&fdw_server_name) else {
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
