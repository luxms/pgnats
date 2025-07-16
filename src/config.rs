use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;

use pgrx::PgList;

use crate::connection::NatsConnectionOptions;
use crate::connection::NatsTlsOptions;

pub fn fetch_connection_options() -> NatsConnectionOptions {
    let name =
        std::env::var("PGNATS_FDW_SERVER_NAME").unwrap_or_else(|_| "nats_fdw_server".to_string());

    let mut options = HashMap::new();

    unsafe {
        let cname = std::ffi::CString::new(name).expect("invalid CString");
        let server = pgrx::pg_sys::GetForeignServerByName(cname.as_ptr(), true);

        if server.is_null() {
            return parse_connection_options(&options);
        }

        let options_list = (*server).options;
        if !options_list.is_null() {
            let list: PgList<pgrx::pg_sys::DefElem> = pgrx::PgList::from_pg(options_list);

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

    parse_connection_options(&options)
}

pub fn parse_connection_options(
    options: &HashMap<Cow<'_, str>, Cow<'_, str>>,
) -> NatsConnectionOptions {
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

    NatsConnectionOptions {
        host,
        port,
        capacity,
        tls,
    }
}
