use std::{
    ffi::CStr,
    io::{Read, Write},
    net::TcpStream,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PgInstanceTransition {
    M2R,
    R2M,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgInstanceNotification {
    pub transition: PgInstanceTransition,
    pub listen_addresses: Vec<String>,
    pub port: u16,
    pub name: Option<String>,
}

impl PgInstanceNotification {
    pub fn new(transition: PgInstanceTransition) -> Option<Self> {
        let listen_addresses = fetch_config_option(c"listen_addresses")?
            .split(',')
            .map(|s| s.trim())
            .map(|s| s.to_string())
            .collect();

        let port = fetch_config_option(c"port")?.parse::<u16>().ok()?;

        let name = try_fetch_patroni_name();

        Some(Self {
            transition,
            listen_addresses,
            port,
            name,
        })
    }
}

fn fetch_config_option(name: &CStr) -> Option<String> {
    unsafe {
        let value_ptr =
            pgrx::pg_sys::GetConfigOptionByName(name.as_ptr(), std::ptr::null_mut(), true);
        if value_ptr.is_null() {
            return None;
        }

        Some(CStr::from_ptr(value_ptr).to_string_lossy().to_string())
    }
}

fn try_fetch_patroni_name() -> Option<String> {
    let mut stream = TcpStream::connect("127.0.0.1:8008").ok()?;

    let request = b"GET /patroni HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(request).ok()?;
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response).ok()?;

    let body = response.split("\r\n\r\n").nth(1)?;

    let json: serde_json::Value = serde_json::from_str(body).ok()?;

    json.get("patroni")
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
        .map(|s| s.to_string())
}
