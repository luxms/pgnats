use std::ffi::CStr;

use serde::{Deserialize, Serialize};

const LISTEN_ADRESSES_GUC_NAME: &CStr = c"listen_addresses";
const PORT_GUC_NAME: &CStr = c"port";

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
    pub fn new(transition: PgInstanceTransition, patroni_url: Option<&str>) -> Option<Self> {
        let listen_addresses = fetch_config_option(LISTEN_ADRESSES_GUC_NAME)?
            .split(',')
            .map(|s| s.trim())
            .map(|s| s.to_string())
            .collect();

        let port = fetch_config_option(PORT_GUC_NAME)?.parse::<u16>().ok()?;

        let name = patroni_url.and_then(try_fetch_patroni_name);

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

fn try_fetch_patroni_name(url: &str) -> Option<String> {
    let result = || {
        let json: serde_json::Value = reqwest::blocking::get(url)?.json()?;
        json.get("patroni")
            .and_then(|p| p.get("name"))
            .and_then(|n| n.as_str())
            .map(|s| s.to_string())
            .ok_or(anyhow::anyhow!("Field name is missing"))
    };

    match result() {
        Ok(ok) => Some(ok),
        Err(err) => Some(err.to_string()),
    }
}
