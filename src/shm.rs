use std::str::FromStr;

use pgrx::PgLwLock;

use crate::connection::{NatsConnectionOptions, NatsTlsOptions};

pub static WORKER_MESSAGE_QUEUE: PgLwLock<heapless::Deque<WorkerMessage, 128>> =
    PgLwLock::new(c"shmem_deque");

#[derive(Debug)]
pub enum StackNatsTlsOptions {
    Tls {
        ca: heapless::String<64>,
    },
    MutualTls {
        ca: heapless::String<64>,
        cert: heapless::String<64>,
        key: heapless::String<64>,
    },
}

#[derive(Debug)]
pub struct StackNatsConnectionOptions {
    pub host: heapless::String<32>,
    pub port: u16,
    pub capacity: usize,
    pub tls: Option<StackNatsTlsOptions>,
}

#[derive(Debug)]
pub enum WorkerMessage {
    Config {
        name: heapless::String<64>,
    },
    Subscribe {
        opt: StackNatsConnectionOptions,
        subject: heapless::String<64>,
        fn_name: heapless::String<64>,
    },
    Unsubscribe {
        subject: heapless::String<64>,
        fn_name: heapless::String<64>,
    },
}

impl TryFrom<NatsTlsOptions> for StackNatsTlsOptions {
    type Error = ();

    fn try_from(value: NatsTlsOptions) -> Result<Self, Self::Error> {
        match value {
            NatsTlsOptions::Tls { ca } => Ok(Self::Tls {
                ca: heapless::String::from_str(&ca.to_string_lossy())?,
            }),
            NatsTlsOptions::MutualTls { ca, cert, key } => Ok(Self::MutualTls {
                ca: heapless::String::from_str(&ca.to_string_lossy())?,
                cert: heapless::String::from_str(&cert.to_string_lossy())?,
                key: heapless::String::from_str(&key.to_string_lossy())?,
            }),
        }
    }
}

impl TryFrom<NatsConnectionOptions> for StackNatsConnectionOptions {
    type Error = ();

    fn try_from(value: NatsConnectionOptions) -> Result<Self, Self::Error> {
        Ok(Self {
            host: heapless::String::from_str(&value.host)?,
            port: value.port,
            capacity: value.capacity,
            tls: value.tls.map(|tls| tls.try_into()).transpose()?,
        })
    }
}

impl From<StackNatsTlsOptions> for NatsTlsOptions {
    fn from(value: StackNatsTlsOptions) -> Self {
        match value {
            StackNatsTlsOptions::Tls { ca } => NatsTlsOptions::Tls {
                ca: std::path::PathBuf::from(ca.as_str()),
            },
            StackNatsTlsOptions::MutualTls { ca, cert, key } => NatsTlsOptions::MutualTls {
                ca: std::path::PathBuf::from(ca.as_str()),
                cert: std::path::PathBuf::from(cert.as_str()),
                key: std::path::PathBuf::from(key.as_str()),
            },
        }
    }
}

impl From<StackNatsConnectionOptions> for NatsConnectionOptions {
    fn from(value: StackNatsConnectionOptions) -> Self {
        NatsConnectionOptions {
            host: value.host.to_string(),
            port: value.port,
            capacity: value.capacity,
            tls: value.tls.map(|tls| tls.into()),
        }
    }
}
