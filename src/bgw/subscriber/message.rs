use std::sync::Arc;

use bincode::{Decode, Encode};
use pgrx::pg_sys;

use crate::config::Config;

#[derive(Encode, Decode)]
pub enum SubscriberMessage {
    NewConfig {
        config: Config,
    },
    Subscribe {
        subject: String,
        fn_oid: u32,
        fn_name: String,
    },
    Unsubscribe {
        subject: String,
        fn_oid: u32,
        fn_name: String,
    },
    #[cfg(any(test, feature = "pg_test"))]
    ChangeStatus {
        is_master: bool,
    },
}

pub(super) enum InternalWorkerMessage {
    Subscribe {
        register: bool,
        subject: String,
        fn_oid: pg_sys::Oid,
        fn_name: String,
    },
    Unsubscribe {
        subject: Arc<str>,
        fn_oid: pg_sys::Oid,
        fn_name: Arc<str>,
    },
    CallbackCall {
        subject: Arc<str>,
        data: Arc<[u8]>,
    },
    UnsubscribeSubject {
        subject: Arc<str>,
        reason: String,
    },
}
