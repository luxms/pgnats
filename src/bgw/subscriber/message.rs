use std::sync::Arc;

use bincode::{Decode, Encode};

use crate::config::Config;

#[derive(Encode, Decode)]
pub enum SubscriberMessage {
    NewConfig { config: Config },
    Subscribe { subject: String, fn_name: String },
    Unsubscribe { subject: String, fn_name: String },
}

pub(super) enum InternalWorkerMessage {
    Subscribe {
        register: bool,
        subject: String,
        fn_name: String,
    },
    Unsubscribe {
        subject: Arc<str>,
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
