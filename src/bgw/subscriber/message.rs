use std::sync::Arc;

use bincode::{Decode, Encode};

#[derive(Encode, Decode)]
pub enum SubscriberMessage {
    NewConfig,
    Subscribe {
        subject: String,
        fn_name: String,
    },
    Unsubscribe {
        subject: String,
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
