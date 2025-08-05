use bincode::{Decode, Encode};

use crate::config::Config;

#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq)]
pub enum ExtensionStatus {
    Exist,
    NoExtension,
    NoForeignServer,
}

#[derive(Debug, Encode, Decode)]
pub enum LauncherMessage {
    DbExtensionStatus {
        db_oid: u32,
        status: ExtensionStatus,
    },
    NewConfig {
        db_oid: u32,
        config: Config,
    },
    Subscribe {
        db_oid: u32,
        subject: String,
        fn_name: String,
    },
    Unsubscribe {
        db_oid: u32,
        subject: String,
        fn_name: String,
    },
    SubscriberExit {
        db_oid: u32,
        reason: Result<(), String>,
    },
    ForeignServerDropped {
        db_oid: u32,
    },
    #[cfg(any(test, feature = "pg_test"))]
    ChangeStatus {
        db_oid: u32,
        master: bool,
    },
}
