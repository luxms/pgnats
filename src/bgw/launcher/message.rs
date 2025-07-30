use bincode::{Decode, Encode};

use crate::config::Config;

#[derive(Encode, Decode)]
pub enum LauncherMessage {
    DbExtensionStatus {
        db_oid: u32,
        contains: bool,
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
}
