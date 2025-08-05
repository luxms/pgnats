#[cfg(feature = "sub")]
use pgrx::{pg_extern, pg_sys::Oid};

#[pg_extern]
#[cfg(feature = "sub")]
pub fn __internal_pgnats_notify_foreign_server_drop(db_oid: Oid) {
    use crate::bgw::launcher::send_message_to_launcher_with_retry;

    send_message_to_launcher_with_retry(
        &crate::bgw::LAUNCHER_MESSAGE_BUS,
        crate::bgw::launcher::message::LauncherMessage::ForeignServerDropped {
            db_oid: db_oid.to_u32(),
        },
        5,
        std::time::Duration::from_secs(1),
    )
    .expect("Failed to send ForeignServerDropped message to launcher")
}
