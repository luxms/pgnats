#[cfg(feature = "sub")]
use pgrx::{pg_extern, pg_sys::Oid};

#[pg_extern]
#[cfg(feature = "sub")]
pub fn __internal_pgnats_notify_foreign_server_drop(db_oid: Oid) -> anyhow::Result<()> {
    if unsafe { pgrx::pg_sys::RecoveryInProgress() } {
        anyhow::bail!("Subscriptions are not allowed in replica mode");
    }

    let msg = crate::bgw::launcher::message::LauncherMessage::ForeignServerDropped {
        db_oid: db_oid.to_u32(),
    };
    let buf = bincode::encode_to_vec(msg, bincode::config::standard())?;

    anyhow::ensure!(
        crate::bgw::LAUNCHER_MESSAGE_BUS
            .exclusive()
            .try_send(&buf)
            .is_ok(),
        "Shared queue is full"
    );

    Ok(())
}
