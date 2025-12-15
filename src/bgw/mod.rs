use pgrx::{
    PgLwLock, PgSharedMemoryInitialization,
    bgworkers::{BackgroundWorkerBuilder, BgWorkerStartTime},
    pg_shmem_init,
    prelude::*,
};

use crate::{bgw::ring_queue::RingQueue, constants::EXTENSION_NAME};

pub mod fdw;
pub mod launcher;
pub mod notification;
pub mod pgrx_wrappers;
pub mod ring_queue;
pub mod subscriber;

pub const SUBSCRIPTIONS_TABLE_NAME: &str = "pgnats.subscriptions";
pub const LAUNCHER_ENTRY_POINT: &str = "background_worker_launcher_entry_point";
pub const SUBSCRIBER_ENTRY_POINT: &str = "background_worker_subscriber_entry_point";

pub const MESSAGE_BUS_SIZE: usize = 0x10000;
pub const DSM_SIZE: usize = MESSAGE_BUS_SIZE >> 3;

extension_sql!(
    r#"
    CREATE SCHEMA IF NOT EXISTS pgnats;

    CREATE TABLE IF NOT EXISTS pgnats.subscriptions (
        subject TEXT NOT NULL,
        fn_oid OID NOT NULL,
        callback TEXT NOT NULL,
        UNIQUE(subject, callback)
    );
    "#,
    name = "create_subscriptions_table",
);

extension_sql!(
    r#"
    CREATE OR REPLACE FUNCTION pgnats.cleanup_subscriptions_on_drop()
    RETURNS event_trigger AS $$
    DECLARE
        obj record;
    BEGIN
        FOR obj IN
            SELECT * FROM pg_event_trigger_dropped_objects()
        LOOP
            IF obj.object_type = 'function' THEN
                DELETE FROM pgnats.subscriptions
                WHERE fn_oid = obj.objid;
            END IF;
        END LOOP;
    END;
    $$ LANGUAGE plpgsql;

    CREATE EVENT TRIGGER pgnats_on_drop_function
    ON sql_drop
    WHEN TAG IN ('DROP FUNCTION')
    EXECUTE FUNCTION pgnats.cleanup_subscriptions_on_drop();
    "#,
    name = "delete_function_from_subscriptions_table",
    requires = ["create_subscriptions_table"]
);

pub static LAUNCHER_MESSAGE_BUS: PgLwLock<RingQueue<MESSAGE_BUS_SIZE>> =
    PgLwLock::new(c"pgnats_launcher_message_bus");

pub fn init_background_worker_launcher() {
    pg_shmem_init!(LAUNCHER_MESSAGE_BUS);

    BackgroundWorkerBuilder::new("PGNats Background Worker Launcher")
        .set_function(LAUNCHER_ENTRY_POINT)
        .set_library(EXTENSION_NAME)
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::ConsistentState)
        .set_restart_time(Some(std::time::Duration::from_secs(20)))
        .set_type("pgnats_bgw_launcher")
        .load();
}
