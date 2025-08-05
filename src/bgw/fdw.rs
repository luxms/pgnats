use pgrx::{PgLwLock, extension_sql, pg_extern, pg_sys as sys};

use crate::{
    bgw::{
        LAUNCHER_MESSAGE_BUS,
        launcher::{message::LauncherMessage, send_message_to_launcher_with_retry},
        ring_queue::RingQueue,
    },
    config::parse_config,
    error,
};

extension_sql!(
    r#"
    CREATE FOREIGN DATA WRAPPER pgnats_fdw VALIDATOR pgnats_fdw_validator;
    -- CREATE SERVER nats_fdw_server FOREIGN DATA WRAPPER pgnats_fdw OPTIONS (host 'localhost', port '4222');
    "#,
    name = "create_fdw",
    requires = [pgnats_fdw_validator]
);

extension_sql!(
    r#"
    CREATE FUNCTION pgnats.enforce_single_pgnats_fdw_server()
    RETURNS event_trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE
        fdw_count int;
    BEGIN
        SELECT COUNT(*) INTO fdw_count
        FROM pg_foreign_server s
        JOIN pg_foreign_data_wrapper f ON f.oid = s.srvfdw
        WHERE f.fdwname = 'pgnats_fdw';

        IF fdw_count > 1 THEN
            RAISE EXCEPTION 'Only one server with FDW pgnats_fdw is allowed.';
        END IF;
    END;
    $$;

    CREATE EVENT TRIGGER enforce_single_pgnats_fdw_server_trigger
    ON ddl_command_end
    WHEN TAG IN ('CREATE SERVER')
    EXECUTE FUNCTION pgnats.enforce_single_pgnats_fdw_server();
    "#,
    name = "create_event_trigger_for_enforce_single_pgnats_fdw_server",
    requires = ["create_subscriptions_table"]
);

extension_sql!(
    r#"
    CREATE OR REPLACE FUNCTION pgnats.on_pgnats_server_drop()
    RETURNS event_trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE
        obj RECORD;
        fdw_count INTEGER;
        db_oid OID := (SELECT oid FROM pg_database WHERE datname = current_database());
    BEGIN
        FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
        LOOP
            IF obj.object_type = 'server' THEN
                SELECT COUNT(*) INTO fdw_count
                FROM pg_foreign_server s
                JOIN pg_foreign_data_wrapper f ON f.oid = s.srvfdw
                WHERE f.fdwname = 'pgnats_fdw';

                IF fdw_count = 0 THEN
                    PERFORM __internal_pgnats_notify_foreign_server_drop(db_oid);
                END IF;

                EXIT;
            END IF;
        END LOOP;
    END;
    $$;

    CREATE EVENT TRIGGER pgnats_fdw_server_drop_trigger
    ON sql_drop
    WHEN TAG IN ('DROP SERVER')
    EXECUTE FUNCTION pgnats.on_pgnats_server_drop();
    "#,
    name = "create_event_trigger_for_handling_dropped_foreign_server",
    requires = ["create_subscriptions_table"]
);

#[pg_extern]
fn pgnats_fdw_validator(options: Vec<String>, oid: sys::Oid) {
    fdw_validator(&LAUNCHER_MESSAGE_BUS, options, oid);
}

pub fn fdw_validator<const N: usize>(
    launcher_bus: &PgLwLock<RingQueue<N>>,
    options: Vec<String>,
    oid: sys::Oid,
) {
    if oid == sys::ForeignServerRelationId {
        let options = options
            .iter()
            .filter_map(|opt| opt.split_once('='))
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        let options = parse_config(&options);

        if let Err(err) = send_message_to_launcher_with_retry(
            launcher_bus,
            LauncherMessage::NewConfig {
                config: options,
                db_oid: unsafe { sys::MyDatabaseId }.to_u32(),
            },
            5,
            std::time::Duration::from_secs(1),
        ) {
            error!("{err}");
        }
    }
}
