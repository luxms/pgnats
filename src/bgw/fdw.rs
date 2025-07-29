use std::ffi::CStr;

use pgrx::{extension_sql, pg_extern, pg_sys as sys};

use crate::{
    bgw::{LAUNCHER_MESSAGE_BUS, WorkerMessage},
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
    name = "create_event_trigger",
    requires = ["create_subscriptions_table"]
);

#[pg_extern]
fn pgnats_fdw_validator(options: Vec<String>, oid: sys::Oid) {
    if oid == sys::ForeignServerRelationId {
        let options = options
            .iter()
            .filter_map(|opt| opt.split_once('='))
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        let options = parse_config(&options);

        let Some(db_name) = get_database_name() else {
            error!("Failed to get Database name");
            return;
        };

        let msg = WorkerMessage::NewConfig {
            config: options,
            db_name,
        };

        if let Ok(buf) = bincode::encode_to_vec(msg, bincode::config::standard()) {
            if LAUNCHER_MESSAGE_BUS.exclusive().try_send(&buf).is_err() {
                error!("Shared queue is full, failed to configurate new connection");
            }
        } else {
            error!("Failed to encode message");
        }
    }
}

fn get_database_name() -> Option<String> {
    let db_name = unsafe {
        let db_name = sys::get_database_name(sys::MyDatabaseId);

        if db_name.is_null() {
            return None;
        }

        CStr::from_ptr(db_name)
    };

    Some(db_name.to_string_lossy().to_string())
}

// #[pg_extern]
// fn pgnats_fdw_handler() -> PgBox<sys::FdwRoutine, AllocatedByRust> {
//     let mut fdwroutine = unsafe {
//         PgBox::<sys::FdwRoutine, AllocatedByRust>::alloc_node(sys::NodeTag::T_FdwRoutine)
//     };

//     fdwroutine.GetForeignRelSize = Some(nats_get_foreign_rel_size);
//     fdwroutine.GetForeignPaths = Some(nats_get_foreign_paths);
//     fdwroutine.GetForeignPlan = Some(nats_get_foreign_plan);
//     fdwroutine.BeginForeignScan = Some(nats_begin_foreign_scan);
//     fdwroutine.IterateForeignScan = Some(nats_iterate_foreign_scan);
//     fdwroutine.ReScanForeignScan = Some(nats_re_scan_foreign_scan);
//     fdwroutine.EndForeignScan = Some(nats_end_foreign_scan);

//     fdwroutine
// }

/*#[pg_guard]
unsafe extern "C-unwind" fn nats_get_foreign_rel_size(
    _root: *mut pg_sys::PlannerInfo,
    _baserel: *mut pg_sys::RelOptInfo,
    _oid: pg_sys::Oid,
) {
    panic!("nats_get_foreign_rel_size is not implemented");
}

#[pg_guard]
unsafe extern "C-unwind" fn nats_get_foreign_paths(
    _root: *mut pg_sys::PlannerInfo,
    _baserel: *mut pg_sys::RelOptInfo,
    _oid: pg_sys::Oid,
) {
    panic!("nats_get_foreign_paths is not implemented");
}

#[pg_guard]
unsafe extern "C-unwind" fn nats_get_foreign_plan(
    _root: *mut pg_sys::PlannerInfo,
    _baserel: *mut pg_sys::RelOptInfo,
    _oid: pg_sys::Oid,
    _best_path: *mut pg_sys::ForeignPath,
    _tlist: *mut pg_sys::List,
    _scan_clauses: *mut pg_sys::List,
    _outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    panic!("nats_get_foreign_plan is not implemented");
}

#[pg_guard]
unsafe extern "C-unwind" fn nats_begin_foreign_scan(
    _node: *mut pg_sys::ForeignScanState,
    _eflags: ::std::os::raw::c_int,
) {
    panic!("nats_begin_foreign_scan is not implemented");
}

#[pg_guard]
unsafe extern "C-unwind" fn nats_iterate_foreign_scan(
    _node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    panic!("nats_iterate_foreign_scan is not implemented");
}

#[pg_guard]
unsafe extern "C-unwind" fn nats_re_scan_foreign_scan(_node: *mut pg_sys::ForeignScanState) {
    panic!("nats_re_scan_foreign_scan is not implemented");
}

#[pg_guard]
unsafe extern "C-unwind" fn nats_end_foreign_scan(_node: *mut pg_sys::ForeignScanState) {
    panic!("nats_end_foreign_scan is not implemented");
}*/
