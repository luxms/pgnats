use std::ptr::null_mut;

use pgrx::pg_sys as sys;

pub fn fetch_database_oids() -> Vec<sys::Oid> {
    unsafe {
        let mut workers = vec![];

        let rel = sys::table_open(sys::DatabaseRelationId, sys::AccessShareLock as _);

        let scan = sys::table_beginscan_catalog(rel, 0, null_mut());

        let mut tup = sys::heap_getnext(scan, sys::ScanDirection::ForwardScanDirection);

        while !tup.is_null() {
            let pgdb = &*(sys::GETSTRUCT(tup) as sys::Form_pg_database);

            if pgdb.datallowconn && !pgdb.datistemplate {
                workers.push(pgdb.oid);
            }

            tup = sys::heap_getnext(scan, sys::ScanDirection::ForwardScanDirection);
        }

        sys::table_endscan(scan);
        sys::table_close(rel, sys::AccessShareLock as _);

        workers
    }
}
