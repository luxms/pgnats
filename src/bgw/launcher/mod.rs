use crate::log;

#[pgrx::pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn background_worker_launcher(_arg: pgrx::pg_sys::Datum) {}
