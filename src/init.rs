// ::pgrx::pg_module_magic!();

use pgrx::prelude::*;
use nats;

use crate::NATS_CONNECT;
use crate::funcs::get_message;


#[pg_guard]
pub extern "C" fn _PG_init() {
    unsafe {
        let c = nats::connect("bi9.spb.luxms.com:4222");
        ereport!(PgLogLevel::INFO, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, 
          get_message(format!("result of connection: {}", c.is_ok()))
        );
        NATS_CONNECT = c.ok();
    }
}
