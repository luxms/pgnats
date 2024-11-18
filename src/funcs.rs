// ::pgrx::pg_module_magic!();

use pgrx::prelude::*;

use crate::NATS_CONNECT;

pub fn get_message(message_text: String) -> String {
  return format!("PGNATS: {}", message_text);
}

#[pg_extern]
pub fn hello_pgnats() -> &'static str {
    "Hello, pgnats!"
}

#[pg_extern]
fn nats_publish(publish_text: String) {
  
  unsafe {
    let exc = NATS_CONNECT.clone()
                          .expect(&get_message("NATS Connection not valid!".to_string()))
                          .publish("luxmsbi.cdc.audit.events", publish_text)
                          .err();

    if exc.is_none() {
      ereport!(PgLogLevel::INFO, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, 
            get_message("Successful publish message at NATS.".to_string())
        );
    }
    else {
      ereport!(PgLogLevel::ERROR, PgSqlErrorCode::ERRCODE_CONNECTION_EXCEPTION, 
            get_message(format!("Exception on publishing message at NATS: {}", exc.expect("can't read exception")))
        );
    }
  } 
}
