use pgrx::prelude::*;
use nats;

::pgrx::pg_module_magic!();

static mut NATS_CONNECT: Option<nats::Connection> = None;

#[pg_extern]
fn hello_pgnats() -> &'static str {
    "Hello, pgnats!!"
}

#[pg_extern]
fn hello() -> String {
    // Spi::connect(|client| {
    //     let res_tuple = client.select(
    //         "select 101", 
    //         None, 
    //         None
    //     );
    // });
    let pg = hello_pgnats();
    let res = format!("{pg}test");
    return res;
}

#[pg_extern]
fn to_title(text: String) -> String {
    let res = text
        .split(' ')
        .map(|word| {
            word.chars()
                .enumerate()
                .map(|(i, c)| {
                    if i == 0 {
                        c.to_uppercase().to_string()
                    } else {
                        c.to_lowercase().to_string()
                    }
                })
                .collect()
        })
        .collect::<Vec<String>>()
        .join(" ");
    unsafe {
        ereport!(PgLogLevel::INFO, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, 
            format!(
                "result of sending: {}", 
                NATS_CONNECT.clone()
                    .expect("NATS Connection not valid!")
                    .publish("luxmsbi.cdc.audit.events", "test event")
                    .is_ok()
            )
        );
            // .expect("Error on publishing");
    } 
    return format!("Submission {}", res);
}

#[pg_guard]
pub extern "C" fn _PG_init() {
    ereport!(PgLogLevel::INFO, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "this is just a message PGNATS pub");
    unsafe {
        let c = nats::connect("bi9.spb.luxms.com:4222");
        ereport!(PgLogLevel::INFO, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, format!("result of connection: {}", c.is_ok()));
        // NATS_CONNECT = nats::connect("bi9.spb.luxms.com:4222").ok();
        NATS_CONNECT = c.ok();
    }
    // nc.publish("luxmsbi.cdc.audit.events", "test event").expect("Error on publishing");
}




#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_pgnats() {
        assert_eq!("Hello, pgnats", crate::hello_pgnats());
    }

    // #[pg_test]
    // fn test_to_title() {
    //     assert_eq!("Hello, pgnats", crate::to_title("Hello, Pgnats"));
    // }

}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
