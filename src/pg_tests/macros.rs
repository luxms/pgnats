#[macro_export]
macro_rules! cstr {
    ($s:expr) => (
        concat!($s, "\0") as *const str as *const [::std::os::raw::c_char] as *const ::std::os::raw::c_char
    )
}

#[macro_export]
macro_rules! generate_test_background_worker {
    ($n:literal, $launcher_name:expr, $result_name:expr, $sql_ext_name:literal, $sql:literal) => {
        ::paste::paste! {
            #[allow(non_upper_case_globals)]
            pub(super) static [<LAUNCHER_MESSAGE_BUS $n>]: pgrx::PgLwLock<crate::bgw::ring_queue::RingQueue<1024>> =
                pgrx::PgLwLock::new($launcher_name);

            #[allow(non_upper_case_globals)]
            pub(super) static [<TEST_RESULT $n>]: pgrx::PgLwLock<u64> =
                pgrx::PgLwLock::new($result_name);

            #[pgrx::pg_guard]
            #[unsafe(no_mangle)]
            pub extern "C-unwind" fn [<background_worker_launcher_entry_point_test_ $n>](
                _arg: pgrx::pg_sys::Datum,
            ) {
                use crate::{bgw::launcher::background_worker_launcher_main, warn};

                if let Err(err) = background_worker_launcher_main(
                    &[<LAUNCHER_MESSAGE_BUS $n>],
                    concat!("background_worker_subscriber_entry_point_test_", stringify!($n)),
                ) {
                    warn!("Launcher worker exited with error: {}", err);
                }
            }

            #[pgrx::pg_guard]
            #[unsafe(no_mangle)]
            pub extern "C-unwind" fn [<background_worker_subscriber_entry_point_test_ $n>](
                arg: pgrx::pg_sys::Datum,
            ) {
                use crate::{
                    bgw::subscriber::background_worker_subscriber_main,
                    utils::unpack_i64_to_oid_dsmh,
                    warn,
                };

                use pgrx::{FromDatum, pg_sys as sys};

                let arg = unsafe {
                    i64::from_polymorphic_datum(arg, false, sys::INT8OID)
                        .expect("Subscriber: failed to extract i64 argument from Datum")
                };

                let (db_oid, dsmh) = unpack_i64_to_oid_dsmh(arg);

                if let Err(err) = background_worker_subscriber_main(
                    &[<LAUNCHER_MESSAGE_BUS $n>],
                    concat!("test_background_worker_sub_call_unsub_call_", stringify!($n)),
                    concat!("pgnats_fdw_test_", stringify!($n)),
                    db_oid,
                    dsmh,
                ) {
                    warn!(
                        context = format!("Database OID {db_oid}"),
                        "Subscriber worker exited with error: {}", err
                    );
                }
            }

            pgrx::extension_sql!(
                $sql,
                name = $sql_ext_name,
                requires = []
            );

            #[pgrx::pg_extern]
            pub fn [<test_ $n _fn>](bytes: Vec<u8>) {
                use std::hash::{DefaultHasher, Hasher};

                let mut hasher = DefaultHasher::new();
                hasher.write(&bytes);

                let hash = hasher.finish();

                *[<TEST_RESULT $n>].exclusive() = hash;
            }
        }
    };
}