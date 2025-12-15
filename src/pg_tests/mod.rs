mod api_tests;

#[cfg(feature = "sub")]
pub(crate) mod bgw_tests;

#[cfg(feature = "sub")]
mod shm_mq_tests;

mod macros;

#[cfg(any(test, feature = "pg_test"))]
pub(super) fn get_function_oid(name: &str) -> Option<u32> {
    pgrx::Spi::connect(|client| {
        let tuple = client
            .select(&format!("SELECT '{name}'::regproc AS oid"), None, &[])
            .unwrap()
            .first();

        tuple
            .get_by_name::<pgrx::pg_sys::Oid, _>("oid")
            .unwrap()
            .map(|v| v.to_u32())
    })
}
