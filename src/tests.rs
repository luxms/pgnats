// ::pgrx::pg_module_magic!();

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::prelude::pg_schema]
mod tests {
  use crate::api;
  use pgrx::prelude::*;

  #[pg_test]
  fn test_hello_pgnats() {
    assert_eq!("Hello, pgnats!", api::hello_pgnats());
  }
}
