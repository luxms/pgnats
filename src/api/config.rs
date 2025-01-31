use pgrx::prelude::*;

use crate::{
  config::{CONFIG_BUCKET_NAME, CONFIG_HOST, CONFIG_PORT},
  ctx::CTX,
  utils::do_panic_with_message,
};

#[pg_extern]
fn get_config(config_name: &str) -> Option<String> {
  Spi::connect(|client| {
    client
      .select(
        &format!("SELECT current_setting('{0}', true);", config_name),
        None,
        None,
      )?
      .first()
      .get_one()
  })
  .ok()?
}

#[pg_extern]
fn set_config(config_name: &str, config_value: &str) {
  Spi::run(&format!("SET {} = {}", config_name, config_value)).unwrap_or_else(|_| {
    do_panic_with_message(format!(
      "Set configuration failed: <{}> -> <{}>",
      config_name, config_value
    ))
  });

  match config_name {
    CONFIG_HOST | CONFIG_PORT => {
      CTX.rt().block_on(async {
        CTX.nats().invalidate_bucket().await;
        CTX.nats().invalidate_connection().await;
      });
    }
    CONFIG_BUCKET_NAME => CTX.rt().block_on(CTX.nats().invalidate_bucket()),
    _ => {}
  }
}

#[pg_extern]
fn set_config_string(config_name: &str, config_value: &str) {
  set_config(
    config_name,
    &format!("'{}'", config_value.replace("'", "''")),
  );
}

#[pg_extern]
fn reset_config(config_name: &str) {
  set_config(config_name, "DEFAULT");
}
