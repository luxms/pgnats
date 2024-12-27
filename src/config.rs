use core::ffi::CStr;
use regex::Regex;

use pgrx::prelude::*;
use pgrx::guc::*;

use nats::Connection;
use nats::jetstream::JetStream;

use crate::funcs::get_message;


static mut NATS_CONNECT: Option<Connection> = None;
static mut NATS_JETSTREAM: Option<JetStream> = None;
static mut NATS_CONNECT_ACTUAL: bool = false;


// configs names
static CONFIG_HOST: &str = "nats.host";
static CONFIG_PORT: &str = "nats.port";

// configs values
static GUC_HOST: GucSetting<Option<&'static CStr>>
             = GucSetting::<Option<&'static CStr>>::new(Some(c"bi9.spb.luxms.com"));
static GUC_PORT: GucSetting<i32> = GucSetting::<i32>::new(4222);


pub fn initialize_configuration() {
  // initialization of postgres userdef configs
  GucRegistry::define_string_guc(
    CONFIG_HOST, 
    "address of rust service", 
    "address of rust service", 
    &GUC_HOST, 
    GucContext::Userset, 
    GucFlags::empty(),
  );
  GucRegistry::define_int_guc(
    CONFIG_PORT, 
    "port of rust service", 
    "port of rust service",
    &GUC_PORT, 
    0, 999999,
    GucContext::Userset, 
    GucFlags::default(),
  );
  ereport!(PgLogLevel::INFO, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, 
    get_message(format!("initialize_configuration success!"))
  );
}


fn init_connection() {
  unsafe {
    if NATS_CONNECT.is_some() {
      ereport!(PgLogLevel::INFO, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, 
        get_message(format!("Disconnect from NATS service"))
      );
      NATS_CONNECT.clone().unwrap().close();
      NATS_CONNECT = None;
    }
    NATS_CONNECT_ACTUAL = false;
    let host = GUC_HOST.get().unwrap().to_str().unwrap();
    let port = GUC_PORT.get();

    NATS_CONNECT = Some(
      nats::connect(format!("{0}:{1}", host, port))
        .expect(&get_message(format!("NATS connection failed: {}:{}", host, port)))
    );

    NATS_JETSTREAM = Some(nats::jetstream::new(NATS_CONNECT.clone().unwrap()));
    NATS_CONNECT_ACTUAL = true;
  }
}

fn get_stream_name_by_subject(subject: String) -> String {
  return Regex::new(r"[.^?]").unwrap().replace_all(
    Regex::new(r"\.[^.]*$").unwrap().replace(subject.as_str(), "").as_ref(), 
    "_",
  ).as_ref().to_owned();
}


pub unsafe fn touch_stream_subject(subject: String) {
  let stream_name = get_stream_name_by_subject(subject.clone());
  let info = get_nats_stream().unwrap().stream_info(stream_name.clone());
  if info.is_ok() {
    let mut subjects = info.ok().unwrap().config.subjects.clone();
    if !subjects.contains(&subject) {
      subjects.push(subject);
      let cfg = nats::jetstream::StreamConfig {
        name: stream_name.clone(),
        subjects: subjects,
        ..Default::default()
      };
      get_nats_stream().unwrap().update_stream(&cfg)
        .expect(&get_message(format!("stream update failed!")));
    }
  }
  else {
    let cfg = nats::jetstream::StreamConfig {
      name: stream_name.clone(),
      subjects: vec![subject],
      ..Default::default()
    };
    get_nats_stream().unwrap().add_stream(cfg)
      .expect(&get_message(format!("stream creating failed!")));

  }
  // show all streams
  // for stream in NATS_JETSTREAM.clone().unwrap().stream_names() {
  //   ereport!(PgLogLevel::INFO, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, 
  //     get_message(format!("stream: {}", stream.unwrap()))
  //   );
  // }
}


unsafe fn is_actual_connection () -> bool {
  return NATS_CONNECT_ACTUAL;
}


pub fn get_nats_connection() -> Option<Connection> {
  unsafe {
    if NATS_CONNECT.is_none() || !is_actual_connection() {
      init_connection();
    }
    return NATS_CONNECT.clone();
  }
}

pub fn get_nats_stream() -> Option<JetStream> {
  unsafe {
    if NATS_CONNECT.is_none() || !is_actual_connection() {
      init_connection();
    }
    return NATS_JETSTREAM.clone();
  }
}


#[pg_extern]
fn get_config(config_name: String) -> Option<String> {
  let name: Result<Option<String>, spi::SpiError> = Spi::connect(|client| {
      return client.select(
        &format!("SELECT current_setting('{0}', true);", config_name), 
        None, 
        None
      )?.first().get_one()
  });
  return name.ok()?;
}


#[pg_extern]
fn set_config(config_name: String, config_value: String) {
  Spi::run(&format!("SET {} = {}", config_name, config_value))
    .expect(&get_message(format!("Set configuration failed: <{}> -> <{}>", config_name, config_value)));
  if config_name.to_lowercase().contains("nats.") {
    unsafe {
      NATS_CONNECT_ACTUAL = false;
    }
  }
}

#[pg_extern]
fn set_config_string(config_name: String, config_value: String) {
  set_config(config_name, format!("'{}'", config_value.replace("'", "''")));
}

#[pg_extern]
fn reset_config(config_name: String) {
  set_config(config_name, "DEFAULT".to_owned());
}
