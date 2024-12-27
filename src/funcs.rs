use pgrx::prelude::*;

use crate::config::{get_nats_connection, get_nats_stream, touch_stream_subject};

pub fn get_message(message_text: String) -> String {
  return format!("PGNATS: {}", message_text);
}


#[pg_extern]
pub fn hello_pgnats() -> &'static str {
    "Hello, pgnats!"
}


#[pg_extern]
fn nats_publish(publish_text: String, subject: String) {
  get_nats_connection()
    .unwrap()
    .publish(subject.as_str(), publish_text)
    .expect(&get_message("Exception on publishing message at NATS!".to_owned()));
}


#[pg_extern]
fn nats_publish_stream(publish_text: String, subject: String) {
  unsafe {
    touch_stream_subject(subject.clone());
  }
  let stream = get_nats_stream().unwrap();
  stream.publish(subject.as_str(), publish_text)
    .expect(&get_message("Exception on publishing message at NATS!".to_owned()));
}


#[pg_extern]
fn nats_init() {
  // Auto run at first call any function at extencion
  // initialize_configuration();
}
