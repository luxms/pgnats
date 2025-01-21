use regex::Regex;
use std::any::Any;
use std::sync::RwLock;
use std::time::Duration;

use pgrx::prelude::*;

use nats::jetstream::JetStream;
use nats::jetstream::PublishOptions;
use nats::Connection;

use crate::config::{GUC_HOST, GUC_PORT};
use crate::funcs::get_message;

pub static NATS_CONNECTION: NatsConnection = NatsConnection {
  connection: RwLock::<Option<Connection>>::new(None),
  jetstream: RwLock::<Option<JetStream>>::new(None),
  valid: RwLock::<bool>::new(false),
};

pub struct NatsConnection {
  connection: RwLock<Option<Connection>>,
  jetstream: RwLock<Option<JetStream>>,
  valid: RwLock<bool>,
}

impl NatsConnection {
  fn catch_panic(&self, result: Result<(), Box<dyn Any + Send>>) {
    match result {
      Ok(_) => {}
      Err(panic) => {
        self.force_unlock();
        std::panic::resume_unwind(panic);
      }
    }
  }

  pub fn publish(&self, message: String, subject: String) {
    self.catch_panic(std::panic::catch_unwind(|| {
      self
        .get_connection()
        .publish(subject.as_str(), message.clone())
        .expect(&get_message(
          "Exception on publishing message at NATS!".to_owned(),
        ));
    }));
  }

  pub fn publish_stream(&self, message: String, subject: String) {
    self.catch_panic(std::panic::catch_unwind(|| {
      self.touch_stream_subject(subject.clone());
      self
        .get_jetstream()
        .publish_with_options(
          subject.as_str(),
          message,
          &NatsConnection::get_publish_options(),
        )
        .expect(&get_message(
          "Exception on publishing message at NATS!".to_owned(),
        ));
    }));
  }

  pub fn invalidate(&self) {
    self.catch_panic(std::panic::catch_unwind(|| {
      if *self.valid.read().unwrap() || (*self.connection.read().unwrap()).clone().is_some() {
        ereport!(
          PgLogLevel::INFO,
          PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
          get_message(format!("Disconnect from NATS service"))
        );
        (*self.connection.read().unwrap()).clone().unwrap().close();
      }
      *self.connection.write().unwrap() = None;
      *self.valid.write().unwrap() = false;
    }));
  }

  fn get_connection(&self) -> Connection {
    if !*self.valid.read().unwrap() {
      self.initialize_connection();
    }
    return (*self.connection.read().unwrap()).clone().unwrap();
  }

  fn get_jetstream(&self) -> JetStream {
    if !*self.valid.read().unwrap() {
      self.initialize_connection();
    }
    return (*self.jetstream.read().unwrap()).clone().unwrap();
  }

  fn force_unlock(&self) {
    if self.connection.is_poisoned() {
      self.connection.clear_poison();
    }
    if self.jetstream.is_poisoned() {
      self.jetstream.clear_poison();
    }
    if self.valid.is_poisoned() {
      self.valid.clear_poison();
    }
  }

  fn initialize_connection(&self) {
    self.invalidate();
    let mut nats_connection = self.connection.write().unwrap();
    let mut nats_jetstream = self.jetstream.write().unwrap();

    let host = GUC_HOST.get().unwrap().to_str().unwrap();
    let port = GUC_PORT.get();

    *nats_connection = Some(
      nats::connect(format!("{0}:{1}", host, port)).expect(&get_message(format!(
        "NATS connection failed: {}:{}",
        host, port
      ))),
    );
    *nats_jetstream = Some(nats::jetstream::new((*nats_connection).clone().unwrap()));
    *self.valid.write().unwrap() = true;
  }

  /// Touch stream by subject
  /// if stream for subject not exists, creat it
  /// if stream for subject exists, but not contains current subject, add subject to config
  fn touch_stream_subject(&self, subject: String) {
    let stream_name = NatsConnection::get_stream_name_by_subject(subject.clone());
    let info = self.get_jetstream().stream_info(stream_name.clone());
    if info.is_ok() {
      // if stream exists
      let mut subjects = info.ok().unwrap().config.subjects.clone();
      if !subjects.contains(&subject) {
        // if not contains current subject
        subjects.push(subject);
        let cfg = nats::jetstream::StreamConfig {
          name: stream_name.clone(),
          subjects: subjects,
          ..Default::default()
        };
        self
          .get_jetstream()
          .update_stream(&cfg)
          .expect(&get_message(format!("stream update failed!")));
      }
    } else {
      // if stream not exists
      let cfg = nats::jetstream::StreamConfig {
        name: stream_name.clone(),
        subjects: vec![subject],
        ..Default::default()
      };
      self
        .get_jetstream()
        .add_stream(cfg)
        .expect(&get_message(format!("stream creating failed!")));
    }
  }

  fn get_publish_options() -> PublishOptions {
    return PublishOptions {
      timeout: Some(Duration::new(5, 0)),
      ..Default::default()
    };
  }

  fn get_stream_name_by_subject(subject: String) -> String {
    return Regex::new(r"[.^?>*]")
      .unwrap()
      .replace_all(
        Regex::new(r"\.[^.]*$")
          .unwrap()
          .replace(subject.as_str(), "")
          .as_ref(),
        "_",
      )
      .as_ref()
      .to_owned();
  }
}
