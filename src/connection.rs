use regex::Regex;
use std::sync::RwLock;
use std::time::Duration;

use pgrx::prelude::*;

use nats::jetstream::JetStream;
use nats::jetstream::PublishOptions;
use nats::Connection;

use crate::config::{GUC_HOST, GUC_PORT};
use crate::errors::PgNatsError;
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
  pub fn publish(&self, message: String, subject: String) -> Result<(), PgNatsError> {
    self
      .get_connection()?
      .publish(subject.as_str(), message.clone())
      .map_err(|err| PgNatsError::PublishIo(err))?;

    Ok(())
  }

  pub fn publish_stream(&self, message: String, subject: String) -> Result<(), PgNatsError> {
    self.touch_stream_subject(subject.clone())?;
    let _ = self
      .get_jetstream()?
      .publish_with_options(
        subject.as_str(),
        message,
        &NatsConnection::get_publish_options(),
      )
      .map_err(|err| PgNatsError::PublishIo(err))?;

    Ok(())
  }

  pub fn invalidate(&self) {
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
  }

  fn get_connection(&self) -> Result<Connection, PgNatsError> {
    if !*self.valid.read().unwrap() {
      self.initialize_connection()?;
    }
    return Ok((*self.connection.read().unwrap()).clone().unwrap());
  }

  fn get_jetstream(&self) -> Result<JetStream, PgNatsError> {
    if !*self.valid.read().unwrap() {
      self.initialize_connection()?;
    }
    return Ok((*self.jetstream.read().unwrap()).clone().unwrap());
  }

  fn initialize_connection(&self) -> Result<(), PgNatsError> {
    self.invalidate();
    let mut nats_connection = self.connection.write().unwrap();
    let mut nats_jetstream = self.jetstream.write().unwrap();

    let host = GUC_HOST.get().unwrap().to_str().unwrap();
    let port = GUC_PORT.get();

    *nats_connection = Some(
      nats::connect(format!("{0}:{1}", host, port)).map_err(|err| PgNatsError::Connection {
        host: host.to_string(),
        port: port as u16,
        io_error: err,
      })?,
    );
    *nats_jetstream = Some(nats::jetstream::new((*nats_connection).clone().unwrap()));
    *self.valid.write().unwrap() = true;

    Ok(())
  }

  /// Touch stream by subject
  /// if stream for subject not exists, creat it
  /// if stream for subject exists, but not contains current subject, add subject to config
  fn touch_stream_subject(&self, subject: String) -> Result<(), PgNatsError> {
    let stream_name = NatsConnection::get_stream_name_by_subject(subject.clone());
    let info = self.get_jetstream()?.stream_info(stream_name.clone());
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
        let _ = self
          .get_jetstream()?
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
      let _ = self
        .get_jetstream()?
        .add_stream(cfg)
        .expect(&get_message(format!("stream creating failed!")));
    }

    Ok(())
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
