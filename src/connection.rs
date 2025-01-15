use parking_lot::RwLock;
use regex::Regex;
use std::sync::atomic;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use pgrx::prelude::*;

use nats::jetstream::JetStream;
use nats::jetstream::PublishOptions;
use nats::Connection;

use crate::config::{GUC_HOST, GUC_PORT};
use crate::errors::PgNatsError;
use crate::funcs::get_message;

pub static NATS_CONNECTION: NatsConnection = NatsConnection {
  connection: RwLock::new(None),
  jetstream: RwLock::new(None),
  valid: AtomicBool::new(false),
};

pub struct NatsConnection {
  connection: RwLock<Option<Connection>>,
  jetstream: RwLock<Option<JetStream>>,
  valid: AtomicBool,
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
    if let Some(conn) = self.connection.write().take() {
      ereport!(
        PgLogLevel::INFO,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        get_message(format!("Disconnect from NATS service"))
      );

      conn.close();
    }

    self.valid.store(false, atomic::Ordering::Relaxed);
  }

  fn get_connection(&self) -> Result<Connection, PgNatsError> {
    if !self.valid.load(atomic::Ordering::Relaxed) {
      self.initialize_connection()?;
    }

    Ok(
      self
        .connection
        .read()
        .clone()
        .expect("Invariant error: connection must be created before then"),
    )
  }

  fn get_jetstream(&self) -> Result<JetStream, PgNatsError> {
    if !self.valid.load(atomic::Ordering::Relaxed) {
      self.initialize_connection()?;
    }

    Ok(
      self
        .jetstream
        .read()
        .clone()
        .expect("Invariant error: jetstream must be created before then"),
    )
  }

  fn initialize_connection(&self) -> Result<(), PgNatsError> {
    self.invalidate();
    let mut nats_connection = self.connection.write();
    let mut nats_jetstream = self.jetstream.write();

    let host = GUC_HOST.get().unwrap_or_default().to_string_lossy();
    let port = GUC_PORT.get();

    let connection =
      nats::connect(format!("{0}:{1}", host, port)).map_err(|err| PgNatsError::Connection {
        host: host.to_string(),
        port: port as u16,
        io_error: err,
      })?;

    let jetstream = nats::jetstream::new(connection.clone());

    *nats_connection = Some(connection);
    *nats_jetstream = Some(jetstream);
    self.valid.store(true, atomic::Ordering::Relaxed);

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
