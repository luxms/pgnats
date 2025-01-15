use parking_lot::RwLock;
use regex::Regex;
use std::sync::atomic;
use std::sync::atomic::AtomicBool;
use std::sync::LazyLock;
use std::time::Duration;

use pgrx::prelude::*;

use nats::jetstream::JetStream;
use nats::jetstream::PublishOptions;
use nats::Connection;

use crate::config::{GUC_HOST, GUC_PORT};
use crate::errors::PgNatsError;
use crate::funcs::do_panic_with_message;
use crate::funcs::get_message;

pub static NATS_CONNECTION: NatsConnection = NatsConnection {
  connection: RwLock::new(None),
  jetstream: RwLock::new(None),
  valid: AtomicBool::new(false),
};

static REGEX_STREAM_NAME_LAST_PART: LazyLock<Regex> =
  LazyLock::new(|| Regex::new(r"\.[^.]*$").unwrap());

static REGEX_SPECIAL_SYM: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[.^?>*]").unwrap());

pub struct NatsConnection {
  connection: RwLock<Option<Connection>>,
  jetstream: RwLock<Option<JetStream>>,
  valid: AtomicBool,
}

impl NatsConnection {
  pub fn publish(
    &self,
    message: impl AsRef<[u8]>,
    subject: impl AsRef<str>,
  ) -> Result<(), PgNatsError> {
    self
      .get_connection()?
      .publish(subject.as_ref(), message)
      .map_err(PgNatsError::PublishIo)?;

    Ok(())
  }

  pub fn publish_stream(
    &self,
    message: impl AsRef<[u8]>,
    subject: impl AsRef<str>,
  ) -> Result<(), PgNatsError> {
    let subject = subject.as_ref();

    let _ask = self
      .touch_stream_subject(subject)?
      .publish_with_options(subject, message, &NatsConnection::get_publish_options())
      .map_err(PgNatsError::PublishIo)?;

    Ok(())
  }

  pub fn invalidate(&self) {
    if let Some(conn) = self.connection.write().take() {
      ereport!(
        PgLogLevel::INFO,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        get_message("Disconnect from NATS service")
      );

      conn.close();
    }

    self.valid.store(false, atomic::Ordering::Relaxed);
  }

  fn get_connection(&self) -> Result<Connection, PgNatsError> {
    if !self.valid.load(atomic::Ordering::Relaxed) {
      self.initialize_connection()?;
    }

    Ok(self.connection.read().clone().unwrap_or_else(|| {
      do_panic_with_message("Invariant error: connection must be created before then")
    }))
  }

  fn get_jetstream(&self) -> Result<JetStream, PgNatsError> {
    if !self.valid.load(atomic::Ordering::Relaxed) {
      self.initialize_connection()?;
    }

    Ok(self.jetstream.read().clone().unwrap_or_else(|| {
      do_panic_with_message("Invariant error: jetstream must be created before then")
    }))
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
  fn touch_stream_subject(&self, subject: impl ToString) -> Result<JetStream, PgNatsError> {
    let subject = subject.to_string();
    let stream_name = NatsConnection::get_stream_name_by_subject(&subject);

    let jetstream = self.get_jetstream()?;
    let info = jetstream.stream_info(&stream_name);

    if let Ok(info) = info {
      // if stream exists
      let mut subjects = info.config.subjects;
      if !subjects.contains(&subject) {
        // if not contains current subject
        subjects.push(subject);

        let cfg = nats::jetstream::StreamConfig {
          name: stream_name,
          subjects: subjects,
          ..Default::default()
        };

        let _stream_info = jetstream
          .update_stream(&cfg)
          .map_err(PgNatsError::UpdateStream)?;
      }
    } else {
      // if stream not exists
      let cfg = nats::jetstream::StreamConfig {
        name: stream_name,
        subjects: vec![subject],
        ..Default::default()
      };

      let _stream_info = jetstream
        .add_stream(cfg)
        .map_err(PgNatsError::UpdateStream)?;
    }

    Ok(jetstream)
  }

  fn get_publish_options() -> PublishOptions {
    PublishOptions {
      timeout: Some(Duration::new(5, 0)),
      ..Default::default()
    }
  }

  fn get_stream_name_by_subject(subject: &str) -> String {
    REGEX_SPECIAL_SYM
      .replace_all(
        REGEX_STREAM_NAME_LAST_PART.replace(subject, "").as_ref(),
        "_",
      )
      .to_string()
  }
}
