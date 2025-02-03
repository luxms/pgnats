use async_nats::jetstream::kv::Store;
use async_nats::jetstream::Context;
use async_nats::Client;
use parking_lot::RwLock;
use regex::Regex;
use std::collections::HashMap;
use std::sync::atomic;
use std::sync::atomic::AtomicBool;
use std::sync::LazyLock;

use pgrx::prelude::*;

use crate::config::{GUC_HOST, GUC_PORT};
use crate::errors::PgNatsError;
use crate::utils::{do_panic_with_message, format_message, FromBytes};

static REGEX_STREAM_NAME_LAST_PART: LazyLock<Regex> =
  LazyLock::new(|| Regex::new(r"\.[^.]*$").expect("Wrong regex"));

static REGEX_SPECIAL_SYM: LazyLock<Regex> =
  LazyLock::new(|| Regex::new(r"[.^?>*]").expect("Wrong regex"));

#[derive(Default)]
pub struct NatsConnection {
  connection: RwLock<Option<Client>>,
  jetstream: RwLock<Option<Context>>,
  cached_buckets: RwLock<HashMap<String, Store>>,
  valid: AtomicBool,
}

impl NatsConnection {
  pub async fn publish(
    &self,
    message: impl Into<Vec<u8>>,
    subject: impl ToString,
  ) -> Result<(), PgNatsError> {
    let subject = subject.to_string();
    let message: Vec<u8> = message.into();
    let connection = self.get_connection().await?;

    connection.publish(subject, message.into()).await?;

    connection.flush().await?;

    Ok(())
  }

  pub async fn publish_stream(
    &self,
    message: impl Into<Vec<u8>>,
    subject: impl ToString,
  ) -> Result<(), PgNatsError> {
    let subject = subject.to_string();
    let message: Vec<u8> = message.into();

    let _ask = self
      .touch_stream_subject(subject.clone())
      .await?
      .publish(subject, message.into())
      .await?;

    self.get_connection().await?.flush().await?;

    Ok(())
  }

  pub async fn invalidate_connection(&self) {
    let connection = { self.connection.write().take() };

    if let Some(conn) = connection {
      ereport!(
        PgLogLevel::INFO,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        format_message("Disconnect from NATS service")
      );

      if let Err(e) = conn.drain().await {
        ereport!(
          PgLogLevel::WARNING,
          PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
          format_message(format!("Failed to close connection {e}"))
        );
      }
    }

    self.valid.store(false, atomic::Ordering::Relaxed);
  }

  pub async fn put_value(
    &self,
    bucket: impl ToString,
    key: impl AsRef<str>,
    data: impl Into<Vec<u8>>,
  ) -> Result<(), PgNatsError> {
    let bucket = self.get_or_create_bucket(bucket).await?;
    let data: Vec<u8> = data.into();

    let _version = bucket.put(key, data.into()).await?;

    Ok(())
  }

  pub async fn get_value<T: FromBytes>(
    &self,
    bucket: impl ToString,
    key: impl Into<String>,
  ) -> Result<Option<T>, PgNatsError> {
    let bucket = self.get_or_create_bucket(bucket).await?;

    bucket
      .get(key)
      .await?
      .map(|d| d.to_vec())
      .map(T::from_bytes)
      .transpose()
  }

  pub async fn delete_value(
    &self,
    bucket: impl ToString,
    key: impl AsRef<str>,
  ) -> Result<(), PgNatsError> {
    let bucket = self.get_or_create_bucket(bucket).await?;

    bucket.delete(key).await?;

    Ok(())
  }

  async fn get_connection(&self) -> Result<Client, PgNatsError> {
    if !self.valid.load(atomic::Ordering::Relaxed) {
      self.initialize_connection().await?;
    }

    Ok(self.connection.read().clone().unwrap_or_else(|| {
      do_panic_with_message("Invariant error: connection must be created before then")
    }))
  }

  async fn get_jetstream(&self) -> Result<Context, PgNatsError> {
    if !self.valid.load(atomic::Ordering::Relaxed) {
      self.initialize_connection().await?;
    }

    Ok(self.jetstream.read().clone().unwrap_or_else(|| {
      do_panic_with_message("Invariant error: jetstream must be created before then")
    }))
  }

  async fn get_or_create_bucket(&self, bucket: impl ToString) -> Result<Store, PgNatsError> {
    let bucket = bucket.to_string();

    {
      let cached = self.cached_buckets.read();
      if let Some(store) = cached.get(&bucket) {
        return Ok(store.clone());
      }
    }

    let jetstream = self.get_jetstream().await?;
    let new_store = jetstream
      .create_key_value(async_nats::jetstream::kv::Config {
        bucket: bucket.clone(),
        ..Default::default()
      })
      .await?;

    let mut cached = self.cached_buckets.write();
    let _ = cached.insert(bucket, new_store.clone());

    Ok(new_store)
  }

  async fn initialize_connection(&self) -> Result<(), PgNatsError> {
    self.invalidate_connection().await;

    let host = GUC_HOST.get().unwrap_or_default().to_string_lossy();
    let port = GUC_PORT.get();

    let connection = async_nats::connect(format!("{0}:{1}", host, port))
      .await
      .map_err(|io_error| PgNatsError::Connection {
        host: host.to_string(),
        port: port as u16,
        io_error,
      })?;

    let mut jetstream = async_nats::jetstream::new(connection.clone());
    jetstream.set_timeout(std::time::Duration::from_secs(5));

    let mut nats_connection = self.connection.write();
    let mut nats_jetstream = self.jetstream.write();

    *nats_connection = Some(connection);
    *nats_jetstream = Some(jetstream);
    self.valid.store(true, atomic::Ordering::Relaxed);

    Ok(())
  }

  /// Touch stream by subject
  /// if stream for subject not exists, creat it
  /// if stream for subject exists, but not contains current subject, add subject to config
  async fn touch_stream_subject(&self, subject: impl ToString) -> Result<Context, PgNatsError> {
    let subject = subject.to_string();
    let stream_name = NatsConnection::get_stream_name_by_subject(&subject);

    let jetstream = self.get_jetstream().await?;
    let info = jetstream.get_stream(&stream_name).await;

    if let Ok(mut info) = info {
      // if stream exists
      let info = info.info().await?;

      let mut subjects = info.config.subjects.clone();
      if !subjects.contains(&subject) {
        // if not contains current subject
        subjects.push(subject);

        let cfg = async_nats::jetstream::stream::Config {
          name: stream_name,
          subjects: subjects,
          ..Default::default()
        };

        let _stream_info = jetstream.update_stream(&cfg).await?;
      }
    } else {
      // if stream not exists
      let cfg = async_nats::jetstream::stream::Config {
        name: stream_name,
        subjects: vec![subject],
        ..Default::default()
      };

      let _stream_info = jetstream.create_stream(cfg).await?;
    }

    Ok(jetstream)
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
