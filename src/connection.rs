use async_nats::jetstream::kv::Store;
use async_nats::jetstream::Context;
use async_nats::Client;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use pgrx::prelude::*;

use crate::config::{GUC_HOST, GUC_PORT};
use crate::errors::PgNatsError;
use crate::utils::{format_message, get_stream_name_by_subject, FromBytes};

#[derive(Default)]
pub struct NatsConnection {
  connection: RwLock<Option<Arc<Client>>>,
  jetstream: RwLock<Option<Arc<Context>>>,
  cached_buckets: RwLock<HashMap<String, Arc<Store>>>,
}

impl NatsConnection {
  pub async fn publish(
    self: &Arc<Self>,
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
    self: &Arc<Self>,
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

    {
      self.cached_buckets.write().clear();
      let _ = self.jetstream.write().take();
    }

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
          format_message(format!("Failed to drain connection {e}"))
        );
      }
    }
  }

  pub async fn put_value(
    self: &Arc<Self>,
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
    self: &Arc<Self>,
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
    self: &Arc<Self>,
    bucket: impl ToString,
    key: impl AsRef<str>,
  ) -> Result<(), PgNatsError> {
    let bucket = self.get_or_create_bucket(bucket).await?;

    bucket.delete(key).await?;

    Ok(())
  }

  async fn get_connection(self: &Arc<Self>) -> Result<Arc<Client>, PgNatsError> {
    if let Some(client) = &*self.connection.read() {
      return Ok(Arc::clone(client));
    }

    let (connection, _) = self.initialize_connection().await?;

    Ok(connection)
  }

  async fn get_jetstream(self: &Arc<Self>) -> Result<Arc<Context>, PgNatsError> {
    if let Some(jetstream) = &*self.jetstream.read() {
      return Ok(Arc::clone(jetstream));
    }

    let (_, jetstream) = self.initialize_connection().await?;

    Ok(jetstream)
  }

  async fn get_or_create_bucket(
    self: &Arc<Self>,
    bucket: impl ToString,
  ) -> Result<Arc<Store>, PgNatsError> {
    let bucket = bucket.to_string();

    {
      let cached = self.cached_buckets.read();
      if let Some(store) = cached.get(&bucket) {
        return Ok(Arc::clone(store));
      }
    }

    let jetstream = self.get_jetstream().await?;
    let new_store = Arc::new(
      jetstream
        .create_key_value(async_nats::jetstream::kv::Config {
          bucket: bucket.clone(),
          ..Default::default()
        })
        .await?,
    );

    let mut cached = self.cached_buckets.write();
    let _ = cached.insert(bucket, Arc::clone(&new_store));

    Ok(new_store)
  }

  async fn initialize_connection(
    self: &Arc<Self>,
  ) -> Result<(Arc<Client>, Arc<Context>), PgNatsError> {
    self.invalidate_connection().await;

    let host = GUC_HOST.get().unwrap_or_default().to_string_lossy();
    let port = GUC_PORT.get();

    let nats = Arc::clone(self);
    let connection = async_nats::ConnectOptions::new()
      .event_callback(move |event| {
        let nats = Arc::clone(&nats);

        async move {
          if let async_nats::Event::Disconnected = event {
            nats.handle_disconnect().await;
          }
        }
      })
      .client_capacity(1)
      .max_reconnects(Some(1))
      .connect(format!("{0}:{1}", host, port))
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

    let connection = Arc::new(connection);
    let jetstream = Arc::new(jetstream);
    *nats_connection = Some(Arc::clone(&connection));
    *nats_jetstream = Some(Arc::clone(&jetstream));

    Ok((connection, jetstream))
  }

  /// Touch stream by subject
  /// if stream for subject not exists, creat it
  /// if stream for subject exists, but not contains current subject, add subject to config
  async fn touch_stream_subject(
    self: &Arc<Self>,
    subject: impl ToString,
  ) -> Result<Arc<Context>, PgNatsError> {
    let subject = subject.to_string();
    let stream_name = get_stream_name_by_subject(&subject);

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

  async fn handle_disconnect(&self) {
    self.invalidate_connection().await;
  }
}
