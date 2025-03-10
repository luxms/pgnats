use async_nats::jetstream::kv::Store;
use async_nats::jetstream::Context;
use async_nats::Client;
use std::collections::HashMap;

use pgrx::prelude::*;

use crate::config::fetch_connection_options;
use crate::errors::PgNatsError;
use crate::utils::{format_message, FromBytes};

#[derive(Default)]
pub struct NatsConnection {
    connection: Option<Client>,
    jetstream: Option<Context>,
    cached_buckets: HashMap<String, Store>,
    current_config: Option<ConnectionOptions>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionOptions {
    pub host: String,
    pub port: u16,
    pub capacity: usize,
}

impl NatsConnection {
    pub fn new(opt: Option<ConnectionOptions>) -> Self {
        Self {
            current_config: opt,
            ..Default::default()
        }
    }

    pub async fn publish(
        &mut self,
        subject: impl ToString,
        message: impl Into<Vec<u8>>,
    ) -> Result<(), PgNatsError> {
        let subject = subject.to_string();
        let message: Vec<u8> = message.into();

        self.get_connection()
            .await?
            .publish(subject, message.into())
            .await?;

        Ok(())
    }

    pub async fn publish_stream(
        &mut self,
        subject: impl ToString,
        message: impl Into<Vec<u8>>,
    ) -> Result<(), PgNatsError> {
        let subject = subject.to_string();
        let message: Vec<u8> = message.into();

        let _ask = self
            .get_jetstream()
            .await?
            .publish(subject, message.into())
            .await?;

        Ok(())
    }

    pub async fn invalidate_connection(&mut self) {
        let connection = { self.connection.take() };

        {
            self.cached_buckets.clear();
            let _ = self.jetstream.take();
            let _ = self.current_config.take();
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

    pub async fn check_and_invalidate_connection(&mut self) {
        let (changed, new_config) = {
            let config = &self.current_config;
            let fetched_config = fetch_connection_options();

            let changed = config.as_ref() != Some(&fetched_config);

            (changed, fetched_config)
        };

        if changed {
            self.invalidate_connection().await;

            self.current_config = Some(new_config);
        }
    }

    pub async fn set_config(&mut self, opt: ConnectionOptions) {
        let (changed, new_config) = {
            let config = &self.current_config;

            let changed = config.as_ref() != Some(&opt);

            (changed, opt)
        };

        if changed {
            self.invalidate_connection().await;

            self.current_config = Some(new_config);
        }
    }

    pub async fn put_value(
        &mut self,
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
        &mut self,
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
        &mut self,
        bucket: impl ToString,
        key: impl AsRef<str>,
    ) -> Result<(), PgNatsError> {
        let bucket = self.get_or_create_bucket(bucket).await?;

        bucket.delete(key).await?;

        Ok(())
    }

    async fn get_connection(&mut self) -> Result<&Client, PgNatsError> {
        if self.connection.is_none() {
            self.initialize_connection().await?;
        }

        Ok(self
            .connection
            .as_ref()
            .expect("unreachable, must be initialized"))
    }

    async fn get_jetstream(&mut self) -> Result<&Context, PgNatsError> {
        if self.connection.is_none() {
            self.initialize_connection().await?;
        }

        Ok(self
            .jetstream
            .as_ref()
            .expect("unreachable, must be initialized"))
    }

    async fn get_or_create_bucket(&mut self, bucket: impl ToString) -> Result<&Store, PgNatsError> {
        let bucket = bucket.to_string();

        if !self.cached_buckets.contains_key(&bucket) {
            let new_store = {
                let jetstream = self.get_jetstream().await?;
                jetstream
                    .create_key_value(async_nats::jetstream::kv::Config {
                        bucket: bucket.clone(),
                        ..Default::default()
                    })
                    .await?
            };

            let _ = self.cached_buckets.insert(bucket.clone(), new_store);
        }

        Ok(self
            .cached_buckets
            .get(&bucket)
            .expect("unreachable, must be initialized"))
    }

    async fn initialize_connection(&mut self) -> Result<(), PgNatsError> {
        let config = self
            .current_config
            .get_or_insert_with(fetch_connection_options);

        let connection = async_nats::ConnectOptions::new()
            .client_capacity(config.capacity)
            .connect(format!("{0}:{1}", config.host, config.port))
            .await
            .map_err(|io_error| PgNatsError::Connection {
                host: config.host.clone(),
                port: config.port,
                io_error,
            })?;

        let mut jetstream = async_nats::jetstream::new(connection.clone());
        jetstream.set_timeout(std::time::Duration::from_secs(5));

        self.connection = Some(connection);
        self.jetstream = Some(jetstream);

        Ok(())
    }

    #[allow(unused, deprecated)]
    #[deprecated]
    async fn touch_stream_subject(
        &mut self,
        subject: impl ToString,
    ) -> Result<&Context, PgNatsError> {
        let subject = subject.to_string();
        let stream_name = crate::utils::get_stream_name_by_subject(&subject);

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
}
