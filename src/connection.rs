use async_nats::jetstream::kv::Store;
use async_nats::jetstream::object_store::{ObjectInfo, ObjectStore};
use async_nats::jetstream::Context;
use async_nats::{Client, Request};
use bincode::{Decode, Encode};
use futures::StreamExt;
use pgrx::warning;
use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, BufReader};

use crate::config::fetch_connection_options;
use crate::errors::PgNatsError;
use crate::info;
use crate::utils::{extract_headers, FromBytes, ToBytes};

#[derive(Default)]
pub struct NatsConnection {
    connection: Option<Client>,
    jetstream: Option<Context>,
    cached_buckets: HashMap<String, Store>,
    cached_object_stores: HashMap<String, ObjectStore>,
    current_config: Option<NatsConnectionOptions>,
}

#[derive(Clone, Debug, PartialEq, Eq, Decode, Encode)]
pub enum NatsTlsOptions {
    Tls {
        ca: PathBuf,
    },
    MutualTls {
        ca: PathBuf,
        cert: PathBuf,
        key: PathBuf,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Decode, Encode)]
pub struct NatsConnectionOptions {
    pub host: String,
    pub port: u16,
    pub capacity: usize,
    pub tls: Option<NatsTlsOptions>,
}

impl NatsConnection {
    pub fn new(opt: Option<NatsConnectionOptions>) -> Self {
        Self {
            current_config: opt,
            ..Default::default()
        }
    }

    pub async fn publish(
        &mut self,
        subject: impl ToString,
        message: impl ToBytes,
        reply: Option<impl ToString>,
        headers: Option<serde_json::Value>,
    ) -> Result<(), PgNatsError> {
        let subject = subject.to_string();
        let message: Vec<u8> = message.to_bytes()?;
        let conn = self.get_connection().await?;
        let headers = headers.map(extract_headers);

        if let Some(reply) = reply {
            let reply = reply.to_string();

            if let Some(headers) = headers {
                conn.publish_with_reply_and_headers(subject, reply, headers, message.into())
                    .await?;
            } else {
                conn.publish_with_reply(subject, reply, message.into())
                    .await?;
            }
        } else if let Some(headers) = headers {
            conn.publish_with_headers(subject, headers, message.into())
                .await?;
        } else {
            conn.publish(subject, message.into()).await?;
        }

        Ok(())
    }

    pub async fn request(
        &mut self,
        subject: impl ToString,
        message: impl ToBytes,
        timeout: Option<u64>,
    ) -> Result<Vec<u8>, PgNatsError> {
        let subject = subject.to_string();
        let message: Vec<u8> = message.to_bytes()?;

        let request = Request::new().payload(message.into());

        let request = if let Some(timeout) = timeout {
            request.timeout(Some(Duration::from_millis(timeout)))
        } else {
            request
        };

        let result = self
            .get_connection()
            .await?
            .send_request(subject, request)
            .await?;

        Ok(result.payload.to_vec())
    }

    pub async fn publish_stream(
        &mut self,
        subject: impl ToString,
        message: impl ToBytes,
        headers: Option<serde_json::Value>,
    ) -> Result<(), PgNatsError> {
        let subject = subject.to_string();
        let message: Vec<u8> = message.to_bytes()?;
        let headers = headers.map(extract_headers);
        let js = self.get_jetstream().await?;

        if let Some(headers) = headers {
            let _ = js
                .publish_with_headers(subject, headers, message.into())
                .await?;
        } else {
            let _ = js.publish(subject, message.into()).await?;
        }

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
            info!("Disconnect from NATS service");

            if let Err(e) = conn.drain().await {
                warning!("Failed to drain connection {e}");
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

    pub async fn set_config(&mut self, opt: NatsConnectionOptions) {
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
        data: impl ToBytes,
    ) -> Result<u64, PgNatsError> {
        let bucket = self.get_or_create_bucket(bucket).await?;
        let data: Vec<u8> = data.to_bytes()?;
        let version = bucket.put(key, data.into()).await?;

        Ok(version)
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

    pub async fn get_server_info(&mut self) -> Result<async_nats::ServerInfo, PgNatsError> {
        let connection = self.get_connection().await?;
        Ok(connection.server_info())
    }

    pub async fn get_file(
        &mut self,
        store: impl ToString,
        name: impl AsRef<str> + Send,
    ) -> Result<Vec<u8>, PgNatsError> {
        let store = self.get_or_create_object_store(store).await?;
        let mut file = store.get(name).await?;
        let mut content = Vec::with_capacity(file.info().size);
        let _ = file.read_to_end(&mut content).await?;

        Ok(content)
    }

    pub async fn put_file(
        &mut self,
        store: impl ToString,
        name: impl AsRef<str>,
        content: Vec<u8>,
    ) -> Result<(), PgNatsError> {
        let store = self.get_or_create_object_store(store).await?;
        let mut reader = BufReader::new(Cursor::new(content));
        let _ = store.put(name.as_ref(), &mut reader).await?;

        Ok(())
    }

    pub async fn delete_file(
        &mut self,
        store: impl ToString,
        name: impl AsRef<str>,
    ) -> Result<(), PgNatsError> {
        let store = self.get_or_create_object_store(store).await?;
        store.delete(name).await.map_err(|e| e.into())
    }

    pub async fn get_file_info(
        &mut self,
        store: impl ToString,
        name: impl AsRef<str>,
    ) -> Result<ObjectInfo, PgNatsError> {
        let store = self.get_or_create_object_store(store).await?;
        store.info(name).await.map_err(|e| e.into())
    }

    pub async fn get_file_list(
        &mut self,
        store: impl ToString,
    ) -> Result<Vec<ObjectInfo>, PgNatsError> {
        let store = self.get_or_create_object_store(store).await?;
        let mut vec = vec![];
        let mut list = store.list().await?;

        while let Some(object) = list.next().await {
            if let Ok(object) = object {
                vec.push(object);
            }
        }

        Ok(vec)
    }

    pub fn get_connection_options(&self) -> Option<NatsConnectionOptions> {
        self.current_config.clone()
    }
}

impl NatsConnection {
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

    async fn get_or_create_object_store(
        &mut self,
        store: impl ToString,
    ) -> Result<&ObjectStore, PgNatsError> {
        let bucket = store.to_string();

        if !self.cached_object_stores.contains_key(&bucket) {
            let new_store = {
                let jetstream = self.get_jetstream().await?;
                jetstream
                    .create_object_store(async_nats::jetstream::object_store::Config {
                        bucket: bucket.clone(),
                        ..Default::default()
                    })
                    .await?
            };

            let _ = self.cached_object_stores.insert(bucket.clone(), new_store);
        }

        Ok(self
            .cached_object_stores
            .get(&bucket)
            .expect("unreachable, must be initialized"))
    }

    async fn initialize_connection(&mut self) -> Result<(), PgNatsError> {
        let config = self
            .current_config
            .get_or_insert_with(fetch_connection_options);

        let mut opts = async_nats::ConnectOptions::new().client_capacity(config.capacity);

        if let Some(tls) = &config.tls {
            if let Ok(root) = std::env::current_dir() {
                match tls {
                    NatsTlsOptions::Tls { ca } => {
                        info!("Trying to find CA cert in '{:?}'", root.join(ca));
                        opts = opts.require_tls(true).add_root_certificates(root.join(ca))
                    }
                    NatsTlsOptions::MutualTls { ca, cert, key } => {
                        info!(
                            "Trying to find CA cert in '{:?}', cert in '{:?}' and key in '{:?}'",
                            root.join(ca),
                            root.join(cert),
                            root.join(key)
                        );
                        opts = opts
                            .require_tls(true)
                            .add_root_certificates(root.join(ca))
                            .add_client_certificate(root.join(cert), root.join(key));
                    }
                }
            }
        }

        let connection = opts
            .connect(format!("{0}:{1}", config.host, config.port))
            .await
            .map_err(|io_error| PgNatsError::Connection {
                host: config.host.clone(),
                port: config.port,
                io_error,
            })
            .inspect_err(|_| {
                self.current_config = None;
            })?;

        let mut jetstream = async_nats::jetstream::new(connection.clone());
        jetstream.set_timeout(std::time::Duration::from_secs(5));

        self.connection = Some(connection);
        self.jetstream = Some(jetstream);

        Ok(())
    }
}
