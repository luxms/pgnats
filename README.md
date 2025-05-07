# pgnats - PostgreSQL extension for NATS messaging

Provides seamless integration between PostgreSQL and NATS messaging system,
enabling:

Provides one-way integration from PostgreSQL to NATS, supporting:
- Message publishing to core NATS subjects from SQL
- JetStream persistent message streams
- Key-Value storage operations from SQL

## Install

See [INSTALL.md](INSTALL.md) for instructions on how to install required system dependencies.

## PostgreSQL Configure options

You can fine tune PostgreSQL build options:

```
cargo pgrx init --configure-flag='--without-icu'
```

## Build package

```sh
cargo pgrx package
```

## Tests

> [!WARNING]
> Before starting the test, NATS-Server should be started on a local host with port 4222.

> [!WARNING]
> You need docker installed for integration testing.

**1. Run all tests**
```sh
cargo pgrx test
```

**2. Skip tests that require JetStream in NATS Server**
```sh
SKIP_PGNATS_JS_TESTS=1 cargo pgrx test
```

**3. Skip tests that require NATS Server**
```sh
SKIP_PGNATS_TESTS=1 cargo pgrx test
```

## Minimum supported Rust version

- `Rust 1.81.0`
- `cargo-pgrx 0.14.*`

# Documentation

To view the documentation, run:

```sh
cargo doc --open`
```

The exported PostgreSQL API is implemented in the `api` module.

# Extension config

- `nats.host` - IP/hostname of the NATS message server (default: `127.0.0.1`)
- `nats.port` - TCP port for NATS connections (default: `4222`)
- `nats.capacity` - Internal command buffer size in messages (default: `128`)
- `nats.tls.ca` – Path to the CA (Certificate Authority) certificate used to verify the NATS server certificate (default: unset, required for TLS)
- `nats.tls.cert` – Path to the client certificate for mutual TLS authentication (default: unset; optional unless server requires client auth)
- `nats.tls.key` – Path to the client private key corresponding to `nats.tls.cert` (default: unset; required if `nats.tls.cert` is set)

## Usage

```sql
-- Configuration
SET nats.host = '127.0.0.1';
SET nats.port = 4222;
SET nats.capacity = 128;
SET nats.tls.ca = 'ca';
SET nats.tls.cert = 'cert';
SET nats.tls.key = 'key';

-- Reload configuration (checks for changes)
SELECT pgnats_reload_conf();

-- Force reload configuration (no change checks)
SELECT pgnats_reload_conf_force();

-- Publish binary data to NATS
SELECT nats_publish_binary('sub.ject', 'binary data'::bytea);

-- Publish binary data with a reply subject
SELECT nats_publish_binary_reply('sub.ject', 'binary data'::bytea, 'reply.subject');

-- Publish binary data with headers
SELECT nats_publish_binary_with_headers(
  'sub.ject',
  'binary data'::bytea,
  '{}'::json
);

-- Publish binary data with both a reply subject and headers
SELECT nats_publish_binary_reply_with_headers(
  'sub.ject',
  'binary data'::bytea,
  'reply.subject',
  '{}'::json
);

-- Publish binary data via JetStream (sync)
SELECT nats_publish_binary_stream('sub.ject', 'binary data'::bytea);

-- Publish text to NATS
SELECT nats_publish_text('sub.ject', 'text data');

-- Publish text data with a reply subject
SELECT nats_publish_text_reply('sub.ject', 'text data', 'reply.subject');

-- Publish text data with headers
SELECT nats_publish_text_with_headers(
  'sub.ject',
  'text data',
  '{}'::json
);

-- Publish text data with both a reply subject and headers
SELECT nats_publish_text_reply_with_headers(
  'sub.ject',
  'text data',
  'reply.subject',
  '{}'::json
);

-- Publish text via JetStream (sync)
SELECT nats_publish_text_stream('sub.ject', 'text data');

-- Publish JSON to NATS
SELECT nats_publish_json('sub.ject', '{}'::json);

-- Publish JSON data with a reply subject
SELECT nats_publish_json_reply('sub.ject', '{"key": "value"}'::json, 'reply.subject');

-- Publish JSON data with headers
SELECT nats_publish_json_with_headers(
  'sub.ject',
  '{"key": "value"}'::json,
  '{"Content-Type": "application/json", "X-Custom": "value"}'::json
);

-- Publish JSON data with both a reply subject and headers
SELECT nats_publish_json_reply_with_headers(
  'sub.ject',
  '{"key": "value"}'::json,
  'reply.subject',
  '{"Content-Type": "application/json", "X-Custom": "value"}'::json
);

-- Publish JSON via JetStream (sync)
SELECT nats_publish_json_stream('sub.ject', '{}'::json);

-- Publish binary JSON (JSONB) to NATS
SELECT nats_publish_jsonb('sub.ject', '{}'::json);

-- Publish JSONB data with a reply subject
SELECT nats_publish_jsonb_reply('sub.ject', '{"key": "value"}'::jsonb, 'reply.subject');

-- Publish JSONB data with headers
SELECT nats_publish_jsonb_with_headers(
  'sub.ject',
  '{"key": "value"}'::jsonb,
  '{"Content-Type": "application/json", "X-Custom": "value"}'::json
);

-- Publish JSONB data with both a reply subject and headers
SELECT nats_publish_jsonb_reply_with_headers(
  'sub.ject',
  '{"key": "value"}'::jsonb,
  'reply.subject',
  '{"Content-Type": "application/json", "X-Custom": "value"}'::json
);

-- Publish binary JSON (JSONB) via JetStream (sync)
SELECT nats_publish_jsonb_stream('sub.ject', '{}'::jsonb);

-- Request binary data from NATS (wait for response with timeout in ms)
SELECT nats_request_binary('sub.ject', 'binary request'::bytea, 1000);

-- Request text from NATS (wait for response with timeout in ms)
SELECT nats_request_text('sub.ject', 'text request', 1000);

-- Request JSON from NATS (wait for response with timeout in ms)
SELECT nats_request_json('sub.ject', '{"query": "value"}'::json, 1000);

-- Request binary JSON (JSONB) from NATS (wait for response with timeout in ms)
SELECT nats_request_jsonb('sub.ject', '{"query": "value"}'::jsonb, 1000);

-- Store binary data in NATS JetStream KV storage with specified key
SELECT nats_put_binary('bucket', 'key', 'binary data'::bytea);

-- Store text data in NATS JetStream KV storage with specified key
SELECT nats_put_text('bucket', 'key', 'text data');

-- Store binary JSON (JSONB) data in NATS JetStream KV storage with specified key
SELECT nats_put_jsonb('bucket', 'key', '{}'::jsonb);

-- Store JSON data in NATS JetStream KV storage with specified key
SELECT nats_put_json('bucket', 'key', '{}'::json);

-- Retrieve binary data by key from specified bucket
SELECT nats_get_binary('bucket', 'key');

-- Retrieve text data by key from specified bucket
SELECT nats_get_text('bucket', 'key');

-- Retrieve binary JSON (JSONB) by key from specified bucket
SELECT nats_get_jsonb('bucket', 'key');

-- Retrieve JSON by key from specified bucket
SELECT nats_get_json('bucket', 'key');

-- Delete value associated with specified key from bucket
SELECT nats_delete_value('bucket', 'key');

-- Retrieves information about the NATS server connection.
SELECT nats_get_server_info();
```
