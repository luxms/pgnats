# Publish

## Binary

```sql
-- Publish binary data to NATS
SELECT nats_publish_binary('sub.ject', 'binary data'::bytea);

-- Publish binary data with a reply subject
SELECT nats_publish_binary('sub.ject', 'binary data'::bytea, 'reply.subject');

-- Publish binary data with headers
SELECT nats_publish_binary(
  'sub.ject',
  'binary data'::bytea,
  NULL,
  '{}'::json
);

-- Publish binary data with both a reply subject and headers
SELECT nats_publish_binary(
  'sub.ject',
  'binary data'::bytea,
  'reply.subject',
  '{}'::json
);

-- Publish binary data via JetStream (sync)
SELECT nats_publish_binary_stream('sub.ject', 'binary data'::bytea);

-- Publish text via JetStream (sync) with headers
SELECT nats_publish_binary_stream(
  'sub.ject',
  'binary data'::bytea,
  '{}'::json
);
```

## Utf-8 Text

```sql
-- Publish text to NATS
SELECT nats_publish_text('sub.ject', 'text data');

-- Publish text data with a reply subject
SELECT nats_publish_text('sub.ject', 'text data', 'reply.subject');

-- Publish text data with headers
SELECT nats_publish_text(
  'sub.ject',
  'text data',
  NULL,
  '{}'::json
);

-- Publish text data with both a reply subject and headers
SELECT nats_publish_text(
  'sub.ject',
  'text data',
  'reply.subject',
  '{}'::json
);

-- Publish text via JetStream (sync)
SELECT nats_publish_text('sub.ject', 'text data');

-- Publish text via JetStream (sync) with headers
SELECT nats_publish_text_stream(
  'sub.ject',
  'text data',
  '{}'::json
);
```

## JSON

```sql
-- Publish JSON to NATS
SELECT nats_publish_json('sub.ject', '{}'::json);

-- Publish JSON data with a reply subject
SELECT nats_publish_json('sub.ject', '{"key": "value"}'::json, 'reply.subject');

-- Publish JSON data with headers
SELECT nats_publish_json(
  'sub.ject',
  '{"key": "value"}'::json,
  NULL,
  '{}'::json
);

-- Publish JSON data with both a reply subject and headers
SELECT nats_publish_json_reply(
  'sub.ject',
  '{"key": "value"}'::json,
  'reply.subject',
  '{}'::json
);

-- Publish JSON via JetStream (sync)
SELECT nats_publish_json_stream('sub.ject', '{}'::json);

-- Publish JSON via JetStream (sync) with headers
SELECT nats_publish_json_stream(
  'sub.ject',
  '{}'::json,
  '{}'::json
);
```

## Binary JSON

```sql
-- Publish binary JSON (JSONB) to NATS
SELECT nats_publish_jsonb('sub.ject', '{}'::json);

-- Publish JSONB data with a reply subject
SELECT nats_publish_jsonb('sub.ject', '{"key": "value"}'::jsonb, 'reply.subject');

-- Publish JSONB data with headers
SELECT nats_publish_jsonb(
  'sub.ject',
  '{"key": "value"}'::jsonb,
  NULL,
  '{}'::json
);

-- Publish JSONB data with both a reply subject and headers
SELECT nats_publish_jsonb_reply(
  'sub.ject',
  '{"key": "value"}'::jsonb,
  'reply.subject',
  '{}'::json
);

-- Publish binary JSON (JSONB) via JetStream (sync)
SELECT nats_publish_jsonb_stream('sub.ject', '{}'::jsonb);

--  Publish binary JSON (JSONB) via JetStream (sync) with headers
SELECT nats_publish_jsonb_stream(
  'sub.ject',
  '{}'::jsonb,
  '{}'::json
);
```
