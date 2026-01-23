# Key-Value

```sql
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
```
