# Request

```sql
-- Request binary data from NATS (wait for response with timeout in ms)
SELECT nats_request_binary('sub.ject', 'binary request'::bytea, 1000);

-- Request text from NATS (wait for response with timeout in ms)
SELECT nats_request_text('sub.ject', 'text request', 1000);

-- Request JSON from NATS (wait for response with timeout in ms)
SELECT nats_request_json('sub.ject', '{"query": "value"}'::json, 1000);

-- Request binary JSON (JSONB) from NATS (wait for response with timeout in ms)
SELECT nats_request_jsonb('sub.ject', '{"query": "value"}'::jsonb, 1000);
```
