# Subscribe

> [!WARNING]
> The specified PostgreSQL function **must accept a single argument of type `bytea`**, which contains the message payload from NATS.

```sql
-- Subscribe a PostgreSQL function to a NATS subject
SELECT nats_subscribe('events.user.created', 'schema.handle_user_created'::regproc);

-- Multiple functions can be subscribed to the same subject
SELECT nats_subscribe('events.user.created', 'schema.log_user_created'::regproc);

-- Unsubscribe a specific PostgreSQL function from a NATS subject
SELECT nats_unsubscribe('events.user.created', 'schema.handle_user_created'::regproc);
```
