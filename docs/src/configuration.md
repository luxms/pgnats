# Configuration

To configure the NATS connection, you need to create a Foreign Server:

```sql
CREATE SERVER nats_fdw_server FOREIGN DATA WRAPPER pgnats_fdw OPTIONS (
    --  IP/hostname of the NATS message server (default: 127.0.0.1)
    host 'localhost',

    -- TCP port for NATS connections (default: 4222)
    port '4222',

    -- Internal command buffer size in messages (default: 128)
    capacity '128',

    -- Path to the CA (Certificate Authority) certificate used to verify the NATS server certificate (default: unset, required for TLS)
    tls_ca_path '/path/ca',

    --  Path to the client certificate for mutual TLS authentication (default: unset; optional unless server requires client auth)
    tls_cert_path '/path/cert',

    -- Path to the client private key corresponding to nats.tls.cert (default: unset; required if nats.tls.cert is set)
    tls_key_path '/path/key',

    -- Name of the NATS subject for sending role change notifications (e.g., when the Postgres instance transitions between master and replica)
    notify_subject 'my.subject'

    -- URL of the Patroni REST API used to retrieve the current Postgres instance name.
    -- This is required when sending role change notifications (e.g., when the Postgres instance transitions between master and replica)
    patroni_url 'http://localhost:8008/patroni'
);
```

## Notification payload example

```json
{
  "status": "Master",
  "listen_adresses": ["127.0.0.1", "127.0.0.2"],
  "port": 5432,
  "name": "pg-instance-01"
}
```

```json
{
  "status": "Replica",
  "listen_adresses": ["127.0.0.1"],
  "port": 5432,
  "name": null
}
```
