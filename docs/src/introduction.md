# Introduction

This extension provides seamless integration between PostgreSQL and NATS messaging system,
enabling:

- Message publishing to core NATS subjects from SQL
- Subscriptions to NATS subjects that invoke PostgreSQL functions on incoming messages
- JetStream persistent message streams
- Key-Value storage operations from SQL
- Object Store operations (uploading, downloading, deleting files) from SQL
- Works on Postgres Cluster
