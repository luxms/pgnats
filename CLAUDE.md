# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**pgnats** is a PostgreSQL extension (written in Rust via PGRX) that integrates PostgreSQL with NATS messaging. It exposes SQL functions for publishing/subscribing to NATS subjects, JetStream, Key-Value store, and Object Store operations.

## Common Commands

### Initialize PGRX (first time)

```sh
cargo pgrx init --pg14 download
```

### Build

```sh
cargo pgrx package --pg-config <PATH_TO_PG_CONFIG>
```

With feature selection (default builds all features):

```sh
# Minimal build
cargo pgrx package --no-default-features --features kv

# Available features: kv, sub, object_store
```

### Lint

```sh
cargo clippy -- -D warnings
```

### Test

```sh
cargo pgrx test
```
Requires: NATS server running on `localhost:4222`, Docker installed.

### Run a single test

```sh
cargo pgrx test -- <test_name>
```

## Architecture

### Modules

- **`src/api/`** — Public SQL-facing functions (always compiled). `nats.rs` implements all exported SQL functions: publish variants, subscribe, request-reply, KV, and object store ops.
- **`src/bgw/`** — Background Worker (compiled only with `sub` feature). Two-process model: a *launcher* manages subscription lifecycle; *subscriber* workers handle multiple NATS subjects per database. Uses ring queue and DSM shared memory for inter-process communication.
- **`src/config.rs`** — Reads NATS connection config from PostgreSQL Foreign Data Wrapper (FDW) options stored in catalog.
- **`src/nats_client.rs`** — Async NATS client wrapper around `async-nats`.
- **`src/ctx.rs`** — Global `CTX` state; Tokio runtime shared across PostgreSQL function calls.
- **`src/pg_tests/`** — PGRX integration tests (`api_tests.rs`, `bgw_tests.rs`, `shm_mq_tests.rs`).

### Feature Flags

| Feature | Content |
|---|---|
| `kv` | NATS Key-Value store SQL functions |
| `sub` | Subscriptions + background workers + Patroni/HTTP integration |
| `object_store` | Binary object storage SQL functions |

PostgreSQL version flags: `pg14`, `pg15`, `pg16`, `pg17`, `pg18`.

### Configuration Pattern

NATS connection options (host, port, TLS certs, etc.) are stored as Foreign Server options in PostgreSQL and read at runtime via FDW — no config files outside the database.

### SQL Schema & Migrations

Extension schema is `pgnats`. The `pgnats.subscriptions` table maps NATS subjects to PostgreSQL function names (callback column). An event trigger auto-cleans subscriptions when functions are dropped.

Migration scripts are in `sql/` (e.g., `pgnats--1.0.0--1.1.0.sql`). When Rust function signatures change (parameters, return types), the migration must DROP and recreate the corresponding SQL function. PGRX auto-generates the initial schema but migrations are hand-written.

### Coding Conventions

Cargo.toml enforces via lint settings:
- No `unwrap()`, `expect()`, or `panic!()` without `#[allow(...)]` and justification
- No unsafe blocks without documentation
- Warnings as errors (`deny(warnings)`)
