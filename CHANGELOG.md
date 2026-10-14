# CHANGELOG

## [1.1.1] - 2026-10-14

### Security

* Upgrade `async-nats` to 0.47 and refresh `rustls` and `rustls-webpki` to fix TLS handshake and certificate/CRL validation vulnerabilities.
* Update `tokio-postgres` and retain the `postgres-protocol` fixes for malformed PostgreSQL responses and excessive SCRAM iteration counts.
* Update both affected `rand` dependency versions and `time` to fix unsoundness and stack-exhaustion vulnerabilities.
* Update the test HTTP server to use patched `h2`, update `anyhow`, and use the Git CLI for build metadata to remove vulnerable `git2` code.

### Changed

* Raise the minimum supported Rust version to 1.88, as required by the updated dependencies.
* Add the extension upgrade path from 1.1.0 to 1.1.1; the SQL API and schema are unchanged.

## [1.1.0] - 2025-12-15

### Changed

* Changed `pgnats_version()` signature: The function providing version information has been updated to return a detailed table of build metadata instead of a single text string.

  * Old Signature: `pgnats_version() RETURNS TEXT`

  * New Signature: `pgnats_version() RETURNS TABLE (version TEXT, commit_date TEXT, short_commit TEXT, branch TEXT, last_tag TEXT)`

* Changed `nats_subscribe()` and `nats_unsubscribe()` signatures: The second parameter changed from function name (`TEXT`) to function OID (`OID`). Use `regproc` cast for convenience: `SELECT nats_subscribe('subject', 'my_function'::regproc);`

  * Old Signature: `nats_subscribe(subject TEXT, fn_name TEXT)`

  * New Signature: `nats_subscribe(subject TEXT, fn_oid OID)`

### Added (New Features)

* When a subscribed PostgreSQL function is dropped, this event trigger automatically removes the corresponding entry from the `pgnats.subscriptions` table, preventing background worker errors.
