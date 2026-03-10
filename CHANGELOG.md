# CHANGELOG

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
