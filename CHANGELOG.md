# CHANGELOG

## [1.1.0] - 2025-12-15

### Changed (Breaking Changes)

* Subscription Table Refactoring: The `pgnats.subscriptions` table structure was fundamentally changed to improve reliability and internal processing:

  * The `callback` column (TEXT) was removed.

  * The `fn_oid` column (OID) was added to store the PostgreSQL function's Object ID.

> [!WARNING] 
> Impact: During the upgrade, all existing subscriptions are migrated by resolving the function name to its OID. Subscriptions referencing non-existent functions will be dropped during migration.

* Changed `pgnats_version()` signature: The function providing version information has been updated to return a detailed table of build metadata instead of a single text string.

  * Old Signature: `pgnats_version() RETURNS TEXT`

  * New Signature: `pgnats_version() RETURNS TABLE (version TEXT, commit_date TEXT, short_commit TEXT, branch TEXT, last_tag TEXT)`

### Added (New Features)

* When a subscribed PostgreSQL function is dropped, this event trigger automatically removes the corresponding entry from the `pgnats.subscriptions` table, preventing background worker errors.
