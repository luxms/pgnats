# Linux

On most Linux distributions (Debian, Ubuntu, Fedora, etc.), initialize pgrx with:

```sh
cargo pgrx init
```

If you want to specify a custom PostgreSQL version or path to `pg_config`:

```sh
cargo pgrx init -pg<POSTGRES_VERSION> <path to pg_config>
```

## ALT-Linux

On ALT Linux, run:

```sh
cargo pgrx init
```

Or, if you want to specify a custom path and skip version checks:

```sh
cargo pgrx init -pg<POSTGRES_VERSION> <path to pg_config> --skip-version-check
```
