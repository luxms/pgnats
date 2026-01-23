# Packaging

```sh
cargo pgrx package --pg-config <PATH TO PG_CONFIG> [--out-dir <THE DIRECTORY TO OUTPUT THE PACKAGE>]
```

## Selecting Features

By default, all features (`kv`, `object_store`, `sub`) are enabled.
If you prefer a smaller build or want to customize the functionality, you can selectively enable features like so:

```sh
cargo pgrx package --no-default-features --features kv
```

This will include only the `kv` feature and exclude `object_store` and `sub`.

For example:

* `--features "kv"` – enables only the NATS key-value store.
* `--features "sub"` – enables subscriptions and HTTP integration with Patroni.
* `--features "object_store"` – enables binary object storage support.

You can combine them as needed:

```sh
cargo pgrx package --no-default-features --features kv sub
```

## Vendors

### PostgresPro Enterprise Edition

You need to use feature `xid8` to build the extension:

```sh
cargo pgrx package -pg_config<path to pg_config> --features xid8
```
