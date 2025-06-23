# Installation

> [!WARNING]
> To use the `subscribe` and `unsubscribe` functions, you must add the following to `postgresql.conf`:
> ```sh
> shared_preload_libraries = 'pgnats.so'
> ```

## Prerequisite

1. Install [rust](https://www.rust-lang.org/tools/install) >= 1.81.0
2. Install prerequisites for [pgrx](https://github.com/pgcentralfoundation/pgrx?tab=readme-ov-file#system-requirements)

## Linux

### ALT Linux

```sh
# 1. Install cargo-pgrx
cargo install cargo-pgrx --git https://github.com/luxms/pgrx --locked

# 2. Initialize pgrx
cargo pgrx init

# 3. Clone repo
git clone https://github.com/luxms/pgnats -b pgpro
```

> [!WARNING]
> If you want to specify the path to the installed `pg_config`, use the following command:
>
> `cargo pgrx init -pg<POSTGRES_VERSION> <path to pg_config> --skip-version-check`


### Other Linux

#### Postgres Official

```sh
# 1. Install cargo-pgrx
cargo install cargo-pgrx --git https://github.com/luxms/pgrx --locked

# 2. Initialize pgrx
cargo pgrx init [-pg<POSTGRES_VERSION> <path to pg_config>]

# 3. Clone repo
git clone https://github.com/luxms/pgnats
```

#### PostgresPro Std. / Ent.

> [!WARNING]
> You need to use feature `xid8` to build the extension:
>
> `cargo pgrx package -pg_config<path to pg_config> --features xid8`

```sh
# 1. Install cargo-pgrx
cargo install cargo-pgrx --git https://github.com/luxms/pgrx --locked

# 2. Initialize pgrx
cargo pgrx init -pg<POSTGRES_VERSION> <path to pg_config>

# 3. Clone repo
git clone https://github.com/luxms/pgnats -b pgpro
```

## Windows

```sh
# 1. Install cargo-pgrx
cargo install cargo-pgrx --git https://github.com/luxms/pgrx --locked

# 2. Initialize pgrx
cargo pgrx init [-pg<POSTGRES_VERSION> <path to pg_config>]

# 3. Clone repo
git clone https://github.com/luxms/pgnats
```
