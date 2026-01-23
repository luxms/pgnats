# Installation

1. Copy control file and `*.sql` files to the PostgreSQL extension directory. (`pg_config --sharedir`, `$PG_SHAREDIR`)
2. Copy `.so`/`.dll` to the PostgreSQL library directory. (`pg_config --libdir`, `$PG_LIBDIR`)
3. Run `CREATE EXTENSION pgnats;`

> [!WARNING]
> To use the `subscribe` and `unsubscribe` functions, you must add the following to `postgresql.conf`:
> ```sh
> shared_preload_libraries = 'pgnats.so'
> ```
