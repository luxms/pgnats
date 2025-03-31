## Installation

### Prerequisite

1. Install [rust](https://www.rust-lang.org/tools/install) >= 1.81.0
2. Install prerequisites for [pgrx](https://github.com/pgcentralfoundation/pgrx?tab=readme-ov-file#system-requirements)

### Linux

#### ALT Linux:

```sh
# 1. Install cargo-pgrx
cargo install cargo-pgrx --git https://github.com/luxms/pgrx --locked

# 2. Initialize pgrx
cargo pgrx init

# 3. Clone repo
git clone https://github.com/luxms/pgnats -b pgpro
```


#### Other Linux:

##### Postgres Official

```sh
# 1. Install cargo-pgrx
cargo install cargo-pgrx --version 0.13.1 --locked

# 2. Initialize pgrx
cargo pgrx init

# 3. Clone repo
git clone https://github.com/luxms/pgnats
```

##### PostgresPro Std. / Ent.

```sh
# 1. Install cargo-pgrx
cargo install cargo-pgrx --git https://github.com/luxms/pgrx --locked

# 2. Initialize pgrx
cargo pgrx init

# 3. Clone repo
git clone https://github.com/luxms/pgnats -b pgpro
```
