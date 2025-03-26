# pgnats

PostgreSQL расширение для работы с NATS

## Install

```sh
# 1. Install Rust >= 1.81.0
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 2. Install pgrx dependencies
sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache pkg-config

# 3. Install cargo-pgrx
cargo install cargo-pgrx --git https://github.com/luxms/pgrx

# 4. Initialize pgrx
cargo pgrx init
```

## PostgreSQL Configure options

You can fine tune PostgreSQL build options:

```
cargo pgrx init --configure-flag='--without-icu'
```

## Build package

```sh
cargo pgrx package
```

## Tests

> [!WARNING]
> Before starting the test, NATS-Server should be started on a local host with port 4222.

**1. Run all tests**
```sh
cargo pgrx test
```

**2. Skip tests that require JetStream in NATS Server**
```sh
SKIP_PGNATS_JS_TESTS=1 cargo pgrx test
```

**3. Skip tests that require NATS Server**
```sh
SKIP_PGNATS_TESTS=1 cargo pgrx test
```

## Minimum supported Rust version

- `Rust 1.81.0`
- `cargo-pgrx 0.13.*`

## Usage

```sql
-- Конфигурируем
Set nats.host = '127.0.0.1';
Set nats.port = 4222;
Set nats.capacity = 128;

-- Перезагружаем конфигурацию
Select pgnats_reload_conf();

-- Перезагружаем конфигурацию без проверок на изменения конфигураций
Select pgnats_reload_conf_force();

-- Публикация в NATS
Select nats_publish('sub.ject', 'publish_text');

-- Публикация с помощью jetstream (sync)
Select nats_publish_stream('sub.ject', 'publish_text');

-- Функция сохраняет бинарные данные в Key-Value (KV) хранилище NATS JetStream, используя указанный ключ
Select nats_put_binary('bucket', 'key', 'binary data'::bytea);

-- Функция сохраняет текстовые данные в Key-Value (KV) хранилище NATS JetStream, используя указанный ключ
Select nats_put_text('bucket', 'key', 'text data');

-- Функция сохраняет данные в формате Binary Json в Key-Value (KV) хранилище NATS JetStream, используя указанный ключ
Select nats_put_jsonb('bucket', 'key', '{}'::jsonb);

-- Функция сохраняет данные в формате Json в Key-Value (KV) хранилище NATS JetStream, используя указанный ключ
Select nats_put_json('bucket', 'key', '{}'::json);

-- Извлекает бинарные данные по указанному ключу из указанного бакета
Select nats_get_binary('bucket', 'key');

-- Извлекает текстовые данные по указанному ключу из указанного бакета
Select nats_get_text('bucket', 'key');

-- Извлекает Binary Json по указанному ключу из указанного бакета
Select nats_get_jsonb('bucket', 'key');

-- Извлекает Json по указанному ключу из указанного бакета
Select nats_get_json('bucket', 'key');

-- Эта функция удаляет значение, связанное с указанным ключом, из указанного бакета
Select nats_delete_value('bucket', 'key');
```

При публикации с помощью `jetstream` создается стрим с именем субъекта без последнего блока. Спецсимволы (`.^?`) заменяются на `_`.

Например:

```text
luxmsbi.cdc.audit.events: luxmsbi_cdc_audit
```

## Source

### init.rs

Системные процедуры инициализации расширения

### lib.rs

Точка входа в расширение, подключение необходимых модулей

### api.rs

Набор функций, экспортируемых в PostgreSQL

#### api/nats.rs

Функции для работы с NATS

### config.rs

Закрытые функции инициализации, настройка параметров по-умолчанию

Список настроек:

- `nats.host` - Адрес сервера NATS. По-умолчанию `127.0.0.1`
- `nats.port` - Порт, на котором работает сервер NATS. По-умолчанию `4222`
- `nats.capacity` - Емкость очереди команд в NATS Client. По-умолчанию `128`

### connection.rs

Внутренние функции для работы с NATS-соединением и NATS-stream

### ctx.rs

Глобальный контекст, в котором хранятся `NatsConnection`, `tokio-runtime`  и `LocalSet`

### errors.rs

Внутренние типы ошибок

### tests.rs

Функции тестирования

### utils.rs

Вспомогательные функции
