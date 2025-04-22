# pgnats

PostgreSQL расширение для работы с NATS

## Install

See [INSTALL.md](INSTALL.md) for instructions on how to install required system dependencies.

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

> [!WARNING]
> You need docker installed for integration testing.

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
- `cargo-pgrx 0.14.*`

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

-- Отправка массива байт в NATS
Select nats_publish_binary('sub.ject', 'binary data'::bytea);

-- Отправка массива байт с помощью jetstream (sync)
Select nats_publish_binary_stream('sub.ject', 'binary data'::bytea);

-- Отправка текста в NATS
Select nats_publish_text('sub.ject', 'text data');

-- Отправка текста с помощью jetstream (sync)
Select nats_publish_text_stream('sub.ject', 'text data');

-- Отправка Json в NATS
Select nats_publish_json('sub.ject', '{}'::json);

-- Отправка Json с помощью jetstream (sync)
Select nats_publish_json_stream('sub.ject', '{}'::json);

-- Отправка Binary Json в NATS
Select nats_publish_jsonb('sub.ject', '{}'::json);

-- Отправка Binary Json с помощью jetstream (sync)
Select nats_publish_jsonb_stream('sub.ject', '{}'::jsonb);

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

#### api/macros.rs

Вспомогательные макросы для генерации однотипного кода для NATS Api

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
