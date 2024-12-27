# pgnats

PostgreSQL расширение для совершения побликации сообщений в NATS

**WIP, Not usable yet!**

## Install
```sh
sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache pkg-config
cargo install --locked cargo-pgrx
cargo install nats-connect
```

## Usage
```sql
-- Ручная инициализация настроек (производится автоматически при первом использовании каких-либо функций расширения)
Select nats_init();

-- Просмотр значения настройки:
Select get_config('nats.host');

-- Установка значения настройки
Select set_config('nats.host', '''test.url''');
Select set_config('nats.port', '1111');
Select set_config_string('nats.host', 'test.url');

Select set_config('nats.host', 'DEFAULT');
Select reset_config('nats.host');

-- Публикация в NATS
Select nats_publish('publish_text', 'sub.ject')

-- Публикация с помощью jetstream (sync)
Select nats_publish_stream('publish_text', 'sub.ject')
```
При публикации с помощью `jetstream` создается стрим с именем субъекта без последнего блока. Спецсимволы (`.^?`) заменяются на `_`.

Например:
```
luxmsbi.cdc.audit.events: luxmsbi_cdc_audit
```

## Source

### init.rs
Системные процедуры инициализации расширения

### lib.rs
Точка входа в расширение, подключение необходимых модулей

### config.rs
Закрытые функции инициализации, настройка параметров умодчания, открытые функции изменения параметров

Настройки:
```py
CONFIG_HOST: str = "адрес NATS сервиса"
CONFIG_PORT: int = "порт NATS сервиса"
```

### funcs.rs
Пользовательские функции

### tests.rs
Функции тестирования

