# CBL-11.2. Загрузка событий в ClickHouse: Kafka engine, materialized view, хранилище сервиса

| Поле | Значение |
| :-- | :-- |
| Эпик | E11, `Reporting и ClickHouse` |
| Репозиторий | `cbl-reporting` |
| Размер | L |
| Волна и параллельная группа | W3-REPORT |
| Зависит от | `CBL-11.1` (схемы `fact_*`), контракты `cbl-protos@cf-3.4`, T-10 |
| Версия контрактов | `cbl-protos@cf-3.4` |
| Статус | done, ревью закрыто |

## 1. Контекст и цель

`CBL-000`: ClickHouse читает события из Kafka через Kafka engine + materialized view
(T-10), дедупликация - ReplacingMergeTree (`CBL-003` §7). Решением 15.10.2026 ClickHouse -
хранилище сервиса `cbl-reporting`, как PostgreSQL у сервисов с БД: схема и миграции в
сервисном репозитории, локальный инстанс в `deploy/docker-compose.yml` сервиса,
интеграционные тесты на testcontainers (Kafka + ClickHouse), e2e на compose сервиса;
глобальный compose держит общий инстанс стенда для сквозных сценариев (GE-7), а init
`cbl-system` из 11.1 остается dev-bootstrap.

Задача 11.2 наполняет `fact_*` из четырех топиков, переносит владение схемой в сервис
(goose-миграции), добавляет конфигурацию сервиса, интеграционные тесты и e2e.

## 2. Границы

In scope: миграции `fact_*` в сервисе (goose, clickhouse dialect), Kafka engine таблицы и
materialized view по четырем топикам, wire-схема декодирования protobuf, конфигурация и
main сервиса (`CLICKHOUSE_DSN`, `KAFKA_BROKERS`, healthz/readyz, метрики-заглушка),
testcontainers-интеграция (Kafka + ClickHouse, реальные protobuf-сообщения), e2e compose
сервиса, Dockerfile/compose/Makefile/AGENTS.

Out of scope: gRPC `Query` и прокси `/v1/reports` (11.4), словарь `dim_accounts` и сверки
(11.3), дашборды (11.5), ретеншен/TTL и квоты (14.7), Kafka Connect (T-10: только при
требовании строгого exactly-once), запись в Kafka сервисом (нет продюсера).

## 3. Функциональные требования

FR-1. Хранилище сервиса: `CLICKHOUSE_DSN` (clickhouse-go/v2, database/sql драйвер),
`/readyz` пингует ClickHouse; compose сервиса поднимает `clickhouse` (как PostgreSQL у
сервисов с БД) и монтирует каталог wire-схемы в `format_schema_path`.

FR-2. Миграции: каталог `migrations/` в goose-формате с диалектом `clickhouse`, применяются
при старте сервиса (`goose_db_version`); только вперед; DDL фактов повторяет 11.1
(владелец схемы с этой задачи - сервис; init `cbl-system` остается dev-bootstrap).

FR-3. Конвейер на топик (`payment.events`, `ledger.events`, `payout.events`,
`recon.alerts`): таблица `kafka_<факт>` (движок Kafka, `ProtobufSingle`) + materialized
view `<факт>_mv` пишет в `fact_*`. Kafka-таблицы реконсилируются на старте: ожидаемое
определение пары (брокеры, схема, топик, группа, колонки, выборка) сворачивается в
отпечаток и хранится в `COMMENT` таблицы; при расхождении отпечатка (включая сокращение
списка брокеров - сравнение значений, а не подстрок) пара пересоздается, при частичном
состоянии (таблица без view) view восстанавливается. Реконсиляция - для одного
писателя; при масштабировании реплик схема обновления согласуется отдельно (14.2/0.6).

FR-4. Декодирование: каталог `deploy/format_schemas/events.proto` - wire-совместимое
уплощение контрактов (`cbl-protos@cf-3.4`, номера и типы полей сохранены, неиспользуемые
поля опущены; декодер пропускает неизвестные wire-поля). Маппинг: `Money` ->
`Tuple(minor_units Int64, asset String, decimals UInt32)`; `Timestamp` ->
`Tuple(seconds Int64, nanos Int32)` с конверсией в `DateTime64(3, 'UTC')` (секунды плюс
наносекунды с усечением до миллисекунд); `LedgerEntry[]` -> `Array(Tuple(...))` с
`ARRAY JOIN ... WITH ORDINALITY` (`entry_index` 0-based); `event_id`/`event_type` - из
заголовков конверта (реконструкция `_headers` через `arrayFirst`, в 24.8 это массив
кортежей), `occurred_at` - из тела (авторитетно по контракту). `SCHEMA_PATH` - путь
относительно `format_schema_path` сервера ClickHouse (абсолютный путь Kafka engine
запрещает).

FR-5. Политика ошибок конвейера: `kafka_handle_error_mode='stream'`,
`kafka_skip_broken_messages=1000` (поврежденное сообщение не останавливает партицию);
MV леджера фильтрует `transaction_id != ''`, `direction IN (1, 2)`, `length(entries) <= 255`;
MV дополнительно отбрасывают строки с пустыми ключевыми полями, недопустимым `decimals`
(> 18) и `severity`/`type` алертов вне замороженных значений.

FR-6. Идемпотентность/дедупликация: MV вставляет строки как есть, схлопывание повторов
Kafka обеспечивает ReplacingMergeTree `fact_*` (11.1); повторный запуск сервиса не создает
дублей конфигурации (CREATE IF NOT EXISTS, пересоздание пары только при дрейфе отпечатка).

FR-7. main: `HTTP_ADDR` (:8110, healthz/readyz), `GRPC_ADDR` (:9110, gRPC health-сервис
для оркестраторов; `ReportingService.Query` - 11.4), `METRICS_ADDR` (:9310, заглушка до
0.7), `MIGRATIONS_DIR`, `SCHEMA_PATH` (файл wire-схемы внутри ClickHouse), graceful
shutdown, `run()` с ненулевым кодом при ошибке инициализации (включая сбой листенеров).

FR-8. Тесты: unit (конфигурация, SQL реконсиляции и фильтров); integration
(testcontainers Kafka + ClickHouse): миграции, идемпотентная реконсиляция, полный поток
по четырем топикам с реальными protobuf (генерация через `cbl-protos`), проверка значений
(деньги, entry_index, конверсия времени), дедупликация повтора, битое сообщение не
останавливает конвейер; e2e compose: up -> создание топиков -> продюсер событий -> строки в
`fact_*` -> повтор события -> readyz; k6 не применим (нет HTTP-нагрузки) - 14.2.

## 4. Контракты

- Kafka (потребление через ClickHouse Kafka engine; сервис собственный консьюмер не
  запускает): топики и ключи по `CBL-003` §6; заголовки конверта - `event_id`,
  `event_type`, `occurred_at`, `producer`, `traceparent`, `schema_version`
  (`cbl-kit/outbox`); группы `cbl-reporting-<факт>`; at-least-once.
- ClickHouse: `reporting.fact_*` (11.1), `reporting.kafka_*` (Kafka engine),
  `reporting.<факт>_mv`; база `reporting`.
- gRPC: health-сервис (:9110); `ReportingService` - 11.4.
- REST: `/healthz`, `/readyz` (административные).

## 5. Модель данных и миграции

Миграция `00001_fact_tables.sql` - DDL четырех `fact_*` (идентичен `CBL-11.1`).
Runtime-реконсиляция (не миграция, документировано trade-off T-RPT-01): например

```sql
CREATE TABLE IF NOT EXISTS reporting.kafka_payment_events (
  payment_id String, merchant_id String,
  expected_amount Tuple(minor_units Int64, asset String, decimals UInt32),
  received_amount Tuple(minor_units Int64, asset String, decimals UInt32),
  fee Tuple(minor_units Int64, asset String, decimals UInt32),
  quote_id String, rate String, txid String, state_version UInt64,
  occurred_at Tuple(seconds Int64, nanos Int32))
ENGINE = Kafka
SETTINGS kafka_broker_list = '...', kafka_topic_list = 'payment.events',
  kafka_group_name = 'cbl-reporting-payment', kafka_format = 'ProtobufSingle',
  format_schema = 'events.proto:PaymentRow', kafka_handle_error_mode = 'stream',
  kafka_skip_broken_messages = 1000, kafka_num_consumers = 1
COMMENT 'cbl-pipeline:<отпечаток DDL и конфигурации>';
```

Отпечаток (`sha256` от DDL обоих объектов) обеспечивает пересоздание пары при любом
дрейфе, включая правку выборки MV в новой версии сервиса; реконсиляция также
восстанавливает отсутствующий view.

## 6. Схемы и правила

Wire-схема (`deploy/format_schemas/events.proto`) фиксирует: сообщения `PaymentRow`,
`LedgerRow`/`LedgerEntryRow`, `PayoutRow`, `ReconAlertRow`, `Money`, `Timestamp`; номера
полей соответствуют контрактам; enum-поля не декодируются в колонки (`type`/`severity`
опущены в таблицах; `direction` - Int32 с фильтром). Подзапросы в MV переименовывают
кортежи и enum-колонки: алиас, совпадающий с именем колонки, перекрывает ее для
остальных выражений SELECT (проверено на ClickHouse 24.8). Монтирование схемы:
сервисный compose использует `deploy/format_schemas`; для infra-only запуска зонтичного
стенда копии лежат в `deploy/clickhouse/schemas` и `deploy/clickhouse/format-config`
(синхронность проверяет `scripts/check-clickhouse.ps1`). Изменение контрактов с новыми
полями фактов - аддитивное добавление полей в схему и таблицы.

## 7. Edge cases

Повторная доставка: строки идентичны, схлопываются FINAL (11.1); повторный запуск сервиса
не дублирует таблицы; смена брокеров пересоздает Kafka-таблицу и MV (данные в фактах не
затрагиваются); битое сообщение - пропуск (skip) с продолжением партиции; пустой
`_headers['event_id']` - строка вставится с пустым key (FINAL-дедуп не сработает), поэтому
MV леджера/платежей/выплат отбрасывают строки с пустыми ключевыми полями; события с пустым
`transaction_id` (reserve.created/released) строк не создают; `entries` пустых нет;
overflow `entry_index` и `decimals` отфильтрован; время конвертируется секундами плюс
наносекундами с усечением до миллисекунд (DateTime64(3, 'UTC')).

## 8. Нефункциональные требования

Задержка конвейера - секунды (`kafka_group_name` один на факт, `kafka_num_consumers=1`
для dev); реконсиляция - секунды на старте; сервис не держит собственного Kafka-клиента
(меньше кода и зависимостей, T-10). Безопасность: DSN с паролем из env, не логируется;
`clickhouse` в сервисном compose - dev-стенд; wire-схема - публичный артефакт контракта.

## 9. План тестов

| Уровень | Сценарии |
| :-- | :-- |
| Unit | сборка SQL реконсиляции (брокеры, схема, комментарий-отпечаток, фильтры), разбор env, валидация конфига (в т.ч. `..` в пути схемы и висячая запятая брокеров), санитизация ошибки DSN |
| Integration | testcontainers Kafka+ClickHouse: goose-миграции и версия; идемпотентная реконсиляция; поток payment/ledger/payout/recon событий (реальные protobuf из `cbl-protos`): значения денег, entry_index 0/1, миллисекундная точность времени, `event_type` из заголовка; повтор события: доставка подтверждается (две строки) и схлопывается FINAL; сообщение с недопустимым `decimals` отфильтровано; битое сообщение не останавливает партицию; сокращение списка брокеров (подстрока) обнаруживается; удаленный MV восстанавливается, данные фактов сохраняются, поток продолжается |
| E2E локальный | compose: up kafka+clickhouse+reporting, создание топиков, продюсер событий (Go, `test/e2e/produce`), строки в `fact_*`, значения, повтор события (доставка и схлопывание), битое сообщение, readyz; down -v |
| E2E глобальный | GE-7 после 11.3/11.4 (сквозной конвейер), здесь проверяется сервисный стенд |
| Load | не применим (нет HTTP-нагрузки сервиса); 14.2 покроет конвейер |

Целевое покрытие unit+integration: не ниже 80%.

## 10. Ревью (CBL-001, шаг 6)

Четыре независимых прохода (15.10.2026) и верификация закрытия. BLOCKER закрыт:
достижимая уязвимость ch-go v0.63.1 (GO-2025-3603) - обновление до clickhouse-go
v2.34.0/ch-go v0.65.1, govulncheck чист; mojibake русских комментариев main.go и
.env.example и посторонний файл в корне удалены. MAJOR закрыты: реконсиляция сверяет
ожидаемый DDL отпечатком в `COMMENT` (ловит сокращение списка брокеров, правки колонок
и выборки) и восстанавливает отсутствующий MV; nanos Timestamp учитываются
(миллисекундная точность подтверждена тестом); недопустимый `decimals` фильтруется;
runbook/спека синхронизированы по относительному `SCHEMA_PATH`; зонтичный стенд не
требует клона сервисов (копии схемы и конфига в `deploy/clickhouse`, синхронность
проверяет `check-clickhouse.ps1`); `severity` вне замороженных значений не попадает в
факты (алиас `severity` перекрывал колонку - alerts MV обернут в подзапрос). MINOR/NIT
закрыты: sanitized-ошибка DSN (без пароля), 127.0.0.1 в тестовом биндинге Kafka,
уникальное имя тестовой сети, явное ожидание доставки дублей в тестах и e2e,
`KAFKA_BROKERS` в `.env.example` с внутренним адресом, `recon.alert` в тестовом
продюсере, doc.go и комментарии пакета, `Ready` (readyz и gRPC health отражают
конвейер).

| Роль | Ключевые замечания | Закрытие |
| :-- | :-- | :-- |
| Без контекста | MAJOR: подстроковый детект брокеров, MV не сверяется/не лечится, фильтр decimals заявлен но отсутствует, спека vs код по SCHEMA_PATH; MINOR: nanos не учитываются, нет поведенческих edge-тестов, слабые dedup-проверки, mojibake, doc.go | отпечаток-COMMENT и heal; фильтр `<= 18`; спека/схема синхронизированы; nanos+тест; доставка-и-схлопывание, decimals-кейс, heal и сокращение брокеров в integration; кодировки; doc.go |
| Архитектуры | BLOCKER: вечный простой при частичном состоянии; MAJOR: дрейф кроме брокеров, nanos, конкурентные реплики; MINOR: severity=0, SCHEMA_PATH, doc.go, mojibake, `.env` брокеры, depends_on clickhouse в зонтичном compose, healthcheck, паритет схемы, recon.alert | heal + отпечаток; single-writer зафиксирован (FR-3, 14.7); nanos; severity-фильтр; доку/`.env`/зависимость исправлены; паритет схемы отмечен (KI-06 проверка копий); продюсер исправлен |
| С контекстом | BLOCKER: кодировка main.go/.env, файл-мусор; MAJOR: heal/полнота сверки, противоречие формата схемы, infra-only стенд требует клона сервисов; MINOR: depends_on, `.env`, TOCTOU-порт, имя сети, DSN-пароль, doc.go, статусы, dim_accounts-ссылка 11.1 | перезапись файлов; отпечаток/heal; относительный путь; копии в `deploy/clickhouse` + проверка синхронности; depends_on/`.env`/127.0.0.1/уникальная сеть; sanitized-ошибка; doc.go; 11.1 поправлен |
| Безопасности | BLOCKER: GO-2025-3603 (ch-go) достижима - апгрейд; MAJOR: пароль DSN в логе ошибки; MINOR: усечение decimals, подстроковая сверка, 0.0.0.0-биндинг теста, мусор, mojibake, SCHEMA_PATH, skip-политика | апгрейд+vulncheck; sanitized-ошибка; фильтр decimals; отпечаток; 127.0.0.1; мусор удален; кодировки; доку исправлена; skip осознан (T-RPT-04, KI-reporting-03/04) |

Принято осознанно: runtime-DDL конвейера (T-RPT-01, single-writer), потеря доставки в
окне смены брокеров (данные фактов целы), skip_broken без DLQ до 11.3, dev-стенд с
root+DO_NOT_CHOWN (KI-reporting-02), образы без digest (0.2), ретеншен/PII - 14.7.

## 11. План работ и коммит

Тесты вперед -> миграции и wire-схема -> storage (ClickHouse, реконсиляция) -> main ->
integration/e2e -> прогоны -> 4 ревью -> AGENTS/README -> коммит
`CBL-11.2: конвейер ClickHouse: Kafka engine, materialized view и хранилище сервиса`.

## 12. Открытые вопросы и решения

- Формат декодирования: wire-совместимое уплощение вместо монтирования `cbl-protos`
  (ClickHouse не резолвит импорты между файлами схем; проверено эмпирически на 24.8.14).
- `format_schema` - путь относительно `format_schema_path` (каталог задается конфигом
  сервиса; абсолютный путь Kafka engine запрещает - проверено).
- Kafka-таблицы и MV - runtime-реконсиляция с отпечатком в `COMMENT` (T-RPT-01):
  сравниваются значения, а не подстроки; heal частичного состояния; один писатель.
- Инфраструктурный стенд без клона сервисов: копии wire-схемы и конфига в
  `deploy/clickhouse` зонтичного репозитория, синхронность - `check-clickhouse.ps1`.
- Словарь `dim_accounts` и сверки - 11.3; `Query`/прокси - 11.4; prod-образы, права
  CH и нагрузка - 0.6/14.2/14.7.
