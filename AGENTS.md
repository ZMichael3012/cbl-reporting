# AGENTS.md - cbl-reporting

Отчетность из ClickHouse: конвейер событий Kafka (Kafka engine + materialized view),
хранилище `reporting.fact_*`.

Обязательные глобальные правила: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, контракты `../../.specs/CBL-003-contracts.md`.
Длинное тире (U+2014) запрещено, используется только дефис.

## 1. Границы

In scope: ClickHouse как хранилище сервиса (как PostgreSQL у сервисов с БД): миграции
`fact_*` (goose, диалект clickhouse), локальный инстанс в `deploy/docker-compose.yml`,
testcontainers-интеграция (Kafka + ClickHouse) и e2e на compose сервиса; конвейер
Kafka engine + materialized view по топикам `payment.events`, `ledger.events`,
`payout.events`, `recon.alerts`; административные `/healthz`/`/readyz` и gRPC health.

Out of scope: gRPC `ReportingService.Query` и прокси `/v1/reports` (11.4); словарь
`dim_accounts`, сверки и backfill (11.3); дашборды (11.5); ретеншен/TTL и квоты (14.7);
prod-кластер ClickHouse (0.6); Kafka Connect (T-10: только при строгом exactly-once).

## 2. Контракты

- Kafka (потребление через Kafka engine ClickHouse, не собственным клиентом):
  `payment.events`, `ledger.events`, `payout.events`, `recon.alerts`; заголовки конверта
  `event_id`, `event_type`, `occurred_at` (cbl-kit/outbox); группы
  `cbl-reporting-payment|ledger|payout|recon`; at-least-once.
- ClickHouse (база `reporting`): `fact_*` (миграции; схема из CBL-11.1), `kafka_*`
  (Kafka engine, `kafka_handle_error_mode=stream`, `kafka_skip_broken_messages=1000`),
  `<факт>_mv` (materialized view).
- REST: `/healthz`, `/readyz` (readyz проверяет доступность ClickHouse и полноту
  конвейера: четыре пары Kafka-таблица+MV; статус отражается и в gRPC health).
- gRPC: `grpc.health.v1` (:9110); `ReportingService` - 11.4.

## 3. Архитектура

```
cmd/reporting/          # main: миграции, реконсиляция, health, метрики-заглушка
internal/storage/       # Open/Ping ClickHouse, Migrate (goose), Pipeline (реконсиляция)
migrations/             # goose-миграции fact_* (владелец схемы - сервис, CBL-11.2)
deploy/format_schemas/  # wire-схема декодирования (уплощение cbl-protos@cf-3.4)
deploy/clickhouse-config/ # format_schema_path для ClickHouse сервиса
test/producer/          # тестовые protobuf-события (integration/e2e)
test/e2e/               # ingestion.ps1 и продюсер сценариев
```

Ключевые решения: сервис не держит собственного Kafka-клиента (T-RPT-05); Kafka-таблицы
и MV реконсилируются при старте из env (T-RPT-01), факты - goose-миграции; wire-схема -
wire-совместимое уплощение контрактов без импортов (T-RPT-02; ClickHouse не резолвит
импорты, а Kafka engine запрещает абсолютный путь - используется `format_schema_path`);
`_headers` в 24.8 - `Array(Tuple(name, value))` (доступ через `arrayFirst`); алиас,
совпадающий с именем колонки, перекрывает ее для остальных выражений SELECT (кортежи
переименовываются в подзапросе MV).

Библиотеки: стандартный набор (`CBL-002` §6): Gin, zap, grpc, goose (диалект clickhouse),
`clickhouse-go/v2`; testcontainers (Kafka, ClickHouse). `AutoMigrate` запрещен.

## 4. Команды

```powershell
make build             # go build ./...
make test              # go test ./...
make test-integration  # integration (testcontainers: Kafka, ClickHouse)
make e2e               # compose-стенд сервиса: конвейер до fact_* (CBL-11.2)
make vet               # go vet ./... (+ теги integration e2e)
make lint              # golangci-lint в Docker (контекст services/ для cbl-kit)
make up                # сервисный стенд: kafka, schema-registry, clickhouse, reporting
make down              # стенд + volume
```

## 5. Порты и окружение

HTTP `8110`, gRPC `9110`, метрики `9310`.
Переменные: `CLICKHOUSE_DSN` (обязателен, `clickhouse://cbl:...@host:9000/reporting`),
`KAFKA_BROKERS` (обязателен; внутренний листенер для ClickHouse, `kafka:29092` в compose),
`SCHEMA_PATH` (`events.proto` относительно `format_schema_path` ClickHouse; абсолютный
путь Kafka engine запрещает), `MIGRATIONS_DIR` (`./migrations`; в образе
`/app/migrations`), `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`.

## 6. Правила разработки

- Конвейер: SDD-спека в `.specs/` -> тесты вперед -> код -> 4 ревью -> AGENTS -> прогоны ->
  коммит `CBL-<эпик>.<номер>: ...` (спека в том же коммите).
- Изменение колонок фактов: только аддитивно, новой goose-миграцией; wire-схема
  расширяется тем же номером поля контракта (номера buf не переиспользуются).
- Покрытие unit+integration не ниже 80% (текущее - 88.2%); интеграционные проверки
  конвейера - только на реальных Kafka и ClickHouse (testcontainers), не на моках.
- Деньги только int64 в минимальных единицах плюс decimals; float запрещен (в CH -
  Int64 + asset + decimals UInt8, решения о балансе из ClickHouse запрещены, T-09).
- Битые сообщения Kafka пропускаются с логированием, партиция не останавливается.
- Docstring на русском в стиле godoc; ошибки через `%w`; панки в бизнес-коде запрещены.

## 7. SLO и метрики

SLO уточняется в задаче 14.4. Задержка конвейера - секунды; базовые метрики после 0.7:
consumer lag Kafka-групп, счетчик skipped-сообщений, задержка ingestion, ошибки
реконсиляции. Нагрузка конвейера - 14.2.

## 8. Trade-offs

| ID | Решение | Плюсы | Минусы и компенсация |
| :-- | :-- | :-- | :-- |
| T-RPT-01 | Kafka-таблицы и MV реконсилируются сервисом при старте; ожидаемое определение пары сворачивается в отпечаток (sha256 от DDL обоих объектов) и хранится в `COMMENT` | ловится любой дрейф (брокеры включая сокращение списка, схема, колонки, выборка) без разбора нормализованного DDL; отсутствующий MV восстанавливается | DDL конвейера вне истории миграций | факты - строго goose; пересоздание не трогает данные; реконсиляция рассчитана на одного писателя (масштабирование реплик - 14.2/0.6) |
| T-RPT-02 | Wire-схема - уплощение контрактов `deploy/format_schemas/events.proto`; для infra-only стенда копии в `deploy/clickhouse` зонтичного репозитория | ClickHouse не требует импортов и монтирования репозитория контрактов; infra-запуск без клона сервисов | дублирование номеров полей и файлов-копий | номера и типы повторяют `cbl-protos@cf-3.4` (buf запрещает переиспользование номеров); интеграционные тесты шлют реальные protobuf; синхронность копий проверяет `check-clickhouse.ps1` (KI-reporting-06) |
| T-RPT-03 | ClickHouse - хранилище сервиса (как PostgreSQL у сервисов с БД): схема, миграции, compose и тесты в репозитории | автономность, единая конвенция; глобальный стенд не требуется для тестов | плюс один инстанс ClickHouse в dev | глобальный compose держит общий стенд (T-08); init cbl-system - dev-bootstrap |
| T-RPT-04 | Битые сообщения пропускаются (`kafka_skip_broken_messages=1000`, `handle_error_mode=stream`) | конвейер не останавливается на poison-сообщении | ошибочные события не попадают в факты | счетчик ошибок в логах ClickHouse; разбор и DLQ-пересылка - 11.3 при необходимости |
| T-RPT-05 | Сервис не держит собственного Kafka-клиента (консьюмер - Kafka engine) | меньше кода и зависимостей; масштабирование через `kafka_num_consumers` | нет тонкой обработки в Go | фильтры и маппинг - в MV; gRPC-слой 11.4 не зависит от Kafka |
| T-RPT-06 | Кортежи Money/Timestamp переименовываются в подзапросе MV | обход перекрытия алиасов ClickHouse (алиас гасит одноименную колонку) | читаемость SQL | зафиксировано тестом `TestBuildViewDDL`; проверено на 24.8 |

## 9. Known issues

| ID | Проблема | Влияние | План | Статус |
| :-- | :-- | :-- | :-- | :-- |
| KI-reporting-01 | CI и pre-commit не настроены | линтеры запускаются вручную | задача 0.2 | open |
| KI-reporting-02 | ClickHouse стенда работает с `CLICKHOUSE_DO_NOT_CHOWN=1` и root-процессом (ro-монтирование wire-схемы) | dev-конвенция; prod-права и пользователь миграций не разделены | разделение прав, read-only-профиль, TLS - 0.6/14.7 | open |
| KI-reporting-03 | Нагрузочный профиль отсутствует (нет HTTP-нагрузки до 11.4) | пропускная способность конвейера не измерена | 14.2 (Kafka -> ClickHouse), профиль Query - 11.4 | open |
| KI-reporting-04 | Квоты и лимиты аналитических запросов не заданы | тяжелый `Query` может влиять на ingestion | readonly, max_memory_usage, max_execution_time - 14.7/11.4 | open |
| KI-reporting-05 | `dim_accounts` не наполнен: `account_type`/`merchant_id` фактов леджера пусты | фильтр по мерчанту в 11.4 не покроет исторические строки леджера | словарь и backfill - 11.3 | open |
| KI-reporting-06 | Wire-схема и конфиг format_schema продублированы в зонтичном `deploy/clickhouse` (для инфраструктурного запуска без клона сервисов) | расхождение копий сломает декодирование на общем стенде | синхронность проверяет `scripts/check-clickhouse.ps1` (SHA256); при переносе схемы на строковый источник - снять копии | open |
| KI-reporting-07 | Integration-тест Kafka использует фиксированный свободный порт хоста (окно TOCTOU при параллельном CI) | редкий флейк при гонке за порт | динамический проброс после старта либо координация портов - при стабилизации CI (0.2) | open |

## 10. Журнал задач

| CBL | Дата | Статус | Спека | Коммит |
| :-- | :-- | :-- | :-- | :-- |
| CBL-11.2 | 2026-10-15 | done, ревью закрыто (конвейер ClickHouse: миграции fact_*, Kafka engine + MV по четырем топикам, wire-схема, реконсиляция по отпечатку с самовосстановлением, testcontainers и e2e; покрытие 88.2%) | `.specs/CBL-11.2-ingestion.md` | этот коммит |

## 11. Runbook

Конвейер (11.2):

1. Строки не появляются в `fact_*`: проверить `SELECT * FROM system.kafka_consumers` -
   ошибки подключения/парсинга, число skipped; проверить `KAFKA_BROKERS` (внутренний
   адрес для ClickHouse, не host-порт) и наличие топиков.
2. Схема декодирования: ошибка `CANNOT_PARSE_PROTOBUF_SCHEMA` - проверить
   `SCHEMA_PATH` (относительный путь) и монтирование `deploy/format_schemas` в
   `format_schema_path`, файл конфигурации `10-format-schema-path.xml`.
3. Смена брокеров Kafka: перезапустить сервис - реконсиляция пересоздаст Kafka-таблицы и
   MV (данные `fact_*` сохраняются); в логе будет `kafka pipeline configuration changed`.
4. Битые сообщения: счетчик `kafka_skip_broken_messages`; найти сообщение по смещению в
   логах ClickHouse; при массовости - остановить продюсера и разобрать контракт.
5. Миграции: применяются при старте (`goose_db_version`); только вперед; изменение
   колонок фактов - новой миграцией, существующие не править.
6. Отставание: группы `cbl-reporting-*`; лаг в `system.kafka_consumers`; при необходимости
   увеличить `kafka_num_consumers` или масштабировать сервис (14.7).
7. Полная перезагрузка стенда: `make down` (уничтожает volume `ch-data` сервиса), затем
   `make up` - миграции и конвейер поднимутся с нуля.
8. Дрейф конвейера: сервис пересоздает пару при несовпадении отпечатка в `COMMENT`
   (лог `kafka pipeline configuration changed`); удаленный MV восстанавливается без
   пересоздания таблицы (`materialized view is missing, recreating`).
9. Инфраструктурный стенд (без клона сервисов): копии wire-схемы и конфига в
   `deploy/clickhouse` зонтичного репозитория; при правке схемы обновить копии и
   прогнать `scripts/check-clickhouse.ps1` (проверяет SHA256, KI-reporting-06).
