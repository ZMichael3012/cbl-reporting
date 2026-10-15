# AGENTS.md - cbl-reporting

Отчетность из ClickHouse: конвейер событий Kafka (Kafka engine + materialized view),
хранилище `reporting.fact_*`, словарь счетов `dim_accounts` и сверка с Ledger.

Обязательные глобальные правила: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, контракты `../../.specs/CBL-003-contracts.md`.
Длинное тире (U+2014) запрещено, используется только дефис.

## 1. Границы

In scope: ClickHouse как хранилище сервиса (как PostgreSQL у сервисов с БД): миграции
`fact_*` и `dim_accounts` (goose, диалект clickhouse), локальный инстанс в
`deploy/docker-compose.yml`, testcontainers-интеграция (Kafka + ClickHouse) и e2e на
compose сервиса; конвейер Kafka engine + materialized view по топикам `payment.events`,
`ledger.events`, `payout.events`, `recon.alerts`; словарь `dim_accounts` (Ledger gRPC
`ListMerchantAccounts` + деривация системных счетов), обогащение фактов леджера и
backfill истории; периодическая сверка с Ledger `ListEntries` (метрики и логи);
административные `/healthz`/`/readyz`, gRPC health и счетчики `/metrics`.

Out of scope: gRPC `ReportingService.Query` и прокси `/v1/reports` (11.4); дашборды
(11.5); `recon.alerts` и бизнес-сверка шлюза (9.x, Reconciler); ретеншен/TTL и квоты
(14.7); prod-кластер ClickHouse (0.6); mTLS и авторизация gRPC до Ledger (0.6,
KI-ledger-02); перенос вывода `account_id` в `cbl-kit` (KI-ledger-08, до 7.4/10.2);
Kafka Connect (T-10: только при строгом exactly-once).

## 2. Контракты

- Kafka (потребление через Kafka engine ClickHouse, не собственным клиентом):
  `payment.events`, `ledger.events`, `payout.events`, `recon.alerts`; заголовки конверта
  `event_id`, `event_type`, `occurred_at` (cbl-kit/outbox); группы
  `cbl-reporting-payment|ledger|payout|recon`; at-least-once.
- gRPC клиент: `billing.ledger.v1.LedgerService` (`ListMerchantAccounts`,
  `ListEntries`; `cbl-protos@cf-3.2`); plaintext внутри доверенной сети (mTLS - 0.6).
- ClickHouse (база `reporting`): `fact_*`, `dim_accounts` (миграции; схема из CBL-11.1,
  словарь - 11.3), `kafka_*` (Kafka engine, `kafka_handle_error_mode=stream`,
  `kafka_skip_broken_messages=1000`), `<факт>_mv` (materialized view; MV леджера
  джойнит словарь FINAL).
- REST: `/healthz`, `/readyz` (readyz проверяет доступность ClickHouse и полноту
  конвейера: четыре пары Kafka-таблица+MV; статус отражается и в gRPC health).
- gRPC сервер: `grpc.health.v1` (:9110); `ReportingService` - 11.4.
- Метрики: `/metrics` (:9310) - текстовые счетчики словаря, backfill и сверки
  (полный набор и Prometheus-клиент - 0.7).

## 3. Архитектура

```
cmd/reporting/          # main: миграции, реконсиляция, воркеры словаря и сверки
internal/config/        # разбор и валидация env (fail-fast)
internal/domain/        # деривация account_id плана счетов (golden-векторы CBL-003 §9)
internal/ledger/        # gRPC-клиент LedgerService (счета мерчанта, выписки)
internal/storage/       # Open/Ping/Migrate, Pipeline (реконсиляция), dim и сверка (SQL)
internal/vocabulary/    # воркер словаря dim_accounts и backfill истории
internal/ledgercheck/   # воркер сверки с Ledger (окно, round-robin, классификация)
internal/metrics/       # счетчики и текстовый Prometheus-формат /metrics
migrations/             # goose-миграции fact_* и dim_accounts (владелец - сервис)
deploy/format_schemas/  # wire-схема декодирования (уплощение cbl-protos@cf-3.4)
deploy/clickhouse-config/ # format_schema_path для ClickHouse сервиса
test/producer/          # тестовые protobuf-события (integration/e2e)
test/ledgerstub/        # тестовый LedgerService (gRPC) для integration/e2e
test/e2e/               # ingestion.ps1, vocabulary.ps1, produce, ledgerstub
```

Ключевые решения: сервис не держит собственного Kafka-клиента (T-RPT-05); Kafka-таблицы
и MV реконсилируются при старте из env (T-RPT-01), факты и словарь - goose-миграции;
wire-схема - wire-совместимое уплощение контрактов без импортов (T-RPT-02; ClickHouse
не резолвит импорты, а Kafka engine запрещает абсолютный путь - используется
`format_schema_path`); `_headers` в 24.8 - `Array(Tuple(name, value))` (доступ через
`arrayFirst`); алиас, совпадающий с именем колонки, перекрывает ее для остальных
выражений SELECT (кортежи переименовываются в подзапросе MV); обогащение словарем -
JOIN с `FINAL` в правой части (T-RPT-07); backfill - мутации по узкому предикату
(T-RPT-08); сверка - окно с лагом и round-robin по счетам (T-RPT-09).

Библиотеки: стандартный набор (`CBL-002` §6): Gin, zap, grpc, goose (диалект clickhouse),
`clickhouse-go/v2`; testcontainers (Kafka, ClickHouse). `AutoMigrate` запрещен.

## 4. Команды

```powershell
make build             # go build ./...
make test              # go test ./...
make test-integration  # integration (testcontainers: Kafka, ClickHouse)
make cover             # объединенное покрытие unit+integration (cover.out)
make e2e               # compose-стенд: конвейер (11.2), словарь и сверка (11.3)
make vet               # go vet ./... (+ теги integration e2e)
make lint              # golangci-lint в Docker (контекст services/ для cbl-kit)
make up                # стенд: kafka, schema-registry, clickhouse, ledger-stub, reporting
make down              # стенд + volume
```

## 5. Порты и окружение

HTTP `8110`, gRPC `9110`, метрики `9310`.
Переменные: `CLICKHOUSE_DSN` (обязателен, `clickhouse://cbl:...@host:9000/reporting`),
`KAFKA_BROKERS` (обязателен; внутренний листенер для ClickHouse, `kafka:29092` в compose),
`SCHEMA_PATH` (`events.proto` относительно `format_schema_path` ClickHouse; абсолютный
путь Kafka engine запрещает), `MIGRATIONS_DIR` (`./migrations`; в образе
`/app/migrations`), `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `LEDGER_GRPC_ADDR`
(обязателен, host:port), `LEDGER_TIMEOUT` (5s; 1s - 60s), `DIM_REFRESH_INTERVAL`
(5m; 1s - 24h), `DIM_MERCHANTS_LIMIT` (200; 1 - 10000), `BACKFILL_LIMIT`
(1000; 1 - 100000), `LEDGER_CHECK_INTERVAL` (5m), `LEDGER_CHECK_WINDOW` (5m; не
меньше интервала - иначе между циклами остаются непроверенные отрезки),
`LEDGER_CHECK_ACCOUNTS` (10), `LEDGER_CHECK_PAGE_LIMIT` (1000; до 10000),
`LEDGER_CHECK_MAX_PAGES` (10; до 100). Невалидные значения - отказ старта.

## 6. Правила разработки

- Конвейер: SDD-спека в `.specs/` -> тесты вперед -> код -> 4 ревью -> AGENTS -> прогоны ->
  коммит `CBL-<эпик>.<номер>: ...` (спека в том же коммите).
- Изменение колонок фактов: только аддитивно, новой goose-миграцией; wire-схема
  расширяется тем же номером поля контракта (номера buf не переиспользуются).
- Покрытие unit+integration не ниже 80% (текущее - 82.7%); интеграционные проверки
  конвейера - только на реальных Kafka и ClickHouse (testcontainers), не на моках.
- Деньги только int64 в минимальных единицах плюс decimals; float запрещен (в CH -
  Int64 + asset + decimals UInt8, решения о балансе из ClickHouse запрещены, T-09).
- Битые сообщения Kafka пропускаются с логированием, партиция не останавливается.
- Docstring на русском в стиле godoc; ошибки через `%w`; панки в бизнес-коде запрещены.

## 7. SLO и метрики

SLO уточняется в задаче 14.4. Задержка конвейера - секунды; словарь - интервал refresh,
сверка - интервал с окном с лагом. `/metrics` отдает счетчики словаря, backfill и сверки
(частично закрывает KI-reporting-04); полный набор (consumer lag, latency, Prometheus-клиент)
после 0.7. Нагрузка конвейера - 14.2.

## 8. Trade-offs

| ID | Решение | Плюсы | Минусы и компенсация |
| :-- | :-- | :-- | :-- |
| T-RPT-01 | Kafka-таблицы и MV реконсилируются сервисом при старте; ожидаемое определение пары сворачивается в отпечаток (sha256 от DDL обоих объектов) и хранится в `COMMENT` | ловится любой дрейф (брокеры включая сокращение списка, схема, колонки, выборка) без разбора нормализованного DDL; отсутствующий MV восстанавливается | DDL конвейера вне истории миграций | факты - строго goose; пересоздание не трогает данные; реконсиляция рассчитана на одного писателя (масштабирование реплик - 14.2/0.6) |
| T-RPT-02 | Wire-схема - уплощение контрактов `deploy/format_schemas/events.proto`; для infra-only стенда копии в `deploy/clickhouse` зонтичного репозитория | ClickHouse не требует импортов и монтирования репозитория контрактов; infra-запуск без клона сервисов | дублирование номеров полей и файлов-копий | номера и типы повторяют `cbl-protos@cf-3.4` (buf запрещает переиспользование номеров); интеграционные тесты шлют реальные protobuf; синхронность копий проверяет `check-clickhouse.ps1` (KI-reporting-06) |
| T-RPT-03 | ClickHouse - хранилище сервиса (как PostgreSQL у сервисов с БД): схема, миграции, compose и тесты в репозитории | автономность, единая конвенция; глобальный стенд не требуется для тестов | плюс один инстанс ClickHouse в dev | глобальный compose держит общий стенд (T-08); init cbl-system - dev-bootstrap |
| T-RPT-04 | Битые сообщения пропускаются (`kafka_skip_broken_messages=1000`, `handle_error_mode=stream`) | конвейер не останавливается на poison-сообщении | ошибочные события не попадают в факты | счетчик ошибок в логах ClickHouse; разбор и DLQ-пересылка - 11.3 при необходимости |
| T-RPT-05 | Сервис не держит собственного Kafka-клиента (консьюмер - Kafka engine) | меньше кода и зависимостей; масштабирование через `kafka_num_consumers` | нет тонкой обработки в Go | фильтры и маппинг - в MV; gRPC-слой 11.4 не зависит от Kafka |
| T-RPT-06 | Кортежи Money/Timestamp переименовываются в подзапросе MV | обход перекрытия алиасов ClickHouse (алиас гасит одноименную колонку) | читаемость SQL | зафиксировано тестом `TestBuildViewDDL`; проверено на 24.8 |
| T-RPT-07 | Обогащение словарем - JOIN c `(SELECT ... FROM dim_accounts FINAL)` в MV, без словаря ClickHouse | нет учетных данных в DDL, меньше сущностей, изменения словаря видны новым вставкам | JOIN на вставке и необходимость FINAL в правой части | словарь мал (счет на мерчанта/актив), per-block hash join дешев; FINAL исключает размножение строк; отсутствующий счет дает пустые значения, backfill догоняет |
| T-RPT-08 | Backfill истории - мутации `ALTER UPDATE ... WHERE account_type = ''` по счетам словаря (лимит на цикл, выборка только счетов из словаря с детерминированным порядком) | точечная перезапись, идемпотентность, сходимость к полному словарю, бюджет цикла не голодает на legacy-строках | мутации асинхронны и переписывают части; при большом отсталом объеме нужны несколько циклов | `WHERE account_type = ''` безопасен и сходится; лимит `BACKFILL_LIMIT`; очередь мутаций и стоимость сканов - KI-reporting-09/11, профиль 14.2 |
| T-RPT-09 | Сверка с Ledger: round-robin по счетам словаря, окно `[now-2W, now-W)`, страницы агрегируются постранично, результат - логи и счетчики | нет глобального перечисления транзакций в контракте Ledger; полная потеря событий счетом детектируется; окно с лагом гасит ложные missing; память - транзакции окна | полный обход словаря растянут (accountsPerCycle за цикл); ложные missing при задержке событий больше W; однократная проверка окна; курсор в памяти - один писатель на стенд (KI-reporting-12) | окно и лимиты конфигурируемы; при полном сбое порции курсор не двигается; расхождения - диагностика (T-09), не алерты и не денежные решения; калибровка - 14.2 |

## 9. Known issues

| ID | Проблема | Влияние | План | Статус |
| :-- | :-- | :-- | :-- | :-- |
| KI-reporting-01 | CI и pre-commit не настроены | линтеры запускаются вручную | задача 0.2 | open |
| KI-reporting-02 | ClickHouse стенда работает с `CLICKHOUSE_DO_NOT_CHOWN=1` и root-процессом (ro-монтирование wire-схемы) | dev-конвенция; prod-права и пользователь миграций не разделены | разделение прав, read-only-профиль, TLS - 0.6/14.7 | open |
| KI-reporting-03 | Нагрузочный профиль отсутствует (нет HTTP-нагрузки до 11.4) | пропускная способность конвейера не измерена | 14.2 (Kafka -> ClickHouse), профиль Query - 11.4 | open |
| KI-reporting-04 | Квоты и лимиты аналитических запросов не заданы; `/metrics` - только счетчики сервиса (без Prometheus-клиента) | тяжелый `Query` может влиять на ingestion; нет consumer lag и latency | readonly, max_memory_usage, max_execution_time - 14.7/11.4; полный набор метрик - 0.7 | open |
| KI-reporting-05 | `dim_accounts` наполнен не мгновенно: строки между циклами refresh и backfill временно содержат пустые `account_type`/`merchant_id` (до первого цикла на новом стенде) | фильтр по мерчанту в 11.4 не покрывает промежуточные строки до сходимости циклов | ЗАКРЫТ 16.10.2026: словарь, обогащение на входе и backfill реализованы (11.3); сходимость - целевое свойство, свежесть строк - по интервалам env | closed |
| KI-reporting-06 | Wire-схема и конфиг format_schema продублированы в зонтичном `deploy/clickhouse` (для инфраструктурного запуска без клона сервисов) | расхождение копий сломает декодирование на общем стенде | синхронность проверяет `scripts/check-clickhouse.ps1` (SHA256); при переносе схемы на строковый источник - снять копии | open |
| KI-reporting-07 | Integration-тест Kafka использует фиксированный свободный порт хоста (окно TOCTOU при параллельном CI) | редкий флейк при гонке за порт | динамический проброс после старта либо координация портов - при стабилизации CI (0.2) | open |
| KI-reporting-08 | gRPC-канал до Ledger - plaintext, клиент не запрашивает TLS/authN | подмена адреса Ledger в сети стенда дает ложный словарь и сверку; ложные расхождения | mTLS/авторизация межсервисных вызовов - 0.6 (KI-ledger-02); порт Ledger не публиковать вне доверенной зоны; прод запрещен до 0.6 | open |
| KI-reporting-09 | Backfill - мутации на каждом цикле по счетам с пустым `account_type`; на большом отсталом объеме очередь мутаций растет | перезапись партиций конкурирует с ingestion за ресурсы | лимит `BACKFILL_LIMIT`; при систематике - батчи/расписание (14.2/14.7); `WHERE account_type = ''` не трогает обогащенные строки | open |
| KI-reporting-10 | Мерчанты с грязным `merchant_id` в фактах (вне плана счетов) логируются WARN каждый цикл; мерчанты с невыполнимым покрытием (несогласованные данные стенда) перезапрашиваются у Ledger каждый цикл (бюджет `DIM_MERCHANTS_LIMIT` с ротацией - хвост не голодает, но вызовы повторяются) | шум в логах на постоянных грязных данных; лишние вызовы Ledger | дедупликация предупреждений и «карантин» непокрываемых мерчантов после k попыток (при подтверждении); данные чинит источник-сервис | open |
| KI-reporting-11 | Стоимость периодических FINAL-сканов фактов (refresh покрытия, unenriched-выборка, агрегаты сверки) и мутаций backfill не измерена на больших объемах | на целевом профиле (14.2) циклы могут конкурировать с ingestion | замер в 14.2; при подтверждении - проекции/skip-index по `account_id`, инкрементальное состояние unenriched-счетов, батч-мутации | open |
| KI-reporting-12 | Воркеры словаря и сверки не координируются между репликами: дубли вызовов Ledger и мутаций, курсор сверки в памяти | при N инстансах лишняя нагрузка и рассинхрон обхода; корректность не страдает (идемпотентно) | single-writer (advisory lock) либо явная фиксация конвенции до масштабирования (0.6/14.2) | open |

## 10. Журнал задач

| CBL | Дата | Статус | Спека | Коммит |
| :-- | :-- | :-- | :-- | :-- |
| CBL-11.2 | 2026-10-15 | done, ревью закрыто (конвейер ClickHouse: миграции fact_*, Kafka engine + MV по четырем топикам, wire-схема, реконсиляция по отпечатку с самовосстановлением, testcontainers и e2e; покрытие 88.2%) | `.specs/CBL-11.2-ingestion.md` | 8a0bd36 |
| CBL-11.3 | 2026-10-16 | done (словарь dim_accounts: мерчантские счета из Ledger gRPC по покрытию фактов, системные деривацией по golden-векторам; обогащение на входе через JOIN MV; backfill истории; сверка с Ledger окном и round-robin; stub Ledger и e2e vocabulary.ps1; покрытие 82.7%) | `.specs/CBL-11.3-ledger-sync.md` | этот коммит |

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

Словарь и сверка (11.3):

1. Словарь не пополняется: логи `vocabulary cycle failed` (Ledger недоступен, проверьте
   `LEDGER_GRPC_ADDR`), либо пустой ответ `ListMerchantAccounts` при отсутствии счетов;
   счетчики `reporting_vocabulary_*` на `/metrics`.
2. Строки фактов леджера с пустым `account_type` дольше нескольких циклов: счет вне
   плана словаря (legacy) либо отставание мутаций; проверить
   `SELECT count() FROM system.mutations WHERE is_done = 0` (KI-reporting-09/11).
3. Расхождения сверки `missing_in_reporting`: сверить окно (`LEDGER_CHECK_WINDOW`),
   лаг конвейера и отставание; единичные ранние missing у границы окна - задержка
   событий, систематические - разбор `system.kafka_consumers` и DLQ-политики (T-RPT-04).
4. Ошибки сверки по счету: WARN `ledger check account failed` в логах (таймаут Ledger,
   превышение страниц - `truncated`); счетчики `reporting_ledger_check_*` на `/metrics`.
   При полном сбое порции (Ledger недоступен) курсор не двигается - цикл повторит ту же
   порцию после восстановления; это видно по росту `errors` при неизменном обходе.
5. Пересоздание словаря с нуля: `TRUNCATE TABLE reporting.dim_accounts` затем цикл
   refresh; backfill повторно обогатит историю (идемпотентно).
6. Новый актив или payout-счет мерчанта появляется с задержкой до одного цикла refresh:
   требуемое покрытие из фактов (платежи - merchant, выплаты - payout) запускает
   пересинхронизацию мерчанта; до этого строки остаются без значений и дозаполняются
   backfill-ом.
