# CBL-11.4. Reporting Query: gRPC-сервис отчетов и прокси /v1/reports

| Поле | Значение |
| :-- | :-- |
| Эпик | E11, `Reporting и ClickHouse` |
| Репозитории | `cbl-reporting` (сервис `Query`), `cbl-billing-api` (прокси `/v1/reports`) |
| Размер | M |
| Волна и параллельная группа | W3-REPORT |
| Зависит от | `CBL-11.2` (конвейер), `CBL-11.3` (словарь: фильтр по `merchant_id` строк леджера), контракты `cbl-protos@cf-3` |
| Версия контрактов | `cbl-protos@cf-3.4` (`billing/reporting/v1` без изменений с CF-3; OpenAPI `/reports/{report_type}`) |
| Статус | done, ревью закрыто |

## 1. Контекст и цель

`CBL-000`: Reporting отдает отчеты по gRPC `Query`, Billing API проксирует `/v1/reports`
и подставляет `merchant_id` из токена, а не из запроса. Контракт `billing.reporting.v1`
заморожен в CF-3 (`QueryRequest`/`QueryResponse`/`ReportRow`, дефолт лимита 20, максимум
100, курсор непрозрачен, деньги - десятичные строки без float). OpenAPI
`billing.v1.yaml` описывает `GET /reports/{report_type}`: закрытый каталог типов 11.4,
неизвестный тип - 404 `unknown_report_type`, `group_by` - только значения whitelist
каталога (параметризация запросов ClickHouse, не пользовательский SQL).

Задача 11.4 открывает отчетный периметр: реализует `ReportingService.Query` в
`cbl-reporting` (каталог отчетов, выборки из фактов с `FINAL`, пагинация, лимиты) и
проксирующий эндпоинт в `cbl-billing-api` (аутентификация 7.2, merchant из токена,
маппинг ошибок в problem+json).

## 2. Границы

In scope: каталог из четырех отчетов и их колонки; выборки из `fact_*`/`dim_accounts`
(только чтение, T-09); непрозрачный курсор; лимиты и валидация; gRPC-сервер
`ReportingService`; BFF-прокси `GET /v1/reports/{report_type}` с мерчантом из токена;
интеграционные тесты (ClickHouse; stub Reporting) и e2e в обоих репозиториях;
документация.

Out of scope: дашборды (11.5); документация и SDK отчетов (15.4); новые контракты
(`cbl-protos` не меняется); квоты/readonly-профиль ClickHouse (14.7); ретеншен (14.7);
проекции по `merchant_id` (KI-reporting-11); HTTP-нагрузочный профиль отчетов (14.1);
административные отчеты back-office (12.x).

## 3. Функциональные требования

FR-1. Закрытый каталог отчетов (тип -> колонки -> сортировка -> курсор):

- `payments` - строки `fact_payment_events` FINAL; колонки `payment_id`, `merchant_id`,
  `event_type`, `expected_amount`, `received_amount`, `fee_amount`, `asset`, `quote_id`,
  `rate`, `txid`, `state_version`, `occurred_at`; порядок `(occurred_at, payment_id,
  event_id)`; курсор - те же три ключа;
- `payouts` - `fact_payouts` FINAL; колонки `payout_id`, `merchant_id`, `event_type`,
  `amount`, `fee_amount`, `asset`, `txid`, `provider`, `failure_reason`,
  `refund_invoice_id`, `state_version`, `occurred_at`; порядок `(occurred_at, payout_id,
  event_id)`;
- `ledger_entries` - `fact_ledger_entries` FINAL (обогащенные словарем 11.3); колонки
  `transaction_id`, `entry_index`, `account_id`, `account_type`, `merchant_id`, `asset`,
  `direction`, `amount`, `ref_type`, `ref_id`, `event_type`, `occurred_at`; порядок
  `(occurred_at, transaction_id, entry_index, event_id)`; фильтр мерчанта - колонка
  `merchant_id` (строки без словарной записи в мерчантский фильтр не попадают);
- `turnover` - агрегат оборота леджера мерчанта по `fact_ledger_entries` FINAL;
  `group_by` из whitelist: `asset` (по умолчанию) и `day_asset`; колонки: значение
  группы (`asset`; `day` + `asset`), `debit_total`, `credit_total`, `entries_count`;
  порядок по значению группы; мерчант обязателен.

Неизвестный тип - `NOT_FOUND` (`unknown report type %q`); `group_by`, не входящий в
whitelist типа, - `INVALID_ARGUMENT`; `group_by`, заданный для типа без группировки, -
`INVALID_ARGUMENT`.

FR-2. Семантика выборки: период `[period_from, period_to]` включительно по `occurred_at`
в UTC (границы приводятся к миллисекундам, точности `DateTime64(3)`; незаданные границы
не ограничивают); `merchant_id` пуст - без фильтра для списков (внутренний вызов),
для `turnover` пустой мерчант - `INVALID_ARGUMENT`; лимит 0 - серверный дефолт 20,
больше 100 - `INVALID_ARGUMENT`; пагинация - `LIMIT limit+1` и `next_cursor` из
последней выданной строки, пустой курсор - конец отчета; курсор длиной до 512 символов,
битый по base64/JSON/структуре/формату ключей - `INVALID_ARGUMENT` (проверка ключей
включает время/день, индекс проводки и строковые идентификаторы: структурно
корректный, но испорченный курсор не доходит до ClickHouse). Все выборки - только
`FINAL` и только чтение (T-09).

FR-3. Форматирование `ReportRow.values` (все значения - строки): деньги из
`minor_units Int64` и `decimals UInt8` в каноническую десятичную строку
`^-?(0|[1-9][0-9]*)(\.[0-9]+)?$` (без экспоненты; `100000000`/6 -> `"100.000000"`,
`decimals 0` -> целое); `occurred_at`/`day` - RFC 3339 в UTC (миллисекунды); числа
(`entry_index`, `state_version`, счетчики) - десятичные строки; остальные поля - как
есть.

FR-4. Ошибки gRPC: `NOT_FOUND` - неизвестный тип; `INVALID_ARGUMENT` - лимит,
`group_by`, период (`period_to` раньше `period_from`), курсор; `INTERNAL` - ошибка
ClickHouse; таймаут клиента решает вызывающая сторона (дедлайн RPC 5 с, `CBL-003` §5).

FR-5. BFF-прокси `GET /v1/reports/{report_type}` в `cbl-billing-api` (маршрут группы
`/v1`, `Require` 7.2 + идемпотентность-middleware не влияет на GET):

- `merchant_id` всегда из токена (`httpmw.MerchantID`); одноименный query-параметр не
  принимается (игнорируется);
- параметры: `period_from`, `period_to` (RFC 3339, невалидная метка - 400), `group_by`
  (до 64 символов, whitelist проверяет Reporting), `limit` (1 - 100, дефолт 20),
  `cursor` (до 512 символов); невалидные значения - 400 `validation_failed`;
- вызов Reporting с дедлайном `REPORTING_TIMEOUT` (5 с по умолчанию, 1 - 10 с:
  контрактный дедлайн RPC 5 с остается дефолтом, запас до 10 с ограничен серверным
  WriteTimeout);
- ответ `ReportPage` (`rows` - объекты строковых значений, `next_cursor`) из
  `QueryResponse` без преобразований;
- маппинг ошибок: `NOT_FOUND` -> 404 `unknown_report_type`; `INVALID_ARGUMENT` -> 400
  `validation_failed` (детали в detail); `UNAVAILABLE`/`DEADLINE_EXCEEDED`/прочие - 503
  `service_unavailable` с `Retry-After: 1`;
- `REPORTING_GRPC_ADDR` опционален: пусто - WARN при старте, эндпоинт отвечает 503
  (отчетность деградирует, остальной BFF жив).

FR-6. Конфигурация: `cbl-reporting` - новых переменных нет (каталог закрыт кодом;
`ClickHouseDSN` уже есть); `cbl-billing-api` - `REPORTING_GRPC_ADDR` (host:port,
опционален) и `REPORTING_TIMEOUT` (1s - 10s, по умолчанию 5s: не больше серверного
HTTP WriteTimeout 15s); невалидные значения - отказ старта (таймаут валидируется
безусловно, даже без адреса).

FR-7. Тесты: unit (каталог и валидация, курсор, формат денег и времени, SQL-фильтры
через интеграцию); integration `cbl-reporting` (testcontainers ClickHouse: все четыре
отчета, фильтр мерчанта, пагинация, период включительно, ошибки через реальный gRPC);
integration `cbl-billing-api` (stub Reporting gRPC на реальном порту: проксирование,
merchant из токена, все коды); e2e в обоих репозиториях.

## 4. Контракты

- gRPC: `billing.reporting.v1.ReportingService.Query` (`cbl-protos@cf-3`, без
  изменений): семантика полей - в комментариях контракта; сервис - сервер
  (`cbl-reporting` :9110), Billing API - клиент.
- REST: `GET /v1/reports/{report_type}` (`openapi/billing.v1.yaml`, cf-3.1); ошибки -
  `application/problem+json` (RFC 9457); коды `unknown_report_type` (404),
  `validation_failed` (400), `service_unavailable` (503).
- ClickHouse: только чтение `fact_*`/`dim_accounts` FINAL; схема не меняется.
- Kafka: не затрагивается.

## 5. Каталог и запросы (решение)

Скелеты выборок (параметры биндятся драйвером; фильтры добавляются при заданных
значениях):

```sql
-- payments
SELECT payment_id, merchant_id, event_type, expected_amount, received_amount, fee_amount,
       asset, decimals, quote_id, rate, txid, state_version, occurred_at, event_id
FROM reporting.fact_payment_events FINAL
WHERE occurred_at >= toDateTime64(?, 3, 'UTC') AND occurred_at <= toDateTime64(?, 3, 'UTC')
  AND (occurred_at, payment_id, event_id) > (toDateTime64(?, 3, 'UTC'), ?, ?)
ORDER BY occurred_at, payment_id, event_id
LIMIT ?;

-- turnover, group_by = asset
SELECT asset AS grp, any(decimals) AS decimals,
       sumIf(amount, direction = 'debit') AS debit_total,
       sumIf(amount, direction = 'credit') AS credit_total,
       count() AS entries_count
FROM reporting.fact_ledger_entries FINAL
WHERE merchant_id = ? AND ...
GROUP BY grp ORDER BY grp LIMIT ?;
```

`turnover`/`day_asset` группирует по `(toDate(occurred_at), asset)`; суммы - в
минимальных единицах одного актива (шкала в строке), float не возникает. Курсор -
`base64url(JSON {"v":1,"report":"<тип>","keys":[...]})`; `keys` имеет длину ключей
сортировки типа, значения - строки (время в формате `2006-01-02 15:04:05.000`).

## 6. Edge cases

- Период не задан - вся история; `period_to` раньше `period_from` - `INVALID_ARGUMENT`;
  границы включаются на обеих сторонах, кромка выровнена по миллисекундам.
- Пустой результат - нулевые строки и пустой `next_cursor`.
- Последняя страница: `limit+1`-я строка не отдается, `next_cursor` пуст.
- Курсор другой длины/чужого отчета/битый base64 - `INVALID_ARGUMENT`.
- `ledger_entries` для мерчанта: строки без словарной записи (`merchant_id = ''`)
  не попадают в фильтр (обогащение - 11.3, сходимость по KI-reporting-05/10).
- `turnover` без мерчанта - `INVALID_ARGUMENT` (агрегат по всем мерчантам не отчет
  мерчанта); пустой период при заданном мерчанте - нулевые суммы не возвращаются
  (групп нет - строк нет).
- Дубли at-least-once - схлопываются `FINAL`; решения денежных вопросов по отчетам
  запрещены (T-09) - зафиксировано в спеках и AGENTS.
- Отрицательные суммы (сторно/корректировки) форматируются со знаком.

## 7. Нефункциональные требования

Дедлайн RPC 5 с (`CBL-003` §5); лимит строк за запрос - 100, чтение по индексам
сортировки фактов (проекции - KI-reporting-11, профиль - 14.1/14.2); запрет float -
деньги только int64 плюс decimals и канонические строки; SQL только с биндингом
параметров; `group_by` - whitelist каталога (пользовательский ввод не попадает в SQL);
данные для BFF - только из ClickHouse сервиса (T-09).

## 8. План тестов

| Уровень | Сценарии |
| :-- | :-- |
| Unit | каталог: типы/колонки/ключи курсора/whitelist `group_by`; валидация (лимит, период, курсор, пустой мерчант `turnover`); формат денег (`decimals 0`, знак, канонический вид) и времени; кодирование/декодирование курсора; BFF: разбор параметров и маппинг кодов |
| Integration `cbl-reporting` | ClickHouse: все четыре отчета на посеянных фактах, фильтр мерчанта (включая исключение строк без словаря), пагинация постранично по каждому отчету (включая `day_asset`; без дублей и потерь), период включительно, пустая страница, отрицательные суммы (знак), подделанный курсор - `InvalidArgument` без обращения к ClickHouse, ошибки через реальный gRPC (404/400) |
| Integration `cbl-billing-api` | stub Reporting gRPC: проксирование страниц, merchant из токена (параметр запроса игнорируется), 404/400/503, пагинация по курсору |
| E2E `cbl-reporting` | compose: события -> факты -> словарь -> grpcurl `Query` (payments, ledger_entries, turnover, unknown type, invalid group_by) |
| E2E `cbl-billing-api` | compose: ключ -> `GET /v1/reports/...` (merchant из токена, пагинация), 404/400/401/503 (stub down), stub echo |
| Load | не в этом масштабе: профиль чтений отчетов - 14.1 |

Целевое покрытие unit+integration: не ниже 80% в каждом репозитории.

## 9. План работ и коммиты

Тесты вперед (unit каталога/курсора/формата) -> `cbl-reporting`: `internal/storage`
(выборки) + `internal/reports` (сервис) + gRPC-транспорт + main -> интеграционные и
e2e -> `cbl-billing-api`: клиент `internal/reporting` + `internal/transport/reports.go`
+ main/compose/stub/e2e -> прогоны обоих -> 4 ревью (общие для задачи) -> AGENTS/README
-> коммиты: `CBL-11.4: Reporting Query: каталог отчетов и gRPC-сервис` в
`cbl-reporting`, `CBL-11.4: прокси /v1/reports с мерчантом из токена` в
`cbl-billing-api`.

## 10. Ревью (CBL-001, шаг 6)

Четыре независимых прохода (16.10.2026) и верификация закрытия. Блокеры/MAJOR закрыты:
неаутентифицированный gRPC `Query` с пустым `merchant_id` и публикацией 9110 наружу
(security) - добавлен KI-reporting-13, порт в dev-стендах только на loopback;
структурно корректный испорченный курсор доходил до ClickHouse и отдавал 503 вместо
400 - добавлена валидация формата ключей (время/день/индекс/текст) и лимит длины 512
на стороне сервиса; курсорная пагинация payouts/ledger_entries/turnover не исполнялась
ни одним тестом - walkPages по всем отчетам с проверкой дублей и полноты; отсутствие
panic recovery на gRPC - подключен `grpcmw.UnaryServer`. MINOR/NIT закрыты:
`day` в RFC 3339 и `toDate(occurred_at, 'UTC')`, безусловная валидация
`REPORTING_TIMEOUT` (1 - 10 с против WriteTimeout), detail 404 сохраняет причину
Reporting, сохранение причины ошибки в цепочке `%w`, гонка Start/Stop стабов,
`buildReportSQL` (gosec без исключений), пустая страница и отрицательные суммы в
тестах, детерминированный rowKey, единый BOM e2e-скриптов, обновленные AGENTS/README,
зонтичный CBL-004 (GE-7 - сервисный контур) и thin-спека в `cbl-billing-api`.

| Роль | Ключевые замечания | Закрытие |
| :-- | :-- | :-- |
| Без контекста | MAJOR: испорченный курсор -> Internal/503; MINOR: `day` не RFC 3339, timeout не валидируется без адреса, пагинация только payments; NIT: omitempty, `+5` | валидация ключей курсора + лимит 512; day RFC 3339; безусловная валидация 1 - 10 с; walkPages по всем отчетам; omitempty осознан (тест) |
| Архитектуры | MINOR: detail 404, timeout, бюджет WriteTimeout, тесты пагинации, KI-11 про предикаты, 9110/пустой merchant, уровень лога, doc.go, кросс-репо спека | все закрыты правками (10 с против 15 с; KI-reporting-13; thin-спека billing; doc.go) |
| С контекстом | MAJOR: пагинация не исполнялась; MINOR: GE-7 преждевременно, timeout, toDate TZ, гонка стабов, описания команд, emdash-охват, BOM, runbook; NIT: комментарии, `%w`, ToolsDir, статус спеки | все закрыты (walkPages; формулировка GE-7; UTC; локальные копии в стабах; BOM; cleanup; статус) |
| Безопасности | MAJOR: gRPC без authN + 9110 наружу (KI отсутствовал); MINOR: нет interceptor, нет границ курсора/дедлайна; NIT: runbook про курсор, токен в argv | KI-reporting-13 + loopback; grpcmw.UnaryServer; лимит 512; формулировки runbook; argv принят осознанно (dev) |

Принято осознанно: пустой `merchant_id` для списков как платформенный внутренний вызов
(KI-reporting-13); plaintext gRPC между BFF и Reporting (KI-billing-api-09); серверный
timeout-интерцептор - 0.6/14.7; стоимость FINAL-сканов отчетов - KI-reporting-11
(профиль 14.2).

## 11. Открытые решения

- Каталог закрытый и минимальный (payments, payouts, ledger_entries, turnover):
  расширяется аддитивно задачей (15.4 - документация; новые типы - новая задача с
  версией каталога); `payments`/`payouts` без `group_by` - строки событий, агрегаты -
  `turnover`.
- Курсор - keyset (ключи сортировки), не offset: стабилен при вставках между страницами;
  каждый ключ валидируется по формату своего типа (время/день/индекс/текст), длина
  курсора ограничена 512 символами и на стороне сервиса (не только BFF).
- `turnover` без мерчанта запрещен: это отчет мерчанта; агрегаты платформы - 11.5.
- Merchant filter строк леджера - по денормализованной колонке 11.3 (строки без
  словаря не видны мерчанту до сходимости; KI-reporting-05/10).
- Прокси не переименовывает ошибки Reporting: detail сохраняет причину, код - из
  контракта (`unknown_report_type`/`validation_failed`).
- Пустой `merchant_id` для списков - платформенный внутренний вызов (FR-2); gRPC без
  authN до 0.6 (KI-reporting-13), в dev порт публикуется только на loopback.
- e2e-скрипты самодостаточны (хелперы внутри файла): разделяемый common.ps1 отложен
  до стабилизации раннера; дублирование осознанное.
- Попутно в коммите `cbl-billing-api` исправлен красный с 7.3 unit-тест
  `TestIdempotencyCacheMalformedFallsToPostgres` (переиспользование уже захваченного
  ключа во втором кейсе; диагноз и фикс - в журнале AGENTS сервиса).
