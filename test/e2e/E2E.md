# E2E cbl-reporting: pipeline, vocabulary, reports and dashboards

Run instruction for the local e2e scenarios of the service. A run is performed by an agent
platform-independently: docker, a command shell and Go utilities from `.additionals/`.
There are no PowerShell run scripts (customer decision of 16.10.2026, `CBL-002` section 7).

Repository: `cbl-reporting`. Stand: `deploy/docker-compose.yml` (kafka, schema-registry,
clickhouse, ledger-stub, reporting and grafana in the `app` profile).

## 1. Scenarios

| Scenario | Checks | Contracts |
| :-- | :-- | :-- |
| `E2E_Reporting_Ingestion` | migrations, pipeline reconciliation, four-topic flow into `fact_*`, values, FINAL dedup, a broken message does not stop the partition | Kafka, ClickHouse, `/readyz` |
| `E2E_Reporting_Vocabulary` | `dim_accounts` vocabulary (Ledger gRPC plus derivation), history backfill, ingest-time enrichment, Ledger check, `/metrics` counters | Kafka, ClickHouse, Ledger gRPC, HTTP metrics |
| `E2E_Reporting_Reports` | `ReportingService.Query`: payments, cursor pagination, merchant-filtered ledger_entries, turnover, NotFound/InvalidArgument errors | gRPC Reporting, ClickHouse |
| `E2E_Reporting_Dashboards` | Grafana provisioning (datasource, three dashboards) and datasource computations: payments, payouts and turnover panel queries reproduce the exact expected numbers | Grafana HTTP API, ClickHouse |

Minimum for a task run: the scenario affected by the task (the full set - before release
runs and when the pipeline, vocabulary or catalog change).

## 2. Isolation and parallelism

- Run mark: UTC lowercase `yyyymmddthhmmssz` (example: `20261016t070916z`; uppercase is
  rejected by compose project naming rules).
- Artifact directory: `test/e2e/.runs/<mark>/` (create before the start).
- Compose project: `cbl-reporting-e2e-<mark>`; its network is `<project>_cbl-net`
  (needed by the grpcurl container and the stub seed).
- Parallel runs of different services are allowed; parallel runs of this service only with
  different projects and a port override file from `.additionals/` (host ports: 9092,
  8123/9000, 8081, 8110/9110/9310, 9040/9041, 3000).
- All commands run from the repository root unless stated otherwise.

## 3. Common run order

1. `<MARK>` = current UTC time in lowercase `yyyymmddthhmmssz`; create `test/e2e/.runs/<MARK>/`.
2. Stop the previous stand: `docker compose -p cbl-reporting-e2e-<MARK> -f deploy/docker-compose.yml --profile app down -v`
   (output to `compose-down.log`).
3. Start infrastructure: `... up -d --build` (`compose-up.log`).
4. Wait until healthy: `docker compose ps` without `starting` statuses (`compose-ps.txt`).
5. Topics (auto-creation is disabled):
   `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic <t> --partitions 1 --replication-factor 1`
   for `payment.events`, `ledger.events`, `payout.events`, `recon.alerts` (`prepare.log`).
6. Start the service: `... --profile app up -d --build`; wait for `GET http://localhost:8110/readyz` = 200.
   The `app` profile also starts grafana (CBL-11.5); its first image build downloads the
   pinned ClickHouse plugin.
7. Execute scenarios per sections 5-8; each output goes to `<scenario>.log`, check pairs
   "expected/actual" go to `<scenario>-checks.txt`. Every scenario starts from a clean
   stand: repeat steps 2-6 (down -v, up, topics, service) before it; one run mark and one
   compose project are reused for the whole run, volumes are wiped between scenarios.
8. After the scenarios: logs of all stand services -
   `... --profile app logs --no-color > compose-logs.txt` (the `--profile app` flag is
   required: without it the reporting and grafana containers are not included).
9. `summary.md` by the `.additionals/summary.template.md` template; finish with `down -v`
   (`compose-down.log`) and make sure the run containers are removed.

Common utilities:

- SQL to ClickHouse: `docker compose exec -T clickhouse clickhouse-client --user cbl --password cbl --format TSV --query "<SQL>"`;
- Test event producer: `go run ./test/e2e/.additionals/produce -scenario <full|duplicate|broken|sentinel> [-payment-id ... -event-id ... -key ...] [-live]`
  (prints identifiers as `key=value` lines; `-live` sets occurred_at to the current time);
- Metrics: `GET http://localhost:9310/metrics` (vocabulary/backfill/check counters);
- grpcurl (Reports scenario): `docker run --rm --network <project>_cbl-net -v <repo>/../cbl-protos/proto:/p fullstorydev/grpcurl:v1.9.1 -plaintext -import-path /p -proto billing/reporting/v1/reporting.proto -d @ reporting:9110 billing.reporting.v1.ReportingService/Query`
  (pass the body through stdin: platform shells eat inline-JSON quotes).

## 4. Waits and timeouts

Async checks run in a polling loop up to 90 s (2 s interval): facts arrive within seconds,
the vocabulary within `DIM_REFRESH_INTERVAL` (5 s on the stand), backfill mutations up to
30 s. On a timeout, record the last actual value in `*-checks.txt` and collect emergency
dumps (section 9).

## 5. E2E_Reporting_Ingestion

1. After the common order (steps 1-6): `SELECT count() FROM system.tables WHERE database = 'reporting' AND name LIKE 'kafka_%'` = 4; `... name LIKE '%_mv'` = 4.
2. Full flow: `go run ./test/e2e/.additionals/produce -scenario full`; read `payment_id`,
   `payment_event_id`, `transaction_id`, `payout_id`, `alert_id` from the output.
3. Wait for rows (SQL with `FINAL`): payment by `payment_id` = 1; ledger by `transaction_id` = 2;
   payout = 1; alert = 1.
4. Values: `event_type|expected_amount|asset|decimals` = `payment.completed|100000000|USDT|6`;
   for `acc-merchant` `entry_index` = 1 and `direction` = `credit`; alert = `amount_mismatch|warning`.
5. Dedup: publish `duplicate` with the same `payment_id`/`event_id`, then `sentinel` with a new
   payment_id and the original event key; wait for sentinel (same partition) and check
   FINAL count of the original payment = 1.
6. Broken message: `produce -scenario broken -payment-id pay-broken-<uuid>`; wait for
   `after_broken_payment_id` in the facts (the partition kept going).
7. Scenario checks go to `ingestion-checks.txt`.

## 6. E2E_Reporting_Vocabulary

1. Full order (sections 3-4) until `readyz`.
2. Event #1: `produce -scenario full -live`; read `transaction_id`, `occurred_at` from the output.
3. Seed the real posting immediately (before the check window opens) through the stub HTTP.
   Put the JSON body into a file (`<MARK>/seed1.json`) and pass it with `-d @<file>`:
   shells mangle inline-JSON quotes, files do not. Body:
   `{"account_id":"acc-merchant","entries":[{"transaction_id":"<tx1>","direction":"credit","amount_minor":100000000,"created_at":"<occurred_at>"}]}`,
   expected response code 204
   (`curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:9041/entries -H "Content-Type: application/json" -d @<MARK>/seed1.json`).
4. Vocabulary: `dim_accounts FINAL` count for `merchant_id = 'm-1'` = 2; system accounts
   (`clearing`/`fee`/`suspense` for USDT/6) = 3.
5. Backfill: the `acc-merchant` row of event #1 is enriched (`account_type='merchant'`,
   `merchant_id='m-1'`); `acc-clearing` stays empty (out of the plan). Record
   `reporting_backfill_mutations_total` from `/metrics`.
6. Ingest-time enrichment: event #2 (`produce -scenario full -live`); put the seed body into
   `<MARK>/seed2.json` (real #1 and #2 plus a fake posting `tx-fake-<uuid>` with amount 1 and
   the event #2 `created_at`) and POST it the same way; wait for #2 rows; check that the
   `acc-merchant` row of event #2 is already enriched, that no empty `account_type` remains
   for `acc-merchant`, and that the mutation counter did not change.
7. Check: wait for `/metrics` counters: `reporting_ledger_check_cycles_total >= 1`,
   `..._accounts_total >= 2`, `..._discrepancies_total{kind="missing_in_reporting"} >= 1`
   (the fake posting); `extra_in_reporting` = 0, `amount_mismatch` = 0, `truncated` = 0,
   `errors` = 0.
8. Checks go to `vocabulary-checks.txt`.

## 7. E2E_Reporting_Reports

1. Full order (sections 3-4) until `readyz`.
2. Two flows: `produce -scenario full` twice; wait for 2 payments and 2 `acc-merchant` postings
   of `m-1` (enriched by the vocabulary).
3. `payments`: body `{"report_type":"payments","merchant_id":"m-1","limit":20}`; expected:
   2 rows, `values.expected_amount` = `100.000000`, `values.asset` = `USDT`, `values.occurred_at`
   starts with `2026-10-15T10:00:00` (grpcurl uses camelCase field names: `nextCursor`).
4. Pagination: `limit=1` -> non-empty `nextCursor`; the second page with `cursor` from the
   first -> 1 row and an empty `nextCursor`.
5. `ledger_entries`: `merchant_id=m-1` -> 2 rows, every `values.account_id` = `acc-merchant`,
   `values.account_type` = `merchant` (the `acc-clearing` row without a vocabulary entry is
   not visible).
6. `turnover`: `group_by=asset` -> 1 row: `values.credit_total` = `200.000000`,
   `values.entries_count` = `2`.
7. Errors: `invoices` -> `NotFound` code; `payments` with `group_by=asset` -> `InvalidArgument`
   (check the grpcurl exit code and error text).
8. Checks go to `reports-checks.txt`.

## 8. E2E_Reporting_Dashboards

1. Full order (sections 3-4) until `readyz` (the `app` profile starts grafana as well).
2. Data: `produce -scenario full` twice; wait for 2 payments and 2 `acc-merchant` postings
   of `m-1` (the same waits as in the reports scenario).
3. Vocabulary convergence: poll up to 90 s:
   `SELECT count() FROM reporting.fact_ledger_entries FINAL WHERE account_type = 'merchant' AND merchant_id = 'm-1'` = 2
   (the vocabulary cycle and the backfill enrich the rows; before convergence the merchant
   filter panels would be empty, KI-reporting-05/10).
4. Grafana API (basic auth `admin:cbl`, the credentials of the dev stand):
   - `GET http://localhost:3000/api/health` -> contains `"database": "ok"`;
   - `GET http://localhost:3000/api/search?type=dash-db` -> contains the uids
     `cbl-payments`, `cbl-payouts`, `cbl-turnover` (provisioning applied);
   - `GET http://localhost:3000/api/datasources/uid/cbl-clickhouse` -> 200 and
     `grafana-clickhouse-datasource`;
   - `GET http://localhost:3000/api/dashboards/uid/cbl-payments` -> 200 and the title
     `CBL Payments`.
5. Datasource computations (`POST http://localhost:3000/api/ds/query`; the JSON body goes
   into a file under `<MARK>/` and is passed with `--data @<file>`: shells mangle
   inline-JSON quotes). The range covers the fixed event time: `"from": "2026-10-14T00:00:00Z"`,
   `"to": "2026-10-17T00:00:00Z"`; the `$__timeFilter` macro is resolved from the range
   (the same way as in the dashboard panels). Body skeleton:
   `{"queries":[{"refId":"A","datasource":{"type":"grafana-clickhouse-datasource","uid":"cbl-clickhouse"},"rawSql":"<query>"}],"from":"<from>","to":"<to>"}`.
   Every expected marker is a `concat` of the checked value (exact strings in the JSON):
   - payments (`ds-payments.json`):
     `SELECT concat('payments_completed=', toString(countIf(event_type = 'payment.completed'))) AS chk FROM reporting.fact_payment_events FINAL WHERE $__timeFilter(occurred_at) AND asset = 'USDT' AND match(merchant_id, 'm-1')`
     -> the response contains `payments_completed=2`;
   - payouts (`ds-payouts.json`):
     `SELECT concat('payouts_requested=', toString(countIf(event_type = 'payout.requested'))) AS chk FROM reporting.fact_payouts FINAL WHERE $__timeFilter(occurred_at) AND asset = 'USDT' AND match(merchant_id, 'm-1')`
     -> `payouts_requested=2`;
   - turnover (`ds-turnover.json`):
     `SELECT concat('turnover_credit=', toString(intDiv(sumIf(amount, direction = 'credit'), toUInt64(pow(10, 6)))), '.', leftPad(toString(modulo(sumIf(amount, direction = 'credit'), toUInt64(pow(10, 6)))), 6, '0'), ';entries=', toString(countIf(account_type = 'merchant'))) AS chk FROM reporting.fact_ledger_entries FINAL WHERE $__timeFilter(occurred_at) AND asset = 'USDT' AND account_type = 'merchant'`
     -> `turnover_credit=200.000000;entries=2` (the integer and fraction parts are built
     with integer arithmetic: `toString` of `Decimal` strips trailing zeros and cannot
     serve as a fixed-scale check; the panels themselves use exact `divideDecimal`);
   - money panel (the `Expected vs received` patterns of `cbl-payments.json`), the sums
     are formatted exactly the same way:
     `SELECT concat('expected=', toString(intDiv(sumIf(expected_amount, event_type = 'payment.completed'), toUInt64(pow(10, 6)))), '.', leftPad(toString(modulo(sumIf(expected_amount, event_type = 'payment.completed'), toUInt64(pow(10, 6)))), 6, '0'), ';received=', toString(intDiv(sumIf(received_amount, event_type = 'payment.completed'), toUInt64(pow(10, 6)))), '.', leftPad(toString(modulo(sumIf(received_amount, event_type = 'payment.completed'), toUInt64(pow(10, 6)))), 6, '0'), ';fee=', toString(intDiv(sumIf(fee_amount, event_type = 'payment.completed'), toUInt64(pow(10, 6)))), '.', leftPad(toString(modulo(sumIf(fee_amount, event_type = 'payment.completed'), toUInt64(pow(10, 6)))), 6, '0')) AS chk FROM reporting.fact_payment_events FINAL WHERE $__timeFilter(occurred_at) AND event_type = 'payment.completed' AND asset = 'USDT' AND match(merchant_id, 'm-1')`
     -> `expected=200.000000;received=200.000000;fee=2.000000`.
6. Checks go to `dashboards-checks.txt`.

## 9. Mandatory run artifacts

- `summary.md` (template `.additionals/summary.template.md`; PASS/FAIL verdict, steps,
  key values, artifact list, time);
- `compose-ps.txt`, `compose-up.log`, `compose-down.log`, `compose-logs.txt`;
- `prepare.log`;
- `<scenario>.log` and `<scenario>-checks.txt` for every executed scenario;
- on failure: `system.kafka_consumers` state, `system.mutations`
  (`SELECT * FROM system.mutations WHERE is_done = 0`), the container list
  (`docker compose ps`), fact/vocabulary table snapshots (`SELECT * ... LIMIT 20`),
  service log fragments with `request_id`.

## 10. Auxiliary files (.additionals)

- `produce/` - Go test event producer (scenarios full/duplicate/broken/sentinel, `-live`);
- `ledgerstub/` - the test Ledger sources (the `ledger-stub` image for compose);
- `summary.template.md` - the `summary.md` template;
- `inventory.md` - the list of auxiliary files and their purpose.

## 11. Cleanup

`docker compose -p cbl-reporting-e2e-<MARK> -f deploy/docker-compose.yml --profile app down -v`
at the end; `.runs/` is not committed; a repeated run must start from a clean stand.
