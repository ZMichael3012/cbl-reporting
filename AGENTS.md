# AGENTS.md - cbl-reporting

Reporting from ClickHouse: the Kafka event pipeline (Kafka engine + materialized view),
the `reporting.fact_*` storage, the `dim_accounts` account vocabulary and reconciliation
against the Ledger.

Mandatory global rules: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, the `../../.specs/CBL-003-contracts.md` contracts.
The em dash (U+2014) is forbidden; only the hyphen is used.

## 1. Boundaries

In scope: ClickHouse as the service storage (like PostgreSQL for DB-backed services):
migrations of `fact_*` and `dim_accounts` (goose, clickhouse dialect), a local instance in
`deploy/docker-compose.yml`, testcontainers integration (Kafka + ClickHouse) and e2e on the
service compose; the Kafka engine + materialized view pipeline over the topics `payment.events`,
`ledger.events`, `payout.events`, `recon.alerts`; the `dim_accounts` vocabulary (Ledger gRPC
`ListMerchantAccounts` + derivation of system accounts), enrichment of ledger facts and
history backfill; periodic reconciliation against the Ledger `ListEntries` (metrics and
logs); gRPC `billing.reporting.v1.ReportingService.Query` (the 11.4 report catalog);
business dashboards (11.5): the Grafana runtime (pinned image and plugin, dashboards as
JSON code, read-only provisioning), the `test/dashboards` artifact validation and the
`E2E_Reporting_Dashboards` scenario; administrative `/healthz`/`/readyz`, gRPC health and
`/metrics` counters.

Out of scope: the `/v1/reports` proxy (`cbl-billing-api` domain, 11.4); new report types
(an additive contract task after 15.4); Grafana production deployment, SSO/authN/TLS
(0.6/0.7, KI-reporting-14) and Grafana alerting rules (14.4); `recon.alerts` and gateway
business reconciliation (9.x, Reconciler); retention/TTL
and quotas (14.7); a prod ClickHouse cluster (0.6); mTLS and gRPC authorization to the
Ledger (0.6, KI-ledger-02); moving the `account_id` derivation into `cbl-kit` (KI-ledger-08,
until 7.4/10.2); Kafka Connect (T-10: only under strict exactly-once).

## 2. Contracts

- Kafka (consumption via the ClickHouse Kafka engine, not a dedicated client):
  `payment.events`, `ledger.events`, `payout.events`, `recon.alerts`; envelope headers
  `event_id`, `event_type`, `occurred_at` (cbl-kit/outbox); groups
  `cbl-reporting-payment|ledger|payout|recon`; at-least-once.
- gRPC client: `billing.ledger.v1.LedgerService` (`ListMerchantAccounts`,
  `ListEntries`; `cbl-protos@cf-3.2`); plaintext inside the trusted network (mTLS - 0.6).
- ClickHouse (database `reporting`): `fact_*`, `dim_accounts` (migrations; the schema comes
  from CBL-11.1, the vocabulary from 11.3), `kafka_*` (Kafka engine,
  `kafka_handle_error_mode=stream`, `kafka_skip_broken_messages=1000`), `<fact>_mv`
  (materialized view; the ledger MV joins the vocabulary FINAL).
- REST: `/healthz`, `/readyz` (readyz checks ClickHouse availability and pipeline
  completeness: four Kafka-table+MV pairs; the status is also reflected in gRPC health).
- gRPC server: `grpc.health.v1` and `billing.reporting.v1.ReportingService.Query` (:9110;
  the 11.4 catalog: `payments`, `payouts`, `ledger_entries`, `turnover` with the `group_by`
  whitelist; contract cf-3).
- Metrics: `/metrics` (:9310) - text counters of the vocabulary, backfill and reconciliation
  (the full set and the Prometheus client - 0.7).

## 3. Architecture

```
cmd/reporting/          # main: migrations, reconciliation, vocabulary and check workers, gRPC
internal/config/        # env parsing and validation (fail-fast)
internal/domain/        # account plan account_id derivation (golden vectors CBL-003 section 9)
internal/ledger/        # gRPC client of LedgerService (merchant accounts, statements)
internal/storage/       # Open/Ping/Migrate, Pipeline (reconciliation), dim and check (SQL)
internal/vocabulary/    # dim_accounts vocabulary worker and history backfill
internal/ledgercheck/   # Ledger check worker (window, round-robin, classification)
internal/reports/       # Query report catalog (validation, cursor, value formatting)
internal/transport/     # gRPC transport of ReportingService (error codes)
internal/metrics/       # counters and the /metrics text Prometheus format
migrations/             # goose migrations of fact_* and dim_accounts (owned by the service)
deploy/format_schemas/  # decoding wire schema (cbl-protos@cf-3.4 flattening)
deploy/clickhouse-config/ # format_schema_path for the service ClickHouse
test/producer/          # test protobuf events (integration/e2e)
test/ledgerstub/        # test LedgerService (gRPC) for integration/e2e
test/dashboards/        # artifact validation of the dashboard JSON (unit, CBL-11.5)
deploy/grafana/         # Grafana runtime: Dockerfile (pinned plugin), provisioning, dashboards (CBL-11.5)
test/e2e/               # E2E.md (run instructions), .additionals (utilities), .runs (artifacts)
```

Key decisions: the service keeps no dedicated Kafka client (T-RPT-05); Kafka tables
and MVs are reconciled at startup from env (T-RPT-01), facts and the vocabulary are goose
migrations; the gRPC server runs with the `grpcmw.UnaryServer` interceptor (panic recovery,
request-id); the wire schema is a wire-compatible flattening of the contracts without
imports (T-RPT-02; ClickHouse does not resolve imports, and the Kafka engine forbids an
absolute path - `format_schema_path` is used); `_headers` in 24.8 is
`Array(Tuple(name, value))` (accessed via `arrayFirst`); an alias matching a column name
shadows it for the remaining SELECT expressions (tuples are renamed in the MV subquery);
vocabulary enrichment is a JOIN with `FINAL` on the right side (T-RPT-07); backfill uses
mutations with a narrow predicate (T-RPT-08); reconciliation uses a window with a lag and
round-robin over accounts (T-RPT-09); business dashboards are Grafana JSON code with
read-only provisioning (T-RPT-11; never balance decisions, T-09).

Libraries: the standard set (`CBL-002` section 6): Gin, zap, grpc, goose (clickhouse
dialect), `clickhouse-go/v2`; testcontainers (Kafka, ClickHouse). `AutoMigrate` is forbidden.

## 4. Commands

```powershell
make build             # go build ./...
make test              # go test ./...
make test-integration  # integration (testcontainers: Kafka, ClickHouse)
make cover             # merged unit+integration coverage (cover.out)
make vet               # go vet ./... (+ integration e2e tags)
make lint              # golangci-lint in Docker (services/ context for cbl-kit)
make up                # stand: kafka, schema-registry, clickhouse, ledger-stub, reporting, grafana
make down              # stand + volume
```

E2E: the run is performed by an agent following `test/e2e/E2E.md` (platform-independent:
bring up the stand, execute the ingestion/vocabulary/reports/dashboards scenarios, save
artifacts to `test/e2e/.runs/<UTC mark>/`, mandatory `summary.md`); there are no PowerShell
run scripts, auxiliary files live in `test/e2e/.additionals/` (`CBL-002` section 7).

## 5. Ports and environment

HTTP `8110`, gRPC `9110`, metrics `9310`. Grafana `127.0.0.1:3000` (business dashboards,
loopback only, dev credentials `admin/cbl`; KI-reporting-14).
Variables: `CLICKHOUSE_DSN` (required, `clickhouse://cbl:...@host:9000/reporting`),
`KAFKA_BROKERS` (required; the internal listener for ClickHouse, `kafka:29092` in compose),
`SCHEMA_PATH` (`events.proto` relative to the ClickHouse `format_schema_path`; the Kafka
engine forbids an absolute path), `MIGRATIONS_DIR` (`./migrations`; `/app/migrations` in
the image), `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `LEDGER_GRPC_ADDR`
(required, host:port), `LEDGER_TIMEOUT` (5s; 1s - 60s), `DIM_REFRESH_INTERVAL`
(5m; 1s - 24h), `DIM_MERCHANTS_LIMIT` (200; 1 - 10000), `BACKFILL_LIMIT`
(1000; 1 - 100000), `LEDGER_CHECK_INTERVAL` (5m), `LEDGER_CHECK_WINDOW` (5m; not
less than the interval - otherwise unchecked segments remain between cycles),
`LEDGER_CHECK_ACCOUNTS` (10), `LEDGER_CHECK_PAGE_LIMIT` (1000; up to 10000),
`LEDGER_CHECK_MAX_PAGES` (10; up to 100). Invalid values - startup failure.

## 6. Development rules

- Pipeline: an SDD spec in `.specs/` -> tests first -> code -> 4 reviews -> AGENTS -> runs ->
  commit `CBL-<epic>.<number>: ...` (the spec is part of the same commit).
- E2E: a mandatory local run following `test/e2e/E2E.md` (an agent, no PowerShell scripts);
  artifacts go to `test/e2e/.runs/<UTC mark>/` (mandatory `summary.md`, `compose-ps.txt`,
  `compose-logs.txt`, scenario and check outputs), auxiliary files to
  `test/e2e/.additionals/`.
- Comments and docstrings are written in English in all repository files (including specs,
  AGENTS.md, README, configurations); exceptions: identifiers, linter directives,
  protocol and contract field names; log and error messages are not translated (they are
  grepped by tests). Comments explain why, in full sentences.
- Changing fact columns: additively only, via a new goose migration; the wire schema is
  extended with the same contract field number (buf numbers are not reused).
- Unit+integration coverage is at least 80% (currently 86.5%); pipeline integration checks
  run only on real Kafka and ClickHouse (testcontainers), never on mocks.
- Money is int64 only in minor units plus decimals; float is forbidden (in CH -
  Int64 + asset + decimals UInt8, balance decisions from ClickHouse are forbidden, T-09).
- Broken Kafka messages are skipped with logging, the partition does not halt.
- Docstring in English in the godoc style; errors via `%w`; panics in business code are
  forbidden.

## 7. SLO and metrics

The SLO is refined in task 14.4. Pipeline latency is seconds; the vocabulary follows the
refresh interval, reconciliation the interval with a lagged window. `/metrics` serves the
vocabulary, backfill and reconciliation counters (partially closing KI-reporting-04); the
full set (consumer lag, latency, Prometheus client) comes after 0.7. Pipeline load - 14.2.
Business dashboards: `http://localhost:3000` (folder `CBL`), informational only (T-09),
not an SLO surface.

## 8. Trade-offs

| ID | Decision | Pros | Cons and compensation |
| :-- | :-- | :-- | :-- |
| T-RPT-01 | Kafka tables and MVs are reconciled by the service at startup; the expected pair definition is folded into a fingerprint (sha256 of the DDL of both objects) stored in `COMMENT` | any drift is caught (brokers including a shortened list, schema, columns, query) without parsing the normalized DDL; a missing MV is restored | pipeline DDL is outside migration history | facts are goose-only; recreation does not touch data; reconciliation assumes a single writer (replica scaling - 14.2/0.6) |
| T-RPT-02 | The wire schema is a flattening of the `deploy/format_schemas/events.proto` contracts; for an infra-only stand, copies live in `deploy/clickhouse` of the umbrella repository | ClickHouse requires no imports and no mounting of the contracts repository; infra startup without cloning the services | duplicated field numbers and copy files | numbers and types repeat `cbl-protos@cf-3.4` (buf forbids number reuse); integration tests send real protobuf; copy sync is checked by `check-clickhouse.ps1` (KI-reporting-06) |
| T-RPT-03 | ClickHouse is the service storage (like PostgreSQL for DB-backed services): schema, migrations, compose and tests in the repository | autonomy, a single convention; the global stand is not required for tests | one more ClickHouse instance in dev | the global compose keeps a shared stand (T-08); the cbl-system init is a dev bootstrap |
| T-RPT-04 | Broken messages are skipped (`kafka_skip_broken_messages=1000`, `handle_error_mode=stream`) | the pipeline does not stop on a poison message | erroneous events never reach the facts | an error counter in the ClickHouse logs; triage and DLQ forwarding - 11.3 if needed |
| T-RPT-05 | The service keeps no dedicated Kafka client (the consumer is the Kafka engine) | less code and fewer dependencies; scaling via `kafka_num_consumers` | no fine-grained processing in Go | filters and mapping live in the MV; the 11.4 gRPC layer does not depend on Kafka |
| T-RPT-06 | Money/Timestamp tuples are renamed in the MV subquery | works around ClickHouse alias shadowing (an alias suppresses the same-named column) | SQL readability | recorded by the `TestBuildViewDDL` test; verified on 24.8 |
| T-RPT-07 | Vocabulary enrichment is a JOIN with `(SELECT ... FROM dim_accounts FINAL)` in the MV, without the ClickHouse dictionary | no credentials in the DDL, fewer entities, vocabulary changes are visible to new inserts | a JOIN on insert and the need for FINAL on the right side | the vocabulary is small (accounts per merchant/asset), the per-block hash join is cheap; FINAL excludes row multiplication; a missing account yields empty values, backfill catches up |
| T-RPT-08 | History backfill uses `ALTER UPDATE ... WHERE account_type = ''` mutations over vocabulary accounts (a per-cycle limit, only vocabulary accounts selected in a deterministic order) | pinpoint rewrite, idempotence, convergence to the full vocabulary, the cycle budget does not starve on legacy rows | mutations are asynchronous and rewrite parts; a large lagging volume needs several cycles | `WHERE account_type = ''` is safe and converges; the `BACKFILL_LIMIT` cap; the mutation queue and scan cost - KI-reporting-09/11, profile 14.2 |
| T-RPT-09 | Ledger reconciliation: round-robin over vocabulary accounts, window `[now-2W, now-W)`, pages are aggregated page by page, the result is logs and counters | there is no global transaction enumeration in the Ledger contract; a complete event loss by an account is detected; the lagged window suppresses false missing; memory holds only window transactions | a full vocabulary walk is stretched (accountsPerCycle per cycle); false missing when event delay exceeds W; a single check of the window; the in-memory cursor - one writer per stand (KI-reporting-12) | the window and limits are configurable; on a complete batch failure the cursor does not move; discrepancies are diagnostics (T-09), not alerts or money decisions; calibration - 14.2 |
| T-RPT-10 | Query reports: a closed catalog in code, a keyset cursor (sort keys), `limit+1` and a served limit ≤100, `group_by` only from the type whitelist, money as canonical decimal strings | user input never reaches SQL; pagination is stable under inserts; no float; catalog drift is visible to tests | a new report is a task with a catalog version; the keyset cursor is tied to the report sorting; aggregates are per-asset only (sums of different scales never mix) | the cursor is opaque and validated; contract limits; the catalog lives in the 11.4 spec |
| T-RPT-11 | Business dashboards: Grafana with the pinned ClickHouse plugin baked into the image, dashboards as JSON code with read-only provisioning (`editable: false`, `allowUiUpdates: false`), money as exact `Decimal` (`divideDecimal`) in SQL | a mature runtime without custom code; dashboards are versioned with the service that owns the data (T-RPT-03); no runtime plugin downloads; no float in SQL | a second UI runtime on the stand; Grafana/plugin version coupling; the JSON blocks duplicate by design (library panels are a deliberate later task); dev basic auth only | loopback-only port; artifact unit test `test/dashboards`; URL-sync variables regex-guarded; plugin signatures verified by Grafana at startup (fail-closed); production authN/TLS and the read-only ClickHouse user - 0.6/14.7 (KI-reporting-14) |

## 9. Known issues

| ID | Problem | Impact | Plan | Status |
| :-- | :-- | :-- | :-- | :-- |
| KI-reporting-01 | CI and pre-commit are not configured | linters run manually | task 0.2 | open |
| KI-reporting-02 | The stand ClickHouse runs with `CLICKHOUSE_DO_NOT_CHOWN=1` and a root process (the wire schema mounted ro) | a dev convention; prod privileges and the migration user are not separated | privilege separation, a read-only profile, TLS - 0.6/14.7 | open |
| KI-reporting-03 | There is no load profile (no HTTP load before 11.4) | pipeline throughput is not measured | 14.2 (Kafka -> ClickHouse), Query profile - 11.4 | open |
| KI-reporting-04 | Analytical query quotas and limits are not set; `/metrics` has service counters only (no Prometheus client) | a heavy `Query` period (a turnover aggregate over the whole history) may affect ingestion; no consumer lag or latency | `Query` is bounded by the 100-row limit and keyset pagination; readonly/max_memory_usage/max_execution_time - 14.7; the full metrics set - 0.7 | open |
| KI-reporting-05 | `dim_accounts` is not populated instantly: rows between refresh and backfill cycles temporarily contain empty `account_type`/`merchant_id` (until the first cycle on a new stand) | the 11.4 merchant filter does not cover intermediate rows until the cycles converge | CLOSED 16.10.2026: the vocabulary, input enrichment and backfill are implemented (11.3); convergence is a target property, row freshness follows the env intervals | closed |
| KI-reporting-06 | The wire schema and the format_schema config are duplicated in the umbrella `deploy/clickhouse` (for infrastructure startup without cloning the services) | diverging copies will break decoding on the shared stand | sync is checked by `scripts/check-clickhouse.ps1` (SHA256); when the schema moves to a string source - remove the copies | open |
| KI-reporting-07 | The Kafka integration test uses a fixed free host port (a TOCTOU window under parallel CI) | a rare flake when racing for the port | dynamic forwarding after startup or port coordination - when CI stabilizes (0.2) | open |
| KI-reporting-08 | The gRPC channel to the Ledger is plaintext, the client does not request TLS/authN | spoofing the Ledger address on the stand network yields a false vocabulary and reconciliation; false discrepancies | mTLS/authorization of inter-service calls - 0.6 (KI-ledger-02); do not publish the Ledger port outside the trusted zone; prod is forbidden before 0.6 | open |
| KI-reporting-09 | Backfill runs mutations every cycle over accounts with an empty `account_type`; with a large lagging volume the mutation queue grows | part rewrites compete with ingestion for resources | the `BACKFILL_LIMIT` cap; if systematic - batches/schedule (14.2/14.7); `WHERE account_type = ''` does not touch enriched rows | open |
| KI-reporting-10 | Merchants with a dirty `merchant_id` in the facts (outside the account plan) are logged WARN every cycle; merchants with impossible coverage (inconsistent stand data) are re-requested from the Ledger every cycle (the `DIM_MERCHANTS_LIMIT` budget with rotation - the tail does not starve, but the calls repeat) | log noise under permanently dirty data; extra Ledger calls | warning deduplication and a "quarantine" of uncoverable merchants after k attempts (if confirmed); the source service fixes the data | open |
| KI-reporting-11 | The cost of periodic FINAL scans of the facts (coverage refresh, the unenriched select, reconciliation aggregates), of the `Query` report selects (the `merchant_id` filter and the report order do not match the fact sorting key, so each page is a scan with sorting) and of backfill mutations is not measured at large volumes | on the target profile (14.2) cycles and reports may compete with ingestion | measurement in 14.2; if confirmed - projections/skip indexes for the actual predicates (`merchant_id`, report order), incremental state of unenriched accounts, batch mutations | open |
| KI-reporting-12 | The vocabulary and reconciliation workers do not coordinate between replicas: duplicate Ledger calls and mutations, the reconciliation cursor is in memory | with N instances there is extra load and a desynchronized walk; correctness does not suffer (idempotent) | single-writer (advisory lock) or an explicit convention fixed before scaling (0.6/14.2) | open |
| KI-reporting-13 | gRPC `ReportingService.Query` has no authN/mTLS, and an empty `merchant_id` for lists means all merchants (an internal call per FR-2) | with access to port 9110 any caller reads events of all merchants bypassing the BFF; in dev the port is published to loopback only (127.0.0.1) | mTLS/authorization and a mandatory call subject - 0.6 (following KI-ledger-02); do not publish the port outside the trusted zone; prod is forbidden before 0.6; the server timeout interceptor - 0.6/14.7 | open |
| KI-reporting-14 | Grafana has no SSO/authN/TLS and runs with dev basic credentials (`admin/cbl`, loopback only); its datasource uses the write-capable `cbl` ClickHouse user, so an authenticated session can execute ad-hoc SQL (including mutations) through Explore or `POST /api/ds/query`; any `cbl-net` container can reach the Grafana API; viewers are not scoped per merchant (aggregates across merchants are visible) | with access to loopback (dev) or the stand network a caller bypasses the reporting access paths; a multi-user prod would expose cross-merchant aggregates | SSO/authN/TLS and a dedicated read-only ClickHouse user/profile - 0.6/14.7; do not publish port 3000 outside the trusted zone; prod is forbidden before 0.6; committed panel SQL is read-only and unit-enforced, URL-sync variables are regex-guarded; dashboards are informational (T-09) | open |

## 10. Task journal

| CBL | Date | Status | Spec | Commit |
| :-- | :-- | :-- | :-- | :-- |
| CBL-11.2 | 2026-10-15 | done, review closed (the ClickHouse pipeline: fact_* migrations, Kafka engine + MV over four topics, wire schema, fingerprint reconciliation with self-healing, testcontainers and e2e; coverage 88.2%) | `.specs/CBL-11.2-ingestion.md` | 8a0bd36 |
| CBL-11.3 | 2026-10-16 | done (the dim_accounts vocabulary: merchant accounts from Ledger gRPC by fact coverage, system accounts by golden-vector derivation; input enrichment via JOIN in the MV; history backfill; Ledger reconciliation with a window and round-robin; the Ledger stub and e2e (the vocabulary scenario; with CBL-16.2 - E2E.md); coverage 82.7%) | `.specs/CBL-11.3-ledger-sync.md` | d977610 |
| CBL-11.4 | 2026-10-16 | done (gRPC ReportingService.Query: the closed payments/payouts/ledger_entries/turnover catalog, a keyset cursor with key validation, limits, canonical money; integration tests on ClickHouse and e2e via grpcurl (with CBL-16.2 - E2E.md); coverage 86.5%) | `.specs/CBL-11.4-reports-query.md` | 49a9480 |
| CBL-16.2 | 2026-10-16 | done (English comments and documentation across the repository: code, tests, configs, specs (11.2-11.4 translated), AGENTS.md, README; e2e runs via `test/e2e/E2E.md` with utilities in `.additionals/` and artifacts in `.runs/`; validation run PASS; coverage unchanged 86.5%) | `.specs/CBL-16.2-conventions.md` | this commit |
| CBL-11.5 | 2026-10-16 | done (Grafana business dashboards: pinned runtime and ClickHouse plugin baked at build, dashboards as code (payments/payouts/turnover) with exact Decimal money and regex-guarded variables, artifact unit test `test/dashboards`, e2e scenario `E2E_Reporting_Dashboards` PASS 10/10; coverage unchanged 86.5%) | `.specs/CBL-11.5-dashboards.md` | this commit |

## 11. Runbook

Pipeline (11.2):

1. Rows do not appear in `fact_*`: check `SELECT * FROM system.kafka_consumers` -
   connection/parsing errors, the skipped count; check `KAFKA_BROKERS` (the internal
   address for ClickHouse, not the host port) and that the topics exist.
2. Decoding schema: the `CANNOT_PARSE_PROTOBUF_SCHEMA` error - check
   `SCHEMA_PATH` (a relative path) and the mounting of `deploy/format_schemas` into
   `format_schema_path`, the `10-format-schema-path.xml` configuration file.
3. Kafka broker change: restart the service - reconciliation recreates the Kafka tables and
   MVs (`fact_*` data is preserved); the log will show `kafka pipeline configuration changed`.
4. Broken messages: the `kafka_skip_broken_messages` counter; find the message by offset in
   the ClickHouse logs; if widespread - stop the producer and review the contract.
5. Migrations: applied at startup (`goose_db_version`); forward only; change
   fact columns with a new migration, never edit existing ones.
6. Lag: the `cbl-reporting-*` groups; lag in `system.kafka_consumers`; if necessary,
   increase `kafka_num_consumers` or scale the service (14.7).
7. Full stand reload: `make down` (destroys the service `ch-data` volume), then
   `make up` - migrations and the pipeline come up from scratch.
8. Pipeline drift: the service recreates the pair when the fingerprint in `COMMENT` does
   not match (the `kafka pipeline configuration changed` log); a deleted MV is restored
   without recreating the table (`materialized view is missing, recreating`).
9. Infrastructure stand (without cloning the services): copies of the wire schema and
   config in `deploy/clickhouse` of the umbrella repository; when editing the schema,
   update the copies and run `scripts/check-clickhouse.ps1` (checks SHA256, KI-reporting-06).

Vocabulary and reconciliation (11.3):

1. The vocabulary is not populated: the `vocabulary cycle failed` logs (the Ledger is
   unavailable, check `LEDGER_GRPC_ADDR`), or an empty `ListMerchantAccounts` response
   when there are no accounts; the `reporting_vocabulary_*` counters on `/metrics`.
2. Ledger fact rows with an empty `account_type` for more than a few cycles: the account is
   outside the vocabulary plan (legacy) or mutations lag; check
   `SELECT count() FROM system.mutations WHERE is_done = 0` (KI-reporting-09/11).
3. `missing_in_reporting` reconciliation discrepancies: verify the window
   (`LEDGER_CHECK_WINDOW`), the pipeline lag and backlog; isolated early missing at the
   window edge is event delay, systematic missing calls for a review of
   `system.kafka_consumers` and the DLQ policy (T-RPT-04).
4. Per-account reconciliation errors: WARN `ledger check account failed` in the logs (a
   Ledger timeout, page overrun - `truncated`); the `reporting_ledger_check_*` counters on
   `/metrics`. On a complete batch failure (the Ledger is unavailable) the cursor does not
   move - the cycle repeats the same batch after recovery; this is visible as growing
   `errors` with an unchanged walk.
5. Recreating the vocabulary from scratch: `TRUNCATE TABLE reporting.dim_accounts` then a
   refresh cycle; backfill re-enriches the history (idempotently).
6. A new asset or merchant payout account appears with a delay of up to one refresh cycle:
   the required coverage from the facts (payments - merchant, payouts - payout) triggers
   merchant resynchronization; until then rows remain without values and are filled in by
   backfill.

Query reports (11.4):

1. `NotFound` (404 at the proxy): the report type is outside the catalog - `payments`,
   `payouts`, `ledger_entries`, `turnover`; the name is checked against the
   `[a-z0-9_]{1,64}` shape.
2. `InvalidArgument` (400 at the proxy): limit 1 - 100, period (`period_to >= period_from`),
   `group_by` from the type whitelist (`turnover`: `asset`, `day_asset`), the cursor is at
   most 512 characters and is validated by shape (version, report, key formats); a
   structurally valid forged cursor grants no access to other merchants' data (the
   merchant filter is applied, values are bound) and only shifts the position within its
   own report.
3. `turnover` without `merchant_id` is an error (a merchant report); in `ledger_entries`
   rows without a vocabulary record are invisible to the merchant filter (vocabulary
   convergence - above).
4. A heavy period (an aggregate over the whole history): narrow the window; quotas and the
   readonly profile - 14.7; internal errors - ERROR `report query failed` in the logs
   (details never reach the client), channel degradation - KI-reporting-13.

Dashboards (11.5):

1. Dashboards missing in Grafana: check the container health (`docker compose --profile app ps`,
   the healthcheck calls `/api/health`), the provisioning mounts
   (`/etc/grafana/provisioning`, `/var/lib/grafana/dashboards`, read-only) and provider
   errors in the grafana logs; provisioned dashboards appear within
   `updateIntervalSeconds` (30 s).
2. Datasource test fails or panels show an error: `GET /api/datasources/uid/cbl-clickhouse`
   and the grafana log lines about `clickhouse`: check the host (`clickhouse:9000`), the
   native protocol and credentials (`cbl/cbl`); the plugin is baked at build - a download
   failure fails the image build visibly and there is no runtime fallback.
3. Empty variables or empty panels: the vocabulary has not converged yet
   (`DIM_REFRESH_INTERVAL`): `decimals` falls back to 6, the `asset`/`merchant` lists stay
   empty until `dim_accounts` is populated, merchant-scoped rows wait for the backfill
   (KI-reporting-05/10).
4. Money looks scaled wrong (off by orders of magnitude): panels must scale with
   `divideDecimal(toDecimal64(x, $decimals), toDecimal64(pow(10, $decimals), 0), $decimals)`;
   `go test ./test/dashboards` rejects regressions, including a bare `$merchant` or a
   missing `$__timeFilter`.
5. Host port 3000 busy (parallel stands): stop the other stand or override the publish;
   the port is loopback-only by design (KI-reporting-14).
6. Changing dashboards: edit the JSON files (dashboards are code; UI edits are not
   persisted), run `go test ./test/dashboards`, then re-run `E2E_Reporting_Dashboards`.
7. Users see cross-merchant aggregates: that is the current trust model of the stand
   (no per-viewer scoping, KI-reporting-14); the informational `Turnover by account type
   (platform aggregate)` panel is platform-wide by design (11.4 deferred platform
   aggregates to 11.5).
