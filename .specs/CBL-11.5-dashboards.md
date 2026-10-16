# CBL-11.5. Business dashboards in Grafana over the ClickHouse facts

| Field | Value |
| :-- | :-- |
| Epic | E11, `Reporting and ClickHouse` |
| Repository | `cbl-reporting` |
| Size | M |
| Wave and parallel group | W3-REPORT |
| Depends on | `CBL-11.3` (facts and the `dim_accounts` vocabulary), `CBL-11.2` (fact schema) |
| Contract version | not affected: no `cbl-protos`/OpenAPI changes (read-only ClickHouse) |
| Status | done, review closed |

## 1. Context and goal

`CBL-000`: Reporting serves reports over gRPC `Query` and the BFF `/v1/reports` proxy
(11.4); the business user view of the same data (`CBL-004` E11, task 11.5 "Business
dashboards") has no implementation yet. The ClickHouse facts (`fact_payment_events`,
`fact_payouts`, `fact_ledger_entries`, `fact_recon_alerts`) and the `dim_accounts`
vocabulary (11.3) already contain everything needed for merchant-facing payment, payout
and turnover views.

Task 11.5 adds the dashboard runtime (Grafana with the official ClickHouse datasource
plugin) and three business dashboards as code in the service repository: the same
autonomy principle as the service storage (T-RPT-03) - dashboards live and are versioned
with the service that owns the data. Dashboards are informational readers: balance
decisions from ClickHouse remain forbidden (T-09).

## 2. Scope

In scope: the Grafana service in `deploy/docker-compose.yml` (pinned image and plugin,
provisioning mounted read-only, loopback port), datasource and dashboard provisioning,
three dashboards as JSON code (`cbl-payments`, `cbl-payouts`, `cbl-turnover`), the
template variable contract (`asset`, `decimals`, `merchant`), a unit validation test of
the dashboard artifacts, the e2e scenario `E2E_Reporting_Dashboards`, docs (AGENTS,
README, runbook) and the umbrella plan row update.

Out of scope: the service Go code (untouched: no coverage impact); new report types or
contracts (a new task after 15.4); alerts and notification rules in Grafana (14.4);
production deployment, SSO/authN/TLS for Grafana (0.6/0.7, KI-reporting-14); the
observability dashboard templates (0.7); the discrepancy dashboard of the Reconciler
(9.4); retention/quotas (14.7); back-office UI (12.4).

## 3. Functional requirements

FR-1. Runtime and provisioning:

- a `grafana` service in `deploy/docker-compose.yml` (profile `app`, like the service),
  built from `deploy/grafana/Dockerfile`: `grafana/grafana:11.6.0` with the plugin
  `grafana-clickhouse-datasource` 4.22.1 baked in (the plugin requires Grafana
  `>= 11.6.0-0`); no runtime plugin downloads;
- provisioning is mounted read-only: `deploy/grafana/provisioning/datasources/clickhouse.yaml`
  (uid `cbl-clickhouse`, type `grafana-clickhouse-datasource`, native protocol, host
  `clickhouse:9000`, user `cbl`, default database `reporting`) and
  `deploy/grafana/provisioning/dashboards/dashboards.yaml` (file provider, folder `CBL`,
  `allowUiUpdates: false`, `disableDeletion: true`, path `/var/lib/grafana/dashboards`);
- dashboards JSON live in `deploy/grafana/dashboards/`;
- the port is published on loopback only: `127.0.0.1:3000` (no SSO, dev stand);
- service dependencies: `clickhouse` healthy; `GF_SECURITY_ADMIN_PASSWORD=cbl`,
  sign-up disabled, analytics and update checks disabled (offline stand);
- the stand credentials (`cbl/cbl`, Grafana `admin/cbl`) are dev-only and documented.

FR-2. Template variables of every dashboard:

- `asset` (query, single): `SELECT DISTINCT asset FROM reporting.dim_accounts WHERE asset != '' ORDER BY asset`;
- `decimals` (hidden query, single): `SELECT if(count() = 0, 6, any(decimals)) FROM reporting.dim_accounts WHERE asset = '$asset'` -
  the fallback 6 keeps panels syntactically valid on an empty vocabulary;
- `merchant` (multi, `Include All`, `allValue .*`), consumers use
  `match(merchant_id, '${merchant:pipe}')` (All -> `.*`).

FR-3. Dashboard set (titles/uid stable, folder `CBL`): `cbl-payments` ("CBL Payments"),
`cbl-payouts` ("CBL Payouts"), `cbl-turnover` ("CBL Turnover"). Every panel SQL:

- reads only `reporting.fact_*`/`dim_accounts` with `FINAL` (at-least-once duplicates);
- is a single read-only `SELECT` (no writes, no DDL, no `SYSTEM`);
- time panels use `$__timeFilter(occurred_at)` (`created_at` for alerts) and
  `toStartOfDay(<time>, 'UTC')` for daily buckets (UTC contract);
- money panels return Decimal with the asset scale:
  `divideDecimal(toDecimal64(sum_of_minor_units, $decimals), toDecimal64(pow(10, $decimals), 0), $decimals)`
  (exact decimal arithmetic, no float in SQL; the scale is resolved per selected asset);
- merchant filters apply `match(merchant_id, '${merchant:pipe}')`; rows without a
  vocabulary record are not visible to the merchant filter (same as 11.4, KI-reporting-05/10).

Panels: payments - completed per day, expected vs received per day, fees per day, data
freshness (minutes, stat); payouts - events per day by type, requested amount and fee per
day, failure reasons (table); turnover - debit/credit per day (merchant accounts), by
account type (table), entries per day, credit total (stat). Minimum three panels per
dashboard.

FR-4. Artifact validation (unit): `test/dashboards/dashboards_test.go` reads the JSON
files and asserts: valid JSON; the three uids with non-empty English titles; every panel
target has the datasource uid `cbl-clickhouse` and a `rawSql` that is `SELECT`-only,
single-statement, `FROM reporting.`, contains `FINAL` and does not contain write/DDL
keywords; time series panels contain `$__timeFilter`; the templating list contains
`asset`, `decimals`, `merchant`; at least three panels per dashboard; no Cyrillic.

FR-5. E2E scenario `E2E_Reporting_Dashboards` (E2E.md): from a clean stand - two full
flows, vocabulary convergence (poll enriched merchant entries up to 90 s), then Grafana
API checks (`/api/health`, dashboard search, datasource and dashboard reads) and
datasource query checks via `POST /api/ds/query` reproducing the panel computations:
`payments_completed=2`, `payouts_requested=2`,
`turnover_credit=200.000000;entries=2` (USDT/6 on the stand).

FR-6. Documentation: AGENTS.md (boundaries, ports, commands, trade-offs, KI, runbook,
journal), README.md (dashboard links and credentials note), E2E.md (scenario and ports),
`CBL-004` row 11.5.

## 4. Contracts

- ClickHouse: read only `fact_*`/`dim_accounts` FINAL; the schema does not change
  (no migrations in this task).
- gRPC/REST/Kafka: not affected; no `cbl-protos`/OpenAPI changes.
- Grafana HTTP API (dev, basic auth): used by the e2e checks only (built-in API, no
  custom contract).

## 5. Runtime and dashboards (decision)

Alternatives rejected: (a) in-service HTML dashboards - duplicates a mature runtime and
grows the service surface; (b) Metabase/Superset - not in the project stack (`CBL-002`);
(c) Grafana Cloud - external dependency. Grafana with the official plugin matches the
project stack, keeps the stand offline (plugin baked at build) and versions dashboards as
code. Money display: SQL returns `Decimal` (exact); Grafana converts to pixels internally
(display-only, no money decisions, T-09). Dev authN is basic admin on loopback only
(KI-reporting-14); production deployment/auth is out of scope (0.6/0.7).

Grafana runs in the `app` profile together with the service: every stand run builds (the
first time; cached afterwards) and starts it, so host port 3000 and network access at
build time must be available. `dim_accounts` is the canonical money scale source for the
`decimals` variable (the account plan, `CBL-003` section 9); fact rows agree with it - the
same assumption as the 11.4 catalog formatting. Read-only is a contract over the artifacts,
not over the credentials: the datasource uses the stand user `cbl`, and the residual risk
(ad-hoc SQL through Explore or the datasource proxy, no per-viewer scoping) is recorded in
KI-reporting-14 with the remediation in 0.6/14.7. Plugin archive integrity: the
version-pinned download happens at build time; Grafana verifies plugin signatures at
startup (fail-closed).

## 6. Edge cases

- Empty vocabulary: `decimals` falls back to 6, `asset`/`merchant` lists are empty, panels
  show no data without query errors.
- Empty facts: panels are empty; freshness shows the age since the last event (global, no
  time filter); `/api/health` stays green.
- Duplicates: collapsed by `FINAL`; dashboard totals never double count.
- Different assets have different decimals: the `decimals` variable resolves per selected
  asset; money values never mix scales (group by is per asset).
- Ledger rows without a vocabulary record: invisible to the merchant filter until
  backfill converges (KI-reporting-05/10).
- Stand events use a fixed `occurred_at` (2026-10-15); e2e queries set an explicit time
  range; the freshness panel is not asserted on historical runs.
- Plugin/image builds needs network once (build time); a failed download fails the build
  visibly (no silent runtime fallback).
- Host port 3000 is loopback-only; parallel stands need the port override file
  (E2E.md section 2).
- Empty facts: the freshness panel returns NULL instead of the epoch
  (`if(count() = 0, NULL, dateDiff('minute', max(occurred_at), now()))`), so an empty stand
  shows no data rather than a huge age.
- URL-synced variables: `asset` and `merchant` are guarded by a value regex
  (`^[A-Za-z0-9._-]{1,64}$`), so a forged `var-*` URL value is dropped before SQL
  substitution.
- Platform aggregate: the `Turnover by account type (platform aggregate)` panel
  intentionally ignores the merchant selector (11.4 deferred platform aggregates to
  11.5); the title and the panel description state it explicitly.

## 7. Non-functional requirements

Read-only and informational by contract (T-09: no balance/decision use); exact decimals
in every money result (no float in SQL); provisioning without UI mutation (dashboards are
code); no new Go dependencies and no service code; dashboard rendering and API checks fit
the e2e time budget (already-running stand).

## 8. Test plan

| Level | Scenarios |
| :-- | :-- |
| Smoke | `go build ./...`; compose build of the Grafana image |
| Unit | `test/dashboards`: JSON validity, uids/titles, datasource uid, SELECT-only + FINAL + `reporting.` + single statement, no write/DDL keywords, `$__timeFilter` on time series, the variable contract, panels per dashboard, no Cyrillic |
| Integration | not applicable (no service code; ClickHouse paths are covered by 11.2-11.4) |
| E2E local | `E2E_Reporting_Dashboards` (FR-5); minimum for the run: this scenario |
| E2E global | GE-7 not affected (pipeline, catalog and contracts unchanged) |
| Load | not in this scope (14.2) |

Target unit+integration coverage: unchanged (86.5%); the service code is not touched.

## 9. Work plan and commit

Spec -> test first (`test/dashboards`, red) -> `deploy/grafana` (Dockerfile,
provisioning, three dashboards) and the compose service -> E2E.md scenario -> battery
(build/vet/test/cover/lint/emdash) -> e2e validation run -> 4 reviews -> AGENTS/README
and the umbrella plan row -> commit
`CBL-11.5: Business dashboards in Grafana over ClickHouse facts`.

## 10. Reviews

Four independent passes (16.10.2026) and closure verification; the validation run after the
fixes: `test/e2e/.runs/20261016t123828z/` PASS 10/10 (the earlier run `20261016t121421z`
failed on two harness defects and is superseded).

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No-context | MAJOR: FR-6 docs not yet landed (expected at review time); MINOR: empty-facts freshness showed the epoch; unit test gaps (no variable consumption, time filter only on timeseries, weak money-scaling check, no asset/decimals single-ness); the panel `divideDecimal` path unguarded by e2e; platform panel not marked; stale E2E.md references; artifacts with NUL noise | freshness NULL guard; the test now rejects a bare `$merchant`, requires `${merchant:pipe}`, `$__timeFilter` everywhere except freshness, the exact `divideDecimal` pattern for every `sumIf`, single-ness and regex guards; a money ds-check added (expected/received/fee exact); the panel retitled with a description; references fixed; artifacts re-captured clean |
| Architecture | MAJOR: no compose healthcheck for grafana; KI-reporting-14 cited but absent (step 7) and the AGENTS checklist; MINOR: read-only not enforced on credentials; scale source divergence; CBL-000/CBL-002 untouched; app profile cost; `editable: true` nit | healthcheck added (`wget /api/health`, consistent with the stand); KI-reporting-14 added with the datasource-user risk; spec records the scale source, the profile cost and the credentials contract; CBL-000/CBL-002 rationale in section 11; `editable: false` |
| With-context | MAJOR: duplication not recorded; `compose-logs.txt` missed the app services (logs captured without `--profile app`); FR-6 pending; MINOR: section refs; dashboards.log encoding; summary rows; the failed run not documented | duplication recorded (section 11 + T-RPT-11); E2E.md captures logs after the scenario with `--profile app`; run artifacts re-captured (single encoding, all services, filled summary, the prior FAIL run documented as superseded) |
| Security (blocking) | no BLOCKER/MAJOR; MINOR: KI-14 absent; plugin-update egress on by default (`GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES`); URL-synced variables injected into SQL literals; NIT: plugin archive integrity unrecorded; panels without merchant scoping | KI-14 added (SSO/authN/TLS and the read-only ClickHouse user - 0.6/14.7; loopback only; prod forbidden before 0.6); the egress flags disabled; regex guards added and test-enforced; integrity recorded (build-pinned download, startup signature verification); scoping folded into KI-14 |

Accepted intentionally: dev basic credentials on loopback (KI-reporting-14); dashboard JSON
duplication (T-RPT-11); the platform-wide turnover panel (title + description); dashboards
as informational readers (T-09).

## 11. Open decisions

- Grafana 11.6.0 and plugin 4.22.1 are pinned; an upgrade is a deliberate task (version
  compatibility with the plugin dependency `>= 11.6.0-0`).
- The dashboard set is minimal and additive: a new dashboard is a new JSON file without
  service changes; report catalog changes (15.4) do not affect dashboards directly.
- `decimals` is a hidden variable: users select the asset, not the scale.
- The e2e asserts the datasource computations (same SQL patterns as panels) and the
  provisioning API, not pixels: rendering regressions are caught by screenshots on the
  stand if needed (not in this scope).
- The three dashboard JSON files are standalone documents: the templating and panel blocks
  repeat by design (file provisioning has no include mechanism); extracting library panels
  is a deliberate later task if the set grows (T-RPT-11).
- `CBL-000`/`CBL-002` are intentionally not updated: the dashboards are a dev-stand
  informational surface (production deployment, authN and the read-only ClickHouse user -
  0.6/0.7/14.7); the stand port is documented in `test/e2e/E2E.md` and the service
  AGENTS.md.
