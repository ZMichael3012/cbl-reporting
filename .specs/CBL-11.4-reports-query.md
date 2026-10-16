# CBL-11.4. Reporting Query: gRPC report service and /v1/reports proxy

| Field | Value |
| :-- | :-- |
| Epic | E11, `Reporting and ClickHouse` |
| Repositories | `cbl-reporting` (the `Query` service), `cbl-billing-api` (the `/v1/reports` proxy) |
| Size | M |
| Wave and parallel group | W3-REPORT |
| Depends on | `CBL-11.2` (pipeline), `CBL-11.3` (vocabulary: the `merchant_id` filter of ledger rows), `cbl-protos@cf-3` contracts |
| Contract version | `cbl-protos@cf-3.4` (`billing/reporting/v1` unchanged since CF-3; OpenAPI `/reports/{report_type}`) |
| Status | done, review closed |

## 1. Context and goal

`CBL-000`: Reporting serves reports over gRPC `Query`, Billing API proxies `/v1/reports`
and injects `merchant_id` from the token, not from the request. The `billing.reporting.v1`
contract was frozen in CF-3 (`QueryRequest`/`QueryResponse`/`ReportRow`, default limit 20,
maximum 100, the cursor is opaque, money is decimal strings without float). OpenAPI
`billing.v1.yaml` describes `GET /reports/{report_type}`: a closed catalog of 11.4 types,
an unknown type - 404 `unknown_report_type`, `group_by` - only catalog whitelist values
(parameterization of ClickHouse queries, not user SQL).

Task 11.4 opens the reporting perimeter: it implements `ReportingService.Query` in
`cbl-reporting` (report catalog, fact queries with `FINAL`, pagination, limits) and
the proxy endpoint in `cbl-billing-api` (7.2 authentication, merchant from the token,
error mapping to problem+json).

## 2. Boundaries

In scope: the catalog of four reports and their columns; queries against `fact_*`/`dim_accounts`
(read only, T-09); the opaque cursor; limits and validation; the gRPC `ReportingService`
server; the BFF proxy `GET /v1/reports/{report_type}` with the merchant from the token;
integration tests (ClickHouse; stub Reporting) and e2e in both repositories; documentation.

Out of scope: dashboards (11.5); report documentation and SDK (15.4); new contracts
(`cbl-protos` is not changed); ClickHouse quotas/readonly profile (14.7); retention (14.7);
projections by `merchant_id` (KI-reporting-11); the HTTP load profile of reports (14.1);
back-office administrative reports (12.x).

## 3. Functional requirements

FR-1. Closed report catalog (type -> columns -> sorting -> cursor):

- `payments` - rows of `fact_payment_events` FINAL; columns `payment_id`, `merchant_id`,
  `event_type`, `expected_amount`, `received_amount`, `fee_amount`, `asset`, `quote_id`,
  `rate`, `txid`, `state_version`, `occurred_at`; order `(occurred_at, payment_id,
  event_id)`; cursor - the same three keys;
- `payouts` - `fact_payouts` FINAL; columns `payout_id`, `merchant_id`, `event_type`,
  `amount`, `fee_amount`, `asset`, `txid`, `provider`, `failure_reason`,
  `refund_invoice_id`, `state_version`, `occurred_at`; order `(occurred_at, payout_id,
  event_id)`;
- `ledger_entries` - `fact_ledger_entries` FINAL (enriched by the 11.3 vocabulary); columns
  `transaction_id`, `entry_index`, `account_id`, `account_type`, `merchant_id`, `asset`,
  `direction`, `amount`, `ref_type`, `ref_id`, `event_type`, `occurred_at`; order
  `(occurred_at, transaction_id, entry_index, event_id)`; merchant filter - the column
  `merchant_id` (rows without a vocabulary record do not match the merchant filter);
- `turnover` - aggregate turnover of the merchant ledger over `fact_ledger_entries` FINAL;
  `group_by` from the whitelist: `asset` (default) and `day_asset`; columns: the group
  value (`asset`; `day` + `asset`), `debit_total`, `credit_total`, `entries_count`;
  order by the group value; merchant is required.

Unknown type - `NOT_FOUND` (`unknown report type %q`); a `group_by` outside the type
whitelist - `INVALID_ARGUMENT`; a `group_by` set for a type without grouping -
`INVALID_ARGUMENT`.

FR-2. Query semantics: the period `[period_from, period_to]` is inclusive by `occurred_at`
in UTC (bounds are converted to milliseconds, the precision of `DateTime64(3)`; unset
bounds do not restrict); an empty `merchant_id` means no filter for lists (an internal
call), while for `turnover` an empty merchant is `INVALID_ARGUMENT`; limit 0 - the server
default of 20, greater than 100 - `INVALID_ARGUMENT`; pagination - `LIMIT limit+1` and
`next_cursor` from the last returned row, an empty cursor - end of the report; a cursor up
to 512 characters long, broken by base64/JSON/structure/key format - `INVALID_ARGUMENT`
(key validation includes the time/day, the entry index and string identifiers: a
structurally valid but tampered cursor does not reach ClickHouse). All queries are `FINAL`
only and read only (T-09).

FR-3. `ReportRow.values` formatting (all values are strings): money from
`minor_units Int64` and `decimals UInt8` into the canonical decimal string
`^-?(0|[1-9][0-9]*)(\.[0-9]+)?$` (no exponent; `100000000`/6 -> `"100.000000"`,
`decimals 0` -> integer); `occurred_at`/`day` - RFC 3339 in UTC (milliseconds); numbers
(`entry_index`, `state_version`, counters) - decimal strings; other fields - as is.

FR-4. gRPC errors: `NOT_FOUND` - unknown type; `INVALID_ARGUMENT` - limit,
`group_by`, period (`period_to` earlier than `period_from`), cursor; `INTERNAL` - a ClickHouse
error; the client timeout is decided by the caller (RPC deadline 5 s, `CBL-003` section 5).

FR-5. BFF proxy `GET /v1/reports/{report_type}` in `cbl-billing-api` (a route of the
`/v1` group, `Require` 7.2 + the idempotency middleware does not affect GET):

- `merchant_id` always from the token (`httpmw.MerchantID`); the query parameter of the
  same name is not accepted (ignored);
- parameters: `period_from`, `period_to` (RFC 3339, an invalid timestamp - 400), `group_by`
  (up to 64 characters, the whitelist is checked by Reporting), `limit` (1 - 100, default 20),
  `cursor` (up to 512 characters); invalid values - 400 `validation_failed`;
- the Reporting call with the `REPORTING_TIMEOUT` deadline (5 s by default, 1 - 10 s:
  the contractual RPC deadline of 5 s remains the default, the margin up to 10 s is bounded
  by the server WriteTimeout);
- the `ReportPage` response (`rows` - objects of string values, `next_cursor`) from
  `QueryResponse` without transformations;
- error mapping: `NOT_FOUND` -> 404 `unknown_report_type`; `INVALID_ARGUMENT` -> 400
  `validation_failed` (details in detail); `UNAVAILABLE`/`DEADLINE_EXCEEDED`/others - 503
  `service_unavailable` with `Retry-After: 1`;
- `REPORTING_GRPC_ADDR` is optional: empty - WARN at startup, the endpoint responds 503
  (reporting degrades, the rest of the BFF stays alive).

FR-6. Configuration: `cbl-reporting` - no new variables (the catalog is closed in code;
`ClickHouseDSN` already exists); `cbl-billing-api` - `REPORTING_GRPC_ADDR` (host:port,
optional) and `REPORTING_TIMEOUT` (1s - 10s, default 5s: not greater than the server
HTTP WriteTimeout of 15s); invalid values - startup failure (the timeout is validated
unconditionally, even without the address).

FR-7. Tests: unit (catalog and validation, cursor, money and time formatting, SQL filters
via integration); integration `cbl-reporting` (testcontainers ClickHouse: all four
reports, merchant filter, pagination, inclusive period, errors over real gRPC);
integration `cbl-billing-api` (stub Reporting gRPC on a real port: proxying,
merchant from the token, all codes); e2e in both repositories.

## 4. Contracts

- gRPC: `billing.reporting.v1.ReportingService.Query` (`cbl-protos@cf-3`, without
  changes): field semantics are in the contract comments; the service is the server
  (`cbl-reporting` :9110), Billing API is the client.
- REST: `GET /v1/reports/{report_type}` (`openapi/billing.v1.yaml`, cf-3.1); errors -
  `application/problem+json` (RFC 9457); codes `unknown_report_type` (404),
  `validation_failed` (400), `service_unavailable` (503).
- ClickHouse: read only `fact_*`/`dim_accounts` FINAL; the schema does not change.
- Kafka: not affected.

## 5. Catalog and queries (decision)

Query skeletons (parameters are bound by the driver; filters are added when values are
set):

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

`turnover`/`day_asset` groups by `(toDate(occurred_at), asset)`; sums are in
minor units of a single asset (the scale is per row), no float arises. The cursor is
`base64url(JSON {"v":1,"report":"<type>","keys":[...]})`; `keys` has the length of the
type sort keys, values are strings (time in the `2006-01-02 15:04:05.000` format).

## 6. Edge cases

- Period not set - the whole history; `period_to` earlier than `period_from` - `INVALID_ARGUMENT`;
  bounds are inclusive on both sides, the edge is aligned to milliseconds.
- Empty result - zero rows and an empty `next_cursor`.
- Last page: the `limit+1`-th row is not returned, `next_cursor` is empty.
- A cursor of a different length/of another report/broken base64 - `INVALID_ARGUMENT`.
- `ledger_entries` for a merchant: rows without a vocabulary record (`merchant_id = ''`)
  do not match the filter (enrichment - 11.3, convergence per KI-reporting-05/10).
- `turnover` without a merchant - `INVALID_ARGUMENT` (an aggregate across all merchants
  is not a merchant report); an empty period with the merchant set - zero sums are not
  returned (no groups - no rows).
- At-least-once duplicates are collapsed by `FINAL`; money decisions based on reports
  are forbidden (T-09) - recorded in the specs and AGENTS.
- Negative sums (reversals/adjustments) are formatted with a sign.

## 7. Non-functional requirements

RPC deadline 5 s (`CBL-003` section 5); the row limit per request - 100, reads follow the
fact sorting indexes (projections - KI-reporting-11, profile - 14.1/14.2); float is
forbidden - money is int64 only plus decimals and canonical strings; SQL only with
parameter binding; `group_by` - the catalog whitelist (user input never reaches SQL);
data for the BFF - only from the service ClickHouse (T-09).

## 8. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit | catalog: types/columns/cursor keys/`group_by` whitelist; validation (limit, period, cursor, empty merchant for `turnover`); money formatting (`decimals 0`, sign, canonical form) and time formatting; cursor encoding/decoding; BFF: parameter parsing and code mapping |
| Integration `cbl-reporting` | ClickHouse: all four reports over seeded facts, merchant filter (including exclusion of rows without a vocabulary record), page-by-page pagination for each report (including `day_asset`; no duplicates or losses), inclusive period, empty page, negative sums (sign), a forged cursor - `InvalidArgument` without touching ClickHouse, errors over real gRPC (404/400) |
| Integration `cbl-billing-api` | stub Reporting gRPC: page proxying, merchant from the token (the query parameter is ignored), 404/400/503, cursor pagination |
| E2E `cbl-reporting` | compose: events -> facts -> vocabulary -> grpcurl `Query` (payments, ledger_entries, turnover, unknown type, invalid group_by) |
| E2E `cbl-billing-api` | compose: key -> `GET /v1/reports/...` (merchant from the token, pagination), 404/400/401/503 (stub down), stub echo |
| Load | not in this scope: the report read profile - 14.1 |

Target unit+integration coverage: at least 80% in each repository.

## 9. Work plan and commits

Tests first (unit of the catalog/cursor/format) -> `cbl-reporting`: `internal/storage`
(queries) + `internal/reports` (service) + gRPC transport + main -> integration and
e2e -> `cbl-billing-api`: the `internal/reporting` client + `internal/transport/reports.go`
+ main/compose/stub/e2e -> runs of both -> 4 reviews (shared for the task) -> AGENTS/README
-> commits: `CBL-11.4: Reporting Query: report catalog and gRPC service` in
`cbl-reporting`, `CBL-11.4: /v1/reports proxy with the merchant from the token` in
`cbl-billing-api`.

## 10. Review (CBL-001, step 6)

Four independent passes (16.10.2026) and closure verification. Blockers/MAJOR closed:
unauthenticated gRPC `Query` with an empty `merchant_id` and port 9110 exposed (security) -
KI-reporting-13 added, the port in dev stands is loopback-only; a structurally valid
tampered cursor reached ClickHouse and returned 503 instead of 400 - key format validation
(time/day/index/text) and a length limit of 512 on the service side were added;
cursor pagination of payouts/ledger_entries/turnover was not exercised by any test -
walkPages over all reports with duplicate and completeness checks; no panic recovery on
gRPC - `grpcmw.UnaryServer` wired in. MINOR/NIT closed:
`day` in RFC 3339 and `toDate(occurred_at, 'UTC')`, unconditional validation of
`REPORTING_TIMEOUT` (1 - 10 s versus WriteTimeout), the 404 detail preserves the Reporting
cause, the error cause preserved in the `%w` chain, the Start/Stop race of the stubs,
`buildReportSQL` (gosec without exceptions), the empty page and negative sums in tests,
deterministic rowKey, a single BOM of the e2e scripts, updated AGENTS/README,
umbrella CBL-004 (GE-7 - the service contour) and the thin spec in `cbl-billing-api`.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No-context | MAJOR: tampered cursor -> Internal/503; MINOR: `day` not RFC 3339, timeout not validated without the address, pagination only for payments; NIT: omitempty, `+5` | cursor key validation + limit of 512; day RFC 3339; unconditional validation 1 - 10 s; walkPages over all reports; omitempty is intentional (test) |
| Architecture | MINOR: 404 detail, timeout, WriteTimeout budget, pagination tests, KI-11 about predicates, 9110/empty merchant, log level, doc.go, cross-repo spec | all closed by fixes (10 s versus 15 s; KI-reporting-13; thin billing spec; doc.go) |
| With-context | MAJOR: pagination was not exercised; MINOR: GE-7 premature, timeout, toDate TZ, stub race, command descriptions, emdash coverage, BOM, runbook; NIT: comments, `%w`, ToolsDir, spec status | all closed (walkPages; GE-7 wording; UTC; local copies in stubs; BOM; cleanup; status) |
| Security | MAJOR: gRPC without authN + 9110 exposed (KI was missing); MINOR: no interceptor, no cursor/deadline bounds; NIT: runbook about the cursor, token in argv | KI-reporting-13 + loopback; grpcmw.UnaryServer; limit of 512; runbook wording; argv accepted intentionally (dev) |

Accepted intentionally: an empty `merchant_id` for lists as a platform internal call
(KI-reporting-13); plaintext gRPC between the BFF and Reporting (KI-billing-api-09);
the server timeout interceptor - 0.6/14.7; the cost of FINAL scans of reports -
KI-reporting-11 (profile 14.2).

## 11. Open decisions

- The catalog is closed and minimal (payments, payouts, ledger_entries, turnover):
  it is extended additively by a task (15.4 - documentation; new types - a new task with
  a catalog version); `payments`/`payouts` without `group_by` - event rows, aggregates -
  `turnover`.
- The cursor is keyset (sort keys), not offset: stable under inserts between pages;
  each key is validated against the format of its type (time/day/index/text), the cursor
  length is bounded by 512 characters on the service side as well (not only the BFF).
- `turnover` without a merchant is forbidden: it is a merchant report; platform
  aggregates - 11.5.
- The merchant filter of ledger rows uses the denormalized 11.3 column (rows without a
  vocabulary record are not visible to the merchant until convergence; KI-reporting-05/10).
- The proxy does not rename Reporting errors: detail preserves the cause, the code comes
  from the contract (`unknown_report_type`/`validation_failed`).
- An empty `merchant_id` for lists is a platform internal call (FR-2); gRPC without
  authN until 0.6 (KI-reporting-13), in dev the port is published to loopback only.
- e2e scripts are self-contained (helpers inside the file): a shared common.ps1 is
  postponed until the runner stabilizes; the duplication is intentional.
- Along the way, the `cbl-billing-api` commit fixed the unit test
  `TestIdempotencyCacheMalformedFallsToPostgres` that had been red since 7.3 (reuse of an
  already acquired key in the second case; the diagnosis and fix are in the service AGENTS
  journal).
