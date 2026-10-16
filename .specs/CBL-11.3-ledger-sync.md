# CBL-11.3. The dim_accounts dictionary, backfill and Reporting vs Ledger reconciliation

| Field | Value |
| :-- | :-- |
| Epic | E11, `Reporting and ClickHouse` |
| Repository | `cbl-reporting` |
| Size | M |
| Wave and parallel group | W3-REPORT |
| Depends on | `CBL-11.2` (pipeline), contracts `cbl-protos@cf-3.2` (LedgerService) |
| Contract version | `cbl-protos@cf-3.4` (Ledger gRPC unchanged since cf-3.2) |
| Status | done, review closed |

## 1. Context and goal

`CBL-11.1` laid down the denormalized columns `account_type` and `merchant_id` in `fact_ledger_entries` and fixed: the `dim_accounts` dictionary is filled by 11.3 (merchant accounts via `ListMerchantAccounts` 2.10, system accounts - derivation by the deterministic `account_id` of the chart of accounts, `CBL-003`, section 9, ledger golden vectors); until it is filled the columns are empty and reports are not blocked. `CBL-11.2` filled the facts leaving the columns empty (KI-reporting-05) and moved out the final part of the original task 11.3: "Deduplication (ReplacingMergeTree), checking discrepancies with the ledger".

Task 11.3 closes three parts:

1. the `reporting.dim_accounts` dictionary and its periodic filling;
2. backfill of the `account_type`/`merchant_id` history of ledger facts;
3. periodic reconciliation of Reporting data with Ledger (control of pipeline validity: discrepancies are recorded by metrics and logs, there are no money decisions from ClickHouse, T-09).

## 2. Scope

In scope: the `dim_accounts` migration, dictionary filling (gRPC `ListMerchantAccounts`, derivation of system accounts), enrichment of new ledger rows in the MV, history backfill, periodic reconciliation with `ListEntries` (window, pagination, discrepancy classification, counters and logs), the Ledger gRPC client, service workers and env, a Ledger stub for e2e, the compose stub service, integration tests and `vocabulary.ps1` e2e, AGENTS/README.

Out of scope: gRPC `ReportingService.Query` and the `/v1/reports` proxy (11.4); dashboards (11.5); `recon.alerts` and gateway business reconciliation (9.x, Reconciler's area); retention/TTL and quotas (14.7); mTLS and gRPC authorization (0.6, KI-ledger-02); moving the `account_id` derivation into `cbl-kit` (KI-ledger-08, before the start of 7.4/10.2); network/address fields of payouts (`PayoutEvent` does not carry them; there is no report that needs them yet).

## 3. Functional requirements

FR-1. The `reporting.dim_accounts` dictionary (migration `00002_dim_accounts.sql`): `account_id String`, `account_type LowCardinality(String)`, `merchant_id String`, `asset LowCardinality(String)`, `decimals UInt8`, `updated_at DateTime64(3, 'UTC')`; `ENGINE = ReplacingMergeTree(updated_at)`, `ORDER BY account_id`; readers - `FINAL`.

FR-2. Dictionary filling (the `vocabulary` worker, cycle `DIM_REFRESH_INTERVAL`):

- required coverage from the facts: payments require a `merchant` row, payouts - `payout` by the (merchant, asset, scale) tuple; a tuple without a dictionary row triggers resynchronization of the merchant (catches new merchants and new assets of known ones);
- merchant accounts: `LedgerService.ListMerchantAccounts(merchant_ref)`; missing and changed rows (diff by `account_id`) are written into the dictionary; response rows are validated by the `account_id` shape and the asset, scale and type rules;
- system accounts: `SELECT DISTINCT asset, decimals` from `fact_ledger_entries` (FINAL) - for each pair `clearing`, `fee`, `suspense` are derived by the rule of `CBL-003`, section 9 (UUIDv5) and written with `merchant_id = ''`;
- merchant budget per cycle with rotation: the tail of large sets does not starve (cursor round-robin, coverage is recalculated every cycle);
- `updated_at = now` of the written rows; a Ledger or ClickHouse error in a cycle - WARN, the cycle repeats after the interval; service startup does not depend on Ledger availability (`readyz` is about ClickHouse and the pipeline).

FR-3. Enrichment at ingestion: the MV `fact_ledger_entries_mv` joins the dictionary (`LEFT JOIN reporting.dim_accounts FINAL ON account_id`) and writes `account_type`/`merchant_id`; an account missing in the dictionary yields empty values (11.2 behavior), backfill catches up later. The MV select changes - the pipeline fingerprint (T-RPT-01) recreates the pair at startup; fact data is not affected.

FR-4. History backfill (a cycle after refresh): accounts from the dictionary with non-empty row sets of `account_type = ''` (FINAL, `BACKFILL_LIMIT` per cycle, deterministic order; accounts outside the dictionary are not selected - the budget does not starve on legacy rows) are compared with the dictionary; for the found ones the mutation is executed: `ALTER TABLE reporting.fact_ledger_entries UPDATE account_type = ..., merchant_id = ... WHERE account_id = ... AND account_type = ''` (idempotent, converges to the full dictionary).

FR-5. Reconciliation with Ledger (the `ledgercheck` worker, cycle `LEDGER_CHECK_INTERVAL`):

- reconciliation window `[now - 2W, now - W)` (`W = LEDGER_CHECK_WINDOW`): lagging by a window protects from false "missing in Reporting" for fresh events;
- traversal of dictionary accounts round-robin (`LEDGER_CHECK_ACCOUNTS` per cycle); the cursor advances for processed accounts, on a complete failure of a batch it stays in place (the batch will repeat), partial failures do not block the traversal;
- Ledger: `ListEntries(account_id, period, limit)` in pages up to `LEDGER_CHECK_MAX_PAGES`; exceeding - the account is skipped with a `truncated` counter; pages are aggregated page by page (memory is limited by the window transactions, not by the number of pages);
- ClickHouse: aggregation by `transaction_id` (number of entries, debit/credit sums) from `fact_ledger_entries` FINAL for the window;
- classification: `missing_in_reporting` (present in Ledger, absent in the facts), `extra_in_reporting`, `amount_mismatch` (count or sums differ);
- every discrepancy - a WARN log (account, transaction, expected/actual), the cycle summary - INFO; `recon.alerts` are not published, there are no money decisions (T-09).

FR-6. Ledger gRPC client: `LEDGER_GRPC_ADDR` (required, host:port), plaintext (mTLS - 0.6), call timeout `LEDGER_TIMEOUT` (default 5s), the client is reused by the workers.

FR-7. main: new env (`LEDGER_GRPC_ADDR`, `LEDGER_TIMEOUT`, `DIM_REFRESH_INTERVAL`, `DIM_MERCHANTS_LIMIT` - merchant budget per cycle, `BACKFILL_LIMIT`, `LEDGER_CHECK_INTERVAL`, `LEDGER_CHECK_WINDOW` (not less than the interval), `LEDGER_CHECK_ACCOUNTS`, `LEDGER_CHECK_PAGE_LIMIT`, `LEDGER_CHECK_MAX_PAGES`), the workers start after migrations and reconciliation, stop by the signal context (waiting for completion before the servers), counters (dictionary, backfill, reconciliation) are exposed in text Prometheus format on `/metrics` (the family name in `# TYPE` without labels; the full platform metric set - 0.7).

FR-8. Tests: unit (derivation golden vectors, env parsing, aggregation comparison and page merging, validation of Ledger rows and mutations, cursor and traversal budget), integration (testcontainers ClickHouse + Ledger stub): dictionary filling, enrichment at ingestion with a populated dictionary, history backfill, all discrepancy classes, pagination and truncated, idempotent refresh repeat; compose e2e: Ledger stub, dictionary filling, backfill, a reconciliation cycle without discrepancies and with a controlled discrepancy (`/metrics` counters).

## 4. Contracts

- gRPC (client): `billing.ledger.v1.LedgerService.ListMerchantAccounts`, `ListEntries` (`cbl-protos@cf-3.2`, unchanged; Alias: `merchant_ref` - the merchant business reference, `Entry` - an entry with `transaction_id`, `direction`, `amount`, `created_at`).
- ClickHouse: `reporting.dim_accounts` (new, owner - the service), reading `fact_*` FINAL; serving `Query`/`/v1/reports` - 11.4, the contract of `CBL-003`, section 7, does not change (the descriptive part is extended with the dictionary).
- Kafka, REST: unchanged (healthz/readyz, metrics).

## 5. Data model and migrations

```sql
CREATE TABLE IF NOT EXISTS reporting.dim_accounts
(
    account_id   String,
    account_type LowCardinality(String),
    merchant_id  String,
    asset        LowCardinality(String),
    decimals     UInt8,
    updated_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY account_id;
```

Derivation of `account_id` of system accounts (`CBL-003`, section 9, replication):

```
account_id = UUIDv5(uuid.NameSpaceURL, "cbl:ledger:account:v1:<type>:<asset>:<decimals>:<merchant_ref>")
```

Golden vectors to reproduce (test): `clearing/USDT/2/` -> `dec27141-ec8f-55de-a325-04a35cd6b0f2`, `fee/USDT/0/` -> `8ee4265f-342c-5dbc-8d80-2516a5641b59`, `suspense/USDT/2/` -> `cb2ccbda-92f9-5889-ba9f-c7ec90b8cfa0`, `merchant/USDT/2/m-1` -> `58686062-c133-5d4e-b08b-0d33a0db924f` and the rest from the table in `CBL-003`, section 9.

## 6. Rules and decisions

- MV with JOIN: `LEFT JOIN (SELECT account_id, account_type, merchant_id FROM reporting.dim_accounts FINAL)` - without the ClickHouse dictionary (dict DDL with `SOURCE(CLICKHOUSE(...))` requires explicit credentials in DDL); `FINAL` in the right part excludes row multiplication on uncollapsed dictionary duplicates; a per-block hash join over a small dictionary is cheap; dictionary changes are visible to new inserts without recreating the MV.
- Backfill - a mutation by a narrow predicate (`account_type = ''`): converges, does not touch enriched rows; a per-cycle account limit against an avalanche of mutations; ClickHouse mutation asynchrony is not awaited (the next cycle counts the remainder).
- Reconciliation - round-robin over the dictionary, not over transactions: there is no global enumeration of transactions either in the Ledger contract or in the facts (the fact table is not a source of truth); traversal of the dictionary also detects a complete loss of all events by an account for the window.
- Reconciliation does not publish `recon.alerts` (Reconciler's area 9.2): discrepancies of the reporting pipeline - metrics and logs; threshold calibration and moving to alerting - 14.x/0.7.
- False `missing_in_reporting` for events that arrived in Kafka later than a window after `occurred_at` are acceptable and visible in logs; the window with lag `W` minimizes them; re-checking the next window is not performed (one window - one pass).
- Startup does not depend on Ledger: the dictionary and reconciliation degrade (WARN, counters), the pipeline and `readyz` live; the KI records the degradation.
- KI-ledger-08: the derivation is reproduced by golden vectors, moving it into `cbl-kit` - before the start of 7.4/10.2 (a separate task per the KI).

## 7. Edge cases

- A merchant without accounts in Ledger: an empty response is not an error, there are no records (no entries).
- Kafka redeliveries: facts collapse with FINAL; reconciliation reads FINAL; the dictionary collapses with ReplacingMergeTree(updated_at).
- `account_id` from facts outside the chart of accounts (legacy): there is no dictionary record, the row stays with empty values, the mutation is not triggered.
- The dictionary is filled after the first batch of events: rows between refreshes are not enriched, backfill closes the gap; the whole history converges in a finite number of cycles.
- Reconciliation of an account without entries for the window: empty sets on both sides - a match, the account is checked.
- Ledger unavailable in a cycle: WARN, error counters; on a complete failure of a batch the cursor does not move (the batch will repeat in the next cycle), partial failures do not block the traversal (the cursor advances past the last processed account); context cancellation on shutdown is not counted as a failure.
- `ListEntries` pages more than the limit: the account is skipped (truncated), without classification of discrepancies for it.
- decimals of an asset that never appeared in the chart: system accounts are derived by the same key (deterministically), there are no collisions.

## 8. Non-functional requirements

Enrichment latency: refresh - the interval (default 5m), backfill - the next cycle after refresh; reconciliation - the interval (default 5m), a window with lag. Load: refresh - requests to Ledger only for merchants with unfulfilled coverage (usually only new ones); reconciliation - at most `LEDGER_CHECK_ACCOUNTS` accounts and `LEDGER_CHECK_MAX_PAGES * LEDGER_CHECK_PAGE_LIMIT` entries per account per cycle (limited by config), pages are aggregated page by page (memory - window transactions; the PAGE_LIMIT x MAX_PAGES product is deliberate - a bound of up to 1M entries per account per cycle at maximum config). Periodic FINAL scans of facts and backfill mutations as volumes grow are profiled in 14.2 (KI-reporting-11). Security: gRPC plaintext until 0.6 (KI-reporting-08), values from Ledger are validated before writing (the `account_id` shape and the asset, scale and type rules, the merchant reference pattern), parameters in SQL are bound by the driver; DSN/addresses are not logged with secrets.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit | derivation of `account_id` over all golden vectors of `CBL-003`, section 9; env parsing and validation (intervals, limits, address); aggregation comparison (all discrepancy classes, match, page merging, unknown direction); validation of Ledger rows (`dimRow`) and mutations (`BackfillAccount`) |
| Integration | testcontainers ClickHouse + Ledger stub (gRPC on a random port): the first refresh fills the dictionary (merchant ones from the stub, system ones derived from the facts); a new asset of a known merchant arrives via resynchronization; a new event after filling is enriched at ingestion (the MV JOIN); an old event is enriched by backfill; a repeated refresh is idempotent; reconciliation with matching entries - 0 discrepancies, a stub-extra entry - `missing_in_reporting`, a different sum - `amount_mismatch`, a CH row without Ledger - `extra_in_reporting`; entries outside the window are silent; exceeding max pages - truncated without classification; a complete batch failure does not move the cursor; an unavailable Ledger does not crash the cycle |
| E2E local | compose: kafka, schema-registry, clickhouse, ledger-stub, reporting; dictionary filling (stub accounts + system ones), backfill of non-empty `account_type`/`merchant_id`, reconciliation cycle: `/metrics` `checked > 0` and discrepancies by counter (a controlled case); down -v |
| E2E global | GE-7 (after 11.4); the service stand here |
| Load | not applicable (no HTTP load of the service); the pipeline - 14.2 |

Target unit+integration coverage: at least 80%.

## 10. Work plan and commit

Tests first (unit golden vectors, comparison and validation) -> the `dim_accounts` migration -> `internal/domain` (derivation) -> `internal/ledger` (client) -> `internal/storage` (dim-diff, backfill, MV JOIN) -> `internal/vocabulary` and `internal/ledgercheck` -> main (env, workers, metrics) -> stub and e2e -> runs (unit+integration+coverage, lint, vet, emdash, e2e) -> 4 reviews -> AGENTS/README/`CBL-003`/`CBL-004` -> commit `CBL-11.3: dim_accounts dictionary, backfill and Reporting vs Ledger reconciliation`.

## 11. Review (CBL-001, step 6)

Four independent passes (16.10.2026) and closure verification. Blockers of the first round: the invalid Prometheus format of `/metrics` (`# TYPE` with labels would break scraping) and non-convergence of the dictionary for new accounts of known merchants (the "merchant traversed" marker was not revisited) - closed: the format prints `# TYPE` once per family with a name without labels; synchronization goes by the required coverage of facts (payments - `merchant`, payouts - `payout` by the merchant/asset/scale tuple) with a per-account diff, the `DIM_MERCHANTS_LIMIT` budget and rotation. MAJOR on load (cost of FINAL scans and mutations) closed with mitigations (backfill selects only dictionary accounts with a deterministic order, page-by-page aggregation of reconciliation, tight page bounds) and KI-reporting-11 with measurement in 14.2. MINOR/NIT closed: the traversal cursor does not move on a complete batch failure and is not blocked by partial failures; context cancellation is not counted as a failure; `Aggregate` is fail-closed on an unknown direction; window bounds are aligned to milliseconds; SQL parameters are bound by the driver (manual escaping removed); `account_id` is validated by the domain; an empty `LEDGER_GRPC_ADDR` is an explicit error; `LEDGER_CHECK_WINDOW >= LEDGER_CHECK_INTERVAL`; batch inserts of the dictionary limit the number of parts; `FactMerchantPairs` with `DISTINCT`; `stop()` before stopping the servers; umbrella CBL-000 and the stand init/smoke are synchronized; documentation leftovers closed. Verification (the fourth round) confirmed closure of all findings; the commit is not blocked (the security point - separately: no BLOCKER/MAJOR).

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | BLOCKER: invalid `# TYPE` with labels; MAJOR: reconciliation cursor vs section 7, dictionary non-convergence (no diff of changed ones), unit SQL plan; MINOR/NIT: `LIMIT` without `ORDER BY`, page accumulation, cancellation as an error, IPv6, window edge | Prometheus format + test; `advanceCursor` (a complete failure does not move it, partial ones do not block); coverage from facts + per-account diff + new asset case; section 9 rewritten for facts; `IN dim + ORDER BY`; page-by-page aggregation; INFO cancellation; ms alignment; batch inserts; `DISTINCT` |
| Architecture | MAJOR: `# TYPE`, dictionary non-convergence, cost of FINAL scans/mutations; MINOR: isolation of Ledger degradation, multi-instance, window<interval, gRPC intake, CBL-000, coverage numbers | format; coverage/rotation; KI-reporting-11 + mitigations; KI-reporting-10/12; W>=I validation; page bounds; CBL-000 updated; `make cover`, 82.7% |
| With context | MAJOR: `# TYPE`, new accounts of a known merchant, `FactMerchantPairs` without `DISTINCT`; MINOR: cursor, backfill budget starvation, numbers, window in e2e, unit plan, texts, Makefile | all closed with fixes; e2e rewritten (seed before the window, two-phase events, mutation stability assert); `DISTINCT` added |
| Security | No BLOCKER/MAJOR; MINOR: page memory, merchant limit, duplicate `# TYPE`, pre-existing env in DDL (outside the diff); NIT: `account_id` shape, binding instead of escaping, stub loopback | page-by-page aggregation and tight bounds; limit with rotation; format; outside the diff/backlog; `ValidAccountID`; parameterized SQL; loopback |

Accepted deliberately: gRPC plaintext to Ledger (KI-reporting-08, mTLS - 0.6); backfill mutations and scan cost (KI-reporting-09/11, measurement 14.2); one writer per stand (KI-reporting-12); diagnostic false missing at the window edge (lag W, calibration 14.2); an IPv6 address in `LEDGER_GRPC_ADDR` is not supported (stand host:port convention).

## 12. Open questions and decisions

- JOIN instead of the ClickHouse dictionary: credentials do not get into the DDL, fewer entities; the price - a JOIN at insert (cheap with a small dictionary) and the need for `FINAL` in the right part.
- Reconciliation by accounts (round-robin), not by merchants: an account is an indivisible unit of the Ledger API.
- Window with lag `W` and a single pass: reconciliation is diagnostic; re-checking the window and retries - if the need is confirmed (14.x).
- Backfill via a mutation, not partition rewrite: a narrow predicate, convergence, minimum rewrite; `ALTER UPDATE` is a standard ClickHouse mechanism.
- mTLS/authorization of the Ledger gRPC - 0.6 (KI-ledger-02); the service does not publish the Ledger port.
- Retention of `dim_accounts` and migration user rights - 0.6/14.7.
