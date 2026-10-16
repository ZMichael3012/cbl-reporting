# E2E auxiliary files (cbl-reporting)

Purpose: support runs per `test/e2e/E2E.md` and analysis of their artifacts.
There are no PowerShell run scripts here (convention `CBL-002` section 7).

| File/directory | Purpose |
| :-- | :-- |
| `produce/` | Go test event producer: scenarios `full`, `duplicate`, `broken`, `sentinel`; the `-live` flag (occurred_at = now) for check runs; prints identifiers to stdout (flag help and error messages are executable output and stay as-is) |
| `ledgerstub/` | Test LedgerService sources (gRPC): `m-1` account fixtures, HTTP posting seed at `:9041/entries`; the `ledger-stub` image is started by the compose stand |
| `summary.template.md` | The `summary.md` run report template |

Rules: utilities must build and run with standard `go`/`docker` commands on any platform;
new auxiliary files are added here and recorded in this table.
