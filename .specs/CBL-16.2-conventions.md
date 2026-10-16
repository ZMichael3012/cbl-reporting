# CBL-16.2. cbl-reporting: English comments and documentation; e2e via E2E.md

| Field | Value |
| :-- | :-- |
| Epic | E16, `Convention alignments` |
| Repository | `cbl-reporting` |
| Size | L |
| Wave and parallel group | W0-CORR |
| Depends on | `CBL-16.1` (convention in `CBL-001`/`CBL-002`, templates) |
| Contract version | not applicable (no contract changes) |
| Status | done, review closed |

## 1. Context and goal

Two cross-cutting customer corrections (16.10.2026): (1) comments and documentation are
written in English in all files (code, specs, AGENTS.md, README, configurations; log and
error messages stay unchanged as executable contracts); (2) e2e runs are not PowerShell
scripts but an agent instruction `test/e2e/E2E.md` with run artifacts in `test/e2e/.runs/`.
The convention is `CBL-002` section 7; templates are `.specs/templates/E2E.template.md` and
`.specs/templates/AGENTS.service-template.md`.

## 2. Scope

In scope: translation of all comments, docstrings, specs, AGENTS.md, README and
configuration comments of the repository into English; `test/e2e/E2E.md` (scenarios
ingestion, vocabulary, reports; command order; mandatory artifacts; parallelism);
relocation of the `produce` and `ledgerstub` utilities into `test/e2e/.additionals/`,
`inventory.md` and `summary.template.md`; removal of `ingestion.ps1`, `vocabulary.ps1`,
`reports.ps1`; Makefile/AGENTS.md/README.md; `.gitignore` (`.runs/`); a validation run of
the scenarios per the new instruction with artifacts.

Out of scope: changing scenario substance (the set of checks is preserved), production code
changes, global e2e (umbrella `test/e2e/E2E.md`, task 16.1), other repositories (16.3 - 16.16).

## 3. Requirements

FR-1. Language: no Russian comments or docstrings remain in the repository (code, tests,
configs, deploy files, migrations, docs, specs); exceptions are identifiers, linter
directives, protocol and contract field names, and log/error messages (executable
contracts). Godoc docstrings keep the entity-name first line and end with a period.

FR-2. E2E: `test/e2e/E2E.md` contains preconditions, isolation (compose project per
lowercase UTC mark, port overrides for parallel runs), the run order, scenarios with
expected values, mandatory artifacts (`summary.md`, `compose-ps.txt`, `compose-up/down.log`,
`compose-logs.txt`, `prepare.log`, `<scenario>.log`, `<scenario>-checks.txt`, emergency
dumps) and cleanup; there are no PowerShell run scripts; AGENTS.md references the
instruction.

FR-3. Auxiliary files live in `test/e2e/.additionals/` (Go utilities build and run through
explicit paths `./test/e2e/.additionals/...`; the dot directory does not hinder
`go build/run`, verified); the compose stand still builds `ledger-stub` from the
`.additionals/ledgerstub/Dockerfile`.

FR-4. Validation run: all three scenarios executed per `E2E.md`, artifacts in
`test/e2e/.runs/<mark>/` (gitignored), `summary.md` filled with a PASS verdict.

## 4. Plan

Comment and documentation translation (parallel over code, tests/configs, specs) ->
E2E.md and `.additionals` -> deletion of the ps1 scripts -> Makefile/AGENTS/README/.gitignore
-> validation run of the three scenarios per the instruction -> 4 reviews -> commit
`CBL-16.2: English comments and docs; e2e via E2E.md`.

## 5. Reviews and runs

Verified: repository unit+integration tests green (no production code changes), lint 0,
scenario re-run per `E2E.md` with run artifacts, em-dash check clean, comment language
verified by a Cyrillic scan. Historical specs 11.2-11.4 were translated as part of this
task; the previous ps1 references in them are superseded by `test/e2e/E2E.md`.
