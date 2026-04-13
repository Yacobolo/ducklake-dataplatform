# AGENTS.md

## Project

Go data platform: secure SQL query layer over DuckDB with RBAC, row-level security, and column masking. SQLite as metadata store.

## Commands

```bash
task dev            # start local dev server; injects dev env and stable per-worktree ports
task build          # go build ./...
task test           # unit + integration tests
task test:unit      # unit tests only
task lint           # Go + OpenAPI linters
task check          # lint + test with CI-style summary — run before PRs
task generate       # regenerate all code
task build-cli      # build CLI binary → bin/duck
```

Lint policy reference: `LINTING.md`

Single package/test: `go test -race -run TestName ./internal/pkg/...`

`task dev` does not require a checked-out `.env`; it injects a local-only dev profile, builds a local server binary, and derives stable HTTP/Flight SQL/PG wire ports from the current worktree path so multiple AI worktrees can run concurrently.

## Workflow

1. Branch as `ai/<type>/<name>` from `origin/main` (`feat`, `fix`, `refactor`, `chore`, `test`, `docs`)
2. Commit with conventional commits: `feat:`, `fix:`, `refactor:`, etc.
3. Run `task check` before pushing
4. Rebase onto `origin/main` if stale
5. Open PR, report URL for review

## Architecture

```
cmd/server/          → HTTP server entry point
cmd/cli/             → CLI binary (duck)
cmd/apigen/          → APIGen CLI wrapper over reusable `pkg/apigen/...` packages
pkg/apigen/          → importable APIGen IR, CUE compiler, emitters, and runtimes
internal/api/        → HTTP handlers and APIGen-generated transport code
internal/service/    → business logic
internal/domain/     → types, interfaces, errors (zero deps)
internal/db/         → repository implementations, sqlc, migrations, mappers
internal/engine/     → DuckDB engine with RBAC + RLS + column masking
internal/declarative/→ plan/apply/validate config engine
internal/middleware/ → JWT + API key auth
pkg/cli/             → CLI commands and declarative client
```

Dependency direction: `api` → `service` → `domain` ← `repository`. Never import upward.

## Testing

- Table-driven tests with `t.Run()` subtests.
- Use `require` (fatal) and `assert` (non-fatal) from testify.
- Prefer real SQLite via `t.TempDir()` over mocks.
- Helpers must call `t.Helper()`.
- Naming: `TestArea_Scenario` (e.g., `TestAPI_SchemaCRUD`).
- Integration tests gated behind `//go:build integration`.

## Code Style

- **Imports:** stdlib, then third-party, then internal — blank-line separated.
- **Errors:** domain errors in `internal/domain/errors.go` (`NotFoundError`, `AccessDeniedError`, `ValidationError`, `ConflictError`). Always wrap: `fmt.Errorf("context: %w", err)`.
- **Interfaces:** no `I` prefix, centralized in `internal/domain/repository.go`.
- **Compile-time checks:** `var _ Interface = (*Impl)(nil)`.
- **Nullables:** `*string` in domain, `sql.NullString` in DB layer. Convert via `internal/db/mapper/`.
- **No `panic`/`recover`** in application code.

## Generated Code — Do Not Edit

- `internal/api/contract/cue/*.cue` — checked-in CUE authoring source for APIGen; this is the application-owned declarative source of truth
- `internal/api/gen/openapi.yaml` — tracked canonical OpenAPI artifact emitted from `internal/api/contract/cue` via `task cue:compile`; includes repo-owned extensions such as `x-authz` and `x-cli-command`
- `internal/api/gen/json-ir.json` — local APIGen intermediate emitted from `internal/api/contract/cue` via `task cue:compile` (generated, not committed)
- `internal/api/gen_request_models.gen.go`, `internal/api/server.apigen.gen.go`, `internal/api/types.gen.go`, `pkg/cli/gen/apigen_registry.gen.go` — APIGen outputs from JSON IR (`internal/api/gen/json-ir.json`) via `cmd/apigen`
- Pipeline: CUE authors the contract, `task cue:compile` emits both canonical OpenAPI and JSON IR, then `cmd/apigen` generates Go transport/CLI artifacts from JSON IR while embedding the canonical OpenAPI into the server
- `pkg/cli/gen/` is primarily generated CLI metadata; do not assume it is the active handwritten CLI runtime boundary
- `internal/db/dbstore/*.sql.go` — from `internal/db/queries/*.sql` via sqlc
- `internal/duckdbsql/catalog/*_gen.go` — from DuckDB introspection via `scripts/genduckdb`
