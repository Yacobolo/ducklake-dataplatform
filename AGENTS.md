# AGENTS.md

## Project

Go data platform: secure SQL query layer over DuckDB with RBAC, row-level security, and column masking. SQLite as metadata store.

## Commands

```bash
task build          # go build ./...
task test           # unit + integration tests
task test:unit      # unit tests only
task lint           # Go + OpenAPI linters
task check          # lint + test with CI-style summary — run before PRs
task generate       # regenerate all code
task build-cli      # build CLI binary → bin/duck
```

Single package/test: `go test -race -run TestName ./internal/pkg/...`

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
internal/api/        → HTTP handlers (generated StrictServerInterface)
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

- `internal/api/types.gen.go`, `server.gen.go` — from `openapi.yaml` via oapi-codegen
- `internal/db/dbstore/*.sql.go` — from `internal/db/queries/*.sql` via sqlc
- `pkg/cli/gen/*.gen.go` — from `openapi.yaml` via `cmd/cli-gen`
- `internal/duckdbsql/catalog/*_gen.go` — from DuckDB introspection via `scripts/genduckdb`
