# AGENTS.md

## Project Overview

Go data platform: secure SQL query layer over DuckDB with RBAC, row-level security, and column masking. DuckDB for analytics, SQLite as metadata/permissions store. C++ DuckDB extension in `extension/duck_access/`. Clean/hexagonal architecture.

## Build & Run Commands

Uses [Task](https://taskfile.dev/) (`Taskfile.yml`):

```bash
task build                # go build ./...
task test                 # go test -race ./...
task vet                  # go vet ./...
task lint                 # run all linters (Go + OpenAPI)
task check                # run lint + test, print CI-style pass/fail summary — run before creating PRs
task generate             # regenerate all code (API types + server + sqlc + CLI + DuckDB catalog)
task generate-api         # regenerate API types/server from openapi.yaml
task generate-cli         # regenerate CLI commands from openapi.yaml
task sqlc                 # regenerate DB query code from SQL
task migrate-up           # run all pending migrations
task new-migration -- X   # create a new migration named X
task integration-test     # integration tests (require S3 creds + built extension)
task build-cli            # build the duck CLI binary → bin/duck
```

Run server: `go run ./cmd/server`

## Branch & PR Conventions

### Branch naming

```
ai/<type>/<name>
```

Types: `feat`, `fix`, `refactor`, `chore`, `test`, `docs`

### Commit messages

[Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `refactor:`, `chore:`, `test:`, `docs:`

### Workflow

1. If a plan exists in `.opencode/plans/`, read it fully before starting
2. Branch as `ai/<type>/<name>` from `origin/main`
3. Implement step by step, commit incrementally
4. Run `task check` — fix failures before pushing
5. Rebase onto `origin/main` (other agents may have merged)
6. Open draft PR referencing the plan if applicable
7. Report PR URL for human review

## Testing

```bash
task test:unit                                                         # all unit tests
task test:integration                                                  # integration tests
go test -race ./internal/sqlrewrite/...                                # single package
go test -race -run TestAdminSeesAllRows ./internal/engine/...          # single test
go test -race -run TestAPI_SchemaCRUD/create_schema ./internal/api/... # single subtest
```

### Conventions

- **Table-driven tests** with `t.Run()` subtests — default pattern for all new tests.
- **Use testify** (`require` for fatal, `assert` for non-fatal) for assertions.
- **Hand-written mocks** preferred (see `internal/api/mock_catalog_test.go`). Most tests use real SQLite via `t.TempDir()` instead of mocks.
- **Helpers** must call `t.Helper()`. Use `t.Cleanup()` for teardown, `t.TempDir()` for temp dirs, `t.Skip()` when prerequisites are missing.
- **Naming:** `TestArea_Scenario` (e.g., `TestAPI_SchemaCRUD`). Subtests use descriptive names.
- **Auth stubbing:** inject a fixed principal via `middleware.WithPrincipal()`.
- Integration tests are gated behind `//go:build integration` and skipped by `task test`.

## Code Style

### Imports

Three groups, blank-line separated, alphabetical within each:

```go
import (
    "context"                               // 1. stdlib

    "github.com/go-chi/chi/v5"             // 2. third-party

    "duck-demo/internal/domain"             // 3. internal
)
```

Use `_` imports for driver side-effects. Aliases only when names conflict.

### Error Handling

- **Domain errors** in `internal/domain/errors.go`: `NotFoundError`, `AccessDeniedError`, `ValidationError`, `ConflictError`. Constructors: `domain.ErrNotFound("schema %s not found", name)`.
- **Always wrap** with context: `fmt.Errorf("classify statement: %w", err)`.
- **Repository layer** maps DB errors via `mapDBError()` in `internal/db/repository/helpers.go` (`sql.ErrNoRows` → `NotFoundError`, unique constraint → `ConflictError`).
- **API boundary** type-switches on domain errors to select HTTP status. See `internal/db/mapper/domain_api.go`.
- **Audit logging is best-effort** — failures silently discarded: `_ = s.audit.Insert(ctx, entry)`.
- No `panic`/`recover` in application code. `log.Fatalf` only in `main()`.

### Architecture

```
cmd/server/main.go          → composition root
cmd/cli/                    → CLI entry point (duck binary)
internal/api/               → HTTP handlers (generated StrictServerInterface)
internal/service/           → business logic (depends on domain interfaces only)
internal/domain/            → types, interfaces, errors (zero external deps)
internal/db/repository/     → implements domain repository interfaces
internal/db/dbstore/        → sqlc-generated code (DO NOT EDIT)
internal/db/mapper/         → conversions between layers
internal/db/migrations/     → goose SQL migrations
internal/engine/            → SecureEngine (DuckDB + RBAC + RLS + column masking)
internal/sqlrewrite/        → SQL parsing/rewriting via pg_query_go
internal/duckdbsql/         → DuckDB SQL parser (custom, not pg_query)
internal/middleware/        → JWT + API key auth
internal/declarative/       → Declarative config engine (plan/apply/validate)
pkg/cli/                    → CLI commands and declarative client
```

Dependency direction: `api` → `service` → `domain` ← `repository`. Never import upward.

### Key Patterns

- No `I` prefix on interfaces. All interfaces centralized in `internal/domain/repository.go`.
- Compile-time interface checks: `var _ StrictServerInterface = (*APIHandler)(nil)`.
- Nullable fields: `*string` / `*int64` in domain, `sql.NullString` / `sql.NullInt64` in DB layer. Convert via mapper functions in `internal/db/mapper/`.
- Constructors follow `New<Type>(deps...)`.
- Section markers in large files: `// === Principals ===`.

## Code Generation — Do Not Hand-Edit

Generated files (never edit manually):

- `internal/api/types.gen.go`, `server.gen.go` — from `openapi.yaml` via `oapi-codegen`
- `internal/db/dbstore/*.sql.go`, `db.go`, `models.go` — from `internal/db/queries/*.sql` via `sqlc`
- `pkg/cli/gen/*.gen.go` — from `openapi.yaml` via `cmd/cli-gen`
- `internal/duckdbsql/catalog/*_gen.go` — from DuckDB introspection via `scripts/genduckdb`

To change API: edit `internal/api/openapi.yaml` → `task generate-api`.
To change queries: edit `internal/db/queries/*.sql` → `task sqlc`.
To change CLI: edit `cli-config.yaml` or `cmd/cli-gen/` → `task generate-cli`.
