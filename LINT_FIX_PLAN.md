# Lint Fix Plan — 580 golangci-lint Issues

## Overview

This plan resolves all 580 golangci-lint issues across 11 linter categories in 8 phases.
Each phase is independently testable via `task lint`. Phases are ordered to minimize
cross-phase conflicts and maximize batch efficiency.

**Verification command after each phase:**
```bash
task lint 2>&1 | tail -20   # check issue count drops
task test                    # confirm no regressions
```

---

## Phase 1: Configuration & Suppressions (~90 issues)

**Goal:** Eliminate issues that should be solved by config changes, not code changes.

### 1a. Suppress revive exported-comment on APIHandler methods (~83 issues)

The ~83 methods on `APIHandler` implement `StrictServerInterface` — a generated interface.
Requiring doc comments on every implementation method adds noise, not value.

**File:** `.golangci.yml`

Add an exclusion rule:
```yaml
exclusions:
  rules:
    - path: _test\.go
      linters:
        - gosec
    # Generated interface methods — doc comments are noise
    - path: internal/api/handler\.go
      linters:
        - revive
      text: "exported: exported method .+ should have comment"
```

### 1b. Suppress gosec false positives (2 issues)

- `G304` in `config.go` — file path from env is by design → add `//nolint:gosec // file path from environment variable`
- `G201` in `information_schema.go` — SQL built from validated identifiers → add `//nolint:gosec // identifiers are validated via ddl.QuoteIdentifier`

### 1c. Suppress revive unused-parameter for HTTP handlers in main.go (2 issues)

The `/openapi.json` and `/docs` handlers receive `r *http.Request` per `http.HandlerFunc`
signature but don't use it. Use `_` parameter naming:
```go
r.Get("/openapi.json", func(w http.ResponseWriter, _ *http.Request) {
r.Get("/docs", func(w http.ResponseWriter, _ *http.Request) {
```

**Files:** `cmd/server/main.go`, `.golangci.yml`, `internal/config/config.go`, `internal/engine/information_schema.go`
**Estimated issues resolved:** ~87
**Risk:** None — config/comment only

---

## Phase 2: Quick Deletions & Mechanical Fixes (~16 issues)

**Goal:** Remove dead code and fix trivially mechanical patterns.

### 2a. Delete unused functions (3 issues — unused)

**File:** `internal/db/mapper/domain_dbstore.go`
- Delete `formatTime` (line 19–21)
- Delete `boolToInt` (line 23–28)

**File:** `internal/api/handler.go`
- Delete `isDomainError` (lines 107–111)

### 2b. Fix gocritic issues (5 issues)

| File | Issue | Fix |
|------|-------|-----|
| `cmd/server/main.go` | exitAfterDefer: `log.Fatalf` after `defer` | Restructure `main()` to use a helper function that returns error; `main()` calls `os.Exit` |
| `internal/api/handler.go` | ifElseChain in `ListGrants` (~line 312) | Convert `if/else if/else` to `switch` |
| `internal/ddl/builder.go` | deprecatedComment format | Fix `// Deprecated: ...` notice to match go convention |
| `internal/engine/engine.go` | deprecatedComment format | Fix `// Deprecated: ...` notice |
| `internal/sqlrewrite/sqlrewrite.go` | singleCaseSwitch | Convert single-case `switch` to `if` |

### 2c. Fix gosec G114 in main.go (1 issue)

Replace:
```go
http.ListenAndServe(cfg.ListenAddr, r)
```
With:
```go
srv := &http.Server{
    Addr:              cfg.ListenAddr,
    Handler:           r,
    ReadHeaderTimeout: 10 * time.Second,
}
srv.ListenAndServe()
```

### 2d. Fix staticcheck issues (2 issues)

Likely deprecated API usage or simplification suggestions — apply the suggested fix.

### 2e. Fix deprecatedComment format (2 gocritic issues, counted above)

Ensure format is exactly: `// Deprecated: use X instead.`

**Files:** `cmd/server/main.go`, `internal/api/handler.go`, `internal/ddl/builder.go`, `internal/engine/engine.go`, `internal/sqlrewrite/sqlrewrite.go`, `internal/db/mapper/domain_dbstore.go`
**Estimated issues resolved:** ~11 (unused 3 + gocritic 5 + gosec 1 + staticcheck 2)
**Risk:** Low — exitAfterDefer restructuring needs care; test after

---

## Phase 3: errorlint — Convert Type Switches to errors.As (~39 issues)

**Goal:** Migrate all `switch err.(type)` and `err == sentinel` to Go 1.13+ patterns.

### 3a. handler.go — Convert 33 `switch err.(type)` blocks

Batch convert all error type switches in `handler.go`. The pattern is mechanical:

**Before:**
```go
switch err.(type) {
case *domain.NotFoundError:
    return GetSchema404JSONResponse{Code: 404, Message: err.Error()}, nil
case *domain.AccessDeniedError:
    return GetSchema403JSONResponse{Code: 403, Message: err.Error()}, nil
default:
    return nil, err
}
```

**After:**
```go
var notFoundErr *domain.NotFoundError
var accessErr *domain.AccessDeniedError
switch {
case errors.As(err, &notFoundErr):
    return GetSchema404JSONResponse{Code: 404, Message: err.Error()}, nil
case errors.As(err, &accessErr):
    return GetSchema403JSONResponse{Code: 403, Message: err.Error()}, nil
default:
    return nil, err
}
```

Note: 3 handlers (`CreateStorageCredential`, `CreateExternalLocation`, `CreateVolume`)
already use `errors.As` — skip those.

### 3b. catalog.go — Convert 6 `err == sql.ErrNoRows` comparisons

**File:** `internal/service/catalog.go` (and possibly `internal/db/repository/catalog.go`)

Convert:
```go
if err == sql.ErrNoRows {
```
To:
```go
if errors.Is(err, sql.ErrNoRows) {
```

### 3c. table_statistics.go and introspection.go

Apply the same `errors.Is` pattern for any sentinel comparisons.

**Files:** `internal/api/handler.go`, `internal/service/catalog.go`, `internal/db/repository/catalog.go`, `internal/db/repository/table_statistics.go`, `internal/db/repository/introspection.go`
**Estimated issues resolved:** ~39
**Risk:** Medium — logic must be preserved exactly. Run `task test` to verify.

---

## Phase 4: nilerr — Fix nil-returning error handlers (5 issues)

**Goal:** Fix handlers that check for a specific error type but return `nil` error to the
framework when the typed response already signals the error.

These 5 cases in `handler.go` are where the linter detects a pattern like:
```go
if err != nil {
    // returns typed response, nil  ← nilerr flags this
}
```

The fix aligns with Phase 3 — once the `switch err.(type)` blocks are converted, the nilerr
issues are typically resolved because the error is properly matched. Verify after Phase 3;
any remaining nilerr issues get explicit error handling.

**Files:** `internal/api/handler.go`
**Estimated issues resolved:** ~5
**Risk:** Low — likely resolved as side-effect of Phase 3

---

## Phase 5: revive — Package Comments & Export Comments (~115 issues)

**Goal:** Add all missing package doc comments and exported-symbol comments.

### 5a. Package doc comments (~13 packages, ~90 issues)

Add a `// Package <name> ...` comment to one file per package. Create a `doc.go` file
only if no natural "main" file exists; otherwise add to the primary file.

| Package | File to Add Comment | Comment |
|---------|-------------------|---------|
| `main` | `cmd/server/main.go` | `// Package main is the entry point for the duck-demo HTTP API server.` |
| `domain` | `internal/domain/errors.go` or new `doc.go` | `// Package domain defines core types, interfaces, and errors for the data platform.` |
| `service` | `internal/service/authorization.go` or new `doc.go` | `// Package service implements business logic for the data platform.` |
| `db` | `internal/db/sqlite.go` | `// Package db provides SQLite connection management and migrations.` |
| `repository` | `internal/db/repository/helpers.go` | `// Package repository implements domain repository interfaces using SQLite.` |
| `mapper` | `internal/db/mapper/domain_dbstore.go` | `// Package mapper converts between domain, API, and database layer types.` |
| `crypto` | `internal/db/crypto/encrypt.go` | `// Package crypto provides AES encryption for stored credentials.` |
| `engine` | `internal/engine/engine.go` | `// Package engine provides a secure DuckDB query engine with RBAC, RLS, and column masking.` |
| `middleware` | `internal/middleware/auth.go` | `// Package middleware provides HTTP authentication middleware (JWT and API key).` |
| `ddl` | `internal/ddl/builder.go` | `// Package ddl builds DuckDB DDL statements with safe identifier quoting.` |
| `config` | `internal/config/config.go` | `// Package config loads application configuration from environment variables.` |
| `api` | Already has package comment in generated files — add to `handler.go` if needed | (verify — may already be covered) |
| `integration` | `test/integration/helpers_test.go` | `// Package integration provides end-to-end HTTP tests for the data platform API.` |

### 5b. Export comments for mapper functions (~24 issues)

**File:** `internal/db/mapper/domain_dbstore.go`

Add comments to all exported functions. Example:
```go
// PrincipalFromDB converts a dbstore Principal to a domain Principal.
func PrincipalFromDB(row dbstore.Principal) domain.Principal {
```

### 5c. Export comments for service types and functions (~30 issues)

Add comments to exported types/functions across:
- `internal/service/*.go` — service constructors and types
- `internal/engine/engine.go` — exported functions
- `internal/domain/*.go` — exported types/constants
- `internal/db/repository/*.go` — repo constructors
- `internal/ddl/*.go` — exported builder functions
- `internal/middleware/auth.go` — exported functions
- `internal/config/config.go` — exported functions

### 5d. Rename APIHandler → Handler (~1 stutter issue)

**File:** `internal/api/handler.go`

Rename `APIHandler` to `Handler` throughout handler.go. Since `APIHandler` is only
referenced within `internal/api/handler.go` (verified — 0 references in tests or main.go;
main.go calls `api.NewHandler` which returns `*APIHandler`), this is safe:

- `type APIHandler struct` → `type Handler struct`
- `func NewHandler(...) *APIHandler` → `func NewHandler(...) *Handler`
- `return &APIHandler{` → `return &Handler{`
- `var _ StrictServerInterface = (*APIHandler)(nil)` → `var _ StrictServerInterface = (*Handler)(nil)`
- All 83 method receivers: `func (h *APIHandler)` → `func (h *Handler)`
- Comment: `// APIHandler implements` → `// Handler implements`

**Files:** ~20 files across service/, domain/, engine/, middleware/, config/, ddl/, mapper/, repository/, api/
**Estimated issues resolved:** ~115
**Risk:** Low — doc comments only, plus safe rename. Run `task build` to verify rename.

---

## Phase 6: Production Code errcheck, rowserrcheck, noctx (~45 issues)

**Goal:** Fix all error-handling issues in production (non-test) code.

### 6a. errcheck — defer rows.Close() (13 instances)

**Files:** `internal/service/catalog.go` (6), `internal/db/repository/introspection.go` (3), `internal/db/repository/search.go` (1), `internal/engine/engine.go` (1), `internal/service/manifest.go` (1), `internal/service/query.go` (1)

Pattern — wrap in error-logging helper or use named return:
```go
defer func() {
    if err := rows.Close(); err != nil {
        slog.Error("close rows", "error", err)
    }
}()
```

Note: The project uses `log/slog`. Check if slog is already imported; if not, use the
simpler `defer rows.Close()` with `//nolint:errcheck` and a comment explaining the Close
error is discarded intentionally.

**Decision:** Use `//nolint:errcheck // rows.Close error is non-actionable` for `defer rows.Close()` patterns since the query already succeeded and the error from Close is not useful.

### 6b. errcheck — conn.Close() / db.Close() (8 instances)

**Files:** `internal/engine/information_schema.go` (3 conn.Close), `internal/db/sqlite.go` (2 db.Close), `cmd/server/main.go` (3 defer db.Close)

For `main.go` defers: already restructured in Phase 2 (exitAfterDefer). The helper function
can check close errors. For others, apply `//nolint:errcheck` since Close on shutdown is best-effort.

### 6c. errcheck — json.Unmarshal (4 instances)

**File:** `internal/db/mapper/domain_dbstore.go`

These must be checked — unmarshal failures are real bugs:
```go
if err := json.Unmarshal([]byte(s), &result); err != nil {
    return nil, fmt.Errorf("unmarshal properties: %w", err)
}
```

### 6d. errcheck — remaining production items

- `json.NewEncoder(w).Encode()` in `cmd/server/main.go` auth.go → already handled with `_ =`
- `f.Close()` + `os.Setenv()` in config.go → check errors
- `fmt.Fprint` in main.go → already suppressed or suppress with `_ =`

### 6e. rowserrcheck — production code (2 instances)

**Files:** `internal/service/catalog.go` (1), `internal/db/repository/search.go` (1)

Add `if err := rows.Err(); err != nil { return ..., err }` after every `for rows.Next()` loop.

### 6f. noctx — production code (0 issues — all in tests)

No production noctx issues.

**Files:** `internal/service/catalog.go`, `internal/db/repository/introspection.go`, `internal/db/repository/search.go`, `internal/engine/engine.go`, `internal/engine/information_schema.go`, `internal/service/manifest.go`, `internal/service/query.go`, `internal/db/sqlite.go`, `cmd/server/main.go`, `internal/config/config.go`, `internal/db/mapper/domain_dbstore.go`
**Estimated issues resolved:** ~30 production errcheck + 2 rowserrcheck = ~32
**Risk:** Medium — json.Unmarshal changes alter function signatures. Test thoroughly.

---

## Phase 7: Test Code Fixes (~270 issues)

**Goal:** Fix errcheck, rowserrcheck, noctx, and testifylint in test files.

This is the largest phase by issue count. Use batch patterns.

### 7a. testifylint — Fix assertion patterns (~42 issues)

**Files:** `internal/api/handler_test.go`, `internal/engine/engine_test.go`, `internal/db/repository/*_test.go`, `internal/service/*_test.go`, `test/integration/*_test.go`

Patterns to apply:

1. `assert.True(t, errors.As(err, &x))` → `require.ErrorAs(t, err, &x)` (~21 instances)
2. `assert.True(t, strings.Contains(s, sub))` → `assert.Contains(t, s, sub)` (~6 instances)
3. `assert.True(t, cond)` with a specific bool → use more specific assertion (~15 instances)

### 7b. errcheck — resp.Body.Close() (~50 issues)

**Files:** `internal/api/handler_test.go`, `test/integration/*_test.go`

Pattern — add `defer` with nolint:
```go
resp, err := http.Get(url)
require.NoError(t, err)
defer resp.Body.Close() //nolint:errcheck // test cleanup
```

Alternative (if preferred): create a test helper:
```go
func closeBody(t *testing.T, resp *http.Response) {
    t.Helper()
    require.NoError(t, resp.Body.Close())
}
```
Then use `defer closeBody(t, resp)` throughout.

### 7c. errcheck — db.Close() / duckDB.Close() in tests (~15 issues)

**Files:** `internal/db/sqlite_test.go`, `internal/engine/engine_test.go`, `test/integration/*_test.go`

Add `t.Cleanup(func() { require.NoError(t, db.Close()) })` or use `//nolint:errcheck`.

### 7d. errcheck — unchecked setup calls in tests (~35 issues)

**Files:** `cmd/server/main.go` (seedCatalog), `internal/api/handler_test.go`, `test/integration/*_test.go`

Calls like `q.CreatePrincipal()`, `q.GrantPrivilege()`, `q.AddGroupMember()`, `q.BindRowFilter()`, `q.BindColumnMask()` — results discarded.

Fix: check errors with `require.NoError(t, err)` for setup operations:
```go
_, err := q.CreatePrincipal(ctx, params)
require.NoError(t, err)
```

### 7e. noctx — test code (~29 issues)

**Files:** `internal/db/sqlite_test.go` (15), `internal/engine/engine_test.go` (2), `internal/service/authorization_test.go` (1), `internal/api/handler_test.go` (11)

Patterns:
1. `db.Exec(...)` → `db.ExecContext(ctx, ...)` (and `db.Query` → `db.QueryContext`)
2. `http.Get(url)` → `http.NewRequestWithContext(ctx, http.MethodGet, url, nil)` + `http.DefaultClient.Do(req)`
3. `http.Post(url, ...)` → `http.NewRequestWithContext(ctx, http.MethodPost, url, body)` + `http.DefaultClient.Do(req)`

For test files, create a helper if many HTTP calls need context:
```go
func httpGet(t *testing.T, url string) *http.Response {
    t.Helper()
    ctx := context.Background()
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
    require.NoError(t, err)
    resp, err := http.DefaultClient.Do(req)
    require.NoError(t, err)
    return resp
}
```

### 7f. rowserrcheck — test code (~23 issues)

**Files:** `internal/db/sqlite_test.go`, `internal/engine/engine_test.go`, `internal/engine/information_schema_test.go`, `test/integration/*_test.go`

Add `require.NoError(t, rows.Err())` after every `for rows.Next()` loop in tests.

### 7g. errcheck — json.Decode in tests (2 issues)

Check the error: `require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))`.

### 7h. errcheck — http.Post discarded result (1 issue)

Check the return: `resp, err := http.Post(...); require.NoError(t, err)`.

**Files:** All `*_test.go` files
**Estimated issues resolved:** ~270
**Risk:** Low — test-only changes. Run `task test` and `task integration-test` (if possible).

---

## Phase 8: Final Verification & Cleanup

### 8a. Run full lint check
```bash
task lint
```
Expect 0 issues.

### 8b. Run full test suite
```bash
task test
task build
```

### 8c. Clean up any remaining issues

If any issues remain (miscounted, edge cases), fix them individually.

### 8d. Remove any temporary nolint comments that are no longer needed

Run:
```bash
golangci-lint run --enable nolintlint
```
to verify all `//nolint` comments are still justified.

---

## Phase Summary

| Phase | Description | Est. Issues | Key Files | Risk |
|-------|------------|-------------|-----------|------|
| 1 | Config & suppressions | ~87 | `.golangci.yml`, `main.go`, `config.go`, `information_schema.go` | None |
| 2 | Dead code & mechanical fixes | ~11 | `main.go`, `handler.go`, `builder.go`, `engine.go`, `sqlrewrite.go`, `domain_dbstore.go` | Low |
| 3 | errorlint (errors.As/Is) | ~39 | `handler.go`, `catalog.go`, repository files | Medium |
| 4 | nilerr | ~5 | `handler.go` | Low |
| 5 | revive (docs + rename) | ~115 | ~20 files across all packages | Low |
| 6 | Production errcheck + rowserrcheck | ~32 | ~11 production files | Medium |
| 7 | Test errcheck + noctx + testifylint + rowserrcheck | ~270 | All `*_test.go` + integration tests | Low |
| 8 | Final verification | ~0 | — | None |
| **Total** | | **~580** | | |

## Execution Notes

1. **Do phases in order.** Phase 3 may resolve Phase 4 (nilerr) as a side-effect.
2. **Phase 7 is largest** (~270 issues) but lowest risk — all test code. Consider splitting
   into sub-PRs by test file or package.
3. **Phase 5 (revive)** is second-largest (~115 issues) but entirely additive (comments).
4. **The APIHandler → Handler rename** (Phase 5d) only touches `handler.go` — verified zero
   external references. `NewHandler` return type changes from `*APIHandler` to `*Handler`,
   but callers in `main.go` use type inference (`handler := api.NewHandler(...)`).
5. **main.go exitAfterDefer** (Phase 2b) is the most structural change: extract `run() error`
   from `main()`. This also fixes the `log.Fatalf` (depguard) pattern.
6. **Run `task test` after every phase.** Run `task lint` after every phase to track progress.
