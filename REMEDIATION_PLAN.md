# Code Remediation Plan — duck-demo

**Generated:** 2026-02-11
**Scope:** 48 code smells across security, architecture, correctness, and maintainability

---

## How to Read This Plan

- **Effort:** S = hours, M = 1-2 days, L = 3-5 days
- **Deps** column lists items that should/must be completed first
- Items within a group can generally be done in parallel unless noted
- Test coverage for each fix is included in the effort estimate

---

## Tier 1: CRITICAL / SECURITY

These issues enable data leakage, privilege bypass, or incorrect HTTP security semantics. Fix before any production exposure.

### Group 1A: SQL Rewrite Security Holes

These three issues are in the same file and form the core security enforcement path. Fix together as a single PR.

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 1 | `SELECT *` bypasses column masks | `internal/sqlrewrite/sqlrewrite.go:322-347` | M | — |
| 3 | Multi-statement SQL bypasses classification | `internal/sqlrewrite/sqlrewrite.go:120` | S | — |
| 16 | Row filter / column mask expressions injected without validation | `internal/sqlrewrite/sqlrewrite.go:177-178, 338-341` | M | — |
| 17 | Unparseable mask expression silently skipped → unmasked column | `internal/sqlrewrite/sqlrewrite.go:341` | S | — |

**Actionable steps:**

1. **`SELECT *` mask bypass (#1):** In `applyMasksToSelectStmt`, detect `A_Star` nodes in the target list. When found, query the table's column list (pass column names as a new parameter), expand `*` into explicit `ColumnRef` nodes, then apply masks to each. Alternatively, reject `SELECT *` with an error when masks are active for the table.

2. **Multi-statement bypass (#3):** In `ClassifyStatement`, reject input when `len(result.Stmts) > 1` — return an error like `"multi-statement queries are not allowed"`. Apply the same check in `ExtractTableNames` and `ExtractTargetTable` (lines 146-150 have the same pattern). Add a dedicated `EnsureSingleStatement(sql) error` helper used by all entry points.

3. **Unvalidated filter/mask expressions (#16):** Before calling `pg_query.Parse("SELECT 1 WHERE " + filterSQL)`, validate that `filterSQL` does not contain semicolons or subqueries (defense-in-depth). Better: parse the expression and walk the AST to reject `SubLink` nodes, DDL, etc. Apply similar validation for mask expressions at line 339.

4. **Silent mask skip (#17):** Change the `continue` at line 341 to return an error: `return "", fmt.Errorf("unparseable mask expression for column %q: %w", colName, err)`. A failed mask must be a hard failure, not a silent pass-through.

5. **Tests:** Add test cases for `SELECT * FROM t` with masks, `SELECT 1; DROP TABLE foo`, expressions with embedded SQL injection attempts, and malformed mask expressions. These are specifically called out as missing in #42.

### Group 1B: C++ Extension Cache Isolation

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 2 | Global manifest cache not per-user | `extension/duck_access/src/duck_access_manifest.hpp:55-56` | M | — |

**Actionable steps:**

1. Change the cache key from `schema.table` to `user:schema.table` (or a composite struct key).
2. Alternatively, add a `user` field to the cache entry and validate on lookup.
3. Add TTL-based eviction if not already present.
4. Test with two users querying the same table with different RLS policies — assert different manifest content.

### Group 1C: Error Handling — Security Semantics

These all affect HTTP status codes returned to clients. Incorrect codes leak information or misrepresent authorization state.

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 4 | `switch err.(type)` doesn't handle wrapped errors; `HTTPStatusFromDomainError` unused | `internal/api/handler.go` (throughout) | M | #30 |
| 5 | Engine access denied returns `fmt.Errorf` instead of `domain.AccessDeniedError` | `internal/engine/engine.go:92` | S | — |
| 6 | `ExecuteQuery` always returns 403 on any error | `internal/api/handler.go:96-98` | S | #5, #30 |
| 7 | `DeleteSchema` maps `ConflictError` to 403 | `internal/api/handler.go:651-652` | S | #4 |
| 12 | `mapDBError` uses `==` instead of `errors.Is` | `internal/db/repository/helpers.go:21` | S | — |
| 13 | `APIKeyRepo` returns raw `sql.ErrNoRows` | `internal/db/repository/api_key.go:19-25` | S | #12 |
| 30 | `HTTPStatusFromDomainError` exists but unused | `internal/db/mapper/domain_api.go:11-29` | — | — |

**Actionable steps:**

1. **Fix `mapDBError` (#12):** Change `err == sql.ErrNoRows` to `errors.Is(err, sql.ErrNoRows)` in `internal/db/repository/helpers.go:21`. Grep for all 11 occurrences of the same pattern across the repository package and fix them.

2. **Fix `APIKeyRepo` (#13):** Wrap the error at `api_key.go:22` through `mapDBError` so `sql.ErrNoRows` becomes `domain.NotFoundError`. The auth middleware can then distinguish 401 (not found = invalid key) from 500.

3. **Fix engine access denied (#5):** At `engine.go:92`, change:
   ```go
   return nil, fmt.Errorf("access denied: ...")
   ```
   to:
   ```go
   return nil, domain.ErrAccessDenied("principal %q lacks %s on table %q", ...)
   ```

4. **Fix `ExecuteQuery` (#6):** Replace the blanket 403 response with a `switch` (or `HTTPStatusFromDomainError`) that maps `AccessDeniedError` → 403, `ValidationError` → 400, parse errors → 400, and unknown → 500.

5. **Fix `DeleteSchema` (#7):** Change line 651 from `DeleteSchema403JSONResponse` to `DeleteSchema409JSONResponse` for `ConflictError`.

6. **Replace all `switch err.(type)` blocks (#4, #21, #30):** Adopt the existing `HTTPStatusFromDomainError` from `internal/db/mapper/domain_api.go`. Create a small helper in the `api` package that maps domain errors to the correct typed response object. Replace all ~20 duplicated switch blocks. This is the largest single change in this group.

### Group 1D: Data Integrity & Audit Correctness

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 8 | Hardcoded insecure JWT secret and encryption key | `internal/config/config.go:64-68` | S | — |
| 9 | "system" hardcoded as audit principal | `internal/service/grant.go:27`, `row_filter.go:28`, `column_mask.go:29` | S | #18 |
| 10 | Missing transactions on cascade deletes | `internal/db/repository/catalog.go:217-301, 451-508` | M | — |
| 11 | Manual JSON construction bug | `internal/db/mapper/domain_dbstore.go:268-270` | S | — |
| 14 | 19 silenced governance cascade errors | `internal/db/repository/catalog.go:260-298, 476-505` | M | #10 |
| 15 | `information_schema` case-incomplete replacement | `internal/engine/information_schema.go:167-169` | S | — |

**Actionable steps:**

1. **Hardcoded secrets (#8):** Add a `log.Println("WARNING: using default JWT secret — set JWT_SECRET for production")` (and similar for encryption key) when the defaults are applied. Better: in a future iteration, require these in non-dev mode via a `MODE=production` env var check.

2. **Audit principal (#9):** These services need the actual principal name. This is tied to #18 (service imports middleware). Once principal is passed as an explicit parameter (Group 2A), use it in the audit call instead of `"system"`. Short-term fix: accept `principalName string` as an additional parameter to `Grant`, `Revoke`, `Create`, `Delete`, etc. in the 3 affected service files.

3. **Transaction wrapping (#10):** Wrap the cascade delete block in `DeleteSchema` (lines 259-300) and `DeleteTable` (lines 476-505) in a `db.BeginTx` / `tx.Commit` pair. The sqlc `Queries` type supports `WithTx(tx)` — use it.

4. **Fix JSON construction (#11):** Replace the string concatenation at `domain_dbstore.go:268-270`:
   ```go
   tablesJSON = `["` + strings.Join(e.TablesAccessed, `","`) + `"]`
   ```
   with:
   ```go
   b, _ := json.Marshal(e.TablesAccessed)
   tablesJSON = string(b)
   ```

5. **Handle cascade errors (#14):** After wrapping in a transaction (#10), check each `DeleteXxx` error. If any fails, roll back the transaction and return the error. The DDL has already succeeded at this point, so consider: (a) logging errors but continuing (current behavior, made explicit), or (b) treating governance cleanup failures as hard errors. Recommendation: log + continue for soft-delete/cleanup, but return error for critical governance (row filters, column masks).

6. **Case-insensitive replacement (#15):** Replace the two `strings.Replace` calls at `information_schema.go:167-169` with a single case-insensitive replacement using `strings.EqualFold` or `regexp.MustCompile("(?i)information_schema\\." + regexp.QuoteMeta(table))`.

---

## Tier 2: HIGH PRIORITY

Architectural issues that compound tech debt and make the codebase harder to change safely.

### Group 2A: Service Layer Architecture Cleanup

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 18 | Service layer imports middleware package | 6 service files | M | — |
| 9 | "system" audit principal (also in Tier 1) | 3 service files | S | #18 |
| 27 | Service directly executes raw SQL | `internal/service/manifest.go:175-217`, `ingestion.go:261-269` | M | — |
| 28 | `ExternalLocationService` mixes 4 concerns | `internal/service/external_location.go` | L | — |

**Actionable steps:**

1. **Remove middleware import (#18):** Change service method signatures from `Method(ctx context.Context, ...)` to `Method(ctx context.Context, principalName string, ...)` where the principal is currently extracted via `middleware.PrincipalFromContext(ctx)`. The API handler (which already has the principal from the middleware) passes it down. This simultaneously fixes #9 since the principal name is now available for audit logging.

2. **Move raw SQL to repository (#27):** Extract the raw DuckDB queries in `manifest.go:175-217` and `ingestion.go:261-269` into new repository methods. The service should only call repository interfaces.

3. **Split `ExternalLocationService` (#28):** Factor into: (a) `ExternalLocationCRUD` — basic CRUD, (b) `StorageSecretManager` — DuckDB secret lifecycle, (c) `CatalogAttacher` — DuckLake attach/detach. The authz checks remain in the service coordinator that composes these.

### Group 2B: Handler Refactoring

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 19 | `APIHandler` god object (1810 lines, 18 deps) | `internal/api/handler.go` | L | #20 |
| 20 | `NewHandler` takes 18 parameters | `internal/api/handler.go:34-53` | S | — |
| 21 | ~20 duplicated error handling switches | `internal/api/handler.go` | M | #4 (Tier 1) |

**Actionable steps:**

1. **Options struct (#20):** Replace the 18-parameter constructor with:
   ```go
   type HandlerDeps struct {
       Query         *service.QueryService
       Principals    *service.PrincipalService
       // ...
   }
   func NewHandler(deps HandlerDeps) *APIHandler { ... }
   ```

2. **Split handler (#19):** After #20, split `handler.go` into domain-specific files: `handler_principals.go`, `handler_catalog.go`, `handler_governance.go`, etc. Each file contains the methods for one API domain. The `APIHandler` struct stays unified (required by the generated interface), but the code is physically separated.

3. **Centralized error mapping (#21):** This is done as part of Tier 1 Group 1C (#4). Ensure the helper is used in all new split files.

### Group 2C: Code Duplication

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 22 | Duplicated `logAudit` helper (6 copies) | 6 service files | S | — |
| 23 | Duplicated `requireCatalogAdmin` (2 copies) | `storage_credential.go`, `external_location.go` | S | — |
| 24 | Duplicated `resolvePresigner` (2 copies) | `manifest.go`, `ingestion.go` | S | — |
| 25 | Duplicated privilege-check pattern (8 copies) | `catalog.go` | M | — |

**Actionable steps:**

1. **`logAudit` (#22):** Extract into `internal/service/audit_helper.go`:
   ```go
   func logAudit(ctx context.Context, repo domain.AuditRepository, principalName, action, status string) {
       _ = repo.Insert(ctx, &domain.AuditEntry{...})
   }
   ```

2. **`requireCatalogAdmin` (#23):** Move to a shared `internal/service/authz.go` file.

3. **`resolvePresigner` (#24):** Move to a shared `internal/service/presigner.go` or into the `ExternalLocationService` refactor (#28).

4. **Privilege-check pattern (#25):** Extract a `checkTablePrivilege(ctx, principalName, schemaName, tableName, priv) error` helper in `catalog.go`.

### Group 2D: Dead Code & Performance

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 26 | N+1 queries (5 locations) | `internal/db/repository/catalog.go` | L | — |
| 29 | Unused `RLSRule` struct and `RewriteQuery` function | `internal/sqlrewrite/sqlrewrite.go:27-78` | S | — |

**Actionable steps:**

1. **N+1 queries (#26):** For `ListSchemas` and `ListTables`, add batch-loading queries that fetch metadata/tags/statistics in one query per type (using `WHERE id IN (?)`) and join in Go. For `DeleteSchema` cascade, batch the governance deletes. This is the highest-effort item in Tier 2.

2. **Dead code (#29):** Remove `RLSRule`, `RewriteQuery`, and related helper functions (`buildRuleExpr`, `operatorToSQL`, etc.) if they are only exercised by tests. If the old `RewriteQuery` API is kept for backward compat, mark it deprecated with a comment and removal date.

---

## Tier 3: MEDIUM PRIORITY

Correctness and maintainability issues that don't directly cause security problems.

### Group 3A: Magic Values & Constants

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 31 | Magic numbers (page sizes, expiry, name length, catalog name, bucket) | 5+ files | M | — |
| 33 | Time format string repeated 14 times | mapper + repository files | S | — |

**Actionable steps:**

1. Create `internal/domain/constants.go` with:
   ```go
   const (
       DefaultPageSize    = 1000
       MaxPageSize        = 10000
       PresignedURLExpiry = 1 * time.Hour
       MaxNameLength      = 128
       CatalogName        = "lake"
       DefaultBucket      = "duck-demo"  // or move to config
       TimeFormat         = "2006-01-02 15:04:05"
   )
   ```
2. Replace all raw occurrences. The `TimeFormat` constant may already exist in the mapper package — if so, re-export from `domain` and use it everywhere.

### Group 3B: Error Handling Gaps

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 34 | Silently discarded `time.Parse` / `json.Unmarshal` errors (14+) | mapper + repository | M | — |
| 40 | Missing `rows.Err()` checks | `catalog.go`, `search.go` | S | — |

**Actionable steps:**

1. **`time.Parse` / `json.Unmarshal` (#34):** Grep for `_ =` and `_ :=` patterns near `time.Parse` and `json.Unmarshal`. Return zero values with the error propagated, or log a warning. In mapper functions that return structs (not errors), either add an error return or use a `mustParse` helper that logs and returns a zero time.

2. **`rows.Err()` (#40):** After every `for rows.Next() { ... }` loop, add `if err := rows.Err(); err != nil { return ..., err }`. There are at least 2 locations in `catalog.go` (line ~236) and `search.go`.

### Group 3C: Transaction Safety

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 36 | Read-modify-write without transactions | `ViewRepo.Update`, `StorageCredentialRepo.Update`, `ExternalLocationRepo.Update` | M | — |

**Actionable steps:**

1. For each `Update` method, wrap the read (to check existence/get current state) and the write in a single `BeginTx` / `Commit` block.
2. Use `sqlc`'s `WithTx` pattern consistently.

### Group 3D: Utility Deduplication

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 32 | Duplicated ptr/null utility functions (7+ copies) | mapper + repository | S | — |
| 35 | Duplicated pagination count-then-list pattern (12 repos) | repository package | M | — |
| 41 | Inconsistent null ↔ empty string handling | group, row_filter, column_mask repos | S | — |

**Actionable steps:**

1. **Utility functions (#32):** Create `internal/db/mapper/convert.go` with canonical `PtrToStr`, `StrToPtr`, `BoolToInt64`, `NullInt64`, `NullStr` helpers. Remove duplicates.

2. **Pagination (#35):** Extract a generic helper:
   ```go
   func PaginatedQuery[T any](ctx context.Context, countFn func() (int64, error), listFn func() ([]T, error)) ([]T, int64, error)
   ```
   Or, if generics feel premature, at least extract the offset/limit calculation.

3. **Null handling (#41):** Decide on a convention: empty string = NULL or empty string = `""`. Document in `AGENTS.md` and fix the 3 repositories to be consistent.

### Group 3E: Code Complexity

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 37 | `SearchRepo.Search` is 171 lines with `fmt.Sprintf` for LIMIT/OFFSET | `internal/db/repository/search.go` | M | — |
| 38 | Deep nesting (4-6 levels) | `ingestion.go`, `manifest.go`, `auth.go` | M | — |
| 39 | `seedCatalog` and `main` are 174/235 lines | `cmd/server/main.go` | M | — |

**Actionable steps:**

1. **Search (#37):** Parameterize LIMIT/OFFSET with `?` placeholders instead of `fmt.Sprintf`. Break the UNION ALL construction into a builder helper.

2. **Deep nesting (#38):** Apply early-return pattern. Extract inner logic into named functions (e.g., `resolvePresignerForLocation` instead of a 6-level nested block).

3. **Main (#39):** Extract `seedCatalog` into `internal/seed/seed.go`. Extract DuckDB setup into a `setup.go` file within `cmd/server/`.

### Group 3F: Test Quality

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 42 | Engine tests: no testify, not table-driven, duplicate setup, discarded errors | engine test files | M | — |

**Actionable steps:**

1. Refactor engine tests to use `require`/`assert` from testify.
2. Convert to table-driven with `t.Run()` subtests.
3. Extract shared setup into a `setupTestEngine(t)` helper.
4. Replace `_ =` with proper error assertions.
5. Add the missing test cases for `SELECT *` with masks and multi-statement SQL (coordinate with Tier 1 Group 1A fixes).

---

## Tier 4: LOW PRIORITY

Polish items. Address opportunistically or during adjacent work.

| # | Issue | File | Effort | Deps |
|---|-------|------|--------|------|
| 43 | Missing doc comments on service types | 5 service files | S | — |
| 44 | Inconsistent naming (`Get` vs `GetByName` vs `GetByID`) | repositories | S | — |
| 45 | Redundant `dbstore.Queries` allocations | repository constructors | S | — |
| 46 | Re-exported domain constants without migration timeline | `authorization.go` | S | — |
| 47 | Temp table accumulation in long-lived DuckDB connections | `information_schema.go` | S | — |
| 48 | C++ extension: no TLS verification, minimal tests, static init risk | `extension/duck_access/` | L | — |

**Actionable steps:**

1. **Doc comments (#43):** Add `// <TypeName> provides ...` doc comments to the 5 service types during any adjacent edit.
2. **Naming (#44):** Adopt `GetByID` / `GetByName` consistently. Rename in a single pass when touching repositories.
3. **Queries allocations (#45):** Share a single `*dbstore.Queries` instance where multiple repos use the same `*sql.DB`. Pass via constructor.
4. **Re-exported constants (#46):** Add `// Deprecated: use domain.PrivSelect directly. Remove by v2.0.` comments and grep for usages.
5. **Temp tables (#47):** Add cleanup after `rows.Close()` using a `defer` or periodic cleanup goroutine.
6. **C++ extension (#48):** Add TLS verification flag, expand test coverage, audit static initialization order. This is a separate workstream.

---

## Recommended Execution Order

```
Week 1:  Tier 1 Group 1A (SQL rewrite security) — blocks production readiness
         Tier 1 Group 1C (error semantics: #5, #12, #13 first, then #4/#6/#7/#30)
         Tier 1 Group 1D (#8, #11 — quick wins)

Week 2:  Tier 1 Group 1B (C++ cache isolation)
         Tier 1 Group 1D (#10, #14 — transaction wrapping)
         Tier 1 Group 1D (#9, #15)

Week 3:  Tier 2 Group 2A (#18 — service architecture, unblocks #9 proper fix)
         Tier 2 Group 2C (#22-25 — deduplication, quick wins)

Week 4:  Tier 2 Group 2B (#20, #19 — handler refactoring)
         Tier 2 Group 2D (#29 dead code, #26 N+1 started)

Week 5+: Tier 3 items by group, interleaved with Tier 4 opportunistically
         Tier 2 #26 (N+1 queries, can be done incrementally)
```

---

## Dependency Graph (Critical Path)

```
#12 (errors.Is) ──→ #13 (APIKeyRepo)
#5  (domain error) ─→ #6 (ExecuteQuery error mapping)
#30 (use existing mapper) ─→ #4 (replace all switch blocks) ─→ #21 (dedup)
                                                              ─→ #7  (ConflictError mapping)
#18 (remove middleware import) ─→ #9 (audit principal)
#10 (transaction wrapping) ─→ #14 (cascade error handling)
#1, #3, #16, #17 (SQL rewrite) ─→ #42 (test coverage for those cases)
```
