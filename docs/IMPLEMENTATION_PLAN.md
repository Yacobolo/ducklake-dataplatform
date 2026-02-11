# Implementation Plan: 6 Features for duck-demo

## Codebase Conventions Reference

Before diving into each feature, these are the patterns every implementation must follow:

- **Migration numbering**: Next available is `010_*.sql` (current max: 009)
- **Domain types**: Structs in `internal/domain/`, interfaces in `internal/domain/repository.go`
- **sqlc queries**: `internal/db/queries/*.sql` → `task sqlc` → `internal/db/dbstore/*.sql.go`
- **Mappers**: DB↔Domain in `internal/db/mapper/domain_dbstore.go`, Domain→API in `internal/api/handler.go` (inline helpers)
- **Repository**: `internal/db/repository/*.go`, constructor `NewXxxRepo(db *sql.DB)`
- **Service**: `internal/service/*.go`, constructor `NewXxxService(deps...)`
- **API Handler**: `internal/api/handler.go` — `APIHandler` struct with service fields, implements `StrictServerInterface`
- **OpenAPI**: `internal/api/openapi.yaml` → `task generate-api` → `types.gen.go` + `server.gen.go`
- **Pagination**: `domain.PageRequest` + `domain.NextPageToken()`, API params `max_results`/`page_token`
- **Auth**: `middleware.PrincipalFromContext(ctx)` returns the principal name
- **Audit**: Best-effort `_ = s.audit.Insert(ctx, entry)`
- **Errors**: `domain.ErrNotFound()`, `ErrAccessDenied()`, `ErrValidation()`, `ErrConflict()`
- **Tests**: Table-driven with `t.Run()`, testify `require`/`assert`, real SQLite via `t.TempDir()`, `mock*` repos for handler tests

---

## Feature 1: Data Lineage

### Goal
Track table-level lineage from query execution. Build a graph of which tables feed into other tables, derived from executed SQL.

### Design Decisions & Tradeoffs

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage | SQLite `lineage_edges` table | Consistent with rest of metastore; lineage is metadata |
| Edge type | Enum string: `READ`, `WRITE`, `READ_WRITE` | Simple, extensible; `READ` = SELECT sources, `WRITE` = INSERT/CTAS target |
| Lineage capture point | After successful query execution in `QueryService.Execute()` | Single integration point, already has statement type + tables info |
| Source table extraction | Reuse `sqlrewrite.ExtractTableNames()` + new `ExtractTargetTable()` | Leverage existing parser; add INSERT/CTAS target extraction |
| Target for pure SELECT | `NULL` target | No write target exists; these are read-only lineage edges |
| API auth | Any authenticated user can read lineage | Lineage is metadata, not data. Filter what the user can see based on catalog privileges in a future iteration. |

### Files to Create

| File | Purpose |
|------|---------|
| `internal/db/migrations/010_create_lineage_edges.sql` | Migration: `lineage_edges` table |
| `internal/db/queries/lineage.sql` | sqlc queries for lineage CRUD |
| `internal/db/repository/lineage.go` | `LineageRepo` implementing `LineageRepository` |
| `internal/service/lineage.go` | `LineageService` with capture + query methods |

### Files to Modify

| File | Changes |
|------|---------|
| `internal/domain/lineage.go` **(new)** | `LineageEdge` struct, `LineageEdgeType` constants |
| `internal/domain/repository.go` | Add `LineageRepository` interface |
| `internal/db/mapper/domain_dbstore.go` | Add `LineageEdgeFromDB` / `LineageEdgeToDBParams` mappers |
| `internal/sqlrewrite/sqlrewrite.go` | Add `ExtractTargetTable(sql) (string, error)` — extracts INSERT/CTAS target |
| `internal/service/query.go` | After successful execution, call `lineage.RecordEdges(...)` |
| `internal/api/openapi.yaml` | Add lineage schemas + 3 endpoints |
| `internal/api/handler.go` | Add `lineage *service.LineageService` field + 3 handler methods + `lineageEdgeToAPI()` mapper |
| `cmd/server/main.go` (or composition root) | Wire `LineageRepo` → `LineageService` → `APIHandler` |

### Implementation Order

1. **Migration** (`010_create_lineage_edges.sql`):
   ```sql
   -- +goose Up
   CREATE TABLE lineage_edges (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       source_table TEXT,          -- NULL for pure writes with no source
       target_table TEXT,          -- NULL for pure reads (SELECT)
       edge_type TEXT NOT NULL,    -- 'READ', 'WRITE', 'READ_WRITE'
       query_id INTEGER,           -- FK to audit_log.id (nullable for manual edges)
       principal_name TEXT NOT NULL,
       created_at TEXT NOT NULL DEFAULT (datetime('now'))
   );
   CREATE INDEX idx_lineage_source ON lineage_edges(source_table);
   CREATE INDEX idx_lineage_target ON lineage_edges(target_table);
   CREATE INDEX idx_lineage_created ON lineage_edges(created_at);
   ```

2. **Domain types** (`internal/domain/lineage.go`):
   ```go
   type LineageEdge struct {
       ID            int64
       SourceTable   *string  // NULL for pure writes
       TargetTable   *string  // NULL for pure reads (SELECT)
       EdgeType      string   // "READ", "WRITE", "READ_WRITE"
       QueryID       *int64   // optional link to audit_log
       PrincipalName string
       CreatedAt     time.Time
   }
   ```

3. **Domain interface** (add to `repository.go`):
   ```go
   type LineageRepository interface {
       InsertEdge(ctx context.Context, e *LineageEdge) error
       InsertEdges(ctx context.Context, edges []LineageEdge) error
       GetUpstream(ctx context.Context, tableName string, page PageRequest) ([]LineageEdge, int64, error)
       GetDownstream(ctx context.Context, tableName string, page PageRequest) ([]LineageEdge, int64, error)
       GetLineageForTable(ctx context.Context, tableName string, page PageRequest) ([]LineageEdge, int64, error)
   }
   ```

4. **sqlc queries** (`internal/db/queries/lineage.sql`):
   - `InsertLineageEdge` — `:exec`
   - `ListUpstreamEdges` — `:many` — `WHERE target_table = ?`
   - `ListDownstreamEdges` — `:many` — `WHERE source_table = ?`
   - `ListLineageForTable` — `:many` — `WHERE source_table = ? OR target_table = ?`
   - `CountUpstreamEdges`, `CountDownstreamEdges`, `CountLineageForTable` — `:one`

5. **Run `task sqlc`** to generate dbstore code.

6. **Mapper** (`domain_dbstore.go`): Add `LineageEdgeFromDB()` and `LineageEdgeToDBParams()`.

7. **Repository** (`internal/db/repository/lineage.go`): Implement `LineageRepository` using generated queries.

8. **sqlrewrite enhancement**: Add `ExtractTargetTable(sql string) (*string, error)`:
   - Parse with pg_query_go
   - For `InsertStmt`: return `Relation.Relname`
   - For `CreateStmt` with `AS SELECT` (CTAS): return `Relation.Relname`
   - For `SelectStmt`: return `nil`
   - This function already has the AST walking patterns in `collectTablesFromNode`

9. **Service** (`internal/service/lineage.go`):
   ```go
   type LineageService struct {
       repo  domain.LineageRepository
       audit domain.AuditRepository
   }

   func (s *LineageService) RecordEdgesFromQuery(ctx context.Context, principalName string, stmtType sqlrewrite.StatementType, tables []string, targetTable *string) {
       // Build edges based on statement type
       // For SELECT: each table is a source, target is nil, edge_type=READ
       // For INSERT/UPDATE/DELETE: target is the DML target, sources are other tables, edge_type=WRITE
   }

   func (s *LineageService) GetUpstream(ctx, tableName, page) ...
   func (s *LineageService) GetDownstream(ctx, tableName, page) ...
   func (s *LineageService) GetLineageForTable(ctx, tableName, page) ...
   ```

10. **Integrate into `QueryService.Execute()`**: After successful execution, call lineage service. Pass statement type, extracted tables, and target table. Best-effort (don't fail the query).

11. **OpenAPI**: Add schemas (`LineageEdge`, `PaginatedLineageEdges`) and endpoints:
    - `GET /v1/lineage/tables/{tableName}` → `GetLineageForTable`
    - `GET /v1/lineage/tables/{tableName}/upstream` → `GetUpstreamLineage`
    - `GET /v1/lineage/tables/{tableName}/downstream` → `GetDownstreamLineage`
    All with `max_results`/`page_token` params.

12. **Run `task generate-api`**, then implement handler methods.

13. **Wire in composition root**: Instantiate `LineageRepo`, `LineageService`, pass to `QueryService` and `APIHandler`.

### Testing Strategy

| Test | Type | Location |
|------|------|----------|
| `TestExtractTargetTable` | Unit | `internal/sqlrewrite/sqlrewrite_test.go` — table-driven: INSERT INTO, CTAS, plain SELECT, UPDATE, DELETE |
| `TestLineageRepo_InsertAndQuery` | Integration (real SQLite) | `internal/db/repository/lineage_test.go` — insert edges, query upstream/downstream, pagination |
| `TestLineageService_RecordEdges` | Unit | `internal/service/lineage_test.go` — mock repo, verify correct edges for different statement types |
| `TestAPI_Lineage` | Handler test | `internal/api/handler_test.go` — execute queries, then verify lineage endpoints return expected edges |

---

## Feature 2: Search API

### Goal
Full-text search across schemas, tables, and columns by name, comment, and properties.

### Design Decisions & Tradeoffs

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Search backend | LIKE queries (Option B) | Simpler to implement; catalog is small-medium size; can upgrade to FTS5 later |
| Query approach | Raw SQL in repository (not sqlc) | The search query JOINs DuckLake tables (not managed by sqlc migrations) with `catalog_metadata`; complex dynamic filtering is easier with raw SQL |
| Result types | Unified `SearchResult` with type discriminator | Single endpoint, consistent pagination, easy to extend |
| Scoring | Position of match: name match > comment match > property match | Simple priority-based, deterministic |
| Scope | Search across all DuckLake schemas/tables/columns + catalog_metadata | Comprehensive catalog search |

### Files to Create

| File | Purpose |
|------|---------|
| `internal/domain/search.go` | `SearchResult` struct |
| `internal/db/repository/search.go` | `SearchRepo` with raw SQL queries |
| `internal/service/search.go` | `SearchService` |

### Files to Modify

| File | Changes |
|------|---------|
| `internal/domain/repository.go` | Add `SearchRepository` interface |
| `internal/api/openapi.yaml` | Add `SearchResult` schema + `GET /v1/search` endpoint |
| `internal/api/handler.go` | Add `search *service.SearchService` + `Search` handler + `searchResultToAPI()` |
| Composition root | Wire `SearchRepo` → `SearchService` → `APIHandler` |

### Implementation Order

1. **Domain types** (`internal/domain/search.go`):
   ```go
   type SearchResult struct {
       Type       string  // "schema", "table", "column"
       Name       string  // the object name
       SchemaName string  // always populated
       TableName  string  // populated for table/column
       ColumnName string  // populated for column
       Comment    string  // if matched
       MatchField string  // "name", "comment", "property"
   }
   ```

2. **Domain interface** (add to `repository.go`):
   ```go
   type SearchRepository interface {
       Search(ctx context.Context, query string, objectType *string, page PageRequest) ([]SearchResult, int64, error)
   }
   ```

3. **Repository** (`internal/db/repository/search.go`):
   - Constructor `NewSearchRepo(metaDB *sql.DB)` — uses the SQLite metastore
   - Single `Search()` method with raw SQL (no sqlc — the query joins DuckLake tables):
   ```sql
   -- Schema name matches
   SELECT 'schema' as type, s.schema_name, s.schema_name, '', '', 'name'
   FROM ducklake_schema s
   WHERE s.end_snapshot IS NULL AND s.schema_name LIKE '%' || ? || '%'

   UNION ALL

   -- Schema comment matches (from catalog_metadata)
   SELECT 'schema', cm.securable_name, cm.securable_name, '', cm.comment, 'comment'
   FROM catalog_metadata cm
   WHERE cm.securable_type = 'schema' AND cm.comment LIKE '%' || ? || '%'

   UNION ALL

   -- Table name matches
   SELECT 'table', t.table_name, s.schema_name, t.table_name, '', 'name'
   FROM ducklake_table t
   JOIN ducklake_schema s ON t.schema_id = s.schema_id
   WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL AND t.table_name LIKE '%' || ? || '%'

   UNION ALL

   -- Column name matches
   SELECT 'column', c.column_name, s.schema_name, t.table_name, '', 'name'
   FROM ducklake_column c
   JOIN ducklake_table t ON c.table_id = t.table_id
   JOIN ducklake_schema s ON t.schema_id = s.schema_id
   WHERE c.end_snapshot IS NULL AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL
     AND c.column_name LIKE '%' || ? || '%'
   ```
   - Deduplicate results (schema may appear from both name and comment match)
   - Apply optional `objectType` filter
   - Apply pagination with LIMIT/OFFSET wrapper

4. **Service** (`internal/service/search.go`):
   ```go
   type SearchService struct {
       repo domain.SearchRepository
   }

   func (s *SearchService) Search(ctx context.Context, query string, objectType *string, page domain.PageRequest) ([]domain.SearchResult, int64, error) {
       if query == "" {
           return nil, 0, domain.ErrValidation("query parameter is required")
       }
       return s.repo.Search(ctx, query, objectType, page)
   }
   ```

5. **OpenAPI**: Add `SearchResult` schema and endpoint:
   ```yaml
   /v1/search:
     get:
       operationId: search
       parameters:
         - name: query
           in: query
           required: true
           schema: { type: string }
         - name: type
           in: query
           required: false
           schema: { type: string, enum: [schema, table, column] }
         - $ref: '#/components/parameters/MaxResults'
         - $ref: '#/components/parameters/PageToken'
       responses:
         200:
           content:
             application/json:
               schema:
                 type: object
                 properties:
                   data:
                     type: array
                     items: { $ref: '#/components/schemas/SearchResult' }
                   next_page_token: { type: string }
   ```

6. **Run `task generate-api`**, implement handler.

7. **Wire in composition root**.

### Testing Strategy

| Test | Type | Location |
|------|------|----------|
| `TestSearchRepo_ByName` | Real SQLite + mock DuckLake tables | `internal/db/repository/search_test.go` — insert DuckLake schema/table/column rows, search, verify results |
| `TestSearchRepo_ByComment` | Real SQLite | Same file — insert catalog_metadata with comments, search |
| `TestSearchRepo_TypeFilter` | Real SQLite | Same file — verify `type` filter narrows results |
| `TestSearchRepo_Pagination` | Real SQLite | Same file — many results, verify pagination |
| `TestSearchService_Validation` | Unit | `internal/service/search_test.go` — empty query returns validation error |
| `TestAPI_Search` | Handler test | `internal/api/handler_test.go` — mock search repo, exercise endpoint |

---

## Feature 3: Tagging System

### Goal
Formal tags (key:value or key-only) assignable to schemas, tables, and columns. Full CRUD API.

### Design Decisions & Tradeoffs

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Tag structure | Separate `tags` (definitions) and `tag_assignments` tables | Reusable tag definitions; many-to-many relationship; allows tag discovery |
| Tag identity | `key` + `value` pair (value optional) | Supports both `pii:email` style and `deprecated` style |
| Securable reference | `securable_type` + `securable_id` + `column_name` | Consistent with existing `privilege_grants` pattern; `column_name` handles column-level tagging |
| Authorization | Admin or owner can manage tags | Consistent with catalog metadata management |
| Cascade delete | Deleting a tag cascades to assignments | Clean, no orphan assignments |

### Files to Create

| File | Purpose |
|------|---------|
| `internal/db/migrations/010_create_tags.sql` | Two tables: `tags` + `tag_assignments` |
| `internal/db/queries/tags.sql` | sqlc queries |
| `internal/domain/tag.go` | `Tag`, `TagAssignment` structs |
| `internal/db/repository/tag.go` | `TagRepo` |
| `internal/service/tag.go` | `TagService` with authorization |

> **Note on migration numbering**: If Feature 1 (Lineage) is implemented first, this becomes `011_create_tags.sql`. The numbers below assume Features are implemented in order; adjust as needed.

### Files to Modify

| File | Changes |
|------|---------|
| `internal/domain/repository.go` | Add `TagRepository` interface |
| `internal/db/mapper/domain_dbstore.go` | Add tag mappers |
| `internal/api/openapi.yaml` | Add tag schemas + 8 endpoints |
| `internal/api/handler.go` | Add `tags *service.TagService` field + 8 handler methods + tag mapping helpers |
| Composition root | Wire tag components |

### Implementation Order

1. **Migration** (`011_create_tags.sql`):
   ```sql
   -- +goose Up
   CREATE TABLE tags (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       key TEXT NOT NULL,
       value TEXT,   -- optional (NULL = key-only tag like "deprecated")
       created_by TEXT NOT NULL,
       created_at TEXT NOT NULL DEFAULT (datetime('now')),
       UNIQUE(key, value)
   );

   CREATE TABLE tag_assignments (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
       securable_type TEXT NOT NULL,  -- 'schema', 'table', 'column'
       securable_id INTEGER NOT NULL, -- schema_id, table_id
       column_name TEXT,              -- non-NULL for column-level tags
       assigned_by TEXT NOT NULL,
       assigned_at TEXT NOT NULL DEFAULT (datetime('now')),
       UNIQUE(tag_id, securable_type, securable_id, column_name)
   );

   CREATE INDEX idx_tag_assignments_securable ON tag_assignments(securable_type, securable_id);
   CREATE INDEX idx_tag_assignments_tag ON tag_assignments(tag_id);
   ```

2. **Domain types** (`internal/domain/tag.go`):
   ```go
   type Tag struct {
       ID        int64
       Key       string
       Value     *string
       CreatedBy string
       CreatedAt time.Time
   }

   type TagAssignment struct {
       ID            int64
       TagID         int64
       SecurableType string  // "schema", "table", "column"
       SecurableID   int64
       ColumnName    *string // non-nil for column-level tags
       AssignedBy    string
       AssignedAt    time.Time
   }

   // TagWithAssignment is a denormalized view for listing tags on a securable.
   type TagWithAssignment struct {
       TagID         int64
       Key           string
       Value         *string
       AssignmentID  int64
       AssignedBy    string
       AssignedAt    time.Time
   }
   ```

3. **Domain interface** (add to `repository.go`):
   ```go
   type TagRepository interface {
       CreateTag(ctx context.Context, t *Tag) (*Tag, error)
       GetTag(ctx context.Context, id int64) (*Tag, error)
       ListTags(ctx context.Context, page PageRequest) ([]Tag, int64, error)
       DeleteTag(ctx context.Context, id int64) error

       AssignTag(ctx context.Context, a *TagAssignment) (*TagAssignment, error)
       UnassignTag(ctx context.Context, id int64) error
       ListTagsForSecurable(ctx context.Context, securableType string, securableID int64, columnName *string, page PageRequest) ([]TagWithAssignment, int64, error)
   }
   ```

4. **sqlc queries** (`internal/db/queries/tags.sql`):
   - `CreateTag` `:one` — INSERT INTO tags
   - `GetTag` `:one` — SELECT by id
   - `ListTags` `:many` — SELECT with LIMIT/OFFSET
   - `CountTags` `:one`
   - `DeleteTag` `:exec` — DELETE (cascades to assignments)
   - `AssignTag` `:one` — INSERT INTO tag_assignments
   - `UnassignTag` `:exec` — DELETE FROM tag_assignments WHERE id = ?
   - `ListTagsForSecurable` `:many` — JOIN tags + tag_assignments by securable
   - `CountTagsForSecurable` `:one`

5. **Run `task sqlc`**.

6. **Mapper**: `TagFromDB()`, `TagAssignmentFromDB()`, `TagWithAssignmentFromDB()`.

7. **Repository** (`internal/db/repository/tag.go`): Standard implementation.

8. **Service** (`internal/service/tag.go`):
   ```go
   type TagService struct {
       repo  domain.TagRepository
       auth  domain.AuthorizationService
       audit domain.AuditRepository
   }
   ```
   - `CreateTag`: any authenticated user can create tags (or require admin)
   - `DeleteTag`: admin only
   - `AssignTag`: check privilege on the target securable (admin or ownership)
   - `UnassignTag`: same authorization check
   - `ListTagsForSecurable`: no auth check (read-only metadata)

9. **OpenAPI**: Add schemas and endpoints. The catalog-path endpoints (`GET /v1/catalog/schemas/{name}/tags`, etc.) require **resolving** schema/table/column names to IDs. The service method will accept names and resolve internally.

10. **Run `task generate-api`**, implement handler methods.

11. **Wire in composition root**.

### Testing Strategy

| Test | Type | Location |
|------|------|----------|
| `TestTagRepo_CRUD` | Real SQLite | `internal/db/repository/tag_test.go` — create, list, delete tags |
| `TestTagRepo_Assignments` | Real SQLite | Same file — assign, unassign, list for securable |
| `TestTagRepo_CascadeDelete` | Real SQLite | Same file — delete tag, verify assignments are gone |
| `TestTagService_Authorization` | Unit with mocks | `internal/service/tag_test.go` — non-admin can't delete tags |
| `TestAPI_TagCRUD` | Handler test | `internal/api/handler_test.go` |

---

## Feature 4: Views Support

### Goal
`CREATE VIEW` as a first-class catalog object, managed through dedicated API endpoints.

### Design Decisions & Tradeoffs

| Decision | Choice | Rationale |
|----------|--------|-----------|
| View storage | SQLite `views` table + actual DuckDB VIEW | Dual-write: metadata in SQLite for catalog, DuckDB view for query execution |
| DDL handling | Allow CREATE VIEW / DROP VIEW through API only (not raw SQL) | Controlled path ensures metadata stays in sync; raw DDL is still blocked |
| Privilege model | Require `CREATE_TABLE` (reuse existing) for creating views | Avoids new privilege type; views are similar to tables from an access perspective |
| View query-time security | RLS/column masks apply to underlying tables (not the view itself) | Correct semantic: views are transparent to security; the engine already resolves table references |
| Lineage integration | Views appear as nodes connecting source tables to downstream queries | Natural extension of Feature 1 |

### Files to Create

| File | Purpose |
|------|---------|
| `internal/db/migrations/012_create_views.sql` | `views` table |
| `internal/db/queries/views.sql` | sqlc queries for view metadata (if using sqlc; may use raw SQL like catalog repo) |
| `internal/domain/view.go` | `ViewDetail` struct |
| `internal/db/repository/view.go` | `ViewRepo` (or extend `CatalogRepo`) |
| `internal/service/view.go` | `ViewService` (or extend `CatalogService`) |

### Files to Modify

| File | Changes |
|------|---------|
| `internal/domain/repository.go` | Add `ViewRepository` interface (or extend `CatalogRepository`) |
| `internal/api/openapi.yaml` | Add view schemas + 4 endpoints |
| `internal/api/handler.go` | Add view handler methods |
| `internal/engine/engine.go` | **No change needed** — views resolve to underlying tables, engine handles them transparently |
| Composition root | Wire view components |

### Implementation Order

1. **Migration** (`012_create_views.sql`):
   ```sql
   -- +goose Up
   CREATE TABLE views (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       schema_name TEXT NOT NULL,
       name TEXT NOT NULL,
       view_definition TEXT NOT NULL,  -- the SELECT SQL
       source_tables TEXT,             -- JSON array of source table names
       comment TEXT,
       properties TEXT,                -- JSON key-value map
       owner TEXT NOT NULL,
       created_at TEXT NOT NULL DEFAULT (datetime('now')),
       updated_at TEXT NOT NULL DEFAULT (datetime('now')),
       UNIQUE(schema_name, name)
   );
   ```

2. **Domain types** (`internal/domain/view.go`):
   ```go
   type ViewDetail struct {
       ID             int64
       SchemaName     string
       Name           string
       CatalogName    string
       ViewDefinition string
       SourceTables   []string
       Comment        string
       Properties     map[string]string
       Owner          string
       CreatedAt      time.Time
       UpdatedAt      time.Time
   }

   type CreateViewRequest struct {
       Name           string
       ViewDefinition string
       Comment        string
   }
   ```

3. **Domain interface** — extend `CatalogRepository` or add new `ViewRepository`:
   ```go
   type ViewRepository interface {
       CreateView(ctx context.Context, schemaName string, req CreateViewRequest, sourceTables []string, owner string) (*ViewDetail, error)
       GetView(ctx context.Context, schemaName, viewName string) (*ViewDetail, error)
       ListViews(ctx context.Context, schemaName string, page PageRequest) ([]ViewDetail, int64, error)
       DeleteView(ctx context.Context, schemaName, viewName string) error
   }
   ```

4. **Repository** (`internal/db/repository/view.go`):
   - Uses both `metaDB` (SQLite) and `duckDB` (for CREATE/DROP VIEW DDL)
   - Constructor: `NewViewRepo(metaDB, duckDB *sql.DB)`
   - `CreateView`: 
     1. Validate identifier names
     2. Parse view SQL to extract source tables via `sqlrewrite.ExtractTableNames()`
     3. Execute `CREATE VIEW lake."schema"."name" AS <sql>` in DuckDB
     4. Insert metadata row in SQLite `views` table
   - `DeleteView`:
     1. Execute `DROP VIEW lake."schema"."name"` in DuckDB
     2. Delete metadata from SQLite
   - Read operations query SQLite only

5. **Service** (`internal/service/view.go`):
   ```go
   type ViewService struct {
       repo  domain.ViewRepository
       auth  domain.AuthorizationService
       audit domain.AuditRepository
   }
   ```
   - `CreateView`:
     1. Check `CREATE_TABLE` privilege on catalog
     2. Parse SQL, extract source tables
     3. For each source table: verify principal has `SELECT` privilege
     4. Call `repo.CreateView()`
     5. Audit log
   - `DeleteView`: check `CREATE_TABLE` privilege + audit
   - `GetView`, `ListViews`: no auth check (read-only metadata)

6. **OpenAPI**: Add endpoints under `/v1/catalog/schemas/{schemaName}/views`:
   - `POST /v1/catalog/schemas/{schemaName}/views` — create
   - `GET /v1/catalog/schemas/{schemaName}/views` — list (paginated)
   - `GET /v1/catalog/schemas/{schemaName}/views/{viewName}` — get
   - `DELETE /v1/catalog/schemas/{schemaName}/views/{viewName}` — drop

7. **Run `task generate-api`**, implement handler methods.

8. **Wire in composition root**.

### Testing Strategy

| Test | Type | Location |
|------|------|----------|
| `TestViewRepo_CreateAndGet` | Integration (real DuckDB + SQLite) | `internal/db/repository/view_test.go` |
| `TestViewRepo_Delete` | Integration | Same file |
| `TestViewService_Authorization` | Unit with mocks | `internal/service/view_test.go` — no privilege → 403 |
| `TestViewService_SourceTablePrivCheck` | Unit | Same file — user needs SELECT on all source tables |
| `TestAPI_ViewCRUD` | Handler test with mock ViewRepo | `internal/api/handler_test.go` |
| `TestEngine_QueryView` | Integration | `internal/engine/engine_test.go` — create a view, query it, verify RLS applies to underlying tables |

### Key Consideration: Engine Interaction
The `SecureEngine.Query()` method calls `sqlrewrite.ExtractTableNames()` which will extract the **view name** from the SQL. When the engine does `LookupTableID()`, the view name won't be in `ducklake_table`. Two approaches:

**Option A (recommended)**: Extend `LookupTableID` to also check the `views` table. Map view lookups to a special handling that:
1. Checks SELECT privilege on the view (as if it were a table, using the view's `id` as securable_id)
2. For RLS/column masks: applies them to the underlying source tables by expanding the view

**Option B**: The engine detects views and rewrites the query to inline the view definition (replacing the view reference with a subquery). This means RLS/column masks apply to the source tables naturally. Simpler but changes the query semantics.

**Recommendation**: Start with Option A for privilege check and rely on DuckDB's native view resolution for RLS (since DuckDB will expand the view into source table references when executing). The RLS injection happens at the SQL level before DuckDB sees it, so we need to handle view expansion in sqlrewrite or have the engine detect views and apply RLS to source tables.

---

## Feature 5: Query History API

### Goal
Dedicated query history endpoint with richer metadata (row count, bytes scanned) than raw audit logs.

### Design Decisions & Tradeoffs

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage approach | Add `rows_returned` column to `audit_log` + specialized query | Avoids separate table; audit_log already has 90% of needed data |
| Implementation | Extend `AuditRepository` with new query method + service | No new table if we add a column; minimal migration |
| Date range filtering | Add `from`/`to` params (parse ISO 8601) | Essential for query history exploration |
| Row count capture | Capture in `QueryService.Execute()` after scanning | Already have `result.RowCount` available |

### Files to Create

| File | Purpose |
|------|---------|
| `internal/db/migrations/012_add_rows_returned_to_audit.sql` | Add column |
| `internal/db/queries/query_history.sql` | Specialized queries (or add to `audit.sql`) |
| `internal/domain/query_history.go` | `QueryHistoryEntry` struct, `QueryHistoryFilter` |

### Files to Modify

| File | Changes |
|------|---------|
| `internal/db/migrations/006_create_audit_log.sql` | **No change** — we add a new migration to ALTER TABLE |
| `internal/domain/audit.go` | Add `RowsReturned *int64` to `AuditEntry` |
| `internal/domain/repository.go` | Add `QueryHistoryRepository` interface (or extend `AuditRepository`) |
| `internal/db/dbstore/models.go` | **Regenerated** — will include new `RowsReturned` column after sqlc |
| `internal/db/mapper/domain_dbstore.go` | Update `AuditEntryFromDB` / `AuditEntriesToDBParams` for `rows_returned` |
| `internal/db/queries/audit.sql` | Update `InsertAuditLog` to include `rows_returned` |
| `internal/db/repository/audit.go` | Update to handle new column |
| `internal/service/query.go` | Pass `result.RowCount` to audit log entry |
| `internal/api/openapi.yaml` | Add query history endpoint + response schema |
| `internal/api/handler.go` | Add handler method for query history |
| Composition root | Wire new service if separate, or add method to existing |

### Implementation Order

1. **Migration** (`012_add_rows_returned_to_audit.sql`):
   ```sql
   -- +goose Up
   ALTER TABLE audit_log ADD COLUMN rows_returned INTEGER;

   -- +goose Down
   -- SQLite doesn't support DROP COLUMN before 3.35.0;
   -- for safety, we leave this as a no-op in down migration
   ```

2. **Update domain** (`internal/domain/audit.go`): Add `RowsReturned *int64` to `AuditEntry`.

3. **Update sqlc query** (`internal/db/queries/audit.sql`): Add `rows_returned` to `InsertAuditLog` params.

4. **Add query history queries** (`internal/db/queries/query_history.sql` or extend `audit.sql`):
   ```sql
   -- name: ListQueryHistory :many
   SELECT * FROM audit_log
   WHERE action = 'QUERY'
     AND (? IS NULL OR principal_name = ?)
     AND (? IS NULL OR status = ?)
     AND (? IS NULL OR created_at >= ?)
     AND (? IS NULL OR created_at <= ?)
   ORDER BY created_at DESC
   LIMIT ? OFFSET ?;

   -- name: CountQueryHistory :one
   SELECT COUNT(*) as cnt FROM audit_log
   WHERE action = 'QUERY'
     AND (? IS NULL OR principal_name = ?)
     AND (? IS NULL OR status = ?)
     AND (? IS NULL OR created_at >= ?)
     AND (? IS NULL OR created_at <= ?);
   ```

5. **Run `task sqlc`** to regenerate.

6. **Update mapper** (`domain_dbstore.go`):
   - `AuditEntryFromDB`: map `RowsReturned` from `sql.NullInt64`
   - `AuditEntriesToDBParams`: map `RowsReturned` to `sql.NullInt64`

7. **Domain types** (`internal/domain/query_history.go`):
   ```go
   type QueryHistoryFilter struct {
       PrincipalName *string
       Status        *string
       From          *time.Time
       To            *time.Time
       Page          PageRequest
   }

   // QueryHistoryEntry is an alias/extension of AuditEntry for the query history context
   // We reuse AuditEntry directly since it now has RowsReturned
   ```

8. **Add interface** (`repository.go`):
   ```go
   type QueryHistoryRepository interface {
       ListQueryHistory(ctx context.Context, filter QueryHistoryFilter) ([]AuditEntry, int64, error)
   }
   ```
   Or extend `AuditRepository` with this method.

9. **Repository implementation**: Add `ListQueryHistory` to `AuditRepo` (or new repo).

10. **Service**: Add `QueryHistoryService` or a method on `AuditService`:
    ```go
    func (s *AuditService) ListQueryHistory(ctx context.Context, filter domain.QueryHistoryFilter) ([]domain.AuditEntry, int64, error)
    ```

11. **Update `QueryService.Execute()`** to pass `RowsReturned`:
    ```go
    // After successful scan:
    entry.RowsReturned = &result.RowCount  // need int64 conversion
    ```

12. **OpenAPI**: Add endpoint:
    ```yaml
    /v1/query-history:
      get:
        operationId: listQueryHistory
        parameters:
          - name: principal_name
          - name: status
          - name: from   # ISO 8601 datetime
          - name: to     # ISO 8601 datetime
          - $ref: MaxResults
          - $ref: PageToken
    ```

13. **Run `task generate-api`**, implement handler.

### Testing Strategy

| Test | Type | Location |
|------|------|----------|
| `TestAuditRepo_RowsReturned` | Real SQLite | `internal/db/repository/audit_test.go` — insert with rows_returned, verify persisted |
| `TestQueryHistory_DateFilter` | Real SQLite | Same file — insert entries with different timestamps, filter |
| `TestQueryService_CapturesRowCount` | Integration | `internal/service/query_test.go` — execute query, verify audit entry has row count |
| `TestAPI_QueryHistory` | Handler test | `internal/api/handler_test.go` — execute queries, then list history with filters |

---

## Feature 6: Information Schema

### Goal
Expose catalog metadata as virtual SQL tables (`information_schema.schemata`, `information_schema.tables`, etc.) queryable via the query engine.

### Design Decisions & Tradeoffs

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Implementation | Intercept in engine + programmatic result generation | Views can't be parameterized by principal for privilege filtering; programmatic approach allows per-user filtering |
| Detection | Pattern match on `information_schema.*` in query SQL | Simple, reliable; pg_query_go can detect schema-qualified references |
| Privilege filtering | Only show tables/schemas the principal has access to | information_schema is metadata-about-metadata; should respect catalog privileges |
| Scope | 4 virtual tables: schemata, tables, columns, table_privileges | Standard information_schema subset; covers main use cases |
| No new REST endpoints | Pure query-engine enhancement | Users query via `SELECT * FROM information_schema.tables` using the existing `/v1/query` endpoint |

### Files to Create

| File | Purpose |
|------|---------|
| `internal/engine/information_schema.go` | `InformationSchemaHandler` with methods for each virtual table |

### Files to Modify

| File | Changes |
|------|---------|
| `internal/engine/engine.go` | Detect `information_schema.*` queries, route to handler |
| `internal/sqlrewrite/sqlrewrite.go` | Add `IsInformationSchemaQuery(sql) bool` and `ExtractInformationSchemaTable(sql) string` |
| `internal/domain/repository.go` | Potentially extend `IntrospectionRepository` if needed for new queries |
| `internal/db/repository/introspection.go` | Add methods for full column listing, privilege listing |

### Implementation Order

1. **sqlrewrite enhancement**: Add detection functions:
   ```go
   // IsInformationSchemaQuery returns true if the query references information_schema.*
   func IsInformationSchemaQuery(sql string) bool

   // ExtractInformationSchemaTable returns the table name from information_schema
   // e.g., "information_schema.tables" → "tables"
   func ExtractInformationSchemaTable(sql string) (string, error)
   ```
   Implementation: parse with pg_query_go, check if any `RangeVar` has `schemaname = "information_schema"`.

2. **Information schema handler** (`internal/engine/information_schema.go`):
   ```go
   type InformationSchemaHandler struct {
       introspection domain.IntrospectionRepository
       catalog       domain.CatalogRepository
       auth          domain.AuthorizationService
       grants        domain.GrantRepository
   }

   // QuerySchemata returns rows for information_schema.schemata
   func (h *InformationSchemaHandler) QuerySchemata(ctx context.Context, principalName string) (*sql.Rows, error)

   // QueryTables returns rows for information_schema.tables
   func (h *InformationSchemaHandler) QueryTables(ctx context.Context, principalName string) (*sql.Rows, error)

   // QueryColumns returns rows for information_schema.columns
   func (h *InformationSchemaHandler) QueryColumns(ctx context.Context, principalName string) (*sql.Rows, error)

   // QueryTablePrivileges returns rows for information_schema.table_privileges
   func (h *InformationSchemaHandler) QueryTablePrivileges(ctx context.Context, principalName string) (*sql.Rows, error)
   ```

   **Key challenge**: The `Query()` method returns `*sql.Rows`, but information_schema results are programmatically generated, not from a real SQL query. Two approaches:

   **Option A**: Generate results as a DuckDB temp table, then query it:
   ```go
   // Create a temp table with results, return rows from it
   func (h *InformationSchemaHandler) QueryTables(ctx context.Context, principalName string) (*sql.Rows, error) {
       // 1. Fetch metadata from introspection repo
       // 2. Filter by principal's privileges
       // 3. Create temp table in DuckDB
       //    CREATE TEMP TABLE IF NOT EXISTS _info_tables AS SELECT ...
       // 4. Return db.QueryContext(ctx, "SELECT * FROM _info_tables")
   }
   ```

   **Option B (recommended)**: Return a custom result type that doesn't use `*sql.Rows`. Change `SecureEngine.Query()` to return a `QueryResult` instead of `*sql.Rows`, or add a separate method.

   **Option C (simplest, recommended)**: Build a VALUES clause dynamically:
   ```go
   func (h *InformationSchemaHandler) QueryTables(ctx context.Context, db *sql.DB, principalName string) (*sql.Rows, error) {
       schemas, _, _ := h.introspection.ListSchemas(ctx, domain.PageRequest{MaxResults: 10000})
       // For each schema, list tables, filter by privilege
       // Build: SELECT * FROM (VALUES ('lake','main','titanic','MANAGED'), ...) AS t(catalog_name, schema_name, table_name, table_type)
       var valueRows []string
       for _, table := range accessibleTables {
           valueRows = append(valueRows, fmt.Sprintf("('%s','%s','%s','%s')", ...))
       }
       sql := fmt.Sprintf("SELECT * FROM (VALUES %s) AS t(catalog_name, schema_name, table_name, table_type)", strings.Join(valueRows, ","))
       return db.QueryContext(ctx, sql)
   }
   ```

3. **Engine modification** (`internal/engine/engine.go`):
   ```go
   func (e *SecureEngine) Query(ctx context.Context, principalName, sqlQuery string) (*sql.Rows, error) {
       // NEW: Check for information_schema queries first
       if sqlrewrite.IsInformationSchemaQuery(sqlQuery) {
           return e.handleInformationSchema(ctx, principalName, sqlQuery)
       }

       // ... existing flow ...
   }

   func (e *SecureEngine) handleInformationSchema(ctx context.Context, principalName, sqlQuery string) (*sql.Rows, error) {
       tableName, err := sqlrewrite.ExtractInformationSchemaTable(sqlQuery)
       if err != nil {
           return nil, err
       }
       switch tableName {
       case "schemata":
           return e.infoSchema.QuerySchemata(ctx, e.db, principalName)
       case "tables":
           return e.infoSchema.QueryTables(ctx, e.db, principalName)
       case "columns":
           return e.infoSchema.QueryColumns(ctx, e.db, principalName)
       case "table_privileges":
           return e.infoSchema.QueryTablePrivileges(ctx, e.db, principalName)
       default:
           return nil, fmt.Errorf("unknown information_schema table: %s", tableName)
       }
   }
   ```

4. **Add `InformationSchemaHandler` as dependency on `SecureEngine`**:
   ```go
   type SecureEngine struct {
       db         *sql.DB
       catalog    domain.AuthorizationService
       infoSchema *InformationSchemaHandler  // NEW
   }
   ```

5. **Wire in composition root**: Create `InformationSchemaHandler`, pass to `SecureEngine`.

### Result Schema per Virtual Table

| Virtual Table | Columns |
|---------------|---------|
| `schemata` | `catalog_name`, `schema_name`, `schema_owner`, `comment` |
| `tables` | `catalog_name`, `schema_name`, `table_name`, `table_type`, `comment`, `owner` |
| `columns` | `catalog_name`, `schema_name`, `table_name`, `column_name`, `data_type`, `ordinal_position`, `comment` |
| `table_privileges` | `grantor`, `grantee`, `grantee_type`, `table_catalog`, `table_schema`, `table_name`, `privilege_type` |

### Testing Strategy

| Test | Type | Location |
|------|------|----------|
| `TestIsInformationSchemaQuery` | Unit | `internal/sqlrewrite/sqlrewrite_test.go` — various SQL patterns |
| `TestExtractInformationSchemaTable` | Unit | Same file |
| `TestInformationSchema_Tables` | Integration | `internal/engine/information_schema_test.go` — real DuckDB + SQLite, verify correct rows |
| `TestInformationSchema_PrivilegeFiltering` | Integration | Same file — admin sees all, analyst sees only granted tables |
| `TestInformationSchema_ViaQueryEndpoint` | Handler test | `internal/api/handler_test.go` — `POST /v1/query` with `SELECT * FROM information_schema.tables` |

---

## Cross-Feature Dependencies

```
Feature 1 (Lineage) ← Feature 4 (Views) depends on lineage to track view relationships
Feature 5 (Query History) depends on audit_log schema changes (do first before Feature 2/3 if they share migration numbers)
Feature 6 (Info Schema) depends on introspection repo but is otherwise independent
Feature 2 (Search) is fully independent
Feature 3 (Tags) is fully independent
```

### Recommended Implementation Order

1. **Feature 5: Query History** — smallest scope, touches existing audit infrastructure, immediate value. Migration: `010_add_rows_returned_to_audit.sql`.

2. **Feature 2: Search** — no migration needed, independent of other features, useful utility.

3. **Feature 1: Lineage** — new table + service, foundational for Feature 4. Migration: `011_create_lineage_edges.sql`.

4. **Feature 3: Tags** — new tables, independent. Migration: `012_create_tags.sql`.

5. **Feature 4: Views** — depends on Feature 1 for lineage integration, most complex. Migration: `013_create_views.sql`.

6. **Feature 6: Information Schema** — engine-level changes, no migration, benefits from all other features being in place.

### Migration Number Summary

| Number | Feature | Table |
|--------|---------|-------|
| 010 | Query History | ALTER audit_log ADD rows_returned |
| 011 | Lineage | CREATE lineage_edges |
| 012 | Tags | CREATE tags + tag_assignments |
| 013 | Views | CREATE views |

---

## Shared Patterns Checklist

For each feature, ensure:

- [ ] Domain types have zero external dependencies
- [ ] Interface defined in `internal/domain/repository.go`
- [ ] Repository constructor: `NewXxxRepo(db *sql.DB)` (or `(metaDB, duckDB *sql.DB)` for dual-DB repos)
- [ ] Service constructor: `NewXxxService(repo, auth, audit)`
- [ ] Compile-time interface check: `var _ domain.XxxRepository = (*XxxRepo)(nil)`
- [ ] Pagination via `domain.PageRequest` + `domain.NextPageToken()`
- [ ] Audit logging is best-effort: `_ = s.audit.Insert(ctx, entry)`
- [ ] Errors use domain error constructors
- [ ] API layer type-switches on domain errors for HTTP status
- [ ] OpenAPI spec updated → `task generate-api`
- [ ] sqlc queries updated (if applicable) → `task sqlc`
- [ ] Tests: table-driven, testify assertions, real SQLite via `t.TempDir()`
- [ ] Handler tests use mock repos (following `mockCatalogRepo` pattern)
- [ ] Wired in composition root
