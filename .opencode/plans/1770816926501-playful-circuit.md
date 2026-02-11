# OpenAPI Spec Restructuring & Naming Normalization

## Goal
Restructure `internal/api/openapi.yaml` for consistent naming, logical grouping (OpenAPI tags), and HTTP convention adherence. Full breaking refactor — regenerate code and update all handlers/tests.

---

## Phase 1: Add OpenAPI Tags (non-breaking, docs-only)

Add a top-level `tags:` block and tag every operation so Scalar shows a grouped sidebar.

**Tags:**
| Tag | Operations |
|-----|-----------|
| Query | `executeQuery` |
| Catalog | All `/catalog/...` schema/table/column/view operations |
| Ingestion | `requestUploadUrl`, `commitIngestion`, `loadExternalFiles` |
| Security | Principals, groups, grants, row filters, column masks |
| Lineage | All `/lineage/...` operations |
| Governance | Tags, classifications, search |
| Observability | Audit logs, query history, metastore summary |
| Storage | Storage credentials, external locations |
| Manifest | `getManifest` |

**Files:** `openapi.yaml` only (tags don't affect codegen)

---

## Phase 2: Remove Deprecated Legacy Endpoints

Remove 3 deprecated endpoints replaced by `/catalog/...` equivalents:
- `GET /schemas` → replaced by `GET /catalog/schemas`
- `GET /schemas/{id}/tables` → replaced by `GET /catalog/schemas/{schemaName}/tables`
- `GET /tables/{id}/columns` → replaced by `GET /catalog/schemas/{schemaName}/tables/{tableName}/columns`

Also remove unused schemas: `Schema`, `Table`, `Column`, `PaginatedSchemas`, `PaginatedTables`, `PaginatedColumns`.

**Note:** `IntrospectionRepository` (domain interface) stays — it's used by `AuthorizationService`. Only the API-facing `IntrospectionService` and its handler methods are removed.

**Files:** `openapi.yaml`, `handler.go` (remove 3 methods + `introspection` field), `handler_test.go`, `cmd/server/main.go` (remove `IntrospectionService` from `NewHandler` call)

---

## Phase 3: Rename OperationIds

Convention: `verb` + `Resource` in camelCase. Verbs: `list`, `create`, `get`, `update`, `delete`.

**Renames (21 changes):**

| Current → New | Reason |
|--------------|--------|
| `dropTable` → `deleteTable` | consistency |
| `dropView` → `deleteView` | consistency |
| `getManifest` → `createManifest` | POST creates a manifest |
| `listCatalogSchemas` → `listSchemas` | legacy removed, drop `Catalog` prefix |
| `listCatalogTables` → `listTables` | same |
| `createCatalogTable` → `createTable` | same |
| `getSchemaByName` → `getSchema` | `ByName` redundant |
| `getTableByName` → `getTable` | same |
| `updateSchemaMetadata` → `updateSchema` | `Metadata` redundant |
| `updateTableMetadata` → `updateTable` | same |
| `updateColumnMetadata` → `updateColumn` | same |
| `addGroupMember` → `createGroupMember` | CRUD consistency |
| `removeGroupMember` → `deleteGroupMember` | CRUD consistency |
| `grantPrivilege` → `createGrant` | CRUD consistency |
| `revokePrivilege` → `deleteGrant` | CRUD consistency |
| `setAdmin` → `updatePrincipalAdmin` | clarify resource |
| `requestUploadUrl` → `createUploadUrl` | CRUD consistency |
| `commitIngestion` → `commitTableIngestion` | clarify context |
| `loadExternalFiles` → `loadTableExternalFiles` | clarify context |
| `assignTag` → `createTagAssignment` | CRUD consistency |
| `unassignTag` → `deleteTagAssignment` | CRUD consistency |

**Files:** `openapi.yaml`, then regenerate → update all method names in `handler.go` (21 renames), update response type references

---

## Phase 4: Rename Path Parameters

Convention: `{resourceId}` or `{resourceName}` — never bare `{id}`.

| Current → New | Affected operations |
|--------------|-------------------|
| `/principals/{id}` → `/principals/{principalId}` | getPrincipal, deletePrincipal, updatePrincipalAdmin |
| `/groups/{id}` → `/groups/{groupId}` | getGroup, deleteGroup, listGroupMembers, createGroupMember, deleteGroupMember |
| `/row-filters/{id}` → `/row-filters/{rowFilterId}` | deleteRowFilter, bindRowFilter, unbindRowFilter |
| `/column-masks/{id}` → `/column-masks/{columnMaskId}` | deleteColumnMask, bindColumnMask, unbindColumnMask |

**Impact:** Generated request structs change `req.Id` → `req.PrincipalId`, `req.GroupId`, `req.RowFilterId`, `req.ColumnMaskId`. Handler code must update all field accesses.

**Files:** `openapi.yaml`, `handler.go` (~14 field renames)

---

## Phase 5: Rename Schemas

| Current → New | Reason |
|--------------|--------|
| `CreateTableApiRequest` → `CreateTableRequest` | remove stray `Api` |
| `CreateColumnDef` → `CreateColumnRequest` | match pattern |
| `AddGroupMemberRequest` → `CreateGroupMemberRequest` | match operationId |
| `SetAdminRequest` → `UpdatePrincipalAdminRequest` | match operationId |
| `GrantRequest` → `CreateGrantRequest` | match operationId |
| `RevokeRequest` → `DeleteGrantRequest` | match operationId |
| `BindingRequest` → `RowFilterBindingRequest` | disambiguate |
| `PaginatedQueryHistory` → `PaginatedQueryHistoryEntries` | pluralize consistently |

**Files:** `openapi.yaml` (schema definitions + all `$ref` references), `handler.go` (type references)

---

## Phase 6: Fix DELETE-with-Body Endpoints

Convert 4 DELETE-with-body operations to POST with action sub-paths:

| Current → New |
|--------------|
| `DELETE /groups/{groupId}/members` + body → `POST /groups/{groupId}/members/remove` |
| `DELETE /grants` + body → `POST /grants/revoke` |
| `DELETE /row-filters/{rowFilterId}/bindings` + body → `POST /row-filters/{rowFilterId}/unbind` |
| `DELETE /column-masks/{columnMaskId}/bindings` + body → `POST /column-masks/{columnMaskId}/unbind` |

**Files:** `openapi.yaml` (path + method changes), handler logic stays the same

---

## Phase 7: Relocate Lineage Paths Under Catalog

| Current → New |
|--------------|
| `/lineage/tables/{schemaName}/{tableName}` → `/catalog/schemas/{schemaName}/tables/{tableName}/lineage` |
| `/lineage/tables/{schemaName}/{tableName}/upstream` → `/catalog/schemas/{schemaName}/tables/{tableName}/lineage/upstream` |
| `/lineage/tables/{schemaName}/{tableName}/downstream` → `/catalog/schemas/{schemaName}/tables/{tableName}/lineage/downstream` |

Keep `/lineage/edges/{edgeId}` and `/lineage/purge` at top level (they're not table-scoped).

**Files:** `openapi.yaml` only (operationIds unchanged)

---

## Phase 8: Add Missing Error Responses

Ensure all endpoints declare standard errors:
- Every mutating endpoint: `400` (validation), `403` (access denied)
- Every endpoint with path param to specific resource: `404` (not found)
- Every create endpoint: `409` (conflict)

Use a shared `$ref` to the existing `Error` schema for all error responses.

**Files:** `openapi.yaml` (~20 endpoints need additions), generated code gets new response type aliases

---

## Phase 9: Bump Version & Final Validation

- Bump `version: "2.0.0"` → `version: "3.0.0"` in openapi.yaml
- Run `task generate-api && task build && task test`
- Verify Scalar docs at `http://localhost:8080/docs` show proper grouping

---

## Execution Workflow (per phase)

```bash
# Edit openapi.yaml
task generate-api    # regenerate types.gen.go + server.gen.go
task build           # compiler catches all handler mismatches
# Fix handler.go / handler_test.go / main.go as needed
task build && task test
git commit
```

---

## Key Files to Modify

| File | Phases |
|------|--------|
| `internal/api/openapi.yaml` | 1-9 |
| `internal/api/types.gen.go` | 2-8 (regenerated) |
| `internal/api/server.gen.go` | 2-8 (regenerated) |
| `internal/api/handler.go` | 2-5 |
| `internal/api/handler_test.go` | 2-3 |
| `cmd/server/main.go` | 2 |
| `test/integration/helpers_test.go` | 2 |

---

## Verification

1. `task generate-api` — regenerates without errors
2. `task build` — compiles cleanly (catches all interface/type mismatches)
3. `task test` — all unit tests pass
4. Manual: `go run ./cmd/server` → visit `http://localhost:8080/docs` → verify grouped sidebar, consistent naming, no flat operation list
