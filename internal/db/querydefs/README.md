# cue-sql Query Definitions

This directory is the source of truth for generated metadata-store queries.

Files:
- `generated_legacy.cue`: the broad query corpus expressed as raw SQL-backed CUE definitions.
- `dynamic_overrides.cue`: structured definitions for queries where `cue-sql` should omit optional predicates or expand slices instead of preserving legacy SQL hacks.

Authoring rules:
- Prefer structured `select` / `insert` / `update` / `delete` definitions when the query benefits from composition, validation, or dynamic clause omission.
- Use `raw` only when the SQL shape is awkward to model or when preserving an exact hand-written statement is more valuable than structuring it.
- Keep params and result row fields aligned with the generated Go API used by repositories and tests.
- SQLite migrations in `internal/db/migrations` remain the schema source of truth; querydefs should validate cleanly against them.

Workflow:
- Edit or add `*.cue` files here.
- Run `task cue-sql` to regenerate `internal/db/cuestore`.
- Run `task check` before shipping changes.

When adding a new dynamic filter:
- Prefer an optional predicate such as `{column: "owner", op: "=", param: "Owner", optional: true}`.
- Prefer slice predicates such as `{column: "folder_id", param: "FolderIDs", slice: true}`.
- Avoid SQL boolean hacks like `(? = '' OR column = ?)` unless a query must stay raw for a specific reason.
