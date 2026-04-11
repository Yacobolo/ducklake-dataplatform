# cue-sql Query Definitions

This directory is the source of truth for generated metadata-store queries.

Files:
- `legacy_*.cue`: the imported compatibility corpus, split by domain/table for reviewability.
- `dynamic_overrides.cue`: structured definitions for queries where `cue-sql` should omit optional predicates or expand slices instead of preserving legacy SQL hacks.
- `setup_state.cue`, `auth_login_attempts.cue`, `auth_providers.cue`: examples of fully structured authoring that replace former raw imports.

Authoring rules:
- Prefer structured `select` / `insert` / `update` / `delete` definitions when the query benefits from composition, validation, or dynamic clause omission.
- Use `raw` only when the SQL shape is awkward to model or when preserving an exact hand-written statement is more valuable than structuring it.
- Keep params and result row fields aligned with the generated Go API used by repositories and tests.
- SQLite migrations in `internal/db/migrations` remain the schema source of truth; querydefs should validate cleanly against them.

Workflow:
- Edit or add `*.cue` files here.
- Optionally run `go run ./cmd/cue-sql-structurize -src internal/db/querydefs` to convert simple legacy raw statements into structured definitions before hand-tuning the remaining complex ones.
- Run `task cue-sql` to regenerate `internal/db/cuestore`.
- Run `task check` before shipping changes.

When adding a new dynamic filter:
- Prefer an optional predicate such as `{column: "owner", op: "=", param: "Owner", optional: true}`.
- Prefer slice predicates such as `{column: "folder_id", op: "=", param: "FolderIDs", slice: true}`.
- Prefer literal predicates such as `{column: "id", op: "=", valueSQL: "1"}` for fixed singleton rows instead of dropping to `raw`.
- Avoid SQL boolean hacks like `(? = '' OR column = ?)` unless a query must stay raw for a specific reason.
