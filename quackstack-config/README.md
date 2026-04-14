# quackstack-config

This module is the canonical seeded developer platform graph.

Layout conventions:

- `workspaces/` own namespace-facing authored content.
  - folders
  - notebooks
  - dashboards
- `projects/` own execution and modeling content.
  - environments
  - macros
  - models
  - semantic models
- `catalogs/` use deeper nesting because catalog -> schema -> table/view is a real hierarchy.
- `security/`, `compute/`, `domains/`, `teams/`, and `data-products/` stay top-level because they are cross-cutting platform resources.

Authoring conventions:

- Keep refs explicit in CUE even when the file path implies ownership.
- Use local `defs.cue` files inside a package directory to remove repeated owners, refs, tags, and paths.
- Split files by what changes together, not by "one resource per file".
- Prefer a few browsable files over a single giant config blob.
