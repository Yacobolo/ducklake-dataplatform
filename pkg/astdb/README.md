# pkg/astdb

Incremental Go AST indexer backed by DuckDB.

## What it does

- scans `.go` files in a repo or subdirectory
- parses AST nodes and stores them in DuckDB
- tracks per-file metadata to support incremental updates on large codebases
- benchmarks common read queries

## Package API

- `astdb.DefaultOptions()` returns sensible defaults
- `astdb.Run(ctx, opts)` executes build/query flow and returns structured results

## CLI entrypoint

Use `cmd/astdbbench`:

```bash
go run ./cmd/astdbbench -repo . -duckdb ./.tmp/astbench/ast.duckdb -mode both -reuse
```

## Typical workflows

- Build or incrementally update + query:

```bash
task ast:bench
```

- Query-only rerun (reuse existing DB):

```bash
task ast:query
```
