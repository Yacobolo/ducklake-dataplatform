# Declarative JSON Schema

The repository publishes versioned JSON Schema artifacts for declarative config under:

- `schemas/declarative/v1/duck.declarative.schema.json` (union root)
- `schemas/declarative/v1/kinds/*.schema.json` (per-kind documents)
- `schemas/declarative/v1/index.json` (manifest + checksums)

Generate schemas with:

```bash
task generate:declarative-schema
```

Schemas are generated from declarative Go document types declared in
`internal/declarative/schema_registry.go`.

Notes:

- Declarative loader defaults to strict unknown-field rejection.
- Use CLI flag `--allow-unknown-fields` on `duck validate`, `duck plan`, or `duck apply`
  for temporary compatibility with legacy configs.
