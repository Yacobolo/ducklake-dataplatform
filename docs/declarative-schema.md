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

## Editor integration

You can attach the union schema to declarative YAML files for inline completion and
basic validation.

### VS Code (YAML extension)

Add to `.vscode/settings.json`:

```json
{
  "yaml.schemas": {
    "schemas/declarative/v1/duck.declarative.schema.json": [
      "examples/**/config/**/*.yaml",
      "duck-config/**/*.yaml"
    ]
  }
}
```

### JetBrains IDEs

- Open **Settings > Languages & Frameworks > Schemas and DTDs > JSON Schema Mappings**.
- Add a new mapping to `schemas/declarative/v1/duck.declarative.schema.json`.
- Apply it to your declarative YAML file patterns.

## Schema vs semantic validation

- JSON Schema catches document shape/type issues (missing required fields, enums,
  conditional requirements).
- `duck validate` also runs semantic checks from `internal/declarative/validator.go`
  (cross-resource references, duplicate conflicts, allowed privilege combinations,
  visibility rules, etc.).

Recommended flow:

```bash
duck validate --config-dir <path>
duck plan --config-dir <path>
```

## Troubleshooting

- If schema validation passes but `duck validate` fails, check for cross-resource
  constraints (unknown principals/groups, unknown securables, duplicate bindings,
  invalid privilege-to-securable combinations).
- If `duck plan` exits with status `2`, that means drift was detected (expected in CI
  checks), not a command crash.
