# @duck/typespec-ir-emitter

Custom TypeSpec emitter that writes the compact JSON IR consumed by `cmd/apigen`.

Current output location is configured in `api/spec/tspconfig.yaml` and defaults to:

- `api/gen/json-ir.json`

`task typespec:compile` also emits the canonical OpenAPI artifact at `api/gen/openapi.yaml`.

TypeSpec is the source of truth for service metadata, tags, auth metadata, manual-operation exclusions, and CLI command metadata. The canonical OpenAPI artifact intentionally carries repo-owned extensions such as `x-authz` and `x-cli-command`, while the JSON IR remains the generated local intermediate consumed by `cmd/apigen`.

The IR schema version is currently `v1`.
