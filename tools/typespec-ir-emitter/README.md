# @duck/typespec-ir-emitter

Custom TypeSpec emitter that writes a compact JSON IR used by `cmd/apigen`.

Current output location is configured in `api/spec/tspconfig.yaml` and defaults to:

- `api/gen/json-ir.json`

The IR schema version is currently `v1`.
