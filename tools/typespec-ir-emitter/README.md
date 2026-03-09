# @duck/typespec-ir-emitter

Custom TypeSpec emitter that writes a compact JSON IR used by `cmd/apigen`.

Current output location is configured in `api/spec/tspconfig.yaml` and defaults to:

- `api/gen/json-ir.json`

This IR file is a generated local intermediate consumed by `cmd/apigen`; it is not committed.

The IR schema version is currently `v1`.
