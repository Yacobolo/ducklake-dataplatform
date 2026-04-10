# APIGen Extraction Notes

This repo remains the source of truth for APIGen until the following stay green together:

- `duck-demo/apigen/...` has no imports of `duck-demo/internal/...` or `duck-demo/pkg/...`
- the nested example consumer compiles TypeSpec to OpenAPI + JSON IR
- the example generates Go server and CLI code using APIGen
- the example builds against `duck-demo/apigen/runtime/chi` and `duck-demo/apigen/runtime/cobra`
- JSON IR `v1` remains documented and fixture-tested

Planned standalone targets after that proof step:

- Go module: `github.com/<org>/apigen`
- npm package: `@<scope>/typespec-ir-emitter`

The intended split is:

- keep JSON IR as the compatibility boundary between the TypeSpec emitter and Go emitters
- keep canonical OpenAPI as the published contract artifact, including repo-owned extensions such as `x-authz` and `x-cli-command`
- move repo-local wrapper concerns into thin CLIs and task wiring, not the library packages
