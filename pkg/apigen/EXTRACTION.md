# APIGen Module Notes

APIGen now lives in an in-repo nested module rooted at `pkg/apigen`.
This repo remains the source of truth for APIGen while the following stay green together:

- `github.com/Yacobolo/quackstack/pkg/apigen/...` has no imports of `github.com/Yacobolo/quackstack/internal/...` or sibling repo-private `github.com/Yacobolo/quackstack/pkg/...` packages outside `github.com/Yacobolo/quackstack/pkg/apigen/...`
- the bundled test fixture under `pkg/apigen/testdata/example_consumer` compiles its source API spec to OpenAPI + JSON IR
- the fixture generates Go server and CLI code using APIGen
- the fixture builds against `github.com/Yacobolo/quackstack/pkg/apigen/runtime/chi` and `github.com/Yacobolo/quackstack/pkg/apigen/runtime/cobra`
- JSON IR `v1` remains documented and fixture-tested

Current module target:

- Go module: `github.com/Yacobolo/quackstack/pkg/apigen`
- optional future standalone repo/module if cross-repo distribution needs a cleaner host boundary

The intended boundary is:

- keep JSON IR as the compatibility boundary between the CUE authoring compiler and Go emitters
- keep canonical OpenAPI as the published contract artifact, including repo-owned extensions such as `x-authz`
- keep repo-local invocation concerns in thin CLI/task wiring around the nested module, not the library packages
