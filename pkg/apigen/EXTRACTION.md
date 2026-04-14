# APIGen Extraction Notes

This repo remains the source of truth for APIGen until the following stay green together:

- `github.com/Yacobolo/quackstack/pkg/apigen/...` has no imports of `github.com/Yacobolo/quackstack/internal/...` or sibling repo-private `github.com/Yacobolo/quackstack/pkg/...` packages outside `github.com/Yacobolo/quackstack/pkg/apigen/...`
- the bundled test fixture under `pkg/apigen/testdata/example_consumer` compiles its source API spec to OpenAPI + JSON IR
- the fixture generates Go server and CLI code using APIGen
- the fixture builds against `github.com/Yacobolo/quackstack/pkg/apigen/runtime/chi` and `github.com/Yacobolo/quackstack/pkg/apigen/runtime/cobra`
- JSON IR `v1` remains documented and fixture-tested

Planned standalone targets after that proof step:

- Go module: `github.com/<org>/apigen`
- optional CUE package/module distribution if cross-repo sharing needs a common authored contract base

The intended split is:

- keep JSON IR as the compatibility boundary between the CUE authoring compiler and Go emitters
- keep canonical OpenAPI as the published contract artifact, including repo-owned extensions such as `x-authz` and `x-cli-command`
- move repo-local wrapper concerns into thin CLIs and task wiring, not the library packages
