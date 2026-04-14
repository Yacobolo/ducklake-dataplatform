// Package apigen documents the supported in-repo APIGen surfaces.
//
// Supported library packages:
//   - github.com/Yacobolo/quackstack/pkg/apigen/ir
//   - github.com/Yacobolo/quackstack/pkg/apigen/emit/openapi
//   - github.com/Yacobolo/quackstack/pkg/apigen/emit/requestmodelgo
//   - github.com/Yacobolo/quackstack/pkg/apigen/emit/servergo
//   - github.com/Yacobolo/quackstack/pkg/apigen/emit/cligo
//   - github.com/Yacobolo/quackstack/pkg/apigen/runtime/chi
//   - github.com/Yacobolo/quackstack/pkg/apigen/runtime/cobra
//
// JSON IR is the generator input contract. Canonical OpenAPI is the published
// API contract artifact and may carry repo-owned extensions such as x-authz and
// x-cli-command.
package apigen
