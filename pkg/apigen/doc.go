// Package apigen documents the supported in-repo APIGen surfaces.
//
// Supported library packages:
//   - duck-demo/pkg/apigen/ir
//   - duck-demo/pkg/apigen/emit/openapi
//   - duck-demo/pkg/apigen/emit/requestmodelgo
//   - duck-demo/pkg/apigen/emit/servergo
//   - duck-demo/pkg/apigen/emit/cligo
//   - duck-demo/pkg/apigen/runtime/chi
//   - duck-demo/pkg/apigen/runtime/cobra
//
// JSON IR is the generator input contract. Canonical OpenAPI is the published
// API contract artifact and may carry repo-owned extensions such as x-authz and
// x-cli-command.
package apigen
