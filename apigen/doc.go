// Package apigen documents the supported in-repo APIGen surfaces.
//
// Supported library packages:
//   - duck-demo/apigen/ir
//   - duck-demo/apigen/emit/openapi
//   - duck-demo/apigen/emit/requestmodelgo
//   - duck-demo/apigen/emit/servergo
//   - duck-demo/apigen/emit/cligo
//   - duck-demo/apigen/runtime/chi
//   - duck-demo/apigen/runtime/cobra
//
// JSON IR is the generator input contract. Canonical OpenAPI is the published
// API contract artifact and may carry repo-owned extensions such as x-authz and
// x-cli-command.
package apigen
