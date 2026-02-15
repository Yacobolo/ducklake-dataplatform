// Package gen contains generated CLI commands.
//
// The *.gen.go files are produced by cmd/cli-gen and gitignored.
// This file ensures their transitive dependencies (golang.org/x/term)
// remain in go.mod/go.sum so downstream CI jobs can build after code generation.
package gen

import _ "golang.org/x/term" // required by generated *.gen.go files
