// Package main generates markdown reference docs from OpenAPI and declarative schemas.
package main

import (
	"flag"
	"fmt"
	"os"

	"duck-demo/internal/docsgen/declarative"
	"duck-demo/internal/docsgen/openapi"
)

func main() {
	openapiPath := flag.String("openapi", "internal/api/openapi.bundled.yaml", "path to bundled OpenAPI spec")
	declIndexPath := flag.String("declarative-index", "schemas/declarative/v1/index.json", "path to declarative schema manifest")
	declDir := flag.String("declarative-dir", "schemas/declarative/v1", "path to declarative schema directory")
	outDir := flag.String("outdir", "docs/reference/generated", "output directory for generated docs")
	flag.Parse()

	apiOut := fmt.Sprintf("%s/api", *outDir)
	if err := openapi.Generate(*openapiPath, apiOut); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: generate API docs: %v\n", err)
		os.Exit(1)
	}

	declOut := fmt.Sprintf("%s/declarative", *outDir)
	if err := declarative.Generate(*declIndexPath, *declDir, declOut); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: generate declarative docs: %v\n", err)
		os.Exit(1)
	}
}
