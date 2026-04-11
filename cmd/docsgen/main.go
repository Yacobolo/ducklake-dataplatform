// Package main generates markdown reference docs from OpenAPI and declarative CUE guidance.
package main

import (
	"flag"
	"fmt"
	"os"

	"duck-demo/internal/docsgen/declarative"
	"duck-demo/internal/docsgen/discovery"
	"duck-demo/internal/docsgen/openapi"
)

func main() {
	openapiPath := flag.String("openapi", "api/gen/openapi.yaml", "path to OpenAPI spec")
	outDir := flag.String("outdir", "site/content/reference/generated", "output directory for derived reference docs")
	docsDir := flag.String("docs-dir", "site/content", "path to source docs directory")
	cliIndexOut := flag.String("cli-index-out", "pkg/cli/gen/discovery_index.gen.go", "path to generated CLI discovery metadata")
	navConfigPath := flag.String("nav-config", "site/config/navigation.yaml", "path to site navigation and API grouping config")
	flag.Parse()

	apiOut := fmt.Sprintf("%s/api", *outDir)
	if err := openapi.Generate(*openapiPath, apiOut, *navConfigPath); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: generate API docs: %v\n", err)
		os.Exit(1)
	}

	declOut := fmt.Sprintf("%s/declarative", *outDir)
	if err := declarative.Generate(declOut); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: generate declarative docs: %v\n", err)
		os.Exit(1)
	}

	if err := discovery.Generate(*docsDir, *openapiPath, *cliIndexOut); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: generate CLI discovery index: %v\n", err)
		os.Exit(1)
	}
}
