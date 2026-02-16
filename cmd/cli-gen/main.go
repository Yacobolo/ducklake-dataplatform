// Package main is the entry point for the CLI code generator.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/getkin/kin-openapi/openapi3"

	cligen "duck-demo/internal/codegen/cli"
)

func main() {
	specPath := flag.String("spec", "internal/api/openapi.yaml", "path to OpenAPI spec")
	configPath := flag.String("config", "cli-config.yaml", "path to CLI config")
	outDir := flag.String("out", "pkg/cli/gen/", "output directory for generated files")
	flag.Parse()

	// Load OpenAPI spec
	loader := openapi3.NewLoader()
	spec, err := loader.LoadFromFile(*specPath)
	if err != nil {
		fatalf("load spec: %v", err)
	}
	if err := spec.Validate(context.Background()); err != nil {
		fatalf("validate spec: %v", err)
	}

	// Load CLI config
	cfg, err := cligen.LoadConfig(*configPath)
	if err != nil {
		fatalf("load config: %v", err)
	}

	// Parse spec + config into model
	groups, err := cligen.Parse(spec, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	// Extract API endpoints for the registry
	endpoints := cligen.ExtractAPIEndpoints(spec, groups)

	// Render to Go source (including API registry)
	if err := cligen.RenderWithEndpoints(groups, cfg, endpoints, *outDir); err != nil {
		fatalf("render: %v", err)
	}

	fmt.Printf("Generated %d groups + %d API endpoints in %s\n", len(groups), len(endpoints), *outDir)
}

func fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
