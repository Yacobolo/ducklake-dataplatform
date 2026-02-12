package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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
		log.Fatalf("load spec: %v", err)
	}
	if err := spec.Validate(context.Background()); err != nil {
		log.Fatalf("validate spec: %v", err)
	}

	// Load CLI config
	cfg, err := cligen.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// Parse spec + config into model
	groups, err := cligen.Parse(spec, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	// Render to Go source
	if err := cligen.Render(groups, cfg, *outDir); err != nil {
		log.Fatalf("render: %v", err)
	}

	fmt.Printf("Generated %d groups in %s\n", len(groups), *outDir)
}
