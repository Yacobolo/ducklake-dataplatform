// Package main provides the apigen CLI entrypoint.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"

	cligoemit "duck-demo/internal/apigen/emit/cligo"
	openapiemit "duck-demo/internal/apigen/emit/openapi"
	requestmodelgoemit "duck-demo/internal/apigen/emit/requestmodelgo"
	servergoemit "duck-demo/internal/apigen/emit/servergo"
	"duck-demo/internal/apigen/ir"
)

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: apigen <openapi|server|cli|all> -ir <path>")
	}

	command := os.Args[1]
	fs := flag.NewFlagSet(command, flag.ExitOnError)
	irPath := fs.String("ir", "api/gen/json-ir.json", "input JSON IR path")
	openapiOut := fs.String("openapi-out", "internal/api/openapi.generated.yaml", "output OpenAPI YAML path")
	serverOut := fs.String("server-out", "internal/api/server.apigen.gen.go", "output server Go path")
	requestModelsOut := fs.String("request-models-out", "internal/api/gen_request_models.gen.go", "output APIGen request models Go path")
	cliOut := fs.String("cli-out", "pkg/cli/gen/apigen_registry.gen.go", "output CLI Go path")
	if err := fs.Parse(os.Args[2:]); err != nil {
		fatalf("parse flags: %v", err)
	}

	doc, err := loadDocument(*irPath)
	if err != nil {
		fatalf("load ir: %v", err)
	}

	switch command {
	case "openapi":
		if err := generateOpenAPI(doc, *openapiOut); err != nil {
			fatalf("generate openapi: %v", err)
		}
	case "server":
		if err := generateServer(doc, *serverOut, *requestModelsOut); err != nil {
			fatalf("generate server: %v", err)
		}
	case "cli":
		if err := generateCLI(doc, *cliOut); err != nil {
			fatalf("generate cli: %v", err)
		}
	case "all":
		if err := generateOpenAPI(doc, *openapiOut); err != nil {
			fatalf("generate openapi: %v", err)
		}
		if err := generateServer(doc, *serverOut, *requestModelsOut); err != nil {
			fatalf("generate server: %v", err)
		}
		if err := generateCLI(doc, *cliOut); err != nil {
			fatalf("generate cli: %v", err)
		}
	default:
		fatalf("unsupported command %q (supported: openapi, server, cli, all)", command)
	}
}

func generateOpenAPI(doc ir.Document, outPath string) error {
	b, err := openapiemit.EmitYAML(doc)
	if err != nil {
		return fmt.Errorf("emit openapi: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o750); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	if err := os.WriteFile(outPath, b, 0o600); err != nil {
		return fmt.Errorf("write openapi output: %w", err)
	}
	return nil
}

func generateServer(doc ir.Document, outPath string, requestModelsOutPath string) error {
	if err := servergoemit.ValidateOperationIDs(doc); err != nil {
		return fmt.Errorf("validate operation ids: %w", err)
	}
	legacyDir := filepath.Dir(outPath)
	legacyServerPath := filepath.Join(legacyDir, "server.gen.go")
	legacyTypesPath := filepath.Join(legacyDir, "types.gen.go")
	b, err := servergoemit.EmitWithLegacyResponses(doc, legacyServerPath, legacyTypesPath)
	if err != nil {
		return fmt.Errorf("emit server go: %w", err)
	}
	formatted, err := format.Source(b)
	if err != nil {
		return fmt.Errorf("format server go output: %w", err)
	}
	if err := writeFile(outPath, formatted); err != nil {
		return err
	}
	requestModels, err := requestmodelgoemit.EmitWithResponseRoots(doc, legacyTypesPath, legacyServerPath)
	if err != nil {
		return fmt.Errorf("emit request models go: %w", err)
	}
	formattedRequestModels, err := format.Source(requestModels)
	if err != nil {
		return fmt.Errorf("format request models go output: %w", err)
	}
	if err := writeFile(requestModelsOutPath, formattedRequestModels); err != nil {
		return err
	}
	return nil
}

func generateCLI(doc ir.Document, outPath string) error {
	b, err := cligoemit.Emit(doc)
	if err != nil {
		return fmt.Errorf("emit cli go: %w", err)
	}
	formatted, err := format.Source(b)
	if err != nil {
		return fmt.Errorf("format cli go output: %w", err)
	}
	if err := writeFile(outPath, formatted); err != nil {
		return err
	}
	return nil
}

func loadDocument(path string) (ir.Document, error) {
	doc, err := ir.Load(path)
	if err != nil {
		return ir.Document{}, fmt.Errorf("load ir document: %w", err)
	}
	return doc, nil
}

func writeFile(outPath string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(outPath), 0o750); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	content = bytes.TrimSpace(content)
	content = append(content, '\n')
	if err := os.WriteFile(outPath, content, 0o600); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
