// Package main provides the apigen CLI entrypoint.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"

	cligoemit "duck-demo/apigen/emit/cligo"
	openapiemit "duck-demo/apigen/emit/openapi"
	requestmodelgoemit "duck-demo/apigen/emit/requestmodelgo"
	servergoemit "duck-demo/apigen/emit/servergo"
	"duck-demo/apigen/ir"
	"go.yaml.in/yaml/v4"
)

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: apigen <openapi|server|cli|all> -ir <path>")
	}

	command := os.Args[1]
	fs := flag.NewFlagSet(command, flag.ExitOnError)
	irPath := fs.String("ir", "api/gen/json-ir.json", "input JSON IR path")
	openapiOut := fs.String("openapi-out", "api/gen/openapi.apigen.yaml", "output OpenAPI YAML path for optional debug/compat emission")
	canonicalOpenAPIPath := fs.String("canonical-openapi", "api/gen/openapi.yaml", "canonical OpenAPI YAML path to embed into generated server code")
	serverOut := fs.String("server-out", "internal/api/server.apigen.gen.go", "output server Go path")
	serverPackage := fs.String("server-package", "api", "generated server Go package name")
	requestModelsOut := fs.String("request-models-out", "internal/api/gen_request_models.gen.go", "output APIGen request models Go path")
	requestModelsPackage := fs.String("request-models-package", "api", "generated request models Go package name")
	compatTypesOut := fs.String("compat-types-out", "", "optional output path for standalone APIGen-owned compatibility schema types")
	compatTypesPackage := fs.String("compat-types-package", "api", "generated compatibility schema types Go package name")
	cliOut := fs.String("cli-out", "pkg/cli/gen/apigen_registry.gen.go", "output CLI Go path")
	cliPackage := fs.String("cli-package", "gen", "generated CLI Go package name")
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
		if err := generateServer(doc, *serverOut, *serverPackage, *requestModelsOut, *requestModelsPackage, *compatTypesOut, *compatTypesPackage, *canonicalOpenAPIPath); err != nil {
			fatalf("generate server: %v", err)
		}
	case "cli":
		if err := generateCLI(doc, *cliOut, *cliPackage); err != nil {
			fatalf("generate cli: %v", err)
		}
	case "all":
		if err := generateServer(doc, *serverOut, *serverPackage, *requestModelsOut, *requestModelsPackage, *compatTypesOut, *compatTypesPackage, *canonicalOpenAPIPath); err != nil {
			fatalf("generate server: %v", err)
		}
		if err := generateCLI(doc, *cliOut, *cliPackage); err != nil {
			fatalf("generate cli: %v", err)
		}
	default:
		fatalf("unsupported command %q (supported: openapi, server, cli, all)", command)
	}
}

func generateOpenAPI(doc ir.Document, outPath string) error {
	b, err := openapiemit.EmitYAML(doc, openapiemit.Options{})
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

func generateServer(doc ir.Document, outPath string, serverPackage string, requestModelsOutPath string, requestModelsPackage string, compatTypesOutPath string, compatTypesPackage string, canonicalOpenAPIPath string) error {
	if err := servergoemit.ValidateOperationIDs(doc); err != nil {
		return fmt.Errorf("validate operation ids: %w", err)
	}
	embeddedSpecJSON, err := loadOpenAPIAsJSON(canonicalOpenAPIPath)
	if err != nil {
		return fmt.Errorf("load canonical openapi: %w", err)
	}
	b, err := servergoemit.EmitWithLegacyResponsesAndSpec(doc, servergoemit.Options{
		PackageName:             serverPackage,
		EmbeddedOpenAPISpecJSON: embeddedSpecJSON,
	})
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
	requestModels, err := requestmodelgoemit.EmitWithResponseRoots(doc, requestmodelgoemit.Options{
		PackageName: requestModelsPackage,
	})
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
	if compatTypesOutPath != "" {
		compatTypes, err := requestmodelgoemit.EmitStandaloneCompatibilityTypes(doc, requestmodelgoemit.Options{
			PackageName: compatTypesPackage,
		})
		if err != nil {
			return fmt.Errorf("emit compatibility types go: %w", err)
		}
		formattedCompatTypes, err := format.Source(compatTypes)
		if err != nil {
			return fmt.Errorf("format compatibility types go output: %w", err)
		}
		if err := writeFile(compatTypesOutPath, formattedCompatTypes); err != nil {
			return err
		}
	}
	return nil
}

func generateCLI(doc ir.Document, outPath string, packageName string) error {
	b, err := cligoemit.Emit(doc, cligoemit.Options{PackageName: packageName})
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

func loadOpenAPIAsJSON(path string) (string, error) {
	//nolint:gosec // Path comes from the checked-in generation pipeline inputs.
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read openapi file: %w", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(content, &doc); err != nil {
		return "", fmt.Errorf("decode openapi yaml: %w", err)
	}
	marshaled, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("marshal openapi json: %w", err)
	}
	return string(marshaled), nil
}

func fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
