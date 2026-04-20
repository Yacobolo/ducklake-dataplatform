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
	"strings"

	"github.com/Yacobolo/quackstack/pkg/apigen/cuegen"
	cligoemit "github.com/Yacobolo/quackstack/pkg/apigen/emit/cligo"
	openapiemit "github.com/Yacobolo/quackstack/pkg/apigen/emit/openapi"
	requestmodelgoemit "github.com/Yacobolo/quackstack/pkg/apigen/emit/requestmodelgo"
	servergoemit "github.com/Yacobolo/quackstack/pkg/apigen/emit/servergo"
	"github.com/Yacobolo/quackstack/pkg/apigen/ir"
	"go.yaml.in/yaml/v4"
)

type commandConfig struct {
	IRPath               string
	IROut                string
	OpenAPIOut           string
	CanonicalOpenAPIPath string
	CueDir               string
	CueOutDir            string
	ServerOut            string
	ServerPackage        string
	RequestModelsOut     string
	RequestModelsPackage string
	CompatTypesOut       string
	CompatTypesPackage   string
	CLIOut               string
	CLIPackage           string
	GenerateCLI          bool
}

type targetManifest struct {
	Targets []targetSpec `yaml:"targets"`
}

type targetSpec struct {
	Name                 string `yaml:"name"`
	CueDir               string `yaml:"cue_dir"`
	IROut                string `yaml:"ir_out"`
	OpenAPIOut           string `yaml:"openapi_out"`
	ServerOut            string `yaml:"server_out"`
	ServerPackage        string `yaml:"server_package"`
	RequestModelsOut     string `yaml:"request_models_out"`
	RequestModelsPackage string `yaml:"request_models_package"`
	CompatTypesOut       string `yaml:"compat_types_out"`
	CompatTypesPackage   string `yaml:"compat_types_package"`
	CLIOut               string `yaml:"cli_out"`
	CLIPackage           string `yaml:"cli_package"`
	GenerateCLI          bool   `yaml:"generate_cli"`
}

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: apigen <openapi|server|cli|all|cue-compile|cue-bootstrap> -ir <path>")
	}

	command := os.Args[1]
	fs := flag.NewFlagSet(command, flag.ExitOnError)
	manifestPath := fs.String("manifest", "", "optional APIGen target manifest path")
	targetName := fs.String("target", "", "manifest target name")
	irPath := fs.String("ir", "gen/json-ir.json", "input JSON IR path")
	irOut := fs.String("ir-out", "gen/json-ir.json", "output JSON IR path for CUE compilation")
	openapiOut := fs.String("openapi-out", "gen/openapi.yaml", "output OpenAPI YAML path for optional debug/compat emission")
	canonicalOpenAPIPath := fs.String("canonical-openapi", "gen/openapi.yaml", "canonical OpenAPI YAML path to embed into generated server code")
	cueDir := fs.String("cue-dir", "api/cue", "input CUE API source directory")
	cueOutDir := fs.String("cue-out-dir", "api/cue", "output CUE API source directory")
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

	config, err := resolveCommandConfig(command, *manifestPath, *targetName, commandConfig{
		IRPath:               *irPath,
		IROut:                *irOut,
		OpenAPIOut:           *openapiOut,
		CanonicalOpenAPIPath: *canonicalOpenAPIPath,
		CueDir:               *cueDir,
		CueOutDir:            *cueOutDir,
		ServerOut:            *serverOut,
		ServerPackage:        *serverPackage,
		RequestModelsOut:     *requestModelsOut,
		RequestModelsPackage: *requestModelsPackage,
		CompatTypesOut:       *compatTypesOut,
		CompatTypesPackage:   *compatTypesPackage,
		CLIOut:               *cliOut,
		CLIPackage:           *cliPackage,
		GenerateCLI:          true,
	})
	if err != nil {
		fatalf("resolve command config: %v", err)
	}

	switch command {
	case "openapi":
		doc, err := loadDocument(config.IRPath)
		if err != nil {
			fatalf("load ir: %v", err)
		}
		if err := generateOpenAPI(doc, config.OpenAPIOut); err != nil {
			fatalf("generate openapi: %v", err)
		}
	case "cue-compile":
		if err := compileCUE(config.CueDir, config.IROut, config.OpenAPIOut); err != nil {
			fatalf("compile cue: %v", err)
		}
	case "cue-bootstrap":
		if err := bootstrapCUE(config.IRPath, config.CueOutDir); err != nil {
			fatalf("bootstrap cue: %v", err)
		}
	case "server":
		doc, err := loadDocument(config.IRPath)
		if err != nil {
			fatalf("load ir: %v", err)
		}
		if err := generateServer(doc, config.ServerOut, config.ServerPackage, config.RequestModelsOut, config.RequestModelsPackage, config.CompatTypesOut, config.CompatTypesPackage, config.CanonicalOpenAPIPath); err != nil {
			fatalf("generate server: %v", err)
		}
	case "cli":
		if !config.GenerateCLI {
			fatalf("generate cli: target %q has generate_cli=false", *targetName)
		}
		doc, err := loadDocument(config.IRPath)
		if err != nil {
			fatalf("load ir: %v", err)
		}
		if err := generateCLI(doc, config.CLIOut, config.CLIPackage); err != nil {
			fatalf("generate cli: %v", err)
		}
	case "all":
		doc, err := loadDocument(config.IRPath)
		if err != nil {
			fatalf("load ir: %v", err)
		}
		if err := generateServer(doc, config.ServerOut, config.ServerPackage, config.RequestModelsOut, config.RequestModelsPackage, config.CompatTypesOut, config.CompatTypesPackage, config.CanonicalOpenAPIPath); err != nil {
			fatalf("generate server: %v", err)
		}
		if config.GenerateCLI {
			if err := generateCLI(doc, config.CLIOut, config.CLIPackage); err != nil {
				fatalf("generate cli: %v", err)
			}
		}
	default:
		fatalf("unsupported command %q (supported: openapi, server, cli, all, cue-compile, cue-bootstrap)", command)
	}
}

func resolveCommandConfig(command string, manifestPath string, targetName string, defaults commandConfig) (commandConfig, error) {
	if strings.TrimSpace(manifestPath) == "" {
		return defaults, nil
	}

	target, err := loadTargetSpec(manifestPath, targetName)
	if err != nil {
		return commandConfig{}, err
	}

	config := defaults
	config.CueDir = target.CueDir
	config.CueOutDir = target.CueDir
	config.IRPath = target.IROut
	config.IROut = target.IROut
	config.OpenAPIOut = target.OpenAPIOut
	config.CanonicalOpenAPIPath = target.OpenAPIOut
	config.ServerOut = target.ServerOut
	config.ServerPackage = coalesceString(target.ServerPackage, defaults.ServerPackage)
	config.RequestModelsOut = target.RequestModelsOut
	config.RequestModelsPackage = coalesceString(target.RequestModelsPackage, defaults.RequestModelsPackage)
	config.CompatTypesOut = target.CompatTypesOut
	config.CompatTypesPackage = coalesceString(target.CompatTypesPackage, defaults.CompatTypesPackage)
	config.CLIOut = target.CLIOut
	config.CLIPackage = coalesceString(target.CLIPackage, defaults.CLIPackage)
	config.GenerateCLI = target.GenerateCLI

	if err := validateCommandConfig(command, config); err != nil {
		return commandConfig{}, err
	}

	return config, nil
}

func loadTargetSpec(manifestPath string, targetName string) (targetSpec, error) {
	if strings.TrimSpace(targetName) == "" {
		return targetSpec{}, fmt.Errorf("-target is required when -manifest is set")
	}

	content, err := os.ReadFile(filepath.Clean(manifestPath))
	if err != nil {
		return targetSpec{}, fmt.Errorf("read manifest: %w", err)
	}

	var manifest targetManifest
	if err := yaml.Unmarshal(content, &manifest); err != nil {
		return targetSpec{}, fmt.Errorf("decode manifest: %w", err)
	}

	manifestDir := filepath.Dir(filepath.Clean(manifestPath))
	for _, target := range manifest.Targets {
		if target.Name != targetName {
			continue
		}
		return resolveTargetPaths(target, manifestDir), nil
	}

	return targetSpec{}, fmt.Errorf("target %q not found in manifest", targetName)
}

func resolveTargetPaths(target targetSpec, baseDir string) targetSpec {
	target.CueDir = resolveManifestPath(baseDir, target.CueDir)
	target.IROut = resolveManifestPath(baseDir, target.IROut)
	target.OpenAPIOut = resolveManifestPath(baseDir, target.OpenAPIOut)
	target.ServerOut = resolveManifestPath(baseDir, target.ServerOut)
	target.RequestModelsOut = resolveManifestPath(baseDir, target.RequestModelsOut)
	target.CompatTypesOut = resolveManifestPath(baseDir, target.CompatTypesOut)
	target.CLIOut = resolveManifestPath(baseDir, target.CLIOut)
	return target
}

func resolveManifestPath(baseDir string, value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	if filepath.IsAbs(value) {
		return filepath.Clean(value)
	}
	return filepath.Join(baseDir, value)
}

func validateCommandConfig(command string, config commandConfig) error {
	switch command {
	case "cue-compile":
		if config.CueDir == "" || config.IROut == "" || config.OpenAPIOut == "" {
			return fmt.Errorf("manifest target must declare cue_dir, ir_out, and openapi_out")
		}
	case "cue-bootstrap":
		if config.IRPath == "" || config.CueOutDir == "" {
			return fmt.Errorf("manifest target must declare cue_dir and ir_out")
		}
	case "openapi":
		if config.IRPath == "" || config.OpenAPIOut == "" {
			return fmt.Errorf("manifest target must declare ir_out and openapi_out")
		}
	case "server":
		if config.IRPath == "" || config.OpenAPIOut == "" || config.ServerOut == "" || config.RequestModelsOut == "" {
			return fmt.Errorf("manifest target must declare ir_out, openapi_out, server_out, and request_models_out")
		}
	case "cli":
		if config.IRPath == "" || config.CLIOut == "" {
			return fmt.Errorf("manifest target must declare ir_out and cli_out")
		}
	case "all":
		if config.IRPath == "" || config.OpenAPIOut == "" || config.ServerOut == "" || config.RequestModelsOut == "" {
			return fmt.Errorf("manifest target must declare ir_out, openapi_out, server_out, and request_models_out")
		}
		if config.GenerateCLI && config.CLIOut == "" {
			return fmt.Errorf("manifest target with generate_cli=true must declare cli_out")
		}
	}
	return nil
}

func coalesceString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func compileCUE(cueDir string, irOutPath string, openAPIOutPath string) error {
	bundle, err := cuegen.CompileDir(cueDir)
	if err != nil {
		return err
	}
	if err := cuegen.WriteBundle(bundle, irOutPath, openAPIOutPath); err != nil {
		return err
	}
	return nil
}

func bootstrapCUE(irPath string, cueOutDir string) error {
	doc, err := loadDocument(irPath)
	if err != nil {
		return err
	}
	if err := cuegen.Bootstrap(doc, cueOutDir); err != nil {
		return err
	}
	return nil
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
