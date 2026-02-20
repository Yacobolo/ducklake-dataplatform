// Command declarative-schema-gen generates declarative JSON Schema artifacts.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"duck-demo/internal/declarative"
)

var (
	grantPrincipalTypes = []string{"user", "group"}
	grantSecurableTypes = []string{"catalog", "schema", "table", "external_location", "storage_credential", "volume"}
	grantPrivileges     = []string{
		"SELECT",
		"INSERT",
		"UPDATE",
		"DELETE",
		"USE_CATALOG",
		"USE_SCHEMA",
		"USAGE",
		"CREATE_TABLE",
		"CREATE_VIEW",
		"CREATE_SCHEMA",
		"MODIFY",
		"MANAGE",
		"APPLY_TAG",
		"MANAGE_TAGS",
		"MANAGE_POLICIES",
		"ALL_PRIVILEGES",
		"CREATE_EXTERNAL_LOCATION",
		"CREATE_STORAGE_CREDENTIAL",
		"CREATE_VOLUME",
		"READ_VOLUME",
		"WRITE_VOLUME",
		"READ_FILES",
		"WRITE_FILES",
		"MANAGE_COMPUTE",
		"MANAGE_PIPELINES",
	}
)

type schemaGenerator struct {
	defs map[string]map[string]interface{}
}

func newSchemaGenerator() *schemaGenerator {
	return &schemaGenerator{defs: make(map[string]map[string]interface{})}
}

func (g *schemaGenerator) typeSchema(t reflect.Type) map[string]interface{} {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.String:
		return map[string]interface{}{"type": "string"}
	case reflect.Bool:
		return map[string]interface{}{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]interface{}{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]interface{}{"type": "number"}
	case reflect.Slice, reflect.Array:
		return map[string]interface{}{"type": "array", "items": g.typeSchema(t.Elem())}
	case reflect.Map:
		return map[string]interface{}{"type": "object", "additionalProperties": g.typeSchema(t.Elem())}
	case reflect.Struct:
		name := t.Name()
		if name == "" {
			return map[string]interface{}{"type": "object", "additionalProperties": true}
		}
		if _, ok := g.defs[name]; !ok {
			g.defs[name] = g.buildStructDefinition(t)
		}
		return map[string]interface{}{"$ref": "#/$defs/" + name}
	default:
		return map[string]interface{}{}
	}
}

func (g *schemaGenerator) buildStructDefinition(t reflect.Type) map[string]interface{} {
	properties := map[string]interface{}{}
	required := make([]string, 0)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		tag := field.Tag.Get("yaml")
		if tag == "-" {
			continue
		}

		name, omitEmpty := yamlFieldName(field.Name, tag)
		if name == "" {
			continue
		}

		properties[name] = g.typeSchema(field.Type)
		if !omitEmpty && field.Type.Kind() != reflect.Pointer && field.Type.Kind() != reflect.Slice && field.Type.Kind() != reflect.Map {
			required = append(required, name)
		}
	}

	sort.Strings(required)

	definition := map[string]interface{}{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		definition["required"] = required
	}

	return definition
}

func yamlFieldName(fieldName, yamlTag string) (string, bool) {
	if yamlTag == "" {
		return lowerFirst(fieldName), false
	}
	parts := strings.Split(yamlTag, ",")
	name := parts[0]
	omitEmpty := false
	for _, part := range parts[1:] {
		if part == "omitempty" {
			omitEmpty = true
			break
		}
	}
	return name, omitEmpty
}

func lowerFirst(value string) string {
	if value == "" {
		return value
	}
	return strings.ToLower(value[:1]) + value[1:]
}

func getDefProperty(defs map[string]map[string]interface{}, defName, propName string) map[string]interface{} {
	def, ok := defs[defName]
	if !ok {
		return nil
	}
	props, ok := def["properties"].(map[string]interface{})
	if !ok {
		return nil
	}
	prop, ok := props[propName].(map[string]interface{})
	if !ok {
		return nil
	}
	return prop
}

func addAllOfRule(def map[string]interface{}, rule map[string]interface{}) {
	existing, ok := def["allOf"]
	if !ok {
		def["allOf"] = []interface{}{rule}
		return
	}
	list, ok := existing.([]interface{})
	if !ok {
		def["allOf"] = []interface{}{rule}
		return
	}
	def["allOf"] = append(list, rule)
}

func setStringEnum(defs map[string]map[string]interface{}, defName, propName string, values []string) {
	prop := getDefProperty(defs, defName, propName)
	if prop == nil {
		return
	}
	prop["enum"] = values
}

func applyKindConstraints(kind string, defs map[string]map[string]interface{}) {
	switch kind {
	case declarative.KindNamePrincipalList:
		setStringEnum(defs, "PrincipalSpec", "type", []string{"user", "service_principal"})

	case declarative.KindNameGrantList:
		setStringEnum(defs, "GrantSpec", "principal_type", grantPrincipalTypes)
		setStringEnum(defs, "GrantSpec", "securable_type", grantSecurableTypes)
		setStringEnum(defs, "GrantSpec", "privilege", grantPrivileges)

	case declarative.KindNameTable:
		setStringEnum(defs, "TableSpec", "table_type", []string{"", "MANAGED", "EXTERNAL"})
		if tableSpec, ok := defs["TableSpec"]; ok {
			addAllOfRule(tableSpec, map[string]interface{}{
				"if": map[string]interface{}{
					"properties": map[string]interface{}{
						"table_type": map[string]interface{}{"const": "EXTERNAL"},
					},
				},
				"then": map[string]interface{}{
					"required": []string{"source_path", "file_format"},
				},
			})
		}

	case declarative.KindNameAPIKeyList:
		prop := getDefProperty(defs, "APIKeySpec", "expires_at")
		if prop != nil {
			prop["format"] = "date-time"
		}

	case declarative.KindNameMacro:
		setStringEnum(defs, "MacroSpec", "macro_type", []string{"", "SCALAR", "TABLE"})
		setStringEnum(defs, "MacroSpec", "visibility", []string{"", "project", "catalog_global", "system"})
		setStringEnum(defs, "MacroSpec", "status", []string{"", "ACTIVE", "DEPRECATED"})

		macroSpec, ok := defs["MacroSpec"]
		if !ok {
			return
		}
		addAllOfRule(macroSpec, map[string]interface{}{
			"if": map[string]interface{}{
				"properties": map[string]interface{}{
					"visibility": map[string]interface{}{"const": "project"},
				},
			},
			"then": map[string]interface{}{
				"required": []string{"project_name"},
			},
		})
		addAllOfRule(macroSpec, map[string]interface{}{
			"if": map[string]interface{}{
				"properties": map[string]interface{}{
					"visibility": map[string]interface{}{"const": "catalog_global"},
				},
			},
			"then": map[string]interface{}{
				"required": []string{"catalog_name"},
			},
		})
		addAllOfRule(macroSpec, map[string]interface{}{
			"if": map[string]interface{}{
				"properties": map[string]interface{}{
					"visibility": map[string]interface{}{"const": "system"},
				},
			},
			"then": map[string]interface{}{
				"properties": map[string]interface{}{
					"project_name": map[string]interface{}{"maxLength": 0},
					"catalog_name": map[string]interface{}{"maxLength": 0},
				},
			},
		})
	}
}

func encodeCanonicalJSON(path string, content interface{}) (string, error) {
	data, err := json.MarshalIndent(content, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal %s: %w", path, err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

func main() {
	var outDir string
	flag.StringVar(&outDir, "outdir", "schemas/declarative/v1", "Output schema directory")
	flag.Parse()

	kindsDir := filepath.Join(outDir, "kinds")
	if err := os.MkdirAll(kindsDir, 0o750); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "create output directories: %v\n", err)
		os.Exit(1)
	}

	registry := declarative.SchemaDocumentTypes()
	checksums := map[string]string{}
	oneOf := make([]map[string]interface{}, 0, len(registry))

	for _, doc := range registry {
		gen := newSchemaGenerator()
		rootRef := gen.typeSchema(doc.Type)

		schema := map[string]interface{}{
			"$schema": "https://json-schema.org/draft/2020-12/schema",
			"$id":     "schemas/declarative/v1/kinds/" + doc.FileName + ".schema.json",
			"title":   "Duck declarative " + doc.Kind,
			"allOf":   []map[string]interface{}{rootRef},
			"$defs":   gen.defs,
		}

		if def, ok := gen.defs[doc.Type.Name()]; ok {
			if props, ok := def["properties"].(map[string]interface{}); ok {
				if apiVersion, ok := props["apiVersion"].(map[string]interface{}); ok {
					apiVersion["enum"] = []string{declarative.SupportedAPIVersion}
				}
				if kind, ok := props["kind"].(map[string]interface{}); ok {
					kind["enum"] = []string{doc.Kind}
				}
			}
		}

		applyKindConstraints(doc.Kind, gen.defs)

		relPath := filepath.ToSlash(filepath.Join("kinds", doc.FileName+".schema.json"))
		fullPath := filepath.Join(outDir, relPath)
		hash, err := encodeCanonicalJSON(fullPath, schema)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		checksums[relPath] = hash

		oneOf = append(oneOf, map[string]interface{}{
			"$ref": relPath,
		})
	}

	rootSchema := map[string]interface{}{
		"$schema":     "https://json-schema.org/draft/2020-12/schema",
		"$id":         "schemas/declarative/v1/duck.declarative.schema.json",
		"title":       "Duck declarative document",
		"description": "Union schema for all declarative duck/v1 YAML documents.",
		"oneOf":       oneOf,
	}
	rootPath := filepath.Join(outDir, "duck.declarative.schema.json")
	rootHash, err := encodeCanonicalJSON(rootPath, rootSchema)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	checksums["duck.declarative.schema.json"] = rootHash

	manifest := map[string]interface{}{
		"version":    "v1",
		"apiVersion": declarative.SupportedAPIVersion,
		"files":      checksums,
	}
	if _, err := encodeCanonicalJSON(filepath.Join(outDir, "index.json"), manifest); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
