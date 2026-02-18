package declarative

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type manifest struct {
	APIVersion string            `json:"apiVersion"`
	Version    string            `json:"version"`
	Files      map[string]string `json:"files"`
}

type fieldDoc struct {
	Name        string
	Type        string
	Required    bool
	Description string
}

type kindDoc struct {
	Title    string
	KindName string
	File     string
	Checksum string
	Fields   []fieldDoc
}

// Generate renders declarative schema docs into markdown files.
func Generate(indexPath, schemaDir, outDir string) error {
	indexBytes, err := os.ReadFile(indexPath)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	var m manifest
	if err := json.Unmarshal(indexBytes, &m); err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}

	if err := os.RemoveAll(outDir); err != nil {
		return fmt.Errorf("clean output dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(outDir, "kinds"), 0o755); err != nil {
		return fmt.Errorf("create kinds dir: %w", err)
	}

	kindFiles := make([]string, 0, len(m.Files))
	for file := range m.Files {
		if strings.HasPrefix(file, "kinds/") && strings.HasSuffix(file, ".schema.json") {
			kindFiles = append(kindFiles, file)
		}
	}
	sort.Strings(kindFiles)

	docs := make([]kindDoc, 0, len(kindFiles))
	for _, relFile := range kindFiles {
		absPath := filepath.Join(schemaDir, relFile)
		doc, err := parseKindSchema(absPath)
		if err != nil {
			return fmt.Errorf("parse kind schema %q: %w", relFile, err)
		}
		doc.File = relFile
		doc.Checksum = m.Files[relFile]
		docs = append(docs, doc)

		outPath := filepath.Join(outDir, "kinds", slug(trimSchemaSuffix(filepath.Base(relFile)))+".md")
		if err := writeKindPage(outPath, doc); err != nil {
			return err
		}
	}

	if err := writeIndexPage(filepath.Join(outDir, "index.md"), m, docs); err != nil {
		return err
	}

	return nil
}

func parseKindSchema(path string) (kindDoc, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return kindDoc{}, fmt.Errorf("read file: %w", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(bytes, &payload); err != nil {
		return kindDoc{}, fmt.Errorf("decode json: %w", err)
	}

	title := asString(payload["title"])
	defs := asMap(payload["$defs"])

	rootDefName := ""
	allOf := asSlice(payload["allOf"])
	if len(allOf) > 0 {
		ref := asString(asMap(allOf[0])["$ref"])
		rootDefName = refName(ref)
	}
	if rootDefName == "" {
		for name := range defs {
			if strings.HasSuffix(name, "Doc") {
				rootDefName = name
				break
			}
		}
	}
	if rootDefName == "" {
		return kindDoc{}, fmt.Errorf("could not resolve root definition")
	}

	rootDef := asMap(defs[rootDefName])
	rootProps := asMap(rootDef["properties"])
	kindProp := asMap(rootProps["kind"])
	kindName := ""
	enums := asSlice(kindProp["enum"])
	if len(enums) > 0 {
		kindName = asString(enums[0])
	}
	if kindName == "" {
		kindName = strings.TrimSuffix(rootDefName, "Doc")
	}

	props := map[string]any{}
	requiredSet := map[string]struct{}{}

	specProp := asMap(rootProps["spec"])
	if specRefName := refName(asString(specProp["$ref"])); specRefName != "" {
		specDef := asMap(defs[specRefName])
		props = asMap(specDef["properties"])
		for _, value := range asSlice(specDef["required"]) {
			requiredSet[asString(value)] = struct{}{}
		}
	} else {
		for name, value := range rootProps {
			if name == "apiVersion" || name == "kind" || name == "metadata" {
				continue
			}
			props[name] = value
		}
		for _, value := range asSlice(rootDef["required"]) {
			fieldName := asString(value)
			if fieldName == "apiVersion" || fieldName == "kind" || fieldName == "metadata" {
				continue
			}
			requiredSet[fieldName] = struct{}{}
		}
	}

	propNames := make([]string, 0, len(props))
	for name := range props {
		propNames = append(propNames, name)
	}
	sort.Strings(propNames)

	fields := make([]fieldDoc, 0, len(propNames))
	for _, name := range propNames {
		fieldSchema := asMap(props[name])
		_, required := requiredSet[name]
		fields = append(fields, fieldDoc{
			Name:        name,
			Type:        schemaType(fieldSchema),
			Required:    required,
			Description: asString(fieldSchema["description"]),
		})
	}

	return kindDoc{
		Title:    title,
		KindName: kindName,
		Fields:   fields,
	}, nil
}

func writeIndexPage(path string, m manifest, docs []kindDoc) error {
	var b strings.Builder
	b.WriteString(generatedHeader())
	b.WriteString("# Declarative Reference\n\n")
	b.WriteString("This section is generated from versioned JSON Schema artifacts.\n\n")
	b.WriteString("- API version: `")
	b.WriteString(m.APIVersion)
	b.WriteString("`\n")
	b.WriteString("- Schema version: `")
	b.WriteString(m.Version)
	b.WriteString("`\n\n")
	b.WriteString("## Kinds\n\n")
	for _, doc := range docs {
		name := trimSchemaSuffix(filepath.Base(doc.File))
		b.WriteString("- [")
		b.WriteString(doc.KindName)
		b.WriteString("](./kinds/")
		b.WriteString(slug(name))
		b.WriteString(")")
		b.WriteString(" (`")
		b.WriteString(doc.File)
		b.WriteString("`)\n")
	}

	b.WriteString("\n## Checksums\n\n")
	b.WriteString("| File | SHA256 |\n")
	b.WriteString("| --- | --- |\n")
	for _, doc := range docs {
		b.WriteString("| `")
		b.WriteString(doc.File)
		b.WriteString("` | `")
		b.WriteString(doc.Checksum)
		b.WriteString("` |\n")
	}

	return writeFile(path, b.String())
}

func writeKindPage(path string, doc kindDoc) error {
	var b strings.Builder
	b.WriteString(generatedHeader())
	b.WriteString("# Kind: `")
	b.WriteString(doc.KindName)
	b.WriteString("`\n\n")
	if doc.Title != "" {
		b.WriteString(doc.Title)
		b.WriteString("\n\n")
	}
	b.WriteString("- Schema file: `")
	b.WriteString(doc.File)
	b.WriteString("`\n")
	b.WriteString("- SHA256: `")
	b.WriteString(doc.Checksum)
	b.WriteString("`\n\n")

	b.WriteString("## Spec Fields\n\n")
	b.WriteString("| Name | Type | Required | Description |\n")
	b.WriteString("| --- | --- | --- | --- |\n")
	for _, field := range doc.Fields {
		b.WriteString("| `")
		b.WriteString(field.Name)
		b.WriteString("` | `")
		b.WriteString(field.Type)
		b.WriteString("` | `")
		if field.Required {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
		b.WriteString("` | ")
		desc := strings.TrimSpace(strings.ReplaceAll(field.Description, "\n", " "))
		desc = strings.ReplaceAll(desc, "|", "\\|")
		if desc == "" {
			desc = "-"
		}
		b.WriteString(desc)
		b.WriteString(" |\n")
	}

	return writeFile(path, b.String())
}

func schemaType(schema map[string]any) string {
	if ref := asString(schema["$ref"]); ref != "" {
		return refName(ref)
	}

	typeValue := schema["type"]
	switch typed := typeValue.(type) {
	case string:
		if typed == "array" {
			items := asMap(schema["items"])
			if len(items) == 0 {
				return "array"
			}
			return "array[" + schemaType(items) + "]"
		}
		return typed
	case []any:
		parts := make([]string, 0, len(typed))
		for _, value := range typed {
			parts = append(parts, asString(value))
		}
		sort.Strings(parts)
		return strings.Join(parts, " | ")
	}

	enums := asSlice(schema["enum"])
	if len(enums) > 0 {
		vals := make([]string, 0, len(enums))
		for _, enumValue := range enums {
			vals = append(vals, asString(enumValue))
		}
		return "enum(" + strings.Join(vals, ", ") + ")"
	}

	return "object"
}

func refName(ref string) string {
	if ref == "" {
		return ""
	}
	parts := strings.Split(ref, "/")
	return parts[len(parts)-1]
}

func trimSchemaSuffix(name string) string {
	return strings.TrimSuffix(name, ".schema.json")
}

func slug(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, " ", "-")
	value = strings.ReplaceAll(value, "/", "-")
	value = strings.ReplaceAll(value, "_", "-")
	for strings.Contains(value, "--") {
		value = strings.ReplaceAll(value, "--", "-")
	}
	return strings.Trim(value, "-")
}

func generatedHeader() string {
	return "<!-- Code generated by cmd/docsgen. DO NOT EDIT. -->\n\n"
}

func writeFile(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create directory %q: %w", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write %q: %w", path, err)
	}
	return nil
}

func asString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", typed)
	}
}

func asMap(value any) map[string]any {
	m, ok := value.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return m
}

func asSlice(value any) []any {
	s, ok := value.([]any)
	if !ok {
		return nil
	}
	return s
}
