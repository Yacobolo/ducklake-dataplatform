// Package discovery renders CLI discovery metadata from docs, OpenAPI, and APIGen IR.
package discovery

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/getkin/kin-openapi/openapi3"
	apigenir "github.com/Yacobolo/quackstack/pkg/apigen/ir"
	"gopkg.in/yaml.v3"
)

type frontMatter struct {
	Title       string   `yaml:"title"`
	Description string   `yaml:"description"`
	Keywords    []string `yaml:"keywords"`
	CLICommands []string `yaml:"cli_commands"`
	OperationID []string `yaml:"operation_ids"`
	APITags     []string `yaml:"api_tags"`
}

type referenceDoc struct {
	ID           string
	Path         string
	Section      string
	Title        string
	Description  string
	Headings     []string
	Excerpt      string
	CodeExamples []string
	Keywords     []string
}

type referenceOperation struct {
	OperationID  string
	Method       string
	Path         string
	Tags         []string
	Summary      string
	Description  string
	Parameters   []referenceParam
	BodyFields   []referenceField
	CLICommand   string
	ContentTypes []string
}

type referenceParam struct {
	Name        string
	In          string
	Type        string
	Description string
	Required    bool
	Enum        []string
}

type referenceField struct {
	Name        string
	Type        string
	Description string
	Required    bool
	Enum        []string
}

type referenceLink struct {
	SourceKind string
	SourceID   string
	TargetKind string
	TargetID   string
	Reason     string
	Confidence int
}

// Generate renders discovery metadata into a generated Go file.
func Generate(docsDir, specPath, irPath, outPath string) error {
	docs, frontMatterByDocID, err := loadDocs(docsDir)
	if err != nil {
		return err
	}

	commandByOperation, err := loadCLICommands(irPath)
	if err != nil {
		return err
	}

	operations, err := loadOperations(specPath, commandByOperation)
	if err != nil {
		return err
	}

	links := buildLinks(docs, frontMatterByDocID, operations)
	sortDocs(docs)
	sortOperations(operations)
	sortLinks(links)

	specBytes, err := os.ReadFile(filepath.Clean(specPath)) // #nosec G304 -- trusted local build input
	if err != nil {
		return fmt.Errorf("read openapi spec: %w", err)
	}

	payload, err := renderGo(docs, operations, links, string(specBytes))
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(outPath), 0o750); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	if err := os.WriteFile(outPath, payload, 0o600); err != nil {
		return fmt.Errorf("write discovery index: %w", err)
	}
	return nil
}

func loadDocs(root string) ([]referenceDoc, map[string]frontMatter, error) {
	var docs []referenceDoc
	frontMatterByDocID := make(map[string]frontMatter)

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			rel, relErr := filepath.Rel(root, path)
			if relErr == nil {
				rel = filepath.ToSlash(rel)
				if rel == "reference/generated" || strings.HasPrefix(rel, "reference/generated/") ||
					rel == "node_modules" || strings.HasPrefix(rel, "node_modules/") ||
					rel == ".vitepress" || strings.HasPrefix(rel, ".vitepress/") {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if filepath.Ext(path) != ".md" {
			return nil
		}

		doc, fm, parseErr := parseDoc(root, path)
		if parseErr != nil {
			return parseErr
		}
		docs = append(docs, doc)
		frontMatterByDocID[doc.ID] = fm
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("walk docs: %w", err)
	}
	return docs, frontMatterByDocID, nil
}

func parseDoc(root, path string) (referenceDoc, frontMatter, error) {
	bytes, err := os.ReadFile(filepath.Clean(path)) // #nosec G304 -- trusted local docs tree
	if err != nil {
		return referenceDoc{}, frontMatter{}, fmt.Errorf("read doc %q: %w", path, err)
	}

	fm, body, err := splitFrontMatter(string(bytes))
	if err != nil {
		return referenceDoc{}, frontMatter{}, fmt.Errorf("parse front matter %q: %w", path, err)
	}

	relPath, err := filepath.Rel(root, path)
	if err != nil {
		return referenceDoc{}, frontMatter{}, fmt.Errorf("relative doc path %q: %w", path, err)
	}
	relPath = filepath.ToSlash(relPath)
	id := strings.TrimSuffix(relPath, filepath.Ext(relPath))
	parts := strings.Split(id, "/")
	section := ""
	if len(parts) > 0 {
		section = parts[0]
	}

	headings, excerpt, codeExamples, headingTitle := parseMarkdownBody(body)
	title := strings.TrimSpace(fm.Title)
	if title == "" {
		title = headingTitle
	}
	description := strings.TrimSpace(fm.Description)
	if description == "" {
		description = excerpt
	}
	keywords := docKeywords(id, title, headings, fm)

	return referenceDoc{
		ID:           id,
		Path:         relPath,
		Section:      section,
		Title:        title,
		Description:  description,
		Headings:     headings,
		Excerpt:      excerpt,
		CodeExamples: codeExamples,
		Keywords:     keywords,
	}, fm, nil
}

func splitFrontMatter(content string) (frontMatter, string, error) {
	var fm frontMatter
	if !strings.HasPrefix(content, "---\n") {
		return fm, content, nil
	}
	rest := strings.TrimPrefix(content, "---\n")
	idx := strings.Index(rest, "\n---\n")
	if idx < 0 {
		return fm, content, nil
	}
	raw := rest[:idx]
	body := rest[idx+5:]
	if err := yaml.Unmarshal([]byte(raw), &fm); err != nil {
		return fm, "", fmt.Errorf("decode yaml: %w", err)
	}
	return fm, body, nil
}

func parseMarkdownBody(body string) ([]string, string, []string, string) {
	lines := strings.Split(body, "\n")
	headings := make([]string, 0)
	codeExamples := make([]string, 0)
	title := ""
	excerpt := ""
	inCodeFence := false
	var codeFence strings.Builder
	var paragraph []string

	flushParagraph := func() {
		if excerpt != "" || len(paragraph) == 0 {
			paragraph = paragraph[:0]
			return
		}
		text := strings.TrimSpace(strings.Join(paragraph, " "))
		text = strings.Join(strings.Fields(text), " ")
		if text != "" {
			excerpt = text
		}
		paragraph = paragraph[:0]
	}

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") {
			if inCodeFence {
				block := strings.TrimSpace(codeFence.String())
				if block != "" {
					codeExamples = append(codeExamples, block)
				}
				codeFence.Reset()
			}
			inCodeFence = !inCodeFence
			continue
		}
		if inCodeFence {
			codeFence.WriteString(line)
			codeFence.WriteByte('\n')
			continue
		}
		if strings.HasPrefix(trimmed, "#") {
			flushParagraph()
			heading := strings.TrimSpace(strings.TrimLeft(trimmed, "#"))
			if heading != "" {
				headings = append(headings, heading)
				if title == "" && strings.HasPrefix(trimmed, "# ") {
					title = heading
				}
			}
			continue
		}
		if trimmed == "" {
			flushParagraph()
			continue
		}
		if strings.HasPrefix(trimmed, "<!--") || strings.HasPrefix(trimmed, "[!") {
			continue
		}
		paragraph = append(paragraph, trimmed)
	}
	flushParagraph()

	return headings, excerpt, codeExamples, title
}

func docKeywords(id, title string, headings []string, fm frontMatter) []string {
	words := make([]string, 0, 32)
	words = append(words, tokenize(id)...)
	words = append(words, tokenize(title)...)
	for _, heading := range headings {
		words = append(words, tokenize(heading)...)
	}
	for _, keyword := range fm.Keywords {
		words = append(words, tokenize(keyword)...)
	}
	for _, tag := range fm.APITags {
		words = append(words, tokenize(tag)...)
	}
	for _, opID := range fm.OperationID {
		words = append(words, tokenize(opID)...)
	}
	return dedupeStrings(words)
}

func loadOperations(specPath string, commandByOperation map[string]string) ([]referenceOperation, error) {
	loader := &openapi3.Loader{IsExternalRefsAllowed: true}
	spec, err := loader.LoadFromFile(specPath)
	if err != nil {
		return nil, fmt.Errorf("load spec: %w", err)
	}

	operations := make([]referenceOperation, 0, len(spec.Paths.Map()))
	for path, pathItem := range spec.Paths.Map() {
		for method, op := range pathItem.Operations() {
			operations = append(operations, buildOperation(path, method, pathItem, op, commandByOperation))
		}
	}
	return operations, nil
}

func loadCLICommands(irPath string) (map[string]string, error) {
	doc, err := apigenir.Load(irPath)
	if err != nil {
		return nil, fmt.Errorf("load apigen ir: %w", err)
	}
	commands := make(map[string]string, len(doc.Endpoints))
	for _, endpoint := range doc.Endpoints {
		command := apigenir.CLICommandString(endpoint.CLI)
		if strings.TrimSpace(command) == "" || strings.TrimSpace(endpoint.OperationID) == "" {
			continue
		}
		commands[endpoint.OperationID] = command
	}
	return commands, nil
}

func buildOperation(path, method string, pathItem *openapi3.PathItem, op *openapi3.Operation, commandByOperation map[string]string) referenceOperation {
	params := append([]*openapi3.ParameterRef{}, pathItem.Parameters...)
	params = append(params, op.Parameters...)

	operation := referenceOperation{
		OperationID: strings.TrimSpace(op.OperationID),
		Method:      strings.ToUpper(method),
		Path:        path,
		Tags:        append([]string(nil), op.Tags...),
		Summary:     strings.TrimSpace(op.Summary),
		Description: strings.TrimSpace(op.Description),
		CLICommand:  strings.TrimSpace(commandByOperation[strings.TrimSpace(op.OperationID)]),
	}

	for _, p := range params {
		if p == nil || p.Value == nil {
			continue
		}
		operation.Parameters = append(operation.Parameters, referenceParam{
			Name:        p.Value.Name,
			In:          p.Value.In,
			Type:        schemaTypeFromSchemaRef(p.Value.Schema),
			Description: strings.TrimSpace(p.Value.Description),
			Required:    p.Value.Required,
			Enum:        schemaEnumFromSchemaRef(p.Value.Schema),
		})
	}
	sort.Slice(operation.Parameters, func(i, j int) bool {
		if operation.Parameters[i].In == operation.Parameters[j].In {
			return operation.Parameters[i].Name < operation.Parameters[j].Name
		}
		return operation.Parameters[i].In < operation.Parameters[j].In
	})

	if op.RequestBody != nil && op.RequestBody.Value != nil {
		operation.ContentTypes = sortedKeys(op.RequestBody.Value.Content)
		if bodyRef := firstRequestSchema(op.RequestBody.Value.Content); bodyRef != nil {
			operation.BodyFields = collectBodyFields(bodyRef)
		}
	}

	return operation
}

func firstRequestSchema(content openapi3.Content) *openapi3.SchemaRef {
	for _, contentType := range sortedKeys(content) {
		media := content[contentType]
		if media == nil || media.Schema == nil {
			continue
		}
		return media.Schema
	}
	return nil
}

func collectBodyFields(schemaRef *openapi3.SchemaRef) []referenceField {
	if schemaRef == nil || schemaRef.Value == nil {
		return nil
	}
	schema := schemaRef.Value
	if schema.Type == nil || len(*schema.Type) == 0 || !strings.EqualFold((*schema.Type)[0], "object") || len(schema.Properties) == 0 {
		return nil
	}

	required := make(map[string]struct{}, len(schema.Required))
	for _, name := range schema.Required {
		required[name] = struct{}{}
	}

	names := sortedKeys(schema.Properties)
	fields := make([]referenceField, 0, len(names))
	for _, name := range names {
		prop := schema.Properties[name]
		if prop == nil {
			continue
		}
		_, isRequired := required[name]
		fields = append(fields, referenceField{
			Name:        name,
			Type:        schemaTypeFromSchemaRef(prop),
			Description: strings.TrimSpace(prop.Value.Description),
			Required:    isRequired,
			Enum:        schemaEnumFromSchemaRef(prop),
		})
	}
	return fields
}

func buildLinks(docs []referenceDoc, fmByDocID map[string]frontMatter, operations []referenceOperation) []referenceLink {
	links := make([]referenceLink, 0)
	docByID := make(map[string]referenceDoc, len(docs))
	opByID := make(map[string]referenceOperation, len(operations))
	for _, doc := range docs {
		docByID[doc.ID] = doc
	}
	for _, op := range operations {
		opByID[op.OperationID] = op
		if op.CLICommand != "" {
			links = append(links, referenceLink{
				SourceKind: "operation",
				SourceID:   op.OperationID,
				TargetKind: "command",
				TargetID:   op.CLICommand,
				Reason:     "apigen-command-spec",
				Confidence: 100,
			})
		}
	}

	for _, doc := range docs {
		fm := fmByDocID[doc.ID]
		for _, opID := range fm.OperationID {
			if _, ok := opByID[opID]; !ok {
				continue
			}
			links = append(links,
				referenceLink{SourceKind: "doc", SourceID: doc.ID, TargetKind: "operation", TargetID: opID, Reason: "frontmatter", Confidence: 100},
				referenceLink{SourceKind: "operation", SourceID: opID, TargetKind: "doc", TargetID: doc.ID, Reason: "frontmatter", Confidence: 100},
			)
		}
		for _, command := range fm.CLICommands {
			command = strings.TrimSpace(command)
			if command == "" {
				continue
			}
			links = append(links, referenceLink{
				SourceKind: "doc",
				SourceID:   doc.ID,
				TargetKind: "command",
				TargetID:   command,
				Reason:     "frontmatter",
				Confidence: 100,
			})
		}
	}

	for _, doc := range docs {
		docTokens := setFromTokens(doc.Keywords)
		for _, op := range operations {
			sharedTokens := overlapCount(docTokens, setFromTokens(operationTokens(op)))
			if sharedTokens < 2 {
				continue
			}
			confidence := 60 + minInt(sharedTokens*5, 25)
			if tagOverlap(fmByDocID[doc.ID].APITags, op.Tags) {
				confidence = 85
			}
			links = append(links,
				referenceLink{SourceKind: "doc", SourceID: doc.ID, TargetKind: "operation", TargetID: op.OperationID, Reason: keywordReason(fmByDocID[doc.ID], op), Confidence: confidence},
				referenceLink{SourceKind: "operation", SourceID: op.OperationID, TargetKind: "doc", TargetID: doc.ID, Reason: keywordReason(fmByDocID[doc.ID], op), Confidence: confidence},
			)
		}
	}

	return dedupeLinks(links)
}

func keywordReason(fm frontMatter, op referenceOperation) string {
	if tagOverlap(fm.APITags, op.Tags) {
		return "tag-match"
	}
	return "keyword-match"
}

func operationTokens(op referenceOperation) []string {
	tokens := tokenize(op.OperationID)
	tokens = append(tokens, tokenize(op.Path)...)
	tokens = append(tokens, tokenize(op.Summary)...)
	tokens = append(tokens, tokenize(op.Description)...)
	for _, tag := range op.Tags {
		tokens = append(tokens, tokenize(tag)...)
	}
	for _, param := range op.Parameters {
		tokens = append(tokens, tokenize(param.Name)...)
	}
	for _, field := range op.BodyFields {
		tokens = append(tokens, tokenize(field.Name)...)
	}
	return dedupeStrings(tokens)
}

func renderGo(docs []referenceDoc, operations []referenceOperation, links []referenceLink, specYAML string) ([]byte, error) {
	var b bytes.Buffer
	b.WriteString("// Code generated by cmd/docsgen. DO NOT EDIT.\n")
	b.WriteString("\npackage gen\n\n")
	b.WriteString("type ReferenceIndex struct {\n\tDocs []ReferenceDoc `json:\"docs\"`\n\tOperations []ReferenceOperation `json:\"operations\"`\n\tLinks []ReferenceLink `json:\"links\"`\n\tOpenAPISpecYAML string `json:\"openapi_spec_yaml\"`\n}\n\n")
	b.WriteString("type ReferenceDoc struct {\n\tID string `json:\"id\"`\n\tPath string `json:\"path\"`\n\tSection string `json:\"section\"`\n\tTitle string `json:\"title\"`\n\tDescription string `json:\"description\"`\n\tHeadings []string `json:\"headings\"`\n\tExcerpt string `json:\"excerpt\"`\n\tCodeExamples []string `json:\"code_examples\"`\n\tKeywords []string `json:\"keywords\"`\n}\n\n")
	b.WriteString("type ReferenceOperation struct {\n\tOperationID string `json:\"operation_id\"`\n\tMethod string `json:\"method\"`\n\tPath string `json:\"path\"`\n\tTags []string `json:\"tags\"`\n\tSummary string `json:\"summary\"`\n\tDescription string `json:\"description\"`\n\tParameters []ReferenceParam `json:\"parameters\"`\n\tBodyFields []ReferenceField `json:\"body_fields\"`\n\tCLICommand string `json:\"cli_command\"`\n\tContentTypes []string `json:\"content_types\"`\n}\n\n")
	b.WriteString("type ReferenceParam struct {\n\tName string `json:\"name\"`\n\tIn string `json:\"in\"`\n\tType string `json:\"type\"`\n\tDescription string `json:\"description\"`\n\tRequired bool `json:\"required\"`\n\tEnum []string `json:\"enum\"`\n}\n\n")
	b.WriteString("type ReferenceField struct {\n\tName string `json:\"name\"`\n\tType string `json:\"type\"`\n\tDescription string `json:\"description\"`\n\tRequired bool `json:\"required\"`\n\tEnum []string `json:\"enum\"`\n}\n\n")
	b.WriteString("type ReferenceLink struct {\n\tSourceKind string `json:\"source_kind\"`\n\tSourceID string `json:\"source_id\"`\n\tTargetKind string `json:\"target_kind\"`\n\tTargetID string `json:\"target_id\"`\n\tReason string `json:\"reason\"`\n\tConfidence int `json:\"confidence\"`\n}\n\n")
	b.WriteString("var CLIReferenceIndex = ReferenceIndex{\n")
	b.WriteString("\tDocs: []ReferenceDoc{\n")
	for _, doc := range docs {
		fmt.Fprintf(&b, "\t\t{ID: %s, Path: %s, Section: %s, Title: %s, Description: %s, Headings: %s, Excerpt: %s, CodeExamples: %s, Keywords: %s},\n",
			goString(doc.ID), goString(doc.Path), goString(doc.Section), goString(doc.Title), goString(doc.Description),
			goStringSlice(doc.Headings), goString(doc.Excerpt), goStringSlice(doc.CodeExamples), goStringSlice(doc.Keywords))
	}
	b.WriteString("\t},\n\tOperations: []ReferenceOperation{\n")
	for _, op := range operations {
		fmt.Fprintf(&b, "\t\t{OperationID: %s, Method: %s, Path: %s, Tags: %s, Summary: %s, Description: %s, Parameters: %s, BodyFields: %s, CLICommand: %s, ContentTypes: %s},\n",
			goString(op.OperationID), goString(op.Method), goString(op.Path), goStringSlice(op.Tags), goString(op.Summary), goString(op.Description),
			goParams(op.Parameters), goFields(op.BodyFields), goString(op.CLICommand), goStringSlice(op.ContentTypes))
	}
	b.WriteString("\t},\n\tLinks: []ReferenceLink{\n")
	for _, link := range links {
		fmt.Fprintf(&b, "\t\t{SourceKind: %s, SourceID: %s, TargetKind: %s, TargetID: %s, Reason: %s, Confidence: %d},\n",
			goString(link.SourceKind), goString(link.SourceID), goString(link.TargetKind), goString(link.TargetID), goString(link.Reason), link.Confidence)
	}
	b.WriteString("\t},\n")
	fmt.Fprintf(&b, "\tOpenAPISpecYAML: %s,\n", strconv.Quote(specYAML))
	b.WriteString("}\n")

	formatted, err := format.Source(b.Bytes())
	if err != nil {
		return nil, fmt.Errorf("format generated Go: %w", err)
	}
	return formatted, nil
}

func schemaTypeFromSchemaRef(ref *openapi3.SchemaRef) string {
	if ref == nil || ref.Value == nil {
		return "string"
	}
	if ref.Value.Type != nil && len(*ref.Value.Type) > 0 {
		if (*ref.Value.Type)[0] == "array" && ref.Value.Items != nil {
			return "array[" + schemaTypeFromSchemaRef(ref.Value.Items) + "]"
		}
		return (*ref.Value.Type)[0]
	}
	if len(ref.Value.OneOf) > 0 {
		return "object"
	}
	return "string"
}

func schemaEnumFromSchemaRef(ref *openapi3.SchemaRef) []string {
	if ref == nil || ref.Value == nil || len(ref.Value.Enum) == 0 {
		return nil
	}
	values := make([]string, 0, len(ref.Value.Enum))
	for _, value := range ref.Value.Enum {
		values = append(values, fmt.Sprint(value))
	}
	return values
}

func tokenize(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	var out []string
	var current []rune
	flush := func() {
		if len(current) == 0 {
			return
		}
		token := strings.ToLower(string(current))
		token = strings.Trim(token, "{}")
		if token != "" {
			out = append(out, token)
		}
		current = current[:0]
	}

	for _, r := range value {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			if len(current) > 0 && unicode.IsUpper(r) && unicode.IsLower(current[len(current)-1]) {
				flush()
			}
			current = append(current, r)
		default:
			flush()
		}
	}
	flush()

	lowerWhole := strings.ToLower(strings.TrimSpace(value))
	if lowerWhole != "" {
		out = append(out, lowerWhole)
	}
	return expandTokenVariants(dedupeStrings(out))
}

func setFromTokens(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		set[value] = struct{}{}
	}
	return set
}

func overlapCount(left, right map[string]struct{}) int {
	count := 0
	for key := range left {
		if _, ok := right[key]; ok {
			count++
		}
	}
	return count
}

func tagOverlap(left, right []string) bool {
	rightSet := make(map[string]struct{}, len(right))
	for _, value := range right {
		rightSet[strings.ToLower(strings.TrimSpace(value))] = struct{}{}
	}
	for _, value := range left {
		if _, ok := rightSet[strings.ToLower(strings.TrimSpace(value))]; ok {
			return true
		}
	}
	return false
}

func dedupeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func expandTokenVariants(tokens []string) []string {
	out := make([]string, 0, len(tokens)*2)
	seen := make(map[string]struct{}, len(tokens)*2)
	for _, token := range tokens {
		for _, candidate := range []string{token, singularize(token)} {
			candidate = strings.TrimSpace(candidate)
			if candidate == "" {
				continue
			}
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			out = append(out, candidate)
		}
	}
	return out
}

func singularize(token string) string {
	switch {
	case strings.HasSuffix(token, "ies") && len(token) > 3:
		return token[:len(token)-3] + "y"
	case strings.HasSuffix(token, "ses") && len(token) > 3:
		return token[:len(token)-2]
	case strings.HasSuffix(token, "s") && len(token) > 3:
		return token[:len(token)-1]
	default:
		return token
	}
}

func dedupeLinks(values []referenceLink) []referenceLink {
	type key struct {
		SourceKind string
		SourceID   string
		TargetKind string
		TargetID   string
	}

	best := make(map[key]referenceLink, len(values))
	for _, value := range values {
		k := key{
			SourceKind: value.SourceKind,
			SourceID:   value.SourceID,
			TargetKind: value.TargetKind,
			TargetID:   value.TargetID,
		}
		if existing, ok := best[k]; !ok || value.Confidence > existing.Confidence {
			best[k] = value
		}
	}

	out := make([]referenceLink, 0, len(best))
	for _, value := range best {
		out = append(out, value)
	}
	return out
}

func sortDocs(docs []referenceDoc) {
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].ID < docs[j].ID
	})
}

func sortOperations(operations []referenceOperation) {
	sort.Slice(operations, func(i, j int) bool {
		if operations[i].OperationID == operations[j].OperationID {
			return operations[i].Path < operations[j].Path
		}
		return operations[i].OperationID < operations[j].OperationID
	})
}

func sortLinks(links []referenceLink) {
	sort.Slice(links, func(i, j int) bool {
		left := links[i]
		right := links[j]
		if left.SourceKind != right.SourceKind {
			return left.SourceKind < right.SourceKind
		}
		if left.SourceID != right.SourceID {
			return left.SourceID < right.SourceID
		}
		if left.TargetKind != right.TargetKind {
			return left.TargetKind < right.TargetKind
		}
		return left.TargetID < right.TargetID
	})
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func goString(value string) string {
	return strconv.Quote(value)
}

func goStringSlice(values []string) string {
	if len(values) == 0 {
		return "nil"
	}
	var b strings.Builder
	b.WriteString("[]string{")
	for i, value := range values {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(goString(value))
	}
	b.WriteString("}")
	return b.String()
}

func goParams(values []referenceParam) string {
	if len(values) == 0 {
		return "nil"
	}
	var b strings.Builder
	b.WriteString("[]ReferenceParam{")
	for i, value := range values {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "{Name: %s, In: %s, Type: %s, Description: %s, Required: %t, Enum: %s}",
			goString(value.Name), goString(value.In), goString(value.Type), goString(value.Description), value.Required, goStringSlice(value.Enum))
	}
	b.WriteString("}")
	return b.String()
}

func goFields(values []referenceField) string {
	if len(values) == 0 {
		return "nil"
	}
	var b strings.Builder
	b.WriteString("[]ReferenceField{")
	for i, value := range values {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "{Name: %s, Type: %s, Description: %s, Required: %t, Enum: %s}",
			goString(value.Name), goString(value.Type), goString(value.Description), value.Required, goStringSlice(value.Enum))
	}
	b.WriteString("}")
	return b.String()
}
