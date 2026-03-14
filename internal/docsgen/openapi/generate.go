// Package openapi renders markdown docs from an OpenAPI specification.
package openapi

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"unicode"

	"github.com/getkin/kin-openapi/openapi3"
	"gopkg.in/yaml.v3"
)

type endpointDoc struct {
	Method       string
	Path         string
	OperationID  string
	Summary      string
	Description  string
	Deprecated   bool
	PathParams   []paramDoc
	QueryParams  []paramDoc
	HeaderParams []paramDoc
	RequestBody  *requestBodyDoc
	Responses    []responseDoc
}

type paramDoc struct {
	Name        string
	Required    bool
	Type        string
	Description string
}

type endpointPageDoc struct {
	Title       string
	Slug        string
	Description string
	Endpoints   []endpointDoc
}

type tagOutput struct {
	Tag         string
	Title       string
	Path        string
	Description string
	Operations  int
	Pages       []endpointPageDoc
}

type apiGroupingConfig struct {
	Tags map[string]apiTagGrouping `yaml:"api_generated"`
}

type apiTagGrouping struct {
	Pages []apiPageGrouping `yaml:"pages"`
}

type apiPageGrouping struct {
	Title         string   `yaml:"title"`
	Slug          string   `yaml:"slug"`
	MatchPrefixes []string `yaml:"match_prefixes"`
}

type requestBodyDoc struct {
	Required     bool
	ContentTypes []string
}

type responseDoc struct {
	Code        string
	Description string
}

// Generate renders OpenAPI docs to markdown files.
func Generate(specPath, outDir, configPath string) error {
	loader := &openapi3.Loader{IsExternalRefsAllowed: true}
	spec, err := loader.LoadFromFile(specPath)
	if err != nil {
		return fmt.Errorf("load spec: %w", err)
	}
	groupingConfig, err := loadGroupingConfig(configPath)
	if err != nil {
		return err
	}

	if err := os.RemoveAll(outDir); err != nil {
		return fmt.Errorf("clean output dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(outDir, "endpoints"), 0o750); err != nil {
		return fmt.Errorf("create endpoints dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(outDir, "schemas"), 0o750); err != nil {
		return fmt.Errorf("create schemas dir: %w", err)
	}

	tagDescriptions := map[string]string{}
	for _, tag := range spec.Tags {
		tagDescriptions[tag.Name] = strings.TrimSpace(tag.Description)
	}

	tagEndpoints := map[string][]endpointDoc{}
	for path, pathItem := range spec.Paths.Map() {
		for method, op := range pathItem.Operations() {
			tags := op.Tags
			if len(tags) == 0 {
				tags = []string{"Untagged"}
			}

			for _, tag := range tags {
				tagEndpoints[tag] = append(tagEndpoints[tag], buildEndpointDoc(path, method, pathItem, op))
			}
		}
	}

	tags := sortedKeys(tagEndpoints)
	tagOutputs := make([]tagOutput, 0, len(tags))
	for _, tag := range tags {
		endpoints := tagEndpoints[tag]
		sortEndpoints(endpoints)
		output, err := writeTagDocs(filepath.Join(outDir, "endpoints"), tag, tagDescriptions[tag], endpoints, groupingConfig.Tags[fileSlug(tag)])
		if err != nil {
			return err
		}
		tagOutputs = append(tagOutputs, output)
	}

	schemaNames := sortedKeys(spec.Components.Schemas)
	for _, name := range schemaNames {
		ref := spec.Components.Schemas[name]
		if err := writeSchemaPage(filepath.Join(outDir, "schemas", fileSlug(name)+".md"), name, ref); err != nil {
			return err
		}
	}

	if err := writeAPIIndex(filepath.Join(outDir, "index.md"), tagOutputs, schemaNames); err != nil {
		return err
	}
	if err := writeFeaturesPage(filepath.Join(outDir, "features.md"), tagOutputs); err != nil {
		return err
	}

	return nil
}

func buildEndpointDoc(path, method string, pathItem *openapi3.PathItem, op *openapi3.Operation) endpointDoc {
	params := append([]*openapi3.ParameterRef{}, pathItem.Parameters...)
	params = append(params, op.Parameters...)

	endpoint := endpointDoc{
		Method:      strings.ToUpper(method),
		Path:        path,
		OperationID: strings.TrimSpace(op.OperationID),
		Summary:     strings.TrimSpace(op.Summary),
		Description: strings.TrimSpace(op.Description),
		Deprecated:  op.Deprecated,
	}

	for _, p := range params {
		if p == nil || p.Value == nil {
			continue
		}
		pd := paramDoc{
			Name:        p.Value.Name,
			Required:    p.Value.Required,
			Type:        schemaTypeFromRef(p.Value.Schema),
			Description: cleanInline(p.Value.Description),
		}
		switch p.Value.In {
		case "path":
			endpoint.PathParams = append(endpoint.PathParams, pd)
		case "query":
			endpoint.QueryParams = append(endpoint.QueryParams, pd)
		case "header":
			endpoint.HeaderParams = append(endpoint.HeaderParams, pd)
		}
	}

	sortParams(endpoint.PathParams)
	sortParams(endpoint.QueryParams)
	sortParams(endpoint.HeaderParams)

	if op.RequestBody != nil && op.RequestBody.Value != nil {
		contentTypes := sortedKeys(op.RequestBody.Value.Content)
		endpoint.RequestBody = &requestBodyDoc{
			Required:     op.RequestBody.Value.Required,
			ContentTypes: contentTypes,
		}
	}

	for code, response := range op.Responses.Map() {
		desc := ""
		if response != nil && response.Value != nil {
			if response.Value.Description != nil {
				desc = cleanInline(*response.Value.Description)
			}
		}
		endpoint.Responses = append(endpoint.Responses, responseDoc{Code: code, Description: desc})
	}
	sortResponses(endpoint.Responses)

	return endpoint
}

func writeAPIIndex(path string, outputs []tagOutput, schemaNames []string) error {
	var b strings.Builder
	b.WriteString(generatedHeader())
	b.WriteString("# API Reference\n\n")
	b.WriteString("This section is generated from the OpenAPI artifact (`api/gen/openapi.yaml` by default).\n\n")
	b.WriteString("- [Feature Overview](./features)\n\n")
	b.WriteString("## Endpoint Groups\n\n")
	for _, output := range outputs {
		b.WriteString(fmt.Sprintf("- [%s](.%s) (%d operations)\n", output.Title, output.Path, output.Operations))
	}
	b.WriteString("\n## Schemas\n\n")
	for _, schema := range schemaNames {
		b.WriteString(fmt.Sprintf("- [%s](./schemas/%s)\n", schema, fileSlug(schema)))
	}

	return writeFile(path, b.String())
}

func writeFeaturesPage(path string, outputs []tagOutput) error {
	var b strings.Builder
	b.WriteString(generatedHeader())
	b.WriteString("# Platform Features\n\n")
	b.WriteString("This page is generated from OpenAPI tags and operations.\n\n")
	b.WriteString("| Feature | What you can do | API coverage |\n")
	b.WriteString("| --- | --- | --- |\n")
	for _, output := range outputs {
		desc := output.Description
		if desc == "" {
			desc = "-"
		}
		b.WriteString(fmt.Sprintf("| [%s](.%s) | %s | %d operations |\n", output.Title, output.Path, tableSafe(desc), output.Operations))
	}

	return writeFile(path, b.String())
}

func writeTagDocs(rootDir, tag, description string, endpoints []endpointDoc, grouping apiTagGrouping) (tagOutput, error) {
	output := tagOutput{
		Tag:         tag,
		Title:       tag,
		Description: description,
		Operations:  len(endpoints),
	}

	pages := splitEndpointPages(tag, description, endpoints, grouping)
	output.Pages = pages
	if len(pages) == 1 && pages[0].Slug == "" {
		output.Path = "/endpoints/" + fileSlug(tag)
		if err := writeEndpointPage(filepath.Join(rootDir, fileSlug(tag)+".md"), tag, description, pages[0].Endpoints); err != nil {
			return tagOutput{}, err
		}
		return output, nil
	}

	tagDir := filepath.Join(rootDir, fileSlug(tag))
	output.Path = "/endpoints/" + fileSlug(tag) + "/"
	if err := writeTagIndexPage(filepath.Join(tagDir, "index.md"), tag, description, pages); err != nil {
		return tagOutput{}, err
	}
	for _, page := range pages {
		if err := writeEndpointPage(filepath.Join(tagDir, page.Slug+".md"), page.Title, page.Description, page.Endpoints); err != nil {
			return tagOutput{}, err
		}
	}
	return output, nil
}

func writeTagIndexPage(path, tag, description string, pages []endpointPageDoc) error {
	var b strings.Builder
	b.WriteString(generatedHeader())
	b.WriteString(fmt.Sprintf("# %s Endpoints\n\n", tag))
	if description != "" {
		b.WriteString(description)
		b.WriteString("\n\n")
	}

	b.WriteString("## Resources\n\n")
	for _, page := range pages {
		b.WriteString(fmt.Sprintf("- [%s](./%s) (%d operations)\n", page.Title, page.Slug, len(page.Endpoints)))
	}
	b.WriteString("\n")

	return writeFile(path, b.String())
}

func writeEndpointPage(path, title, description string, endpoints []endpointDoc) error {
	var b strings.Builder
	b.WriteString(generatedHeader())
	b.WriteString(fmt.Sprintf("# %s\n\n", title))
	if description != "" {
		b.WriteString(description)
		b.WriteString("\n\n")
	}

	for _, endpoint := range endpoints {
		title := fmt.Sprintf("## `%s %s`", endpoint.Method, endpoint.Path)
		b.WriteString(title)
		b.WriteString("\n\n")

		if endpoint.Summary != "" {
			b.WriteString(endpoint.Summary)
			b.WriteString("\n\n")
		}
		if endpoint.Description != "" {
			b.WriteString(endpoint.Description)
			b.WriteString("\n\n")
		}

		if endpoint.OperationID != "" {
			b.WriteString(fmt.Sprintf("- Operation ID: `%s`\n", endpoint.OperationID))
		}
		if endpoint.Deprecated {
			b.WriteString("- Deprecated: `true`\n")
		}
		if endpoint.OperationID != "" || endpoint.Deprecated {
			b.WriteString("\n")
		}

		if len(endpoint.PathParams) > 0 {
			writeParamTable(&b, "Path Parameters", endpoint.PathParams)
		}
		if len(endpoint.QueryParams) > 0 {
			writeParamTable(&b, "Query Parameters", endpoint.QueryParams)
		}
		if len(endpoint.HeaderParams) > 0 {
			writeParamTable(&b, "Header Parameters", endpoint.HeaderParams)
		}

		if endpoint.RequestBody != nil {
			b.WriteString("### Request Body\n\n")
			b.WriteString(fmt.Sprintf("- Required: `%t`\n", endpoint.RequestBody.Required))
			if len(endpoint.RequestBody.ContentTypes) > 0 {
				b.WriteString("- Content types: ")
				for i, c := range endpoint.RequestBody.ContentTypes {
					if i > 0 {
						b.WriteString(", ")
					}
					b.WriteString("`")
					b.WriteString(c)
					b.WriteString("`")
				}
				b.WriteString("\n")
			}
			b.WriteString("\n")
		}

		if len(endpoint.Responses) > 0 {
			b.WriteString("### Responses\n\n")
			b.WriteString("| Code | Description |\n")
			b.WriteString("| --- | --- |\n")
			for _, response := range endpoint.Responses {
				b.WriteString(fmt.Sprintf("| `%s` | %s |\n", response.Code, tableSafe(response.Description)))
			}
			b.WriteString("\n")
		}
	}

	return writeFile(path, b.String())
}

func splitEndpointPages(tag, description string, endpoints []endpointDoc, grouping apiTagGrouping) []endpointPageDoc {
	if len(grouping.Pages) > 0 {
		pages := splitConfiguredEndpointPages(tag, endpoints, grouping)
		if len(pages) > 0 {
			return pages
		}
	}
	resourceGroups := groupEndpointsByResource(tag, endpoints)
	if len(endpoints) < 18 || len(resourceGroups) < 3 {
		return []endpointPageDoc{{
			Title:       tag + " Endpoints",
			Description: description,
			Endpoints:   endpoints,
		}}
	}

	pages := make([]endpointPageDoc, 0, len(resourceGroups))
	for _, group := range resourceGroups {
		pageDescription := fmt.Sprintf("%s operations within the %s API group.", group.Title, tag)
		pages = append(pages, endpointPageDoc{
			Title:       group.Title,
			Slug:        group.Slug,
			Description: pageDescription,
			Endpoints:   group.Endpoints,
		})
	}
	return pages
}

func splitConfiguredEndpointPages(tag string, endpoints []endpointDoc, grouping apiTagGrouping) []endpointPageDoc {
	pages := make([]endpointPageDoc, 0, len(grouping.Pages))
	assignments := make([]int, len(endpoints))
	for idx := range assignments {
		assignments[idx] = -1
	}

	for endpointIdx, endpoint := range endpoints {
		bestGroup := -1
		bestLen := -1
		for groupIdx, group := range grouping.Pages {
			matchLen := longestMatchingPrefixLen(endpoint.Path, group.MatchPrefixes)
			if matchLen > bestLen {
				bestLen = matchLen
				bestGroup = groupIdx
			}
		}
		assignments[endpointIdx] = bestGroup
	}

	for groupIdx, group := range grouping.Pages {
		matched := make([]endpointDoc, 0)
		for endpointIdx, assignedGroup := range assignments {
			if assignedGroup == groupIdx {
				matched = append(matched, endpoints[endpointIdx])
			}
		}
		if len(matched) == 0 {
			continue
		}
		sortEndpoints(matched)
		title := strings.TrimSpace(group.Title)
		if title == "" {
			title = humanizeKey(group.Slug)
		}
		slug := strings.TrimSpace(group.Slug)
		if slug == "" {
			slug = fileSlug(title)
		}
		pages = append(pages, endpointPageDoc{
			Title:       title,
			Slug:        slug,
			Description: fmt.Sprintf("%s operations within the %s API group.", title, tag),
			Endpoints:   matched,
		})
	}

	for idx, endpoint := range endpoints {
		if assignments[idx] >= 0 {
			continue
		}
		key := resourceKeyForEndpoint(tag, endpoint.Path)
		found := false
		for pageIdx := range pages {
			if pages[pageIdx].Slug == fileSlug(key) {
				pages[pageIdx].Endpoints = append(pages[pageIdx].Endpoints, endpoint)
				found = true
				break
			}
		}
		if !found {
			title := resourceTitle(tag, key)
			pages = append(pages, endpointPageDoc{
				Title:       title,
				Slug:        fileSlug(key),
				Description: fmt.Sprintf("%s operations within the %s API group.", title, tag),
				Endpoints:   []endpointDoc{endpoint},
			})
		}
	}

	for idx := range pages {
		sortEndpoints(pages[idx].Endpoints)
	}

	return pages
}

func longestMatchingPrefixLen(path string, prefixes []string) int {
	best := -1
	for _, prefix := range prefixes {
		prefix = strings.TrimSpace(prefix)
		if prefix == "" {
			continue
		}
		if strings.HasPrefix(path, prefix) {
			if len(prefix) > best {
				best = len(prefix)
			}
		}
	}
	return best
}

func loadGroupingConfig(path string) (apiGroupingConfig, error) {
	var cfg apiGroupingConfig
	path = strings.TrimSpace(path)
	if path == "" {
		return cfg, nil
	}
	body, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return cfg, fmt.Errorf("read grouping config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(body, &cfg); err != nil {
		return cfg, fmt.Errorf("decode grouping config %s: %w", path, err)
	}
	if cfg.Tags == nil {
		cfg.Tags = map[string]apiTagGrouping{}
	}
	return cfg, nil
}

func groupEndpointsByResource(tag string, endpoints []endpointDoc) []endpointPageDoc {
	grouped := make(map[string][]endpointDoc)
	for _, endpoint := range endpoints {
		key := resourceKeyForEndpoint(tag, endpoint.Path)
		grouped[key] = append(grouped[key], endpoint)
	}

	keys := sortedKeys(grouped)
	pages := make([]endpointPageDoc, 0, len(keys))
	for _, key := range keys {
		groupEndpoints := grouped[key]
		sortEndpoints(groupEndpoints)
		pages = append(pages, endpointPageDoc{
			Title:     resourceTitle(tag, key),
			Slug:      fileSlug(key),
			Endpoints: groupEndpoints,
		})
	}
	sort.Slice(pages, func(i, j int) bool {
		return pages[i].Title < pages[j].Title
	})
	return pages
}

func resourceKeyForEndpoint(tag, path string) string {
	segments := strings.Split(strings.Trim(path, "/"), "/")
	lastMeaningful := fileSlug(tag)
	for _, segment := range segments {
		if segment == "" || strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") {
			continue
		}
		if isIgnoredResourceSegment(segment) {
			continue
		}
		lastMeaningful = segment
	}
	return lastMeaningful
}

func isIgnoredResourceSegment(segment string) bool {
	switch strings.TrimSpace(strings.ToLower(segment)) {
	case "history", "info", "metastore", "summary", "set-default", "version-summary", "ingestion", "load", "upload-url", "commit", "profile", "columns", "manifest":
		return true
	default:
		return false
	}
}

func resourceTitle(tag, key string) string {
	return humanizeKey(key)
}

func humanizeKey(value string) string {
	parts := strings.FieldsFunc(strings.TrimSpace(value), func(r rune) bool {
		return r == '-' || r == '_' || r == '/' || unicode.IsSpace(r)
	})
	for i := range parts {
		if parts[i] == "" {
			continue
		}
		part := strings.ToLower(parts[i])
		runes := []rune(part)
		runes[0] = unicode.ToUpper(runes[0])
		parts[i] = string(runes)
	}
	return strings.Join(parts, " ")
}

func writeSchemaPage(path, name string, ref *openapi3.SchemaRef) error {
	var b strings.Builder
	b.WriteString(generatedHeader())
	b.WriteString(fmt.Sprintf("# Schema: `%s`\n\n", name))

	if ref == nil {
		b.WriteString("Schema definition is missing.\n")
		return writeFile(path, b.String())
	}

	if ref.Ref != "" {
		b.WriteString(fmt.Sprintf("- Source ref: `%s`\n\n", ref.Ref))
	}

	schema := ref.Value
	if schema == nil {
		b.WriteString("Schema body is empty.\n")
		return writeFile(path, b.String())
	}

	if schema.Description != "" {
		b.WriteString(cleanInline(schema.Description))
		b.WriteString("\n\n")
	}

	b.WriteString(fmt.Sprintf("- Type: `%s`\n", schemaType(schema)))
	if len(schema.Required) > 0 {
		required := slices.Clone(schema.Required)
		slices.Sort(required)
		b.WriteString("- Required fields: ")
		for i, field := range required {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("`")
			b.WriteString(field)
			b.WriteString("`")
		}
		b.WriteString("\n")
	}
	b.WriteString("\n")

	if len(schema.Properties) > 0 {
		propNames := sortedKeys(schema.Properties)
		b.WriteString("## Properties\n\n")
		b.WriteString("| Name | Type | Required | Description |\n")
		b.WriteString("| --- | --- | --- | --- |\n")
		reqSet := make(map[string]struct{}, len(schema.Required))
		for _, field := range schema.Required {
			reqSet[field] = struct{}{}
		}
		for _, propName := range propNames {
			propRef := schema.Properties[propName]
			_, required := reqSet[propName]
			desc := ""
			if propRef != nil && propRef.Value != nil {
				desc = cleanInline(propRef.Value.Description)
			}
			b.WriteString(fmt.Sprintf("| `%s` | `%s` | `%t` | %s |\n", propName, schemaTypeFromRef(propRef), required, tableSafe(desc)))
		}
		b.WriteString("\n")
	}

	return writeFile(path, b.String())
}

func writeParamTable(b *strings.Builder, title string, params []paramDoc) {
	b.WriteString("### ")
	b.WriteString(title)
	b.WriteString("\n\n")
	b.WriteString("| Name | Type | Required | Description |\n")
	b.WriteString("| --- | --- | --- | --- |\n")
	for _, param := range params {
		_, _ = fmt.Fprintf(b, "| `%s` | `%s` | `%t` | %s |\n", param.Name, param.Type, param.Required, tableSafe(param.Description))
	}
	b.WriteString("\n")
}

func sortParams(params []paramDoc) {
	sort.Slice(params, func(i, j int) bool {
		return params[i].Name < params[j].Name
	})
}

func sortEndpoints(endpoints []endpointDoc) {
	methodOrder := map[string]int{
		"GET":     0,
		"POST":    1,
		"PUT":     2,
		"PATCH":   3,
		"DELETE":  4,
		"OPTIONS": 5,
		"HEAD":    6,
	}

	sort.Slice(endpoints, func(i, j int) bool {
		if endpoints[i].Path != endpoints[j].Path {
			return endpoints[i].Path < endpoints[j].Path
		}
		mi := methodOrder[endpoints[i].Method]
		mj := methodOrder[endpoints[j].Method]
		if mi != mj {
			return mi < mj
		}
		return endpoints[i].OperationID < endpoints[j].OperationID
	})
}

func sortResponses(responses []responseDoc) {
	sort.Slice(responses, func(i, j int) bool {
		ci := responses[i].Code
		cj := responses[j].Code
		if ci == "default" {
			return false
		}
		if cj == "default" {
			return true
		}
		return ci < cj
	})
}

func schemaTypeFromRef(ref *openapi3.SchemaRef) string {
	if ref == nil {
		return "unknown"
	}
	if ref.Ref != "" {
		parts := strings.Split(ref.Ref, "/")
		return parts[len(parts)-1]
	}
	if ref.Value == nil {
		return "unknown"
	}
	return schemaType(ref.Value)
}

func schemaType(schema *openapi3.Schema) string {
	if schema == nil || schema.Type == nil || len(*schema.Type) == 0 {
		return "object"
	}
	if (*schema.Type)[0] == "array" {
		if schema.Items != nil {
			return "array[" + schemaTypeFromRef(schema.Items) + "]"
		}
		return "array"
	}
	return (*schema.Type)[0]
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func fileSlug(value string) string {
	lower := strings.ToLower(strings.TrimSpace(value))
	lower = strings.ReplaceAll(lower, " ", "-")
	lower = strings.ReplaceAll(lower, "/", "-")
	lower = strings.ReplaceAll(lower, "_", "-")
	lower = strings.ReplaceAll(lower, ".", "-")
	for strings.Contains(lower, "--") {
		lower = strings.ReplaceAll(lower, "--", "-")
	}
	return strings.Trim(lower, "-")
}

func generatedHeader() string {
	return "<!-- Code generated by cmd/docsgen. DO NOT EDIT. -->\n\n"
}

func writeFile(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create directory %q: %w", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return fmt.Errorf("write %q: %w", path, err)
	}
	return nil
}

func cleanInline(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "\n", " ")
	for strings.Contains(value, "  ") {
		value = strings.ReplaceAll(value, "  ", " ")
	}
	return value
}

func tableSafe(value string) string {
	value = cleanInline(value)
	value = strings.ReplaceAll(value, "|", "\\|")
	if value == "" {
		return "-"
	}
	return value
}
