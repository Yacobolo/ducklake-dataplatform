package cli

import (
	"sort"
	"strings"
	"unicode"

	"github.com/getkin/kin-openapi/openapi3"
)

// === Tag-to-Group Mapping ===

// defaultTagToGroup maps OpenAPI tags to CLI group names.
// Only entries that differ from lowercase(tag) need to be listed.
var defaultTagToGroup = map[string]string{
	"Catalogs": "catalog", // plural -> singular
}

// defaultGroupDescriptions provides short descriptions for groups.
var defaultGroupDescriptions = map[string]string{
	"catalog":       "Manage the data catalog",
	"security":      "Manage principals, groups, grants, and access controls",
	"query":         "Execute SQL queries",
	"ingestion":     "Data ingestion operations",
	"lineage":       "Table lineage tracking",
	"governance":    "Tags, classifications, and search",
	"observability": "Audit logs, query history, and metastore info",
	"storage":       "Storage credentials and external locations",
	"manifest":      "Client-side query manifest",
	"compute":       "Compute endpoint management",
}

// inferGroup returns the CLI group name for a given operation based on its tags.
func inferGroup(tags []string, tagOverrides map[string]GroupOverride) string {
	if len(tags) == 0 {
		return "default"
	}
	tag := tags[0]

	// Check user-provided group overrides first
	if ov, ok := tagOverrides[tag]; ok && ov.Name != "" {
		return ov.Name
	}

	// Check built-in tag-to-group mapping
	if group, ok := defaultTagToGroup[tag]; ok {
		return group
	}

	// Default: lowercase the tag name
	return strings.ToLower(tag)
}

// groupDescription returns the short description for a CLI group.
func groupDescription(groupName string, tagOverrides map[string]GroupOverride) string {
	// Check user-provided overrides first.
	// A tag override matches if its explicit Name equals the groupName,
	// or if inferring the group from the tag key would yield groupName.
	for tag, ov := range tagOverrides {
		if ov.Short == "" {
			continue
		}
		if ov.Name == groupName {
			return ov.Short
		}
		// Also match if the tag naturally maps to this group name
		if ov.Name == "" && inferGroupFromTag(tag) == groupName {
			return ov.Short
		}
	}
	if desc, ok := defaultGroupDescriptions[groupName]; ok {
		return desc
	}
	return "Manage " + groupName
}

// inferGroupFromTag returns the group name for a tag without considering overrides.
func inferGroupFromTag(tag string) string {
	if group, ok := defaultTagToGroup[tag]; ok {
		return group
	}
	return strings.ToLower(tag)
}

// === Verb Inference ===

// crudPrefixes are the standard CRUD prefixes we can auto-detect from operationId.
var crudPrefixes = []string{"list", "create", "get", "update", "delete"}

// inferVerb extracts the verb from an operationId by stripping the resource suffix.
// Returns the inferred verb and whether it was successfully inferred.
func inferVerb(operationID, _ string) (string, bool) {
	for _, prefix := range crudPrefixes {
		if strings.HasPrefix(operationID, prefix) {
			suffix := operationID[len(prefix):]
			// Verify the suffix starts with an uppercase letter (it's a resource name)
			if len(suffix) > 0 && unicode.IsUpper(rune(suffix[0])) {
				return prefix, true
			}
		}
	}
	return "", false
}

// === Command Path Inference ===

// inferCommandPath extracts the subcommand path from a URL path.
// It finds the last non-parameter segment(s) to determine the resource type.
func inferCommandPath(urlPath string) []string {
	parts := strings.Split(strings.TrimPrefix(urlPath, "/"), "/")

	// Find the last non-parameter segment
	lastResource := ""
	for _, p := range parts {
		if !strings.HasPrefix(p, "{") {
			lastResource = p
		}
	}

	if lastResource != "" {
		return []string{lastResource}
	}
	return nil
}

// === Positional Args Inference ===

// inferPositionalArgs returns path parameters that should be positional CLI args.
// It excludes parameters listed in implicitParams (e.g., catalogName).
func inferPositionalArgs(params []*openapi3.ParameterRef, implicitParams map[string]bool) []string {
	var result []string
	for _, pRef := range params {
		if pRef.Value == nil || pRef.Value.In != "path" {
			continue
		}
		if implicitParams[pRef.Value.Name] {
			continue
		}
		result = append(result, pRef.Value.Name)
	}
	// Sort for deterministic output — path params appear in URL order
	// but map iteration over openapi3 is not guaranteed
	return result
}

// === Table Columns Inference ===

// excludedColumnNames are specific field names to exclude from auto-derived table columns.
var excludedColumnNames = map[string]bool{
	"deleted_at": true,
	"updated_at": true,
}

// maxTableColumns is the maximum number of auto-derived table columns.
const maxTableColumns = 7

// inferTableColumns derives table column names from a paginated response schema.
// It selects scalar fields suitable for table display, excluding complex types
// and secondary metadata fields.
func inferTableColumns(op *openapi3.Operation) []string {
	// Find the paginated response schema
	for _, code := range []string{"200", "201"} {
		resp, ok := op.Responses.Map()[code]
		if !ok || resp == nil || resp.Value == nil {
			continue
		}
		ct, ok := resp.Value.Content["application/json"]
		if !ok || ct.Schema == nil || ct.Schema.Value == nil {
			continue
		}
		schema := ct.Schema.Value

		// Check for paginated pattern: has "data" array + "next_page_token"
		dataProp, hasData := schema.Properties["data"]
		_, hasToken := schema.Properties["next_page_token"]
		if !hasData || !hasToken || dataProp.Value == nil {
			continue
		}
		dataSchema := dataProp.Value
		if dataSchema.Type == nil || len(*dataSchema.Type) == 0 || (*dataSchema.Type)[0] != "array" {
			continue
		}

		// Get the item schema
		if dataSchema.Items == nil || dataSchema.Items.Value == nil {
			continue
		}
		itemSchema := dataSchema.Items.Value
		return selectTableColumns(itemSchema)
	}
	return nil
}

// selectTableColumns picks appropriate columns from a schema for table display.
func selectTableColumns(schema *openapi3.Schema) []string {
	if schema.Properties == nil {
		return nil
	}

	var candidates []string
	for name, propRef := range schema.Properties {
		if propRef.Value == nil {
			continue
		}
		prop := propRef.Value

		// Skip excluded names
		if excludedColumnNames[name] {
			continue
		}

		// Skip complex types (arrays, objects without additionalProperties)
		propType := schemaType(prop)
		if propType == "array" || (propType == "object" && !isMapType(prop)) {
			continue
		}

		// Skip long text fields (heuristic: description mentions "definition" or field is known long)
		if isLongTextField(name, prop) {
			continue
		}

		candidates = append(candidates, name)
	}

	sort.Strings(candidates)

	// Prioritize: id-like fields first, then name, then others, then timestamps
	prioritized := prioritizeColumns(candidates)

	if len(prioritized) > maxTableColumns {
		prioritized = prioritized[:maxTableColumns]
	}
	return prioritized
}

// isLongTextField returns true if the field is likely long text unsuitable for table columns.
func isLongTextField(name string, _ *openapi3.Schema) bool {
	longNames := map[string]bool{
		"view_definition": true,
		"storage_path":    true,
		"source_path":     true,
		"data_path":       true,
		"dsn":             true,
		"file_format":     true,
		"location_name":   true,
	}
	return longNames[name]
}

// prioritizeColumns sorts columns with id/name fields first, timestamps last.
func prioritizeColumns(cols []string) []string {
	type ranked struct {
		name string
		rank int
	}
	var items []ranked
	for _, c := range cols {
		r := 50 // default middle rank
		switch {
		case c == "id" || strings.HasSuffix(c, "_id"):
			r = 10
		case c == "name" || c == "key":
			r = 20
		case c == "type" || strings.HasSuffix(c, "_type") || c == "status":
			r = 30
		case c == "owner" || c == "created_by":
			r = 60
		case c == "created_at" || c == "granted_at" || c == "expires_at":
			r = 70
		}
		items = append(items, ranked{name: c, rank: r})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].rank != items[j].rank {
			return items[i].rank < items[j].rank
		}
		return items[i].name < items[j].name
	})
	result := make([]string, len(items))
	for i, item := range items {
		result[i] = item.name
	}
	return result
}

// === Confirm Inference ===

// inferConfirm returns true if the operation should require confirmation.
func inferConfirm(httpMethod string) bool {
	return strings.EqualFold(httpMethod, "DELETE")
}

// === Item Schema Extraction (for drift validation) ===

// getItemSchema extracts the item schema from a paginated response.
func getItemSchema(op *openapi3.Operation) *openapi3.Schema {
	for _, code := range []string{"200", "201"} {
		resp, ok := op.Responses.Map()[code]
		if !ok || resp == nil || resp.Value == nil {
			continue
		}
		ct, ok := resp.Value.Content["application/json"]
		if !ok || ct.Schema == nil || ct.Schema.Value == nil {
			continue
		}
		schema := ct.Schema.Value

		dataProp, hasData := schema.Properties["data"]
		_, hasToken := schema.Properties["next_page_token"]
		if !hasData || !hasToken || dataProp.Value == nil {
			continue
		}
		dataSchema := dataProp.Value
		if dataSchema.Type == nil || len(*dataSchema.Type) == 0 || (*dataSchema.Type)[0] != "array" {
			continue
		}
		if dataSchema.Items == nil || dataSchema.Items.Value == nil {
			continue
		}
		return dataSchema.Items.Value
	}
	return nil
}
