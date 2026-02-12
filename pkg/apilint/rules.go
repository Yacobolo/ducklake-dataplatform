package apilint

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

func init() {
	Register(ruleOAL001{})
	Register(ruleOAL002{})
	Register(ruleOAL003{})
	Register(ruleOAL004{})
	Register(ruleOAL005{})
	Register(ruleOAL006{})
	Register(ruleOAL007{})
	Register(ruleOAL008{})
	Register(ruleOAL009{})
	Register(ruleOAL010{})
	Register(ruleOAL011{})
	Register(ruleOAL012{})
	Register(ruleOAL013{})
	Register(ruleOAL014{})
	Register(ruleOAL015{})
	Register(ruleOAL016{})
	Register(ruleOAL017{})
	Register(ruleOAL018{})
	Register(ruleOAL019{})
	Register(ruleOAL020{})
	Register(ruleOAL021{})
	Register(ruleOAL022{})
	Register(ruleOAL023{})
	Register(ruleOAL024{})
	Register(ruleOAL025{})
}

// === OAL001: Every operation must have a tags field ===

type ruleOAL001 struct{}

func (ruleOAL001) ID() string                { return "OAL001" }
func (ruleOAL001) Description() string       { return "Every operation must have a tags field" }
func (ruleOAL001) DefaultSeverity() Severity { return SeverityError }

func (ruleOAL001) Check(ctx *LintContext) []Violation {
	var vs []Violation
	ctx.ForEachOperation(func(path, method string, op *yaml.Node) {
		if mapGet(op, "tags") == nil {
			opID := operationID(op)
			if opID == "" {
				opID = method + " " + path
			}
			vs = append(vs, ctx.Violation(op.Line, "OAL001", SeverityError,
				fmt.Sprintf("operation %q is missing 'tags' field", opID)))
		}
	})
	return vs
}

// === OAL002: operationId must be present and unique ===

type ruleOAL002 struct{}

func (ruleOAL002) ID() string                { return "OAL002" }
func (ruleOAL002) Description() string       { return "operationId must be present and unique" }
func (ruleOAL002) DefaultSeverity() Severity { return SeverityError }

func (ruleOAL002) Check(ctx *LintContext) []Violation {
	var vs []Violation
	seen := map[string]int{}
	ctx.ForEachOperation(func(path, method string, op *yaml.Node) {
		idNode := mapGet(op, "operationId")
		if idNode == nil {
			vs = append(vs, ctx.Violation(op.Line, "OAL002", SeverityError,
				fmt.Sprintf("operation %s %s is missing 'operationId'", method, path)))
			return
		}
		if prev, ok := seen[idNode.Value]; ok {
			vs = append(vs, ctx.Violation(idNode.Line, "OAL002", SeverityError,
				fmt.Sprintf("duplicate operationId %q (first seen at line %d)", idNode.Value, prev)))
			return
		}
		seen[idNode.Value] = idNode.Line
	})
	return vs
}

// === OAL003: DELETE operations should not have a requestBody ===

type ruleOAL003 struct{}

func (ruleOAL003) ID() string                { return "OAL003" }
func (ruleOAL003) Description() string       { return "DELETE operations should not have a requestBody" }
func (ruleOAL003) DefaultSeverity() Severity { return SeverityWarning }

func (ruleOAL003) Check(ctx *LintContext) []Violation {
	var vs []Violation
	ctx.ForEachOperation(func(path, method string, op *yaml.Node) {
		if method != "delete" {
			return
		}
		if body := mapGet(op, "requestBody"); body != nil {
			opID := operationID(op)
			if opID == "" {
				opID = method + " " + path
			}
			vs = append(vs, ctx.Violation(body.Line, "OAL003", SeverityWarning,
				fmt.Sprintf("DELETE operation %q has a requestBody", opID)))
		}
	})
	return vs
}

// === OAL004: All response schemas should use $ref (no inline anonymous objects) ===

type ruleOAL004 struct{}

func (ruleOAL004) ID() string { return "OAL004" }
func (ruleOAL004) Description() string {
	return "All response schemas should use $ref (no inline anonymous objects)"
}
func (ruleOAL004) DefaultSeverity() Severity { return SeverityWarning }

func (ruleOAL004) Check(ctx *LintContext) []Violation {
	var vs []Violation
	ctx.ForEachOperation(func(path, method string, op *yaml.Node) {
		responses := mapGet(op, "responses")
		if responses == nil {
			return
		}
		opID := operationID(op)
		if opID == "" {
			opID = method + " " + path
		}
		for i := 0; i < len(responses.Content)-1; i += 2 {
			statusCode := responses.Content[i].Value
			responseObj := responses.Content[i+1]
			content := mapGet(responseObj, "content")
			if content == nil {
				continue
			}
			appJSON := mapGet(content, "application/json")
			if appJSON == nil {
				continue
			}
			schema := mapGet(appJSON, "schema")
			if schema == nil {
				continue
			}
			if mapGet(schema, "$ref") == nil {
				vs = append(vs, ctx.Violation(schema.Line, "OAL004", SeverityWarning,
					fmt.Sprintf("operation %q response %s uses inline schema instead of $ref", opID, statusCode)))
			}
		}
	})
	return vs
}

// === OAL005: GET endpoints returning Paginated* schemas must include MaxResults + PageToken params ===

type ruleOAL005 struct{}

func (ruleOAL005) ID() string { return "OAL005" }
func (ruleOAL005) Description() string {
	return "Paginated GET endpoints must include MaxResults + PageToken params"
}
func (ruleOAL005) DefaultSeverity() Severity { return SeverityError }

func (ruleOAL005) Check(ctx *LintContext) []Violation {
	var vs []Violation
	ctx.ForEachOperation(func(path, method string, op *yaml.Node) {
		if method != "get" {
			return
		}
		responses := mapGet(op, "responses")
		if responses == nil {
			return
		}
		resp200 := mapGet(responses, "200")
		if resp200 == nil {
			return
		}
		content := mapGet(resp200, "content")
		if content == nil {
			return
		}
		appJSON := mapGet(content, "application/json")
		if appJSON == nil {
			return
		}
		schema := mapGet(appJSON, "schema")
		if schema == nil {
			return
		}
		ref := mapGet(schema, "$ref")
		if ref == nil {
			return
		}
		parts := strings.Split(ref.Value, "/")
		schemaName := parts[len(parts)-1]
		if !strings.HasPrefix(schemaName, "Paginated") {
			return
		}

		hasMaxResults, hasPageToken := checkPaginationParams(ctx, path, op)

		opID := operationID(op)
		if opID == "" {
			opID = method + " " + path
		}

		if !hasMaxResults || !hasPageToken {
			var missing []string
			if !hasMaxResults {
				missing = append(missing, "MaxResults")
			}
			if !hasPageToken {
				missing = append(missing, "PageToken")
			}
			vs = append(vs, ctx.Violation(op.Line, "OAL005", SeverityError,
				fmt.Sprintf("paginated endpoint %q missing %s parameters", opID, strings.Join(missing, ", "))))
		}
	})
	return vs
}

// checkPaginationParams checks both operation-level and path-level params for MaxResults/PageToken.
func checkPaginationParams(ctx *LintContext, path string, op *yaml.Node) (hasMaxResults, hasPageToken bool) {
	checkParams := func(params *yaml.Node) {
		if params == nil {
			return
		}
		for _, p := range params.Content {
			if p.Kind == yaml.MappingNode {
				nameNode := mapGet(p, "name")
				if nameNode != nil {
					if nameNode.Value == "max_results" {
						hasMaxResults = true
					}
					if nameNode.Value == "page_token" {
						hasPageToken = true
					}
				}
				refNode := mapGet(p, "$ref")
				if refNode != nil {
					if strings.HasSuffix(refNode.Value, "/MaxResults") {
						hasMaxResults = true
					}
					if strings.HasSuffix(refNode.Value, "/PageToken") {
						hasPageToken = true
					}
				}
			}
		}
	}

	checkParams(mapGet(op, "parameters"))

	pathsNode := mapGet(ctx.Root, "paths")
	if pathsNode != nil {
		pathItem := mapGet(pathsNode, path)
		if pathItem != nil {
			checkParams(mapGet(pathItem, "parameters"))
		}
	}
	return
}

// === OAL006: On collection paths, GET should be declared before POST ===

type ruleOAL006 struct{}

func (ruleOAL006) ID() string { return "OAL006" }
func (ruleOAL006) Description() string {
	return "On collection paths, GET should be declared before POST"
}
func (ruleOAL006) DefaultSeverity() Severity { return SeverityInfo }

func (ruleOAL006) Check(ctx *LintContext) []Violation {
	var vs []Violation
	paths := mapGet(ctx.Root, "paths")
	if paths == nil {
		return nil
	}
	for i := 0; i < len(paths.Content)-1; i += 2 {
		pathItem := paths.Content[i+1]
		getLine := 0
		postLine := 0
		for j := 0; j < len(pathItem.Content)-1; j += 2 {
			method := pathItem.Content[j].Value
			line := pathItem.Content[j].Line
			if method == "get" {
				getLine = line
			}
			if method == "post" {
				postLine = line
			}
		}
		if getLine > 0 && postLine > 0 && postLine < getLine {
			pathKey := paths.Content[i].Value
			vs = append(vs, ctx.Violation(postLine, "OAL006", SeverityInfo,
				fmt.Sprintf("on %q, POST (line %d) is declared before GET (line %d)", pathKey, postLine, getLine)))
		}
	}
	return vs
}

// === OAL007: Path parameters must be camelCase ===

type ruleOAL007 struct{}

func (ruleOAL007) ID() string                { return "OAL007" }
func (ruleOAL007) Description() string       { return "Path parameters must be camelCase" }
func (ruleOAL007) DefaultSeverity() Severity { return SeverityError }

func (ruleOAL007) Check(ctx *LintContext) []Violation {
	var vs []Violation
	checkParams := func(params *yaml.Node) {
		if params == nil {
			return
		}
		for _, p := range params.Content {
			if p.Kind != yaml.MappingNode {
				continue
			}
			inNode := mapGet(p, "in")
			if inNode == nil || inNode.Value != "path" {
				continue
			}
			nameNode := mapGet(p, "name")
			if nameNode == nil {
				continue
			}
			if !camelCaseRe.MatchString(nameNode.Value) {
				vs = append(vs, ctx.Violation(nameNode.Line, "OAL007", SeverityError,
					fmt.Sprintf("path parameter %q is not camelCase", nameNode.Value)))
			}
		}
	}

	paths := mapGet(ctx.Root, "paths")
	if paths == nil {
		return nil
	}
	for i := 0; i < len(paths.Content)-1; i += 2 {
		pathItem := paths.Content[i+1]
		checkParams(mapGet(pathItem, "parameters"))
		for j := 0; j < len(pathItem.Content)-1; j += 2 {
			method := pathItem.Content[j].Value
			if httpMethods[method] {
				checkParams(mapGet(pathItem.Content[j+1], "parameters"))
			}
		}
	}
	return vs
}

// === OAL008: Query parameters must be snake_case ===

type ruleOAL008 struct{}

func (ruleOAL008) ID() string                { return "OAL008" }
func (ruleOAL008) Description() string       { return "Query parameters must be snake_case" }
func (ruleOAL008) DefaultSeverity() Severity { return SeverityError }

func (ruleOAL008) Check(ctx *LintContext) []Violation {
	var vs []Violation
	checkParams := func(params *yaml.Node) {
		if params == nil {
			return
		}
		for _, p := range params.Content {
			if p.Kind != yaml.MappingNode {
				continue
			}
			inNode := mapGet(p, "in")
			if inNode == nil || inNode.Value != "query" {
				continue
			}
			nameNode := mapGet(p, "name")
			if nameNode == nil {
				continue
			}
			if !snakeCaseRe.MatchString(nameNode.Value) {
				vs = append(vs, ctx.Violation(nameNode.Line, "OAL008", SeverityError,
					fmt.Sprintf("query parameter %q is not snake_case", nameNode.Value)))
			}
		}
	}

	// Check shared component parameters.
	components := mapGet(ctx.Root, "components")
	if components != nil {
		compParams := mapGet(components, "parameters")
		if compParams != nil {
			for i := 0; i < len(compParams.Content)-1; i += 2 {
				p := compParams.Content[i+1]
				inNode := mapGet(p, "in")
				if inNode != nil && inNode.Value == "query" {
					nameNode := mapGet(p, "name")
					if nameNode != nil && !snakeCaseRe.MatchString(nameNode.Value) {
						vs = append(vs, ctx.Violation(nameNode.Line, "OAL008", SeverityError,
							fmt.Sprintf("query parameter %q is not snake_case", nameNode.Value)))
					}
				}
			}
		}
	}

	paths := mapGet(ctx.Root, "paths")
	if paths == nil {
		return vs
	}
	for i := 0; i < len(paths.Content)-1; i += 2 {
		pathItem := paths.Content[i+1]
		checkParams(mapGet(pathItem, "parameters"))
		for j := 0; j < len(pathItem.Content)-1; j += 2 {
			method := pathItem.Content[j].Value
			if httpMethods[method] {
				checkParams(mapGet(pathItem.Content[j+1], "parameters"))
			}
		}
	}
	return vs
}

// === OAL009: All endpoints with global security should include 401 in responses ===

type ruleOAL009 struct{}

func (ruleOAL009) ID() string                { return "OAL009" }
func (ruleOAL009) Description() string       { return "Secured endpoints should include 401 response" }
func (ruleOAL009) DefaultSeverity() Severity { return SeverityWarning }

func (ruleOAL009) Check(ctx *LintContext) []Violation {
	if !ctx.HasGlobalSecurity() {
		return nil
	}
	var vs []Violation
	ctx.ForEachOperation(func(path, method string, op *yaml.Node) {
		sec := mapGet(op, "security")
		if sec != nil && len(sec.Content) == 0 {
			return
		}
		responses := mapGet(op, "responses")
		if responses == nil {
			return
		}
		if mapGet(responses, "401") != nil {
			return
		}
		opID := operationID(op)
		if opID == "" {
			opID = method + " " + path
		}
		vs = append(vs, ctx.Violation(responses.Line, "OAL009", SeverityWarning,
			fmt.Sprintf("operation %q has global security but no 401 response", opID)))
	})
	return vs
}

// === OAL010: Paginated response schemas must have data (array) + next_page_token (string) ===

type ruleOAL010 struct{}

func (ruleOAL010) ID() string { return "OAL010" }
func (ruleOAL010) Description() string {
	return "Paginated schemas must have data (array) + next_page_token (string)"
}
func (ruleOAL010) DefaultSeverity() Severity { return SeverityError }

func (ruleOAL010) Check(ctx *LintContext) []Violation {
	var vs []Violation
	components := mapGet(ctx.Root, "components")
	if components == nil {
		return nil
	}
	schemas := mapGet(components, "schemas")
	if schemas == nil {
		return nil
	}
	for i := 0; i < len(schemas.Content)-1; i += 2 {
		name := schemas.Content[i].Value
		if !strings.HasPrefix(name, "Paginated") {
			continue
		}
		schema := schemas.Content[i+1]
		props := mapGet(schema, "properties")
		if props == nil {
			vs = append(vs, ctx.Violation(schema.Line, "OAL010", SeverityError,
				fmt.Sprintf("paginated schema %q has no properties", name)))
			continue
		}
		data := mapGet(props, "data")
		if data == nil {
			vs = append(vs, ctx.Violation(schema.Line, "OAL010", SeverityError,
				fmt.Sprintf("paginated schema %q missing 'data' field", name)))
		} else {
			typeNode := mapGet(data, "type")
			if typeNode == nil || typeNode.Value != "array" {
				vs = append(vs, ctx.Violation(data.Line, "OAL010", SeverityError,
					fmt.Sprintf("paginated schema %q 'data' field must be type: array", name)))
			}
		}
		npt := mapGet(props, "next_page_token")
		if npt == nil {
			vs = append(vs, ctx.Violation(schema.Line, "OAL010", SeverityError,
				fmt.Sprintf("paginated schema %q missing 'next_page_token' field", name)))
		} else {
			typeNode := mapGet(npt, "type")
			if typeNode == nil || typeNode.Value != "string" {
				vs = append(vs, ctx.Violation(npt.Line, "OAL010", SeverityError,
					fmt.Sprintf("paginated schema %q 'next_page_token' field must be type: string", name)))
			}
		}
	}
	return vs
}

// === OAL011: All $ref values must resolve to defined components ===

type ruleOAL011 struct{}

func (ruleOAL011) ID() string                { return "OAL011" }
func (ruleOAL011) Description() string       { return "All $ref values must resolve to defined components" }
func (ruleOAL011) DefaultSeverity() Severity { return SeverityError }

func (ruleOAL011) Check(ctx *LintContext) []Violation {
	var vs []Violation
	var walk func(n *yaml.Node)
	walk = func(n *yaml.Node) {
		if n == nil {
			return
		}
		if n.Kind == yaml.MappingNode {
			ref := mapGet(n, "$ref")
			if ref != nil && !ctx.ResolveRef(ref.Value) {
				vs = append(vs, ctx.Violation(ref.Line, "OAL011", SeverityError,
					fmt.Sprintf("unresolved $ref %q", ref.Value)))
			}
		}
		for _, c := range n.Content {
			walk(c)
		}
	}
	walk(ctx.Root)
	return vs
}

// === OAL012: POST-create endpoints should return 201 (not 200) ===

type ruleOAL012 struct{}

func (ruleOAL012) ID() string                { return "OAL012" }
func (ruleOAL012) Description() string       { return "POST-create endpoints should return 201 (not 200)" }
func (ruleOAL012) DefaultSeverity() Severity { return SeverityWarning }

// actionVerbs are operationIds for POST endpoints that are actions, not creates.
var actionVerbs = map[string]bool{
	"executeQuery": true, "profileTable": true, "commitTableIngestion": true,
	"loadTableExternalFiles": true, "purgeLineage": true, "cleanupExpiredAPIKeys": true,
	"createManifest": true, "createUploadUrl": true, "searchCatalog": true,
}

func (ruleOAL012) Check(ctx *LintContext) []Violation {
	var vs []Violation
	ctx.ForEachOperation(func(path, method string, op *yaml.Node) {
		if method != "post" {
			return
		}
		opID := operationID(op)
		if actionVerbs[opID] {
			return
		}
		responses := mapGet(op, "responses")
		if responses == nil {
			return
		}
		has200 := mapGet(responses, "200") != nil
		has201 := mapGet(responses, "201") != nil
		if has200 && !has201 {
			if opID == "" {
				opID = method + " " + path
			}
			vs = append(vs, ctx.Violation(responses.Line, "OAL012", SeverityWarning,
				fmt.Sprintf("POST operation %q returns 200 instead of 201", opID)))
		}
	})
	return vs
}

// ============================================================
// New rules OAL013–OAL025
// ============================================================

// === OAL013: Operations must have a summary ===

type ruleOAL013 struct{}

func (ruleOAL013) ID() string                { return "OAL013" }
func (ruleOAL013) Description() string       { return "Operations must have a summary" }
func (ruleOAL013) DefaultSeverity() Severity { return SeverityWarning }

func (ruleOAL013) Check(ctx *LintContext) []Violation {
	var vs []Violation
	ctx.ForEachOperation(func(path, method string, op *yaml.Node) {
		if mapGet(op, "summary") == nil {
			opID := operationID(op)
			if opID == "" {
				opID = method + " " + path
			}
			vs = append(vs, ctx.Violation(op.Line, "OAL013", SeverityWarning,
				fmt.Sprintf("operation %q is missing 'summary'", opID)))
		}
	})
	return vs
}

// === OAL014: Mutating ops with global security should include 403 response ===

type ruleOAL014 struct{}

func (ruleOAL014) ID() string                { return "OAL014" }
func (ruleOAL014) Description() string       { return "Mutating operations should include 403 response" }
func (ruleOAL014) DefaultSeverity() Severity { return SeverityWarning }

var mutatingMethods = map[string]bool{
	"post": true, "put": true, "patch": true, "delete": true,
}

func (ruleOAL014) Check(ctx *LintContext) []Violation {
	if !ctx.HasGlobalSecurity() {
		return nil
	}
	var vs []Violation
	ctx.ForEachOperation(func(path, method string, op *yaml.Node) {
		if !mutatingMethods[method] {
			return
		}
		// Skip if operation overrides security to empty.
		sec := mapGet(op, "security")
		if sec != nil && len(sec.Content) == 0 {
			return
		}
		responses := mapGet(op, "responses")
		if responses == nil {
			return
		}
		if mapGet(responses, "403") != nil {
			return
		}
		opID := operationID(op)
		if opID == "" {
			opID = method + " " + path
		}
		vs = append(vs, ctx.Violation(responses.Line, "OAL014", SeverityWarning,
			fmt.Sprintf("mutating operation %q should include a 403 response", opID)))
	})
	return vs
}

// === OAL015: GET-by-ID operations should include 404 response ===

type ruleOAL015 struct{}

func (ruleOAL015) ID() string { return "OAL015" }
func (ruleOAL015) Description() string {
	return "GET operations on resource paths should include 404 response"
}
func (ruleOAL015) DefaultSeverity() Severity { return SeverityWarning }

func (ruleOAL015) Check(ctx *LintContext) []Violation {
	var vs []Violation
	ctx.ForEachOperation(func(path, method string, op *yaml.Node) {
		if method != "get" {
			return
		}
		// Only check paths that contain a path parameter (resource endpoints).
		if !strings.Contains(path, "{") {
			return
		}
		responses := mapGet(op, "responses")
		if responses == nil {
			return
		}
		if mapGet(responses, "404") != nil {
			return
		}
		opID := operationID(op)
		if opID == "" {
			opID = method + " " + path
		}
		vs = append(vs, ctx.Violation(responses.Line, "OAL015", SeverityWarning,
			fmt.Sprintf("GET operation %q on resource path should include 404 response", opID)))
	})
	return vs
}

// === OAL016: Schema properties must be snake_case ===

type ruleOAL016 struct{}

func (ruleOAL016) ID() string                { return "OAL016" }
func (ruleOAL016) Description() string       { return "Schema properties must be snake_case" }
func (ruleOAL016) DefaultSeverity() Severity { return SeverityWarning }

func (ruleOAL016) Check(ctx *LintContext) []Violation {
	var vs []Violation
	components := mapGet(ctx.Root, "components")
	if components == nil {
		return nil
	}
	schemas := mapGet(components, "schemas")
	if schemas == nil {
		return nil
	}
	for i := 0; i < len(schemas.Content)-1; i += 2 {
		schemaName := schemas.Content[i].Value
		schema := schemas.Content[i+1]
		checkSchemaProps(ctx, &vs, schemaName, schema)
	}
	return vs
}

// checkSchemaProps recursively checks property names in a schema (including allOf/oneOf/anyOf).
func checkSchemaProps(ctx *LintContext, vs *[]Violation, schemaName string, schema *yaml.Node) {
	props := mapGet(schema, "properties")
	if props != nil {
		for i := 0; i < len(props.Content)-1; i += 2 {
			propName := props.Content[i].Value
			if propName == "$ref" {
				continue
			}
			if !snakeCaseRe.MatchString(propName) {
				*vs = append(*vs, ctx.Violation(props.Content[i].Line, "OAL016", SeverityWarning,
					fmt.Sprintf("schema %q property %q is not snake_case", schemaName, propName)))
			}
		}
	}
	// Walk allOf / oneOf / anyOf.
	for _, combiner := range []string{"allOf", "oneOf", "anyOf"} {
		list := mapGet(schema, combiner)
		if list != nil && list.Kind == yaml.SequenceNode {
			for _, item := range list.Content {
				checkSchemaProps(ctx, vs, schemaName, item)
			}
		}
	}
}

// === OAL017: Create*Request schemas should have a required array ===

type ruleOAL017 struct{}

func (ruleOAL017) ID() string                { return "OAL017" }
func (ruleOAL017) Description() string       { return "Create*Request schemas should have a required array" }
func (ruleOAL017) DefaultSeverity() Severity { return SeverityInfo }

func (ruleOAL017) Check(ctx *LintContext) []Violation {
	var vs []Violation
	components := mapGet(ctx.Root, "components")
	if components == nil {
		return nil
	}
	schemas := mapGet(components, "schemas")
	if schemas == nil {
		return nil
	}
	for i := 0; i < len(schemas.Content)-1; i += 2 {
		name := schemas.Content[i].Value
		if !strings.HasPrefix(name, "Create") || !strings.HasSuffix(name, "Request") {
			continue
		}
		schema := schemas.Content[i+1]
		if mapGet(schema, "required") == nil {
			vs = append(vs, ctx.Violation(schema.Line, "OAL017", SeverityInfo,
				fmt.Sprintf("schema %q should have a 'required' array", name)))
		}
	}
	return vs
}

// === OAL018: Unused component schemas ===

type ruleOAL018 struct{}

func (ruleOAL018) ID() string                { return "OAL018" }
func (ruleOAL018) Description() string       { return "Component schemas should be referenced at least once" }
func (ruleOAL018) DefaultSeverity() Severity { return SeverityInfo }

func (ruleOAL018) Check(ctx *LintContext) []Violation {
	components := mapGet(ctx.Root, "components")
	if components == nil {
		return nil
	}
	schemas := mapGet(components, "schemas")
	if schemas == nil {
		return nil
	}

	// Collect all $ref targets in the document.
	referenced := map[string]bool{}
	var walk func(n *yaml.Node)
	walk = func(n *yaml.Node) {
		if n == nil {
			return
		}
		if n.Kind == yaml.MappingNode {
			ref := mapGet(n, "$ref")
			if ref != nil && strings.HasPrefix(ref.Value, "#/components/schemas/") {
				parts := strings.Split(ref.Value, "/")
				referenced[parts[len(parts)-1]] = true
			}
		}
		for _, c := range n.Content {
			walk(c)
		}
	}
	walk(ctx.Root)

	var vs []Violation
	for i := 0; i < len(schemas.Content)-1; i += 2 {
		name := schemas.Content[i].Value
		if !referenced[name] {
			vs = append(vs, ctx.Violation(schemas.Content[i].Line, "OAL018", SeverityInfo,
				fmt.Sprintf("schema %q is defined but never referenced", name)))
		}
	}
	return vs
}

// === OAL019: operationId should be lowerCamelCase ===

type ruleOAL019 struct{}

func (ruleOAL019) ID() string                { return "OAL019" }
func (ruleOAL019) Description() string       { return "operationId should be lowerCamelCase" }
func (ruleOAL019) DefaultSeverity() Severity { return SeverityInfo }

func (ruleOAL019) Check(ctx *LintContext) []Violation {
	var vs []Violation
	ctx.ForEachOperation(func(_, _ string, op *yaml.Node) {
		idNode := mapGet(op, "operationId")
		if idNode == nil {
			return // OAL002 handles missing operationId
		}
		if !camelCaseRe.MatchString(idNode.Value) {
			vs = append(vs, ctx.Violation(idNode.Line, "OAL019", SeverityInfo,
				fmt.Sprintf("operationId %q is not lowerCamelCase", idNode.Value)))
		}
	})
	return vs
}

// === OAL020: Path segments must be kebab-case ===

type ruleOAL020 struct{}

func (ruleOAL020) ID() string                { return "OAL020" }
func (ruleOAL020) Description() string       { return "Path segments must be kebab-case" }
func (ruleOAL020) DefaultSeverity() Severity { return SeverityError }

func (ruleOAL020) Check(ctx *LintContext) []Violation {
	var vs []Violation
	paths := mapGet(ctx.Root, "paths")
	if paths == nil {
		return nil
	}
	for i := 0; i < len(paths.Content)-1; i += 2 {
		pathKey := paths.Content[i].Value
		line := paths.Content[i].Line
		segments := strings.Split(pathKey, "/")
		for _, seg := range segments {
			if seg == "" {
				continue
			}
			// Skip path parameters like {schemaName}.
			if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
				continue
			}
			if !kebabCaseRe.MatchString(seg) {
				vs = append(vs, ctx.Violation(line, "OAL020", SeverityError,
					fmt.Sprintf("path segment %q in %q is not kebab-case", seg, pathKey)))
			}
		}
	}
	return vs
}

// === OAL021: Non-2xx responses should reference the standard Error schema ===

type ruleOAL021 struct{}

func (ruleOAL021) ID() string { return "OAL021" }
func (ruleOAL021) Description() string {
	return "Error responses should reference the standard Error schema"
}
func (ruleOAL021) DefaultSeverity() Severity { return SeverityWarning }

func (ruleOAL021) Check(ctx *LintContext) []Violation {
	var vs []Violation
	ctx.ForEachOperation(func(path, method string, op *yaml.Node) {
		responses := mapGet(op, "responses")
		if responses == nil {
			return
		}
		opID := operationID(op)
		if opID == "" {
			opID = method + " " + path
		}
		for i := 0; i < len(responses.Content)-1; i += 2 {
			statusCode := responses.Content[i].Value
			// Only check non-2xx responses.
			if strings.HasPrefix(statusCode, "2") {
				continue
			}
			responseObj := responses.Content[i+1]
			content := mapGet(responseObj, "content")
			if content == nil {
				continue // no body (e.g., some error responses are empty)
			}
			appJSON := mapGet(content, "application/json")
			if appJSON == nil {
				continue
			}
			schema := mapGet(appJSON, "schema")
			if schema == nil {
				continue
			}
			ref := mapGet(schema, "$ref")
			if ref == nil || !strings.HasSuffix(ref.Value, "/Error") {
				vs = append(vs, ctx.Violation(schema.Line, "OAL021", SeverityWarning,
					fmt.Sprintf("operation %q response %s should reference Error schema", opID, statusCode)))
			}
		}
	})
	return vs
}

// === OAL022: Enums should have >= 2 values ===

type ruleOAL022 struct{}

func (ruleOAL022) ID() string                { return "OAL022" }
func (ruleOAL022) Description() string       { return "Enums should have at least 2 values" }
func (ruleOAL022) DefaultSeverity() Severity { return SeverityInfo }

func (ruleOAL022) Check(ctx *LintContext) []Violation {
	var vs []Violation
	var walk func(n *yaml.Node, context string)
	walk = func(n *yaml.Node, context string) {
		if n == nil {
			return
		}
		if n.Kind == yaml.MappingNode {
			enumNode := mapGet(n, "enum")
			if enumNode != nil && enumNode.Kind == yaml.SequenceNode && len(enumNode.Content) < 2 {
				vs = append(vs, ctx.Violation(enumNode.Line, "OAL022", SeverityInfo,
					fmt.Sprintf("enum%s has only %d value(s)", context, len(enumNode.Content))))
			}
		}
		for _, c := range n.Content {
			walk(c, context)
		}
	}

	// Walk schemas to get better context in messages.
	components := mapGet(ctx.Root, "components")
	if components != nil {
		schemas := mapGet(components, "schemas")
		if schemas != nil {
			for i := 0; i < len(schemas.Content)-1; i += 2 {
				schemaName := schemas.Content[i].Value
				walk(schemas.Content[i+1], fmt.Sprintf(" in schema %q", schemaName))
			}
		}
	}

	// Also walk paths for inline enums in parameters.
	paths := mapGet(ctx.Root, "paths")
	if paths != nil {
		walk(paths, "")
	}
	return vs
}

// === OAL023: Operations should have a description ===

type ruleOAL023 struct{}

func (ruleOAL023) ID() string                { return "OAL023" }
func (ruleOAL023) Description() string       { return "Operations should have a description" }
func (ruleOAL023) DefaultSeverity() Severity { return SeverityInfo }

func (ruleOAL023) Check(ctx *LintContext) []Violation {
	var vs []Violation
	ctx.ForEachOperation(func(path, method string, op *yaml.Node) {
		if mapGet(op, "description") == nil {
			opID := operationID(op)
			if opID == "" {
				opID = method + " " + path
			}
			vs = append(vs, ctx.Violation(op.Line, "OAL023", SeverityInfo,
				fmt.Sprintf("operation %q is missing 'description'", opID)))
		}
	})
	return vs
}

// === OAL024: DELETE operations should include 204 response ===

type ruleOAL024 struct{}

func (ruleOAL024) ID() string                { return "OAL024" }
func (ruleOAL024) Description() string       { return "DELETE operations should include 204 response" }
func (ruleOAL024) DefaultSeverity() Severity { return SeverityWarning }

func (ruleOAL024) Check(ctx *LintContext) []Violation {
	var vs []Violation
	ctx.ForEachOperation(func(path, method string, op *yaml.Node) {
		if method != "delete" {
			return
		}
		responses := mapGet(op, "responses")
		if responses == nil {
			return
		}
		if mapGet(responses, "204") != nil {
			return
		}
		opID := operationID(op)
		if opID == "" {
			opID = method + " " + path
		}
		vs = append(vs, ctx.Violation(responses.Line, "OAL024", SeverityWarning,
			fmt.Sprintf("DELETE operation %q should include a 204 response", opID)))
	})
	return vs
}

// === OAL025: Pagination params without Paginated* response ===

type ruleOAL025 struct{}

func (ruleOAL025) ID() string { return "OAL025" }
func (ruleOAL025) Description() string {
	return "GET with pagination params should return a Paginated* schema"
}
func (ruleOAL025) DefaultSeverity() Severity { return SeverityWarning }

func (ruleOAL025) Check(ctx *LintContext) []Violation {
	var vs []Violation
	ctx.ForEachOperation(func(path, method string, op *yaml.Node) {
		if method != "get" {
			return
		}

		hasMaxResults, hasPageToken := checkPaginationParams(ctx, path, op)
		if !hasMaxResults && !hasPageToken {
			return // no pagination params, nothing to check
		}

		// Check if 200 response references a Paginated* schema.
		responses := mapGet(op, "responses")
		if responses == nil {
			return
		}
		resp200 := mapGet(responses, "200")
		if resp200 == nil {
			return
		}
		content := mapGet(resp200, "content")
		if content == nil {
			return
		}
		appJSON := mapGet(content, "application/json")
		if appJSON == nil {
			return
		}
		schema := mapGet(appJSON, "schema")
		if schema == nil {
			return
		}
		ref := mapGet(schema, "$ref")
		if ref == nil {
			return // inline schema — OAL004 handles that
		}
		parts := strings.Split(ref.Value, "/")
		schemaName := parts[len(parts)-1]
		if strings.HasPrefix(schemaName, "Paginated") {
			return // consistent
		}

		opID := operationID(op)
		if opID == "" {
			opID = method + " " + path
		}
		vs = append(vs, ctx.Violation(op.Line, "OAL025", SeverityWarning,
			fmt.Sprintf("GET operation %q has pagination params but response references %q (not Paginated*)", opID, schemaName)))
	})
	return vs
}
