package apilint

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// OAL001: Every operation must have a tags field.
func (l *Linter) checkOperationTags() []Violation {
	var vs []Violation
	l.forEachOperation(func(path, method string, op *yaml.Node) {
		if mapGet(op, "tags") == nil {
			opID := operationID(op)
			if opID == "" {
				opID = method + " " + path
			}
			vs = append(vs, l.violation(op.Line, "OAL001", SeverityError,
				fmt.Sprintf("operation %q is missing 'tags' field", opID)))
		}
	})
	return vs
}

// OAL002: operationId must be present and unique.
func (l *Linter) checkOperationIDs() []Violation {
	var vs []Violation
	seen := map[string]int{} // operationId → first line
	l.forEachOperation(func(path, method string, op *yaml.Node) {
		idNode := mapGet(op, "operationId")
		if idNode == nil {
			vs = append(vs, l.violation(op.Line, "OAL002", SeverityError,
				fmt.Sprintf("operation %s %s is missing 'operationId'", method, path)))
			return
		}
		if prev, ok := seen[idNode.Value]; ok {
			vs = append(vs, l.violation(idNode.Line, "OAL002", SeverityError,
				fmt.Sprintf("duplicate operationId %q (first seen at line %d)", idNode.Value, prev)))
			return
		}
		seen[idNode.Value] = idNode.Line
	})
	return vs
}

// OAL003: DELETE operations should not have a requestBody.
func (l *Linter) checkDeleteRequestBody() []Violation {
	var vs []Violation
	l.forEachOperation(func(path, method string, op *yaml.Node) {
		if method != "delete" {
			return
		}
		if body := mapGet(op, "requestBody"); body != nil {
			opID := operationID(op)
			if opID == "" {
				opID = method + " " + path
			}
			vs = append(vs, l.violation(body.Line, "OAL003", SeverityWarning,
				fmt.Sprintf("DELETE operation %q has a requestBody", opID)))
		}
	})
	return vs
}

// OAL004: All response schemas should use $ref (no inline anonymous objects).
func (l *Linter) checkInlineResponseSchemas() []Violation {
	var vs []Violation
	l.forEachOperation(func(path, method string, op *yaml.Node) {
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
			// Check if schema uses $ref
			if mapGet(schema, "$ref") == nil {
				vs = append(vs, l.violation(schema.Line, "OAL004", SeverityWarning,
					fmt.Sprintf("operation %q response %s uses inline schema instead of $ref", opID, statusCode)))
			}
		}
	})
	return vs
}

// OAL005: GET endpoints returning Paginated* schemas must include MaxResults + PageToken params.
func (l *Linter) checkPaginatedEndpointParams() []Violation {
	var vs []Violation
	l.forEachOperation(func(path, method string, op *yaml.Node) {
		if method != "get" {
			return
		}
		// Check if 200 response references a Paginated* schema
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
		// Check if it references a Paginated* schema
		parts := strings.Split(ref.Value, "/")
		schemaName := parts[len(parts)-1]
		if !strings.HasPrefix(schemaName, "Paginated") {
			return
		}

		// Check for MaxResults and PageToken in params
		hasMaxResults := false
		hasPageToken := false

		params := mapGet(op, "parameters")
		if params != nil {
			for _, p := range params.Content {
				if p.Kind == yaml.MappingNode {
					// Check inline param
					nameNode := mapGet(p, "name")
					if nameNode != nil {
						if nameNode.Value == "max_results" {
							hasMaxResults = true
						}
						if nameNode.Value == "page_token" {
							hasPageToken = true
						}
					}
					// Check $ref param
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

		// Also check path-level parameters
		pathsNode := mapGet(l.root, "paths")
		if pathsNode != nil {
			pathItem := mapGet(pathsNode, path)
			if pathItem != nil {
				pathParams := mapGet(pathItem, "parameters")
				if pathParams != nil {
					for _, p := range pathParams.Content {
						if p.Kind == yaml.MappingNode {
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
			}
		}

		opID := operationID(op)
		if opID == "" {
			opID = method + " " + path
		}

		if !hasMaxResults || !hasPageToken {
			missing := []string{}
			if !hasMaxResults {
				missing = append(missing, "MaxResults")
			}
			if !hasPageToken {
				missing = append(missing, "PageToken")
			}
			vs = append(vs, l.violation(op.Line, "OAL005", SeverityError,
				fmt.Sprintf("paginated endpoint %q missing %s parameters", opID, strings.Join(missing, ", "))))
		}
	})
	return vs
}

// OAL006: On collection paths, GET should be declared before POST.
func (l *Linter) checkCollectionMethodOrder() []Violation {
	var vs []Violation
	paths := mapGet(l.root, "paths")
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
			vs = append(vs, l.violation(postLine, "OAL006", SeverityInfo,
				fmt.Sprintf("on %q, POST (line %d) is declared before GET (line %d)", pathKey, postLine, getLine)))
		}
	}
	return vs
}

// OAL007: Path parameters must be camelCase.
func (l *Linter) checkPathParamCamelCase() []Violation {
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
				vs = append(vs, l.violation(nameNode.Line, "OAL007", SeverityError,
					fmt.Sprintf("path parameter %q is not camelCase", nameNode.Value)))
			}
		}
	}

	paths := mapGet(l.root, "paths")
	if paths == nil {
		return nil
	}
	for i := 0; i < len(paths.Content)-1; i += 2 {
		pathItem := paths.Content[i+1]
		// Path-level params
		checkParams(mapGet(pathItem, "parameters"))
		// Operation-level params
		for j := 0; j < len(pathItem.Content)-1; j += 2 {
			method := pathItem.Content[j].Value
			if httpMethods[method] {
				checkParams(mapGet(pathItem.Content[j+1], "parameters"))
			}
		}
	}
	return vs
}

// OAL008: Query parameters must be snake_case.
func (l *Linter) checkQueryParamSnakeCase() []Violation {
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
				vs = append(vs, l.violation(nameNode.Line, "OAL008", SeverityError,
					fmt.Sprintf("query parameter %q is not snake_case", nameNode.Value)))
			}
		}
	}

	// Also check shared component parameters
	components := mapGet(l.root, "components")
	if components != nil {
		compParams := mapGet(components, "parameters")
		if compParams != nil {
			for i := 0; i < len(compParams.Content)-1; i += 2 {
				p := compParams.Content[i+1]
				inNode := mapGet(p, "in")
				if inNode != nil && inNode.Value == "query" {
					nameNode := mapGet(p, "name")
					if nameNode != nil && !snakeCaseRe.MatchString(nameNode.Value) {
						vs = append(vs, l.violation(nameNode.Line, "OAL008", SeverityError,
							fmt.Sprintf("query parameter %q is not snake_case", nameNode.Value)))
					}
				}
			}
		}
	}

	paths := mapGet(l.root, "paths")
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

// OAL009: All endpoints with global security should include 401 in responses.
func (l *Linter) checkMissing401() []Violation {
	if !l.hasGlobalSecurity() {
		return nil
	}
	var vs []Violation
	l.forEachOperation(func(path, method string, op *yaml.Node) {
		// Skip if operation overrides security to empty
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
		vs = append(vs, l.violation(responses.Line, "OAL009", SeverityWarning,
			fmt.Sprintf("operation %q has global security but no 401 response", opID)))
	})
	return vs
}

// OAL010: Paginated response schemas must have data (array) + next_page_token (string).
func (l *Linter) checkPaginatedSchemaShape() []Violation {
	var vs []Violation
	components := mapGet(l.root, "components")
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
			vs = append(vs, l.violation(schema.Line, "OAL010", SeverityError,
				fmt.Sprintf("paginated schema %q has no properties", name)))
			continue
		}
		data := mapGet(props, "data")
		if data == nil {
			vs = append(vs, l.violation(schema.Line, "OAL010", SeverityError,
				fmt.Sprintf("paginated schema %q missing 'data' field", name)))
		} else {
			typeNode := mapGet(data, "type")
			if typeNode == nil || typeNode.Value != "array" {
				vs = append(vs, l.violation(data.Line, "OAL010", SeverityError,
					fmt.Sprintf("paginated schema %q 'data' field must be type: array", name)))
			}
		}
		npt := mapGet(props, "next_page_token")
		if npt == nil {
			vs = append(vs, l.violation(schema.Line, "OAL010", SeverityError,
				fmt.Sprintf("paginated schema %q missing 'next_page_token' field", name)))
		} else {
			typeNode := mapGet(npt, "type")
			if typeNode == nil || typeNode.Value != "string" {
				vs = append(vs, l.violation(npt.Line, "OAL010", SeverityError,
					fmt.Sprintf("paginated schema %q 'next_page_token' field must be type: string", name)))
			}
		}
	}
	return vs
}

// OAL011: All $ref values must resolve to defined components.
func (l *Linter) checkRefsResolve() []Violation {
	var vs []Violation
	var walk func(n *yaml.Node)
	walk = func(n *yaml.Node) {
		if n == nil {
			return
		}
		if n.Kind == yaml.MappingNode {
			ref := mapGet(n, "$ref")
			if ref != nil && !l.resolveRef(ref.Value) {
				vs = append(vs, l.violation(ref.Line, "OAL011", SeverityError,
					fmt.Sprintf("unresolved $ref %q", ref.Value)))
			}
		}
		for _, c := range n.Content {
			walk(c)
		}
	}
	walk(l.root)
	return vs
}

// OAL012: POST-create endpoints should return 201 (not 200).
func (l *Linter) checkPostCreate201() []Violation {
	// Non-create POST endpoints (actions, queries, etc.) are excluded by name heuristic.
	actionVerbs := []string{"execute", "profile", "commit", "load", "purge",
		"cleanup", "search", "create_manifest", "bind", "unbind", "createManifest",
		"createUploadUrl", "commitTableIngestion", "loadTableExternalFiles",
		"purgeLineage", "cleanupExpiredAPIKeys", "profileTable"}
	isAction := func(opID string) bool {
		for _, v := range actionVerbs {
			if opID == v || strings.EqualFold(opID, v) {
				return true
			}
		}
		return false
	}

	var vs []Violation
	l.forEachOperation(func(path, method string, op *yaml.Node) {
		if method != "post" {
			return
		}
		opID := operationID(op)
		if isAction(opID) {
			return
		}
		// Check if there's a 200 but no 201
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
			vs = append(vs, l.violation(responses.Line, "OAL012", SeverityWarning,
				fmt.Sprintf("POST operation %q returns 200 instead of 201", opID)))
		}
	})
	return vs
}
