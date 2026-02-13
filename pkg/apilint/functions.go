package apilint

import (
	"fmt"
	"strings"

	"github.com/daveshanley/vacuum/model"
	"go.yaml.in/yaml/v4"
)

// customFunctions returns the map of custom vacuum rule functions.
func customFunctions() map[string]model.RuleFunction {
	return map[string]model.RuleFunction{
		"checkSchemaRef":             &fnCheckSchemaRef{},
		"checkPaginationParams":      &fnCheckPaginationParams{},
		"checkPaginatedSchema":       &fnCheckPaginatedSchema{},
		"checkPostCreateStatus":      &fnCheckPostCreateStatus{},
		"checkMutatingOps403":        &fnCheckMutatingOps403{},
		"checkGetResource404":        &fnCheckGetResource404{},
		"checkErrorSchemaRef":        &fnCheckErrorSchemaRef{},
		"checkDeleteReturns204":      &fnCheckDeleteReturns204{},
		"checkPaginationSchemaMatch": &fnCheckPaginationSchemaMatch{},
		"checkDiscriminatorRequired": &fnCheckDiscriminatorRequired{},
	}
}

// === YAML helpers ===

func yGet(m *yaml.Node, key string) *yaml.Node {
	if m == nil || m.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i < len(m.Content)-1; i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

func yOpID(op *yaml.Node) string {
	n := yGet(op, "operationId")
	if n != nil {
		return n.Value
	}
	return ""
}

var httpMethodSet = map[string]bool{
	"get": true, "put": true, "post": true, "delete": true,
	"options": true, "head": true, "patch": true, "trace": true,
}

type opVisitor = func(path, method string, op *yaml.Node)

func forEachOp(root *yaml.Node, fn opVisitor) {
	paths := yGet(root, "paths")
	if paths == nil {
		return
	}
	for i := 0; i < len(paths.Content)-1; i += 2 {
		pathKey := paths.Content[i].Value
		pathItem := paths.Content[i+1]
		if pathItem.Kind != yaml.MappingNode {
			continue
		}
		for j := 0; j < len(pathItem.Content)-1; j += 2 {
			method := pathItem.Content[j].Value
			if httpMethodSet[method] {
				fn(pathKey, method, pathItem.Content[j+1])
			}
		}
	}
}

func hasGlobalSecurity(root *yaml.Node) bool {
	sec := yGet(root, "security")
	return sec != nil && len(sec.Content) > 0
}

func rootNode(nodes []*yaml.Node) *yaml.Node {
	if len(nodes) == 0 {
		return nil
	}
	n := nodes[0]
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		return n.Content[0]
	}
	return n
}

func makeResult(msg, path, ruleID string, node *yaml.Node, ctx model.RuleFunctionContext) model.RuleFunctionResult {
	return model.RuleFunctionResult{
		Message:   msg,
		Path:      path,
		RuleId:    ruleID,
		StartNode: node,
		EndNode:   node,
		Rule:      ctx.Rule,
	}
}

// ================================================================
// OAL004 — response + request schemas must use $ref
// ================================================================

type fnCheckSchemaRef struct{}

func (f *fnCheckSchemaRef) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkSchemaRef"}
}
func (f *fnCheckSchemaRef) GetCategory() string { return model.CategoryOperations }

func (f *fnCheckSchemaRef) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
	root := rootNode(nodes)
	if root == nil {
		return nil
	}
	var results []model.RuleFunctionResult
	forEachOp(root, func(path, method string, op *yaml.Node) {
		opID := yOpID(op)
		if opID == "" {
			opID = method + " " + path
		}
		// Check response schemas.
		responses := yGet(op, "responses")
		if responses != nil {
			for i := 0; i < len(responses.Content)-1; i += 2 {
				statusCode := responses.Content[i].Value
				responseObj := responses.Content[i+1]
				if n := findInlineSchema(responseObj); n != nil {
					results = append(results, makeResult(
						fmt.Sprintf("operation %q response %s uses inline schema instead of $ref", opID, statusCode),
						fmt.Sprintf("$.paths.%s.%s.responses.%s", path, method, statusCode),
						"check-schema-ref", n, ctx))
				}
			}
		}
		// Check request body schema.
		reqBody := yGet(op, "requestBody")
		if reqBody != nil {
			if n := findInlineSchema(reqBody); n != nil {
				results = append(results, makeResult(
					fmt.Sprintf("operation %q requestBody uses inline schema instead of $ref", opID),
					fmt.Sprintf("$.paths.%s.%s.requestBody", path, method),
					"check-schema-ref", n, ctx))
			}
		}
	})
	return results
}

func findInlineSchema(obj *yaml.Node) *yaml.Node {
	content := yGet(obj, "content")
	if content == nil {
		return nil
	}
	appJSON := yGet(content, "application/json")
	if appJSON == nil {
		return nil
	}
	schema := yGet(appJSON, "schema")
	if schema == nil {
		return nil
	}
	if yGet(schema, "$ref") == nil {
		return schema
	}
	return nil
}

// ================================================================
// OAL005 — paginated GET endpoints must include MaxResults + PageToken
// ================================================================

type fnCheckPaginationParams struct{}

func (f *fnCheckPaginationParams) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkPaginationParams"}
}
func (f *fnCheckPaginationParams) GetCategory() string { return model.CategoryOperations }

func (f *fnCheckPaginationParams) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
	root := rootNode(nodes)
	if root == nil {
		return nil
	}
	var results []model.RuleFunctionResult
	forEachOp(root, func(path, method string, op *yaml.Node) {
		if method != "get" {
			return
		}
		ref := getResponseSchemaRef(op, "200")
		if ref == "" {
			return
		}
		parts := strings.Split(ref, "/")
		schemaName := parts[len(parts)-1]
		if !strings.HasPrefix(schemaName, "Paginated") {
			return
		}
		hasMax, hasPage := checkPagParams(root, path, op)
		if hasMax && hasPage {
			return
		}
		opID := yOpID(op)
		if opID == "" {
			opID = method + " " + path
		}
		var missing []string
		if !hasMax {
			missing = append(missing, "MaxResults")
		}
		if !hasPage {
			missing = append(missing, "PageToken")
		}
		results = append(results, makeResult(
			fmt.Sprintf("paginated endpoint %q missing %s parameters", opID, strings.Join(missing, ", ")),
			fmt.Sprintf("$.paths.%s.get", path),
			"check-pagination-params", op, ctx))
	})
	return results
}

func getResponseSchemaRef(op *yaml.Node, status string) string {
	responses := yGet(op, "responses")
	if responses == nil {
		return ""
	}
	resp := yGet(responses, status)
	if resp == nil {
		return ""
	}
	content := yGet(resp, "content")
	if content == nil {
		return ""
	}
	appJSON := yGet(content, "application/json")
	if appJSON == nil {
		return ""
	}
	schema := yGet(appJSON, "schema")
	if schema == nil {
		return ""
	}
	ref := yGet(schema, "$ref")
	if ref == nil {
		return ""
	}
	return ref.Value
}

func checkPagParams(root *yaml.Node, path string, op *yaml.Node) (hasMaxResults, hasPageToken bool) {
	check := func(params *yaml.Node) {
		if params == nil {
			return
		}
		for _, p := range params.Content {
			if p.Kind == yaml.MappingNode {
				nameNode := yGet(p, "name")
				if nameNode != nil {
					if nameNode.Value == "max_results" {
						hasMaxResults = true
					}
					if nameNode.Value == "page_token" {
						hasPageToken = true
					}
				}
				refNode := yGet(p, "$ref")
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
	check(yGet(op, "parameters"))
	pathsNode := yGet(root, "paths")
	if pathsNode != nil {
		pathItem := yGet(pathsNode, path)
		if pathItem != nil {
			check(yGet(pathItem, "parameters"))
		}
	}
	return
}

// ================================================================
// OAL010 — Paginated* schema structure
// ================================================================

type fnCheckPaginatedSchema struct{}

func (f *fnCheckPaginatedSchema) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkPaginatedSchema"}
}
func (f *fnCheckPaginatedSchema) GetCategory() string { return model.CategorySchemas }

func (f *fnCheckPaginatedSchema) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
	root := rootNode(nodes)
	if root == nil {
		return nil
	}
	schemas := yGet(yGet(root, "components"), "schemas")
	if schemas == nil {
		return nil
	}
	var results []model.RuleFunctionResult
	for i := 0; i < len(schemas.Content)-1; i += 2 {
		name := schemas.Content[i].Value
		if !strings.HasPrefix(name, "Paginated") {
			continue
		}
		schema := schemas.Content[i+1]
		props := yGet(schema, "properties")
		if props == nil {
			results = append(results, makeResult(
				fmt.Sprintf("paginated schema %q has no properties", name),
				fmt.Sprintf("$.components.schemas.%s", name),
				"check-paginated-schema", schema, ctx))
			continue
		}
		data := yGet(props, "data")
		if data == nil {
			results = append(results, makeResult(
				fmt.Sprintf("paginated schema %q missing 'data' field", name),
				fmt.Sprintf("$.components.schemas.%s", name),
				"check-paginated-schema", schema, ctx))
		} else {
			typeNode := yGet(data, "type")
			if typeNode == nil || typeNode.Value != "array" {
				results = append(results, makeResult(
					fmt.Sprintf("paginated schema %q 'data' field must be type: array", name),
					fmt.Sprintf("$.components.schemas.%s.properties.data", name),
					"check-paginated-schema", data, ctx))
			}
		}
		npt := yGet(props, "next_page_token")
		if npt == nil {
			results = append(results, makeResult(
				fmt.Sprintf("paginated schema %q missing 'next_page_token' field", name),
				fmt.Sprintf("$.components.schemas.%s", name),
				"check-paginated-schema", schema, ctx))
		} else {
			typeNode := yGet(npt, "type")
			if typeNode == nil || typeNode.Value != "string" {
				results = append(results, makeResult(
					fmt.Sprintf("paginated schema %q 'next_page_token' field must be type: string", name),
					fmt.Sprintf("$.components.schemas.%s.properties.next_page_token", name),
					"check-paginated-schema", npt, ctx))
			}
		}
	}
	return results
}

// ================================================================
// OAL012 — POST-create should return 201
// ================================================================

type fnCheckPostCreateStatus struct{}

func (f *fnCheckPostCreateStatus) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkPostCreateStatus"}
}
func (f *fnCheckPostCreateStatus) GetCategory() string { return model.CategoryOperations }

var actionVerbSet = map[string]bool{
	"executeQuery": true, "profileTable": true, "commitTableIngestion": true,
	"loadTableExternalFiles": true, "purgeLineage": true, "cleanupExpiredAPIKeys": true,
	"createManifest": true, "createUploadUrl": true, "searchCatalog": true,
	"setDefaultCatalog": true,
}

func (f *fnCheckPostCreateStatus) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
	root := rootNode(nodes)
	if root == nil {
		return nil
	}
	var results []model.RuleFunctionResult
	forEachOp(root, func(path, method string, op *yaml.Node) {
		if method != "post" {
			return
		}
		opID := yOpID(op)
		if actionVerbSet[opID] {
			return
		}
		responses := yGet(op, "responses")
		if responses == nil {
			return
		}
		has200 := yGet(responses, "200") != nil
		has201 := yGet(responses, "201") != nil
		if has200 && !has201 {
			if opID == "" {
				opID = method + " " + path
			}
			results = append(results, makeResult(
				fmt.Sprintf("POST operation %q returns 200 instead of 201", opID),
				fmt.Sprintf("$.paths.%s.post.responses", path),
				"check-post-create-status", responses, ctx))
		}
	})
	return results
}

// ================================================================
// OAL014 — mutating ops should include 403
// ================================================================

type fnCheckMutatingOps403 struct{}

func (f *fnCheckMutatingOps403) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkMutatingOps403"}
}
func (f *fnCheckMutatingOps403) GetCategory() string { return model.CategorySecurity }

var mutatingMethodSet = map[string]bool{
	"post": true, "put": true, "patch": true, "delete": true,
}

func (f *fnCheckMutatingOps403) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
	root := rootNode(nodes)
	if root == nil {
		return nil
	}
	if !hasGlobalSecurity(root) {
		return nil
	}
	var results []model.RuleFunctionResult
	forEachOp(root, func(path, method string, op *yaml.Node) {
		if !mutatingMethodSet[method] {
			return
		}
		sec := yGet(op, "security")
		if sec != nil && len(sec.Content) == 0 {
			return
		}
		responses := yGet(op, "responses")
		if responses == nil {
			return
		}
		if yGet(responses, "403") != nil {
			return
		}
		opID := yOpID(op)
		if opID == "" {
			opID = method + " " + path
		}
		results = append(results, makeResult(
			fmt.Sprintf("mutating operation %q should include a 403 response", opID),
			fmt.Sprintf("$.paths.%s.%s.responses", path, method),
			"check-mutating-ops-403", responses, ctx))
	})
	return results
}

// ================================================================
// OAL015 — GET resource paths should include 404
// ================================================================

type fnCheckGetResource404 struct{}

func (f *fnCheckGetResource404) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkGetResource404"}
}
func (f *fnCheckGetResource404) GetCategory() string { return model.CategoryOperations }

func (f *fnCheckGetResource404) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
	root := rootNode(nodes)
	if root == nil {
		return nil
	}
	var results []model.RuleFunctionResult
	forEachOp(root, func(path, method string, op *yaml.Node) {
		if method != "get" {
			return
		}
		if !strings.Contains(path, "{") {
			return
		}
		responses := yGet(op, "responses")
		if responses == nil {
			return
		}
		if yGet(responses, "404") != nil {
			return
		}
		opID := yOpID(op)
		if opID == "" {
			opID = method + " " + path
		}
		results = append(results, makeResult(
			fmt.Sprintf("GET operation %q on resource path should include 404 response", opID),
			fmt.Sprintf("$.paths.%s.get.responses", path),
			"check-get-resource-404", responses, ctx))
	})
	return results
}

// ================================================================
// OAL021 — error responses should use Error schema
// ================================================================

type fnCheckErrorSchemaRef struct{}

func (f *fnCheckErrorSchemaRef) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkErrorSchemaRef"}
}
func (f *fnCheckErrorSchemaRef) GetCategory() string { return model.CategoryOperations }

func (f *fnCheckErrorSchemaRef) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
	root := rootNode(nodes)
	if root == nil {
		return nil
	}
	var results []model.RuleFunctionResult
	forEachOp(root, func(path, method string, op *yaml.Node) {
		responses := yGet(op, "responses")
		if responses == nil {
			return
		}
		opID := yOpID(op)
		if opID == "" {
			opID = method + " " + path
		}
		for i := 0; i < len(responses.Content)-1; i += 2 {
			statusCode := responses.Content[i].Value
			if strings.HasPrefix(statusCode, "2") {
				continue
			}
			responseObj := responses.Content[i+1]
			content := yGet(responseObj, "content")
			if content == nil {
				continue
			}
			appJSON := yGet(content, "application/json")
			if appJSON == nil {
				continue
			}
			schema := yGet(appJSON, "schema")
			if schema == nil {
				continue
			}
			ref := yGet(schema, "$ref")
			if ref == nil || !strings.HasSuffix(ref.Value, "/Error") {
				results = append(results, makeResult(
					fmt.Sprintf("operation %q response %s should reference Error schema", opID, statusCode),
					fmt.Sprintf("$.paths.%s.%s.responses.%s", path, method, statusCode),
					"check-error-schema-ref", schema, ctx))
			}
		}
	})
	return results
}

// ================================================================
// OAL024 — DELETE should include 204
// ================================================================

type fnCheckDeleteReturns204 struct{}

func (f *fnCheckDeleteReturns204) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkDeleteReturns204"}
}
func (f *fnCheckDeleteReturns204) GetCategory() string { return model.CategoryOperations }

func (f *fnCheckDeleteReturns204) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
	root := rootNode(nodes)
	if root == nil {
		return nil
	}
	var results []model.RuleFunctionResult
	forEachOp(root, func(path, method string, op *yaml.Node) {
		if method != "delete" {
			return
		}
		responses := yGet(op, "responses")
		if responses == nil {
			return
		}
		if yGet(responses, "204") != nil {
			return
		}
		opID := yOpID(op)
		if opID == "" {
			opID = method + " " + path
		}
		results = append(results, makeResult(
			fmt.Sprintf("DELETE operation %q should include a 204 response", opID),
			fmt.Sprintf("$.paths.%s.delete.responses", path),
			"check-delete-returns-204", responses, ctx))
	})
	return results
}

// ================================================================
// OAL025 — pagination params should match Paginated* response
// ================================================================

type fnCheckPaginationSchemaMatch struct{}

func (f *fnCheckPaginationSchemaMatch) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkPaginationSchemaMatch"}
}
func (f *fnCheckPaginationSchemaMatch) GetCategory() string { return model.CategoryOperations }

func (f *fnCheckPaginationSchemaMatch) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
	root := rootNode(nodes)
	if root == nil {
		return nil
	}
	var results []model.RuleFunctionResult
	forEachOp(root, func(path, method string, op *yaml.Node) {
		if method != "get" {
			return
		}
		hasMax, hasPage := checkPagParams(root, path, op)
		if !hasMax && !hasPage {
			return
		}
		ref := getResponseSchemaRef(op, "200")
		if ref == "" {
			return
		}
		parts := strings.Split(ref, "/")
		schemaName := parts[len(parts)-1]
		if strings.HasPrefix(schemaName, "Paginated") {
			return
		}
		opID := yOpID(op)
		if opID == "" {
			opID = method + " " + path
		}
		results = append(results, makeResult(
			fmt.Sprintf("GET operation %q has pagination params but response references %q (not Paginated*)", opID, schemaName),
			fmt.Sprintf("$.paths.%s.get", path),
			"check-pagination-schema-match", op, ctx))
	})
	return results
}

// ================================================================
// NEW — oneOf/anyOf should have discriminator
// ================================================================

type fnCheckDiscriminatorRequired struct{}

func (f *fnCheckDiscriminatorRequired) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkDiscriminatorRequired"}
}
func (f *fnCheckDiscriminatorRequired) GetCategory() string { return model.CategorySchemas }

func (f *fnCheckDiscriminatorRequired) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
	root := rootNode(nodes)
	if root == nil {
		return nil
	}
	schemas := yGet(yGet(root, "components"), "schemas")
	if schemas == nil {
		return nil
	}
	var results []model.RuleFunctionResult
	for i := 0; i < len(schemas.Content)-1; i += 2 {
		name := schemas.Content[i].Value
		schema := schemas.Content[i+1]
		if schema.Kind != yaml.MappingNode {
			continue
		}
		for _, combiner := range []string{"oneOf", "anyOf"} {
			list := yGet(schema, combiner)
			if list == nil || list.Kind != yaml.SequenceNode || len(list.Content) < 2 {
				continue
			}
			if yGet(schema, "discriminator") == nil {
				results = append(results, makeResult(
					fmt.Sprintf("schema %q uses %s without a discriminator", name, combiner),
					fmt.Sprintf("$.components.schemas.%s", name),
					"check-discriminator-required", schema, ctx))
			}
		}
	}
	return results
}
