package apilint

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/daveshanley/vacuum/model"
	"go.yaml.in/yaml/v4"
)

// customFunctions returns the map of custom vacuum rule functions.
func customFunctions() map[string]model.RuleFunction {
	return map[string]model.RuleFunction{
		"checkSchemaRef":             &fnCheckSchemaRef{},
		"checkPaginationParams":      &fnCheckPaginationParams{},
		"checkCollectionOrdering":    &fnCheckCollectionOrdering{},
		"checkSecuredEndpoint401":    &fnCheckSecuredEndpoint401{},
		"checkPaginatedSchema":       &fnCheckPaginatedSchema{},
		"checkPostCreateStatus":      &fnCheckPostCreateStatus{},
		"checkMutatingOps403":        &fnCheckMutatingOps403{},
		"checkGetResource404":        &fnCheckGetResource404{},
		"checkCreateRequestRequired": &fnCheckCreateRequestRequired{},
		"checkErrorSchemaRef":        &fnCheckErrorSchemaRef{},
		"checkEnumMinValues":         &fnCheckEnumMinValues{},
		"checkDeleteReturns204":      &fnCheckDeleteReturns204{},
		"checkPaginationSchemaMatch": &fnCheckPaginationSchemaMatch{},
		"checkDiscriminatorRequired": &fnCheckDiscriminatorRequired{},
		"checkReadOnlySystemFields":  &fnCheckReadOnlySystemFields{},
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

func yKeys(m *yaml.Node) []string {
	if m == nil || m.Kind != yaml.MappingNode {
		return nil
	}
	var keys []string
	for i := 0; i < len(m.Content)-1; i += 2 {
		keys = append(keys, m.Content[i].Value)
	}
	return keys
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

var camelRe = regexp.MustCompile(`^[a-z][a-zA-Z0-9]*$`)
var snakeRe = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)

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
// OAL006 — GET before POST ordering on collection paths
// ================================================================

type fnCheckCollectionOrdering struct{}

func (f *fnCheckCollectionOrdering) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkCollectionOrdering"}
}
func (f *fnCheckCollectionOrdering) GetCategory() string { return model.CategoryOperations }

func (f *fnCheckCollectionOrdering) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
	root := rootNode(nodes)
	if root == nil {
		return nil
	}
	paths := yGet(root, "paths")
	if paths == nil {
		return nil
	}
	var results []model.RuleFunctionResult
	for i := 0; i < len(paths.Content)-1; i += 2 {
		pathKey := paths.Content[i].Value
		pathItem := paths.Content[i+1]
		if pathItem.Kind != yaml.MappingNode {
			continue
		}
		getLine, postLine := 0, 0
		var postNode *yaml.Node
		for j := 0; j < len(pathItem.Content)-1; j += 2 {
			m := pathItem.Content[j].Value
			l := int(pathItem.Content[j].Line)
			if m == "get" {
				getLine = l
			}
			if m == "post" {
				postLine = l
				postNode = pathItem.Content[j]
			}
		}
		if getLine > 0 && postLine > 0 && postLine < getLine && postNode != nil {
			results = append(results, makeResult(
				fmt.Sprintf("on %q, POST (line %d) is declared before GET (line %d)", pathKey, postLine, getLine),
				fmt.Sprintf("$.paths.%s", pathKey),
				"check-collection-ordering", postNode, ctx))
		}
	}
	return results
}

// ================================================================
// OAL009 — secured endpoints should include 401
// ================================================================

type fnCheckSecuredEndpoint401 struct{}

func (f *fnCheckSecuredEndpoint401) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkSecuredEndpoint401"}
}
func (f *fnCheckSecuredEndpoint401) GetCategory() string { return model.CategorySecurity }

func (f *fnCheckSecuredEndpoint401) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
	root := rootNode(nodes)
	if root == nil {
		return nil
	}
	if !hasGlobalSecurity(root) {
		return nil
	}
	var results []model.RuleFunctionResult
	forEachOp(root, func(path, method string, op *yaml.Node) {
		sec := yGet(op, "security")
		if sec != nil && len(sec.Content) == 0 {
			return
		}
		responses := yGet(op, "responses")
		if responses == nil {
			return
		}
		if yGet(responses, "401") != nil {
			return
		}
		opID := yOpID(op)
		if opID == "" {
			opID = method + " " + path
		}
		results = append(results, makeResult(
			fmt.Sprintf("operation %q has global security but no 401 response", opID),
			fmt.Sprintf("$.paths.%s.%s.responses", path, method),
			"check-secured-endpoint-401", responses, ctx))
	})
	return results
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
// OAL017 — Create*Request schemas should have required
// ================================================================

type fnCheckCreateRequestRequired struct{}

func (f *fnCheckCreateRequestRequired) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkCreateRequestRequired"}
}
func (f *fnCheckCreateRequestRequired) GetCategory() string { return model.CategorySchemas }

func (f *fnCheckCreateRequestRequired) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
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
		if !strings.HasPrefix(name, "Create") || !strings.HasSuffix(name, "Request") {
			continue
		}
		schema := schemas.Content[i+1]
		if yGet(schema, "required") == nil {
			results = append(results, makeResult(
				fmt.Sprintf("schema %q should have a 'required' array", name),
				fmt.Sprintf("$.components.schemas.%s", name),
				"check-create-request-required", schema, ctx))
		}
	}
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
// OAL022 — enums should have >= 2 values
// ================================================================

type fnCheckEnumMinValues struct{}

func (f *fnCheckEnumMinValues) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkEnumMinValues"}
}
func (f *fnCheckEnumMinValues) GetCategory() string { return model.CategorySchemas }

func (f *fnCheckEnumMinValues) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
	root := rootNode(nodes)
	if root == nil {
		return nil
	}
	var results []model.RuleFunctionResult

	var walk func(n *yaml.Node, context string)
	walk = func(n *yaml.Node, context string) {
		if n == nil {
			return
		}
		if n.Kind == yaml.MappingNode {
			enumNode := yGet(n, "enum")
			if enumNode != nil && enumNode.Kind == yaml.SequenceNode && len(enumNode.Content) < 2 {
				results = append(results, makeResult(
					fmt.Sprintf("enum%s has only %d value(s)", context, len(enumNode.Content)),
					"$",
					"check-enum-min-values", enumNode, ctx))
			}
		}
		for _, c := range n.Content {
			walk(c, context)
		}
	}

	// Walk schemas for better context.
	schemas := yGet(yGet(root, "components"), "schemas")
	if schemas != nil {
		for i := 0; i < len(schemas.Content)-1; i += 2 {
			schemaName := schemas.Content[i].Value
			walk(schemas.Content[i+1], fmt.Sprintf(" in schema %q", schemaName))
		}
	}

	// Walk paths for inline enums.
	paths := yGet(root, "paths")
	if paths != nil {
		walk(paths, "")
	}
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

// ================================================================
// NEW — system fields should be readOnly
// ================================================================

type fnCheckReadOnlySystemFields struct{}

func (f *fnCheckReadOnlySystemFields) GetSchema() model.RuleFunctionSchema {
	return model.RuleFunctionSchema{Name: "checkReadOnlySystemFields"}
}
func (f *fnCheckReadOnlySystemFields) GetCategory() string { return model.CategorySchemas }

var systemFields = map[string]bool{
	"id": true, "created_at": true, "updated_at": true,
}

func (f *fnCheckReadOnlySystemFields) RunRule(nodes []*yaml.Node, ctx model.RuleFunctionContext) []model.RuleFunctionResult {
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
		props := yGet(schema, "properties")
		if props == nil || props.Kind != yaml.MappingNode {
			continue
		}
		for j := 0; j < len(props.Content)-1; j += 2 {
			propName := props.Content[j].Value
			if !systemFields[propName] {
				continue
			}
			propSchema := props.Content[j+1]
			if propSchema.Kind != yaml.MappingNode {
				continue
			}
			roNode := yGet(propSchema, "readOnly")
			if roNode == nil || roNode.Value != "true" {
				results = append(results, makeResult(
					fmt.Sprintf("schema %q property %q should be readOnly: true", name, propName),
					fmt.Sprintf("$.components.schemas.%s.properties.%s", name, propName),
					"check-read-only-system-fields", props.Content[j], ctx))
			}
		}
	}
	return results
}
