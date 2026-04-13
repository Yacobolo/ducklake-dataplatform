// Package openapi emits canonical OpenAPI YAML from JSON IR.
package openapi

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v4"

	"duck-demo/pkg/apigen/ir"
)

// Options configures OpenAPI emission.
type Options struct{}

// EmitYAML renders the canonical OpenAPI document from IR.
func EmitYAML(docIR ir.Document, _ Options) ([]byte, error) {
	root := mappingNode()
	appendKeyValue(root, "openapi", stringNode(openAPIVersion(docIR)))
	appendKeyValue(root, "info", infoNode(docIR.Info))

	if len(docIR.Tags) > 0 {
		appendKeyValue(root, "tags", tagsNode(docIR))
	}

	paths, err := pathsNode(docIR)
	if err != nil {
		return nil, err
	}
	appendKeyValue(root, "paths", paths)

	if len(docIR.OpenAPI.Security) > 0 {
		appendKeyValue(root, "security", securityRequirementsNode(docIR.OpenAPI.Security))
	}

	components, err := componentsNode(docIR)
	if err != nil {
		return nil, err
	}
	appendKeyValue(root, "components", components)

	if len(docIR.Servers) > 0 {
		appendKeyValue(root, "servers", serversNode(docIR.Servers))
	}

	var buffer bytes.Buffer
	encoder := yaml.NewEncoder(&buffer)
	encoder.SetIndent(2)
	if err := encoder.Encode(root); err != nil {
		return nil, fmt.Errorf("marshal openapi yaml: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return nil, fmt.Errorf("close openapi yaml encoder: %w", err)
	}
	return normalizeCanonicalYAML(buffer.Bytes()), nil
}

func openAPIVersion(doc ir.Document) string {
	if doc.OpenAPI.Version != "" {
		return doc.OpenAPI.Version
	}
	return "3.0.0"
}

func infoNode(info ir.Info) *yaml.Node {
	node := mappingNode()
	appendKeyValue(node, "title", stringNode(info.Title))
	appendKeyValue(node, "version", stringNode(info.Version))
	if info.Description != "" {
		appendKeyValue(node, "description", stringNode(info.Description))
	}
	return node
}

func tagsNode(doc ir.Document) *yaml.Node {
	node := sequenceNode()
	index := make(map[string]ir.Tag, len(doc.Tags))
	for _, tag := range doc.Tags {
		index[tag.Name] = tag
	}

	seen := map[string]struct{}{}
	for _, name := range doc.OpenAPI.TagOrder {
		tag, ok := index[name]
		if !ok {
			continue
		}
		node.Content = append(node.Content, tagNode(tag))
		seen[name] = struct{}{}
	}

	for _, tag := range doc.Tags {
		if _, ok := seen[tag.Name]; ok {
			continue
		}
		node.Content = append(node.Content, tagNode(tag))
	}
	return node
}

func tagNode(tag ir.Tag) *yaml.Node {
	node := mappingNode()
	appendKeyValue(node, "name", stringNode(tag.Name))
	if tag.Description != "" {
		appendKeyValue(node, "description", stringNode(tag.Description))
	}
	return node
}

func pathsNode(doc ir.Document) (*yaml.Node, error) {
	paths := mappingNode()
	grouped := map[string][]ir.Endpoint{}
	pathKeys := make([]string, 0)
	for _, endpoint := range doc.Endpoints {
		if _, ok := grouped[endpoint.Path]; !ok {
			pathKeys = append(pathKeys, endpoint.Path)
		}
		grouped[endpoint.Path] = append(grouped[endpoint.Path], endpoint)
	}

	for _, path := range pathKeys {
		itemNode := mappingNode()
		for _, endpoint := range grouped[path] {
			op, err := operationNode(endpoint)
			if err != nil {
				return nil, err
			}
			appendKeyValue(itemNode, strings.ToLower(endpoint.Method), op)
		}
		appendKeyValue(paths, path, itemNode)
	}

	return paths, nil
}

func operationNode(endpoint ir.Endpoint) (*yaml.Node, error) {
	if len(endpoint.Responses) == 0 {
		return nil, fmt.Errorf("at least one response is required for %s", endpoint.OperationID)
	}

	node := mappingNode()
	appendKeyValue(node, "operationId", stringNode(endpoint.OperationID))
	if endpoint.Summary != "" {
		appendKeyValue(node, "summary", stringNode(endpoint.Summary))
	}
	if endpoint.Description != "" {
		appendKeyValue(node, "description", stringNode(endpoint.Description))
	}

	parameters := sequenceNode()
	for _, parameter := range endpoint.Parameters {
		parameters.Content = append(parameters.Content, parameterNode(parameter))
	}
	appendKeyValue(node, "parameters", parameters)

	responses, err := responsesNode(endpoint.Responses)
	if err != nil {
		return nil, err
	}
	appendKeyValue(node, "responses", responses)

	if len(endpoint.Tags) > 0 {
		tags := sequenceNode()
		for _, tag := range endpoint.Tags {
			tags.Content = append(tags.Content, stringNode(tag))
		}
		appendKeyValue(node, "tags", tags)
	}

	if endpoint.RequestBody != nil {
		appendKeyValue(node, "requestBody", requestBodyNode(*endpoint.RequestBody))
	}

	security := endpoint.Security
	if len(security) == 0 {
		security = extensionSecurity(endpoint.Extensions["security"])
	}
	if len(security) > 0 && !isDefaultAuthenticatedSecurity(security) {
		appendKeyValue(node, "security", securityRequirementsNode(security))
	}

	extensionKeys := make([]string, 0, len(endpoint.Extensions))
	for key := range endpoint.Extensions {
		if key == "security" {
			continue
		}
		if key == "x-authz" && isAuthenticatedAuthz(endpoint.Extensions[key]) {
			continue
		}
		extensionKeys = append(extensionKeys, key)
	}
	sort.Slice(extensionKeys, func(i, j int) bool {
		order := map[string]int{
			"x-authz":         0,
			"x-apigen-manual": 1,
			"x-cli-command":   2,
		}
		if order[extensionKeys[i]] == order[extensionKeys[j]] {
			return extensionKeys[i] < extensionKeys[j]
		}
		return order[extensionKeys[i]] < order[extensionKeys[j]]
	})
	for _, key := range extensionKeys {
		appendKeyValue(node, key, anyToNode(endpoint.Extensions[key]))
	}

	return node, nil
}

func parameterNode(parameter ir.Parameter) *yaml.Node {
	node := mappingNode()
	appendKeyValue(node, "name", stringNode(parameter.Name))
	appendKeyValue(node, "in", stringNode(parameter.In))
	appendKeyValue(node, "required", boolNode(parameter.Required))
	appendKeyValue(node, "schema", schemaRefNode(parameter.Schema))
	if parameter.Explode != nil {
		appendKeyValue(node, "explode", boolNode(*parameter.Explode))
	}
	return node
}

func requestBodyNode(requestBody ir.RequestBody) *yaml.Node {
	node := mappingNode()
	appendKeyValue(node, "required", boolNode(requestBody.Required))
	contentType := requestBody.ContentType
	if contentType == "" {
		contentType = "application/json"
	}
	appendKeyValue(node, "content", singleContentNode(contentType, requestBody.Schema))
	return node
}

func responsesNode(responses []ir.Response) (*yaml.Node, error) {
	node := mappingNode()
	for _, response := range responses {
		entry := mappingNode()
		appendKeyValue(entry, "description", stringNode(canonicalResponseDescription(response)))
		if len(response.Headers) > 0 {
			headers := mappingNode()
			for _, header := range response.Headers {
				headerNode := mappingNode()
				appendKeyValue(headerNode, "required", boolNode(header.Required))
				if header.Description != "" {
					appendKeyValue(headerNode, "description", stringNode(header.Description))
				}
				appendKeyValue(headerNode, "schema", schemaRefNode(header.Schema))
				appendKeyValue(headers, header.Name, headerNode)
			}
			appendKeyValue(entry, "headers", headers)
		}
		if len(response.AnyOf) > 0 {
			contentType := response.ContentType
			if contentType == "" {
				contentType = "application/json"
			}
			appendKeyValue(entry, "content", singleAnyOfContentNode(contentType, response.AnyOf))
		} else if response.Schema != nil {
			contentType := response.ContentType
			if contentType == "" {
				contentType = "application/json"
			}
			appendKeyValue(entry, "content", singleContentNode(contentType, *response.Schema))
		}
		appendKeyValue(node, strconv.Itoa(response.StatusCode), entry)
	}
	return node, nil
}

func singleContentNode(contentType string, schema ir.SchemaRef) *yaml.Node {
	content := mappingNode()
	mediaType := mappingNode()
	appendKeyValue(mediaType, "schema", schemaRefNode(schema))
	appendKeyValue(content, contentType, mediaType)
	return content
}

func singleAnyOfContentNode(contentType string, schemas []ir.SchemaRef) *yaml.Node {
	content := mappingNode()
	mediaType := mappingNode()
	schema := mappingNode()
	anyOf := sequenceNode()
	for _, item := range schemas {
		anyOf.Content = append(anyOf.Content, schemaRefNode(item))
	}
	appendKeyValue(schema, "anyOf", anyOf)
	appendKeyValue(mediaType, "schema", schema)
	appendKeyValue(content, contentType, mediaType)
	return content
}

func securityRequirementsNode(requirements []ir.SecurityRequirement) *yaml.Node {
	node := sequenceNode()
	for _, requirement := range requirements {
		mapping := mappingNode()
		keys := make([]string, 0, len(requirement))
		for key := range requirement {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			order := map[string]int{"BearerAuth": 0, "ApiKeyAuth": 1}
			if order[keys[i]] == order[keys[j]] {
				return keys[i] < keys[j]
			}
			return order[keys[i]] < order[keys[j]]
		})
		for _, key := range keys {
			scopes := sequenceNode()
			for _, scope := range requirement[key] {
				scopes.Content = append(scopes.Content, stringNode(scope))
			}
			appendKeyValue(mapping, key, scopes)
		}
		node.Content = append(node.Content, mapping)
	}
	return node
}

func componentsNode(doc ir.Document) (*yaml.Node, error) {
	node := mappingNode()

	schemas := mappingNode()
	schemaKeys := make([]string, 0, len(doc.Schemas))
	for key := range doc.Schemas {
		if key == "Record" {
			continue
		}
		schemaKeys = append(schemaKeys, key)
	}
	sort.Strings(schemaKeys)
	for _, key := range schemaKeys {
		appendKeyValue(schemas, key, schemaNode(doc.Schemas[key]))
	}
	appendKeyValue(node, "schemas", schemas)

	securitySchemes := mappingNode()
	securityKeys := make([]string, 0, len(doc.OpenAPI.SecuritySchemes))
	for key := range doc.OpenAPI.SecuritySchemes {
		securityKeys = append(securityKeys, key)
	}
	sort.Slice(securityKeys, func(i, j int) bool {
		order := map[string]int{"BearerAuth": 0, "ApiKeyAuth": 1}
		if order[securityKeys[i]] == order[securityKeys[j]] {
			return securityKeys[i] < securityKeys[j]
		}
		return order[securityKeys[i]] < order[securityKeys[j]]
	})
	for _, key := range securityKeys {
		appendKeyValue(securitySchemes, key, securitySchemeNode(doc.OpenAPI.SecuritySchemes[key]))
	}
	appendKeyValue(node, "securitySchemes", securitySchemes)

	return node, nil
}

func schemaNode(schema ir.Schema) *yaml.Node {
	node := mappingNode()
	appendKeyValue(node, "type", stringNode(schema.Type))
	if len(schema.Enum) > 0 {
		enum := sequenceNode()
		for _, value := range schema.Enum {
			enum.Content = append(enum.Content, stringNode(value))
		}
		appendKeyValue(node, "enum", enum)
	}
	if len(schema.Required) > 0 {
		required := sequenceNode()
		for _, value := range schema.Required {
			required.Content = append(required.Content, stringNode(value))
		}
		appendKeyValue(node, "required", required)
	}
	if schema.Items != nil {
		appendKeyValue(node, "items", schemaRefNode(*schema.Items))
	}
	if len(schema.Properties) > 0 {
		properties := mappingNode()
		propertyKeys := orderedPropertyKeys(schema)
		for _, name := range propertyKeys {
			appendKeyValue(properties, name, schemaPropertyNode(schema.Properties[name]))
		}
		appendKeyValue(node, "properties", properties)
	}
	if schema.Description != "" {
		appendKeyValue(node, "description", stringNode(schema.Description))
	}
	if schema.Title != "" {
		appendKeyValue(node, "title", stringNode(schema.Title))
	}
	return node
}

func orderedPropertyKeys(schema ir.Schema) []string {
	if len(schema.Properties) == 0 {
		return nil
	}
	keys := make([]string, 0, len(schema.Properties))
	seen := map[string]struct{}{}
	for _, key := range schema.PropertyOrder {
		if _, ok := schema.Properties[key]; ok {
			keys = append(keys, key)
			seen[key] = struct{}{}
		}
	}
	remaining := make([]string, 0, len(schema.Properties)-len(keys))
	for key := range schema.Properties {
		if _, ok := seen[key]; ok {
			continue
		}
		remaining = append(remaining, key)
	}
	sort.Strings(remaining)
	keys = append(keys, remaining...)
	return keys
}

func schemaPropertyNode(property ir.SchemaProperty) *yaml.Node {
	node := schemaRefNode(property.Schema)
	if property.Description != "" {
		appendKeyValue(node, "description", stringNode(property.Description))
	}
	return node
}

func schemaRefNode(ref ir.SchemaRef) *yaml.Node {
	if ref.Ref != "" && ref.AdditionalProperties == nil {
		node := mappingNode()
		appendKeyValue(node, "$ref", stringNode("#/components/schemas/"+ref.Ref))
		return node
	}
	if ref.Type == "" && ref.Format == "" && ref.Items == nil && ref.AdditionalProperties == nil {
		return mappingNode()
	}

	node := mappingNode()
	appendKeyValue(node, "type", stringNode(ref.Type))
	if ref.Format != "" {
		appendKeyValue(node, "format", stringNode(ref.Format))
	}
	if ref.Items != nil {
		appendKeyValue(node, "items", schemaRefNode(*ref.Items))
	}
	if ref.AdditionalProperties != nil {
		appendKeyValue(node, "additionalProperties", additionalPropertiesNode(*ref.AdditionalProperties))
	}
	return node
}

func additionalPropertiesNode(additional ir.AdditionalProperties) *yaml.Node {
	if additional.Schema != nil {
		return schemaRefNode(*additional.Schema)
	}
	if additional.Any {
		return mappingNode()
	}
	return mappingNode()
}

func securitySchemeNode(scheme ir.SecurityScheme) *yaml.Node {
	node := mappingNode()
	appendKeyValue(node, "type", stringNode(scheme.Type))
	if scheme.In != "" {
		appendKeyValue(node, "in", stringNode(scheme.In))
	}
	if scheme.Name != "" {
		appendKeyValue(node, "name", stringNode(scheme.Name))
	}
	if scheme.Scheme != "" {
		appendKeyValue(node, "scheme", stringNode(scheme.Scheme))
	}
	return node
}

func serversNode(servers []ir.Server) *yaml.Node {
	node := sequenceNode()
	for _, server := range servers {
		entry := mappingNode()
		appendKeyValue(entry, "url", stringNode(server.URL))
		if server.Description != "" {
			appendKeyValue(entry, "description", stringNode(server.Description))
		}
		variables := mappingNode()
		variableKeys := make([]string, 0, len(server.Variables))
		for key := range server.Variables {
			variableKeys = append(variableKeys, key)
		}
		sort.Strings(variableKeys)
		for _, key := range variableKeys {
			variableNode := mappingNode()
			if server.Variables[key].Default != "" {
				appendKeyValue(variableNode, "default", stringNode(server.Variables[key].Default))
			}
			if server.Variables[key].Description != "" {
				appendKeyValue(variableNode, "description", stringNode(server.Variables[key].Description))
			}
			if len(server.Variables[key].Enum) > 0 {
				enum := sequenceNode()
				for _, value := range server.Variables[key].Enum {
					enum.Content = append(enum.Content, stringNode(value))
				}
				appendKeyValue(variableNode, "enum", enum)
			}
			appendKeyValue(variables, key, variableNode)
		}
		appendKeyValue(entry, "variables", variables)
		node.Content = append(node.Content, entry)
	}
	return node
}

func anyToNode(value any) *yaml.Node {
	switch typed := value.(type) {
	case nil:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null"}
	case string:
		return stringNode(typed)
	case bool:
		return boolNode(typed)
	case int:
		return intNode(typed)
	case int32:
		return intNode(int(typed))
	case int64:
		return int64Node(typed)
	case float64:
		return floatNode(typed)
	case []string:
		node := sequenceNode()
		for _, item := range typed {
			node.Content = append(node.Content, stringNode(item))
		}
		return node
	case []any:
		node := sequenceNode()
		for _, item := range typed {
			node.Content = append(node.Content, anyToNode(item))
		}
		return node
	case map[string]any:
		node := mappingNode()
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			left := keys[i]
			right := keys[j]
			order := map[string]int{
				"mode":                0,
				"checks":              1,
				"securable_type":      0,
				"privilege":           1,
				"securable_id_source": 2,
			}
			if order[left] == order[right] {
				return left < right
			}
			return order[left] < order[right]
		})
		for _, key := range keys {
			appendKeyValue(node, key, anyToNode(typed[key]))
		}
		return node
	case ir.SecurityRequirement:
		return securityRequirementsNode([]ir.SecurityRequirement{typed}).Content[0]
	default:
		var intermediate any
		raw, _ := yaml.Marshal(typed)
		_ = yaml.Unmarshal(raw, &intermediate)
		switch converted := intermediate.(type) {
		case map[string]any:
			return anyToNode(converted)
		case []any:
			return anyToNode(converted)
		default:
			return stringNode(fmt.Sprint(typed))
		}
	}
}

func mappingNode() *yaml.Node {
	return &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
}

func sequenceNode() *yaml.Node {
	return &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
}

func stringNode(value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}
}

func boolNode(value bool) *yaml.Node {
	if value {
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"}
	}
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "false"}
}

func intNode(value int) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.Itoa(value)}
}

func int64Node(value int64) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.FormatInt(value, 10)}
}

func floatNode(value float64) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!float", Value: strconv.FormatFloat(value, 'f', -1, 64)}
}

func appendKeyValue(node *yaml.Node, key string, value *yaml.Node) {
	node.Content = append(node.Content, stringNode(key), value)
}

func isDefaultAuthenticatedSecurity(security []ir.SecurityRequirement) bool {
	if len(security) != 2 {
		return false
	}
	seenBearer := false
	seenAPIKey := false
	for _, requirement := range security {
		if scopes, ok := requirement["BearerAuth"]; ok && len(scopes) == 0 {
			seenBearer = true
		}
		if scopes, ok := requirement["ApiKeyAuth"]; ok && len(scopes) == 0 {
			seenAPIKey = true
		}
	}
	return seenBearer && seenAPIKey
}

func isAuthenticatedAuthz(value any) bool {
	authz, ok := value.(map[string]any)
	if !ok {
		return false
	}
	mode, _ := authz["mode"].(string)
	return mode == "authenticated"
}

func extensionSecurity(value any) []ir.SecurityRequirement {
	switch typed := value.(type) {
	case nil:
		return nil
	case []ir.SecurityRequirement:
		return typed
	case []any:
		requirements := make([]ir.SecurityRequirement, 0, len(typed))
		for _, item := range typed {
			mapping, ok := item.(map[string]any)
			if !ok {
				continue
			}
			requirement := ir.SecurityRequirement{}
			for key, rawScopes := range mapping {
				switch scopes := rawScopes.(type) {
				case []string:
					requirement[key] = append([]string(nil), scopes...)
				case []any:
					converted := make([]string, 0, len(scopes))
					for _, scope := range scopes {
						if value, ok := scope.(string); ok {
							converted = append(converted, value)
						}
					}
					requirement[key] = converted
				default:
					requirement[key] = nil
				}
			}
			requirements = append(requirements, requirement)
		}
		return requirements
	default:
		return nil
	}
}

func canonicalResponseDescription(response ir.Response) string {
	if response.StatusCode == 204 && response.Description == "There is no content to send for this request, but the headers may be useful." {
		return "There is no content to send for this request, but the headers may be useful. "
	}
	return response.Description
}

var (
	quotedYAMLKeyPattern      = regexp.MustCompile(`^(\s*)"([A-Za-z0-9_]+)":(.*)$`)
	quotedYAMLListItemPattern = regexp.MustCompile(`^(\s*)- "([A-Za-z0-9_]+)"\s*$`)
	bareYAMLYKeyPattern       = regexp.MustCompile(`^(\s*)y:(.*)$`)
	bareYAMLYListItemPattern  = regexp.MustCompile(`^(\s*)- y\s*$`)
)

func normalizeCanonicalYAML(content []byte) []byte {
	lines := strings.Split(string(content), "\n")
	for i, line := range lines {
		if match := quotedYAMLKeyPattern.FindStringSubmatch(line); len(match) == 4 {
			lines[i] = match[1] + "'" + match[2] + "':" + match[3]
			continue
		}
		if match := quotedYAMLListItemPattern.FindStringSubmatch(line); len(match) == 3 {
			lines[i] = match[1] + "- '" + match[2] + "'"
			continue
		}
		if match := bareYAMLYKeyPattern.FindStringSubmatch(line); len(match) == 3 {
			lines[i] = match[1] + "'y':" + match[2]
			continue
		}
		if match := bareYAMLYListItemPattern.FindStringSubmatch(line); len(match) == 2 {
			lines[i] = match[1] + "- 'y'"
		}
	}
	return []byte(strings.Join(lines, "\n"))
}
