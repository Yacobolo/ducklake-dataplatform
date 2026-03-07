// Package openapi emits OpenAPI YAML from JSON IR.
package openapi

import (
	"fmt"
	"sort"
	"strconv"

	"go.yaml.in/yaml/v4"

	"duck-demo/internal/apigen/ir"
)

// EmitYAML renders an OpenAPI 3.2 document from IR.
func EmitYAML(docIR ir.Document) ([]byte, error) {
	doc := make(map[string]any)
	doc["openapi"] = "3.2.0"
	doc["info"] = map[string]any{
		"title":       docIR.Info.Title,
		"version":     docIR.Info.Version,
		"description": docIR.Info.Description,
	}

	if len(docIR.Servers) > 0 {
		servers := make([]map[string]any, 0, len(docIR.Servers))
		for _, server := range docIR.Servers {
			servers = append(servers, map[string]any{
				"url":         server.URL,
				"description": server.Description,
			})
		}
		doc["servers"] = servers
	}

	if len(docIR.Tags) > 0 {
		tags := make([]map[string]any, 0, len(docIR.Tags))
		for _, tag := range docIR.Tags {
			tags = append(tags, map[string]any{"name": tag.Name, "description": tag.Description})
		}
		doc["tags"] = tags
	}

	paths, err := buildPaths(docIR.Endpoints)
	if err != nil {
		return nil, err
	}
	doc["paths"] = paths

	components := buildComponents(docIR.Schemas)
	if len(components) > 0 {
		doc["components"] = components
	}

	b, err := yaml.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal openapi yaml: %w", err)
	}
	return b, nil
}

func buildPaths(endpoints []ir.Endpoint) (map[string]any, error) {
	paths := make(map[string]any)
	for _, endpoint := range endpoints {
		pathItem, _ := paths[endpoint.Path].(map[string]any)
		if pathItem == nil {
			pathItem = make(map[string]any)
		}
		op, err := buildOperation(endpoint)
		if err != nil {
			return nil, err
		}
		pathItem[string(endpoint.Method)] = op
		paths[endpoint.Path] = pathItem
	}
	return paths, nil
}

func buildOperation(endpoint ir.Endpoint) (map[string]any, error) {
	operation := map[string]any{
		"operationId": endpoint.OperationID,
	}
	if endpoint.Summary != "" {
		operation["summary"] = endpoint.Summary
	}
	if endpoint.Description != "" {
		operation["description"] = endpoint.Description
	}
	if len(endpoint.Tags) > 0 {
		operation["tags"] = endpoint.Tags
	}
	if len(endpoint.Parameters) > 0 {
		params := make([]map[string]any, 0, len(endpoint.Parameters))
		for _, parameter := range endpoint.Parameters {
			params = append(params, map[string]any{
				"name":        parameter.Name,
				"in":          parameter.In,
				"required":    parameter.Required,
				"description": parameter.Description,
				"schema":      schemaRefMap(parameter.Schema),
			})
		}
		operation["parameters"] = params
	}
	if endpoint.RequestBody != nil {
		operation["requestBody"] = map[string]any{
			"required":    endpoint.RequestBody.Required,
			"description": endpoint.RequestBody.Description,
			"content": map[string]any{
				"application/json": map[string]any{
					"schema": schemaRefMap(endpoint.RequestBody.Schema),
				},
			},
		}
	}
	responses := make(map[string]any)
	for _, response := range endpoint.Responses {
		key := strconv.Itoa(response.StatusCode)
		entry := map[string]any{"description": response.Description}
		if len(response.Headers) > 0 {
			headers := make(map[string]any, len(response.Headers))
			for _, header := range response.Headers {
				headers[header.Name] = map[string]any{
					"required":    header.Required,
					"description": header.Description,
					"schema":      schemaRefMap(header.Schema),
				}
			}
			entry["headers"] = headers
		}
		if response.Schema != nil {
			entry["content"] = map[string]any{
				"application/json": map[string]any{
					"schema": schemaRefMap(*response.Schema),
				},
			}
		}
		responses[key] = entry
	}
	if len(responses) == 0 {
		return nil, fmt.Errorf("at least one response is required for %s", endpoint.OperationID)
	}
	operation["responses"] = responses

	for key, value := range endpoint.Extensions {
		operation[key] = value
	}

	return operation, nil
}

func schemaRefMap(ref ir.SchemaRef) map[string]any {
	if ref.Ref != "" {
		return map[string]any{"$ref": "#/components/schemas/" + ref.Ref}
	}
	entry := map[string]any{"type": ref.Type}
	if ref.Format != "" {
		entry["format"] = ref.Format
	}
	return entry
}

func buildComponents(schemas map[string]ir.Schema) map[string]any {
	components := make(map[string]any)
	components["securitySchemes"] = map[string]any{
		"ApiKeyAuth": map[string]any{
			"type": "apiKey",
			"in":   "header",
			"name": "X-API-Key",
		},
		"BearerAuth": map[string]any{
			"type":         "http",
			"scheme":       "bearer",
			"bearerFormat": "JWT",
			"description":  "JWT bearer authentication. Tokens are expected to follow RFC8725 best practices.",
		},
	}
	if len(schemas) == 0 {
		return components
	}
	keys := make([]string, 0, len(schemas))
	for key := range schemas {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	schemaMap := make(map[string]any, len(schemas))
	for _, key := range keys {
		schemaMap[key] = buildSchema(schemas[key])
	}
	components["schemas"] = schemaMap
	return components
}

func buildSchema(schema ir.Schema) map[string]any {
	entry := map[string]any{"type": schema.Type}
	if schema.Description != "" {
		entry["description"] = schema.Description
	}
	if len(schema.Enum) > 0 {
		entry["enum"] = schema.Enum
	}
	if len(schema.Required) > 0 {
		entry["required"] = schema.Required
	}
	if schema.Items != nil {
		entry["items"] = schemaRefMap(*schema.Items)
	}
	if len(schema.Properties) > 0 {
		props := make(map[string]any, len(schema.Properties))
		propKeys := make([]string, 0, len(schema.Properties))
		for name := range schema.Properties {
			propKeys = append(propKeys, name)
		}
		sort.Strings(propKeys)
		for _, name := range propKeys {
			property := schema.Properties[name]
			propEntry := schemaRefMap(property.Schema)
			if property.Description != "" {
				propEntry["description"] = property.Description
			}
			props[name] = propEntry
		}
		entry["properties"] = props
	}
	return entry
}
