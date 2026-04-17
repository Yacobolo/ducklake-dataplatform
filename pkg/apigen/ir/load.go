package ir

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// CurrentSchemaVersion is the supported JSON IR schema version.
const CurrentSchemaVersion = "v1"

// Load parses and validates an IR document from disk.
func Load(path string) (Document, error) {
	cleanPath := filepath.Clean(path)
	// #nosec G304 -- path is an explicit CLI/task input by design.
	content, err := os.ReadFile(cleanPath)
	if err != nil {
		return Document{}, fmt.Errorf("read ir file: %w", err)
	}

	dec := json.NewDecoder(strings.NewReader(string(content)))
	dec.DisallowUnknownFields()

	var doc Document
	if err := dec.Decode(&doc); err != nil {
		return Document{}, fmt.Errorf("decode ir json: %w", err)
	}
	if err := Validate(doc); err != nil {
		return Document{}, err
	}
	Normalize(&doc)
	return doc, nil
}

// Validate checks required fields and uniqueness constraints.
func Validate(doc Document) error {
	if strings.TrimSpace(doc.SchemaVersion) == "" {
		return fmt.Errorf("schema_version is required")
	}
	if doc.SchemaVersion != CurrentSchemaVersion {
		return fmt.Errorf("unsupported schema_version %q", doc.SchemaVersion)
	}
	if err := ValidateBasePath(doc.API.BasePath); err != nil {
		return err
	}
	if strings.TrimSpace(doc.Info.Title) == "" {
		return fmt.Errorf("info.title is required")
	}
	if strings.TrimSpace(doc.Info.Version) == "" {
		return fmt.Errorf("info.version is required")
	}
	if len(doc.Endpoints) == 0 {
		return fmt.Errorf("at least one endpoint is required")
	}

	seenOperation := make(map[string]struct{}, len(doc.Endpoints))
	seenRoute := make(map[string]struct{}, len(doc.Endpoints))
	for _, endpoint := range doc.Endpoints {
		if strings.TrimSpace(endpoint.Method) == "" {
			return fmt.Errorf("endpoint method is required")
		}
		if strings.TrimSpace(endpoint.Path) == "" {
			return fmt.Errorf("endpoint path is required")
		}
		if !strings.HasPrefix(strings.TrimSpace(endpoint.Path), "/") {
			return fmt.Errorf("endpoint %q path must start with \"/\"", endpoint.OperationID)
		}
		if strings.TrimSpace(endpoint.OperationID) == "" {
			return fmt.Errorf("endpoint operation_id is required")
		}
		for _, parameter := range endpoint.Parameters {
			if err := validateParameterSchema(doc, endpoint, parameter); err != nil {
				return err
			}
		}
		if endpoint.RequestBody != nil {
			if err := validateRequestBodySchema(doc, endpoint); err != nil {
				return err
			}
		}
		if len(endpoint.Responses) == 0 {
			return fmt.Errorf("endpoint %q must have at least one response", endpoint.OperationID)
		}
		if endpoint.RequestBody != nil && strings.TrimSpace(endpoint.RequestBody.ContentType) == "" {
			endpoint.RequestBody.ContentType = "application/json"
		}
		for _, response := range endpoint.Responses {
			if response.StatusCode <= 0 {
				return fmt.Errorf("endpoint %q has invalid response status_code %d", endpoint.OperationID, response.StatusCode)
			}
			if strings.TrimSpace(response.Description) == "" {
				return fmt.Errorf("endpoint %q response %d description is required", endpoint.OperationID, response.StatusCode)
			}
			if shape, ok, err := ResponseShapeMetadata(response); err != nil {
				return fmt.Errorf("endpoint %q response %d shape metadata: %w", endpoint.OperationID, response.StatusCode, err)
			} else if ok {
				switch shape.Kind {
				case "wrapped_json":
					if shape.BodyType == "" {
						return fmt.Errorf("endpoint %q response %d wrapped_json body_type is required", endpoint.OperationID, response.StatusCode)
					}
				default:
					return fmt.Errorf("endpoint %q response %d has unsupported shape kind %q", endpoint.OperationID, response.StatusCode, shape.Kind)
				}
			}
			seenHeaders := make(map[string]struct{}, len(response.Headers))
			for _, header := range response.Headers {
				name := strings.TrimSpace(header.Name)
				if name == "" {
					return fmt.Errorf("endpoint %q response %d header name is required", endpoint.OperationID, response.StatusCode)
				}
				if err := validateSchemaRefExists(doc, header.Schema, fmt.Sprintf("endpoint %q response %d header %q", endpoint.OperationID, response.StatusCode, header.Name)); err != nil {
					return err
				}
				if _, exists := seenHeaders[strings.ToLower(name)]; exists {
					return fmt.Errorf("endpoint %q response %d has duplicate header %q", endpoint.OperationID, response.StatusCode, header.Name)
				}
				seenHeaders[strings.ToLower(name)] = struct{}{}
			}
			if response.Schema != nil {
				if err := validateSchemaRefExists(doc, *response.Schema, fmt.Sprintf("endpoint %q response %d schema", endpoint.OperationID, response.StatusCode)); err != nil {
					return err
				}
			}
			for idx, schemaRef := range response.AnyOf {
				if err := validateSchemaRefExists(doc, schemaRef, fmt.Sprintf("endpoint %q response %d any_of[%d]", endpoint.OperationID, response.StatusCode, idx)); err != nil {
					return err
				}
			}
		}

		opKey := endpoint.OperationID
		if _, exists := seenOperation[opKey]; exists {
			return fmt.Errorf("duplicate operation_id %q", opKey)
		}
		seenOperation[opKey] = struct{}{}

		routeKey := strings.ToLower(endpoint.Method) + " " + endpoint.Path
		if _, exists := seenRoute[routeKey]; exists {
			return fmt.Errorf("duplicate endpoint route %q", routeKey)
		}
		seenRoute[routeKey] = struct{}{}
	}

	for name, schema := range doc.Schemas {
		if err := validateSchemaDefinition(doc, name, schema); err != nil {
			return err
		}
		if len(schema.PropertyOrder) > 0 {
			for _, propertyName := range schema.PropertyOrder {
				if _, ok := schema.Properties[propertyName]; !ok {
					return fmt.Errorf("schema %q property_order references unknown property %q", name, propertyName)
				}
			}
		}
	}

	return nil
}

func validateParameterSchema(doc Document, endpoint Endpoint, parameter Parameter) error {
	if parameter.In == "" {
		return fmt.Errorf("endpoint %q parameter %q location is required", endpoint.OperationID, parameter.Name)
	}

	schemaType, format, err := resolvedParameterSchemaType(doc, parameter.Schema, fmt.Sprintf("endpoint %q parameter %q", endpoint.OperationID, parameter.Name))
	if err != nil {
		return err
	}

	switch schemaType {
	case "string":
		if format == "date-time" || format == "" {
			return nil
		}
		return nil
	case "array":
		if parameter.In != "query" {
			return fmt.Errorf("endpoint %q parameter %q arrays are only supported in query parameters", endpoint.OperationID, parameter.Name)
		}
		itemType, itemFormat, err := resolvedParameterArrayItemType(doc, parameter.Schema, fmt.Sprintf("endpoint %q parameter %q", endpoint.OperationID, parameter.Name))
		if err != nil {
			return err
		}
		switch itemType {
		case "string":
			if itemFormat == "" || itemFormat == "date-time" {
				return nil
			}
			return nil
		case "boolean", "bool":
			return nil
		case "integer":
			switch itemFormat {
			case "", "int32", "int64":
				return nil
			default:
				return fmt.Errorf("endpoint %q parameter %q has unsupported array item integer format %q", endpoint.OperationID, parameter.Name, itemFormat)
			}
		default:
			return fmt.Errorf("endpoint %q parameter %q has unsupported array item schema type %q", endpoint.OperationID, parameter.Name, itemType)
		}
	case "boolean", "bool":
		return nil
	case "integer":
		switch format {
		case "", "int32", "int64":
			return nil
		default:
			return fmt.Errorf("endpoint %q parameter %q has unsupported integer format %q", endpoint.OperationID, parameter.Name, format)
		}
	default:
		return fmt.Errorf("endpoint %q parameter %q has unsupported schema type %q", endpoint.OperationID, parameter.Name, schemaType)
	}
}

func validateRequestBodySchema(doc Document, endpoint Endpoint) error {
	if endpoint.RequestBody == nil {
		return nil
	}
	ref := endpoint.RequestBody.Schema
	if ref.Ref == "GenericRequest" {
		if _, ok := ResolveGenericRequestBodySchemaName(doc, endpoint.OperationID); !ok {
			return fmt.Errorf("endpoint %q generic request body schema could not be resolved", endpoint.OperationID)
		}
		return nil
	}
	return validateSchemaRefExists(doc, ref, fmt.Sprintf("endpoint %q request_body schema", endpoint.OperationID))
}

func validateSchemaDefinition(doc Document, name string, schema Schema) error {
	if strings.TrimSpace(schema.Type) == "" {
		return fmt.Errorf("schema %q type is required", name)
	}
	for propertyName, property := range schema.Properties {
		if err := validateSchemaRefExists(doc, property.Schema, fmt.Sprintf("schema %q property %q", name, propertyName)); err != nil {
			return err
		}
	}
	if schema.Items != nil {
		if err := validateSchemaRefExists(doc, *schema.Items, fmt.Sprintf("schema %q items", name)); err != nil {
			return err
		}
	}
	return nil
}

func validateSchemaRefExists(doc Document, schemaRef SchemaRef, context string) error {
	if schemaRef.Ref != "" {
		if schemaRef.Ref == "GenericRequest" {
			return nil
		}
		name, ok := NormalizedSchemaRefName(schemaRef)
		if !ok {
			return fmt.Errorf("%s has invalid schema ref %q", context, schemaRef.Ref)
		}
		if _, ok := doc.Schemas[name]; !ok {
			return fmt.Errorf("%s references unknown schema %q", context, name)
		}
	}
	if schemaRef.Items != nil {
		if err := validateSchemaRefExists(doc, *schemaRef.Items, context+" items"); err != nil {
			return err
		}
	}
	if schemaRef.AdditionalProperties != nil && schemaRef.AdditionalProperties.Schema != nil {
		if err := validateSchemaRefExists(doc, *schemaRef.AdditionalProperties.Schema, context+" additional_properties"); err != nil {
			return err
		}
	}
	return nil
}

func resolvedParameterSchemaType(doc Document, schemaRef SchemaRef, context string) (string, string, error) {
	if schemaRef.Ref != "" {
		schema, ok := ResolveSchema(doc, schemaRef)
		if !ok {
			name, _ := NormalizedSchemaRefName(schemaRef)
			return "", "", fmt.Errorf("%s references unknown schema %q", context, name)
		}
		return strings.ToLower(strings.TrimSpace(schema.Type)), "", nil
	}
	return strings.ToLower(strings.TrimSpace(schemaRef.Type)), strings.ToLower(strings.TrimSpace(schemaRef.Format)), nil
}

func resolvedParameterArrayItemType(doc Document, schemaRef SchemaRef, context string) (string, string, error) {
	if schemaRef.Ref != "" {
		schema, ok := ResolveSchema(doc, schemaRef)
		if !ok {
			name, _ := NormalizedSchemaRefName(schemaRef)
			return "", "", fmt.Errorf("%s references unknown schema %q", context, name)
		}
		if schema.Items == nil {
			return "", "", fmt.Errorf("%s array schema must declare items", context)
		}
		return resolvedParameterSchemaType(doc, *schema.Items, context+" items")
	}
	if schemaRef.Items == nil {
		return "", "", fmt.Errorf("%s array schema must declare items", context)
	}
	return resolvedParameterSchemaType(doc, *schemaRef.Items, context+" items")
}

// Normalize applies deterministic ordering for generation.
func Normalize(doc *Document) {
	sort.Slice(doc.Endpoints, func(i, j int) bool {
		if doc.Endpoints[i].Path == doc.Endpoints[j].Path {
			return strings.ToLower(doc.Endpoints[i].Method) < strings.ToLower(doc.Endpoints[j].Method)
		}
		return doc.Endpoints[i].Path < doc.Endpoints[j].Path
	})
	for i := range doc.Endpoints {
		if doc.Endpoints[i].RequestBody != nil && strings.TrimSpace(doc.Endpoints[i].RequestBody.ContentType) == "" {
			doc.Endpoints[i].RequestBody.ContentType = "application/json"
		}
		for j := range doc.Endpoints[i].Parameters {
			if doc.Endpoints[i].Parameters[j].In == "query" && doc.Endpoints[i].Parameters[j].Explode == nil {
				explode := false
				doc.Endpoints[i].Parameters[j].Explode = &explode
			}
		}
		sort.Slice(doc.Endpoints[i].Responses, func(a, b int) bool {
			return doc.Endpoints[i].Responses[a].StatusCode < doc.Endpoints[i].Responses[b].StatusCode
		})
		for j := range doc.Endpoints[i].Responses {
			if strings.TrimSpace(doc.Endpoints[i].Responses[j].ContentType) == "" {
				doc.Endpoints[i].Responses[j].ContentType = "application/json"
			}
			sort.Slice(doc.Endpoints[i].Responses[j].Headers, func(a, b int) bool {
				return strings.ToLower(doc.Endpoints[i].Responses[j].Headers[a].Name) < strings.ToLower(doc.Endpoints[i].Responses[j].Headers[b].Name)
			})
		}
	}
}
