package ir

import (
	"fmt"
	"sort"
	"strings"
)

// NormalizedSchemaRefName resolves a schema ref to a registry key.
func NormalizedSchemaRefName(schema SchemaRef) (string, bool) {
	if schema.Ref == "" {
		return "", false
	}
	ref := strings.TrimSpace(schema.Ref)
	ref = strings.TrimPrefix(ref, "#/components/schemas/")
	ref = strings.TrimPrefix(ref, "#/schemas/")
	if idx := strings.LastIndex(ref, "/"); idx >= 0 {
		ref = ref[idx+1:]
	}
	if ref == "" {
		return "", false
	}
	return ref, true
}

// ResolveSchema returns the concrete schema referenced by the schema ref.
func ResolveSchema(doc Document, schemaRef SchemaRef) (Schema, bool) {
	name, ok := NormalizedSchemaRefName(schemaRef)
	if !ok {
		return Schema{}, false
	}
	schema, ok := doc.Schemas[name]
	return schema, ok
}

// ResolveRequestBodySchema returns the concrete request body schema when present.
func ResolveRequestBodySchema(doc Document, endpoint Endpoint) (Schema, bool) {
	if endpoint.RequestBody == nil {
		return Schema{}, false
	}
	ref := endpoint.RequestBody.Schema
	if ref.Ref == "GenericRequest" {
		name, ok := ResolveGenericRequestBodySchemaName(doc, endpoint.OperationID)
		if !ok {
			return Schema{}, false
		}
		schema, ok := doc.Schemas[name]
		return schema, ok
	}
	return ResolveSchema(doc, ref)
}

// SuccessResponse returns the preferred success response for CLI generation.
func SuccessResponse(endpoint Endpoint) (*Response, bool) {
	var best *Response
	for i := range endpoint.Responses {
		response := &endpoint.Responses[i]
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			continue
		}
		if best == nil || response.StatusCode < best.StatusCode {
			best = response
		}
	}
	return best, best != nil
}

// ResolveResponseBodySchema returns the schema used for the CLI-visible success body.
func ResolveResponseBodySchema(doc Document, response Response) (Schema, bool) {
	if shape, ok, _ := ResponseShapeMetadata(response); ok && shape.Kind == "wrapped_json" && shape.BodyType != "" {
		schema, ok := doc.Schemas[shape.BodyType]
		return schema, ok
	}
	if response.Schema == nil {
		return Schema{}, false
	}
	return ResolveSchema(doc, *response.Schema)
}

// JoinAPIPath combines a contract base path with an authored endpoint path.
func JoinAPIPath(basePath string, endpointPath string) string {
	basePath = strings.TrimSpace(basePath)
	endpointPath = strings.TrimSpace(endpointPath)

	if basePath == "/" {
		basePath = ""
	}
	if endpointPath == "" {
		endpointPath = "/"
	}
	if endpointPath == "/" {
		if basePath == "" {
			return "/"
		}
		return basePath
	}
	if basePath == "" {
		return endpointPath
	}
	return strings.TrimRight(basePath, "/") + endpointPath
}

// ValidateBasePath checks APIGen API base path formatting.
func ValidateBasePath(basePath string) error {
	basePath = strings.TrimSpace(basePath)
	if basePath == "" {
		return fmt.Errorf("api.base_path is required")
	}
	if !strings.HasPrefix(basePath, "/") {
		return fmt.Errorf("api.base_path must start with \"/\"")
	}
	if basePath != "/" && strings.HasSuffix(basePath, "/") {
		return fmt.Errorf("api.base_path must not end with \"/\" unless it is exactly \"/\"")
	}
	return nil
}

// CLICommandString renders a CLI command path as a space-delimited string.
func CLICommandString(cli *CLI) string {
	if cli == nil {
		return ""
	}
	return strings.Join(cli.Command, " ")
}

// CloneCLI returns a deep copy of CLI metadata.
func CloneCLI(in *CLI) *CLI {
	if in == nil {
		return nil
	}
	out := *in
	if len(in.Command) > 0 {
		out.Command = append([]string(nil), in.Command...)
	}
	if len(in.Args) > 0 {
		out.Args = append([]CLIArg(nil), in.Args...)
	}
	if in.Output != nil {
		output := *in.Output
		if len(in.Output.TableColumns) > 0 {
			output.TableColumns = append([]string(nil), in.Output.TableColumns...)
		}
		if len(in.Output.QuietFields) > 0 {
			output.QuietFields = append([]string(nil), in.Output.QuietFields...)
		}
		out.Output = &output
	}
	if in.Pagination != nil {
		pagination := *in.Pagination
		out.Pagination = &pagination
	}
	return &out
}

// PathParameterNames extracts ordered "{param}" names from an endpoint path.
func PathParameterNames(path string) []string {
	params := make([]string, 0, strings.Count(path, "{"))
	for i := 0; i < len(path); i++ {
		if path[i] != '{' {
			continue
		}
		j := i + 1
		for j < len(path) && path[j] != '}' {
			j++
		}
		if j >= len(path) || j == i+1 {
			continue
		}
		params = append(params, path[i+1:j])
		i = j
	}
	return params
}

// OrderedPropertyNames returns a deterministic property order for a schema.
func OrderedPropertyNames(schema Schema) []string {
	if len(schema.Properties) == 0 {
		return nil
	}
	if len(schema.PropertyOrder) > 0 {
		names := make([]string, 0, len(schema.PropertyOrder))
		for _, name := range schema.PropertyOrder {
			if _, ok := schema.Properties[name]; ok {
				names = append(names, name)
			}
		}
		if len(names) > 0 {
			return names
		}
	}
	names := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ResolveGenericRequestBodySchemaName returns the concrete schema name backing a
// GenericRequest placeholder when one can be inferred from the contract.
func ResolveGenericRequestBodySchemaName(doc Document, operationID string) (string, bool) {
	if schemaName, ok := genericRequestBodySchemaOverrides[operationID]; ok {
		return schemaName, true
	}
	for _, candidate := range genericRequestBodySchemaCandidates(operationID) {
		if _, ok := doc.Schemas[candidate]; ok {
			return candidate, true
		}
	}
	return "", false
}

func genericRequestBodySchemaCandidates(operationID string) []string {
	return []string{exportedName(operationID) + "Request"}
}

var genericRequestBodySchemaOverrides = map[string]string{
	"bindColumnMask":                  "ColumnMaskBindingRequest",
	"bindRowFilter":                   "RowFilterBindingRequest",
	"commitTableIngestion":            "CommitIngestionRequest",
	"createCell":                      "CreateCellRequest",
	"createComputeAssignment":         "CreateComputeAssignmentRequest",
	"createComputeEndpoint":           "CreateComputeEndpointRequest",
	"createGitRepo":                   "CreateGitRepoRequest",
	"createMacro":                     "CreateMacroRequest",
	"createManifest":                  "ManifestRequest",
	"createModelTest":                 "CreateModelTestRequest",
	"createNotebook":                  "CreateNotebookRequest",
	"createPipeline":                  "CreatePipelineRequest",
	"createPipelineJob":               "CreatePipelineJobRequest",
	"createSemanticMetric":            "CreateSemanticMetricRequest",
	"createSemanticModel":             "CreateSemanticModelRequest",
	"createSemanticPreAggregation":    "CreateSemanticPreAggregationRequest",
	"createSemanticModelRelationship": "CreateSemanticRelationshipRequest",
	"createTag":                       "CreateTagRequest",
	"createTagAssignment":             "CreateTagAssignmentRequest",
	"createUploadUrl":                 "UploadUrlRequest",
	"executeQuery":                    "QueryRequest",
	"explainMetricQuery":              "MetricQueryRequest",
	"loadTableExternalFiles":          "LoadExternalRequest",
	"promoteNotebookToModel":          "PromoteNotebookRequest",
	"purgeLineage":                    "PurgeLineageRequest",
	"reorderCells":                    "ReorderCellsRequest",
	"runMetricQuery":                  "MetricQueryRequest",
	"triggerModelRun":                 "TriggerModelRunRequest",
	"triggerPipelineRun":              "TriggerPipelineRunRequest",
	"updateCell":                      "UpdateCellRequest",
	"updateComputeEndpoint":           "UpdateComputeEndpointRequest",
	"updateMacro":                     "UpdateMacroRequest",
	"updateModel":                     "UpdateModelRequest",
	"updateNotebook":                  "UpdateNotebookRequest",
	"updatePipeline":                  "UpdatePipelineRequest",
	"updateSemanticMetric":            "UpdateSemanticMetricRequest",
	"updateSemanticModel":             "UpdateSemanticModelRequest",
	"updateSemanticPreAggregation":    "UpdateSemanticPreAggregationRequest",
	"updateSemanticModelRelationship": "UpdateSemanticRelationshipRequest",
}

func exportedName(value string) string {
	parts := splitIdentifier(value)
	if len(parts) == 0 {
		return "Operation"
	}
	for i := range parts {
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}

func splitIdentifier(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	replacer := strings.NewReplacer("-", " ", "_", " ", ".", " ", "/", " ")
	value = replacer.Replace(value)
	chunks := strings.Fields(value)
	if len(chunks) > 0 {
		return chunks
	}
	return []string{value}
}
