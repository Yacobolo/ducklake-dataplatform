package ir

import (
	"fmt"
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
