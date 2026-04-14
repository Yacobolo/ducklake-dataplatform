package architecture_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/api"
)

type openAPICoreOperation struct {
	Method         string
	Path           string
	OperationID    string
	HasRequestBody bool
	ResponseCodes  []string
}

func TestOpenAPIContractParity_CoreOperationShape(t *testing.T) {
	t.Helper()

	canonicalDoc := loadOpenAPISpec(t, filepath.Join(repoRootDir(), "internal", "api", "gen", "openapi.yaml"))
	generatedDoc := loadEmbeddedOpenAPISpec(t)

	canonicalOps := collectOpenAPICoreOperations(t, canonicalDoc)
	generatedOps := collectOpenAPICoreOperations(t, generatedDoc)

	require.Equal(t, sortedOperationKeys(canonicalOps), sortedOperationKeys(generatedOps), "openapi parity: method/path pairs must match between canonical and generated specs")

	for key, expected := range canonicalOps {
		actual := generatedOps[key]
		require.Equalf(t, expected.OperationID, actual.OperationID, "openapi parity: operationId drift for %s %s", expected.Method, expected.Path)
		require.Equalf(t, expected.HasRequestBody, actual.HasRequestBody, "openapi parity: requestBody presence drift for %s %s", expected.Method, expected.Path)
		require.Equalf(t, expected.ResponseCodes, actual.ResponseCodes, "openapi parity: response code drift for %s %s", expected.Method, expected.Path)
	}
}

func TestCanonicalOpenAPI_HardeningEndpointsExposeExpectedDomainErrors(t *testing.T) {
	t.Helper()

	doc := loadOpenAPISpec(t, filepath.Join(repoRootDir(), "internal", "api", "gen", "openapi.yaml"))
	operations := collectOpenAPICoreOperations(t, doc)

	expects := map[string][]string{
		"deleteAPIKey":              {"403"},
		"getNotebook":               {"403", "404"},
		"updateNotebook":            {"403", "404"},
		"deleteNotebook":            {"403", "404"},
		"createCell":                {"400", "403", "404"},
		"reorderCells":              {"400", "403", "404"},
		"updateCell":                {"400", "403", "404"},
		"deleteCell":                {"403", "404"},
		"createNotebookSession":     {"403", "404"},
		"closeNotebookSession":      {"403", "404"},
		"executeCell":               {"400", "403", "404"},
		"runAllCells":               {"403", "404"},
		"runAllCellsAsync":          {"403", "404"},
		"listNotebookJobs":          {"403", "404"},
		"getNotebookJob":            {"403", "404"},
		"getGitRepo":                {"403", "404"},
		"deleteGitRepo":             {"403", "404"},
		"syncGitRepo":               {"404"},
		"getStorageCredential":      {"403", "404"},
		"updateStorageCredential":   {"403", "404"},
		"deleteStorageCredential":   {"403", "404"},
		"getExternalLocation":       {"403", "404"},
		"updateExternalLocation":    {"403", "404"},
		"deleteExternalLocation":    {"403", "404"},
		"updateComputeEndpoint":     {"403", "404"},
		"deleteComputeEndpoint":     {"403", "404"},
		"deleteComputeAssignment":   {"403", "404"},
		"getCatalog":                {"403", "404"},
		"updateCatalogRegistration": {"403", "404"},
		"deleteCatalogRegistration": {"403", "404"},
		"createManifest":            {"403"},
	}

	for operationID, requiredCodes := range expects {
		operation, ok := findOpenAPIOperationByID(operations, operationID)
		require.Truef(t, ok, "openapi parity: missing operationId %s", operationID)
		for _, code := range requiredCodes {
			require.Containsf(t, operation.ResponseCodes, code, "openapi parity: operationId %s must document response code %s", operationID, code)
		}
	}
}

func loadEmbeddedOpenAPISpec(t *testing.T) *openapi3.T {
	t.Helper()

	docMap, err := api.GetEmbeddedOpenAPISpec()
	require.NoError(t, err)

	jsonBytes, err := json.Marshal(docMap)
	require.NoError(t, err)

	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(jsonBytes)
	require.NoError(t, err)

	return doc
}

func loadOpenAPISpec(t *testing.T, specPath string) *openapi3.T {
	t.Helper()

	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile(specPath)
	require.NoErrorf(t, err, "load openapi spec %s", specPath)

	return doc
}

func collectOpenAPICoreOperations(t *testing.T, doc *openapi3.T) map[string]openAPICoreOperation {
	t.Helper()

	operations := make(map[string]openAPICoreOperation)
	operationIDToKey := make(map[string]string)

	for path, pathItem := range doc.Paths.Map() {
		for method, operation := range pathItem.Operations() {
			require.NotNilf(t, operation, "openapi parity: nil operation for %s %s", method, path)

			key := fmt.Sprintf("%s %s", method, path)
			core := openAPICoreOperation{
				Method:         method,
				Path:           path,
				OperationID:    operation.OperationID,
				HasRequestBody: operation.RequestBody != nil,
				ResponseCodes:  sortedResponseCodes(operation),
			}

			require.NotEmptyf(t, core.OperationID, "openapi parity: missing operationId for %s", key)
			if existingKey, exists := operationIDToKey[core.OperationID]; exists {
				require.Failf(t, "duplicate operationId", "openapi parity: operationId %q is used by both %s and %s", core.OperationID, existingKey, key)
			}

			operationIDToKey[core.OperationID] = key
			operations[key] = core
		}
	}

	return operations
}

func findOpenAPIOperationByID(operations map[string]openAPICoreOperation, operationID string) (openAPICoreOperation, bool) {
	for _, operation := range operations {
		if operation.OperationID == operationID {
			return operation, true
		}
	}
	return openAPICoreOperation{}, false
}

func sortedResponseCodes(operation *openapi3.Operation) []string {
	if operation == nil || operation.Responses == nil {
		return nil
	}

	codes := make([]string, 0, len(operation.Responses.Map()))
	for code := range operation.Responses.Map() {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}

func sortedOperationKeys(operations map[string]openAPICoreOperation) []string {
	keys := make([]string, 0, len(operations))
	for key := range operations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
