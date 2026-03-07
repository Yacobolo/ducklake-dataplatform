package architecture_test

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
)

type openAPICoreOperation struct {
	Method         string
	Path           string
	OperationID    string
	HasRequestBody bool
}

func TestOpenAPIContractParity_CoreOperationShape(t *testing.T) {
	t.Helper()

	typespecDoc := loadOpenAPISpec(t, filepath.Join(repoRootDir(), "api", "gen", "openapi.yaml"))
	generatedDoc := loadOpenAPISpec(t, filepath.Join(repoRootDir(), "internal", "api", "openapi.generated.yaml"))

	typespecOps := collectOpenAPICoreOperations(t, typespecDoc)
	generatedOps := collectOpenAPICoreOperations(t, generatedDoc)

	require.Equal(t, sortedOperationKeys(typespecOps), sortedOperationKeys(generatedOps), "openapi parity: method/path pairs must match between TypeSpec and generated specs")

	for key, expected := range typespecOps {
		actual := generatedOps[key]
		require.Equalf(t, expected.OperationID, actual.OperationID, "openapi parity: operationId drift for %s %s", expected.Method, expected.Path)
		require.Equalf(t, expected.HasRequestBody, actual.HasRequestBody, "openapi parity: requestBody presence drift for %s %s", expected.Method, expected.Path)
	}
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

func sortedOperationKeys(operations map[string]openAPICoreOperation) []string {
	keys := make([]string, 0, len(operations))
	for key := range operations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
