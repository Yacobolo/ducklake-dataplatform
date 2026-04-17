package architecture_test

import (
	"path/filepath"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
)

func TestCanonicalOpenAPI_ComputeEndpointsUseConcreteSchemas(t *testing.T) {
	t.Helper()

	doc := loadOpenAPISpec(t, filepath.Join(repoRootDir(), "internal", "api", "gen", "openapi.yaml"))

	assertOperationSchemas(t, doc, "GET", "/v1/compute-endpoints", "", "#/components/schemas/PaginatedComputeEndpoints")
	assertOperationSchemas(t, doc, "POST", "/v1/compute-endpoints", "#/components/schemas/CreateComputeEndpointRequest", "#/components/schemas/ComputeEndpoint")
	assertOperationSchemas(t, doc, "GET", "/v1/compute-endpoints/{endpoint_name}", "", "#/components/schemas/ComputeEndpoint")
	assertOperationSchemas(t, doc, "PATCH", "/v1/compute-endpoints/{endpoint_name}", "#/components/schemas/UpdateComputeEndpointRequest", "#/components/schemas/ComputeEndpoint")
	assertOperationSchemas(t, doc, "GET", "/v1/compute-endpoints/{endpoint_name}/assignments", "", "#/components/schemas/PaginatedComputeAssignments")
	assertOperationSchemas(t, doc, "POST", "/v1/compute-endpoints/{endpoint_name}/assignments", "#/components/schemas/CreateComputeAssignmentRequest", "#/components/schemas/ComputeAssignment")
	assertOperationSchemas(t, doc, "GET", "/v1/compute-endpoints/{endpoint_name}/health", "", "#/components/schemas/ComputeEndpointHealth")
}

func assertOperationSchemas(t *testing.T, doc *openapi3.T, method string, path string, requestRef string, responseRef string) {
	t.Helper()

	pathItem := doc.Paths.Value(path)
	require.NotNilf(t, pathItem, "missing path %s", path)

	operation := operationForMethod(pathItem, method)
	require.NotNilf(t, operation, "missing %s operation for %s", method, path)

	if requestRef == "" {
		require.Nilf(t, operation.RequestBody, "%s %s should not declare a request body", method, path)
	} else {
		require.NotNilf(t, operation.RequestBody, "%s %s should declare a request body", method, path)
		require.Equal(t, requestRef, operation.RequestBody.Value.Content.Get("application/json").Schema.Ref)
	}

	response := operation.Responses.Status(200)
	if response == nil {
		response = operation.Responses.Status(201)
	}
	require.NotNilf(t, response, "%s %s should declare a success response", method, path)
	require.Equal(t, responseRef, response.Value.Content.Get("application/json").Schema.Ref)
}

func operationForMethod(pathItem *openapi3.PathItem, method string) *openapi3.Operation {
	switch method {
	case "GET":
		return pathItem.Get
	case "POST":
		return pathItem.Post
	case "PATCH":
		return pathItem.Patch
	default:
		return nil
	}
}
