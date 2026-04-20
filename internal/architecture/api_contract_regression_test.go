package architecture_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIContractRegression_ComputeEndpointURLDocumentsGRPCRequirement(t *testing.T) {
	t.Helper()

	doc := loadOpenAPISpec(t, filepath.Join(repoRootDir(), "internal", "api", "gen", "openapi.yaml"))

	schemaRef := doc.Components.Schemas["CreateComputeEndpointRequest"]
	require.NotNil(t, schemaRef)
	require.NotNil(t, schemaRef.Value)

	urlSchema := schemaRef.Value.Properties["url"]
	require.NotNil(t, urlSchema)
	require.NotNil(t, urlSchema.Value)
	assert.Contains(t, urlSchema.Value.Description, "grpc:// or grpcs://")
}

func TestAPIContractRegression_ExternalLocationCreateDocumentsNoCatalogAttachment(t *testing.T) {
	t.Helper()

	doc := loadOpenAPISpec(t, filepath.Join(repoRootDir(), "internal", "api", "gen", "openapi.yaml"))

	pathItem := doc.Paths.Find("/v1/external-locations")
	require.NotNil(t, pathItem)
	require.NotNil(t, pathItem.Post)
	assert.Contains(t, pathItem.Post.Description, "does not attach or create a DuckLake catalog")
}
