package architecture_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalOpenAPI_IngestionEndpointsUseConcreteSchemas(t *testing.T) {
	t.Helper()

	doc := loadOpenAPISpec(t, filepath.Join(repoRootDir(), "internal", "api", "gen", "openapi.yaml"))

	assertOperationSchemas(t, doc, "POST", "/v1/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/upload-urls", "#/components/schemas/UploadUrlRequest", "#/components/schemas/UploadUrlResponse")
	assertOperationSchemas(t, doc, "POST", "/v1/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/ingestion-commits", "#/components/schemas/CommitIngestionRequest", "#/components/schemas/IngestionResult")
	assertOperationSchemas(t, doc, "POST", "/v1/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/ingestion-loads", "#/components/schemas/LoadExternalRequest", "#/components/schemas/IngestionResult")
}

func TestCanonicalOpenAPI_IngestionEndpointsExposeInsertAuthz(t *testing.T) {
	t.Helper()

	authzByOperation := loadAuthzByOperation(t)

	for _, operationID := range []string{"createUploadUrl", "commitTableIngestion", "loadTableExternalFiles"} {
		authz, ok := authzByOperation[operationID]
		require.Truef(t, ok, "missing x-authz metadata for %s", operationID)
		require.Equalf(t, "privilege", authz.Mode, "authz mode drift for %s", operationID)
		require.Truef(t, hasMatchingCheck(authz.Checks, authzContractExpectation{
			securableType:     "table",
			privilege:         "INSERT",
			securableIDSource: "runtime_resolved_object_id",
		}), "authz check drift for %s", operationID)
	}
}
