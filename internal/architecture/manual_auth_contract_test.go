package architecture_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/pkg/cli/gen"
)

func TestManualAuthEndpoints_AreCanonicalButNotAPIGenerated(t *testing.T) {
	t.Helper()

	doc := loadOpenAPISpec(t, filepath.Join(repoRootDir(), "internal", "api", "gen", "openapi.yaml"))
	operations := collectOpenAPICoreOperations(t, doc)

	generated := make(map[string]struct{}, len(gen.APIGeneratedCommandSpecs))
	for _, endpoint := range gen.APIGeneratedCommandSpecs {
		generated[endpoint.OperationID] = struct{}{}
	}

	manualOps := []string{
		"bootstrapComplete",
		"localLogin",
		"createBootstrapToken",
		"getOIDCProvider",
		"upsertOIDCProvider",
		"revokeAllWebSessions",
		"getWebSessionStats",
	}

	for _, operationID := range manualOps {
		require.Truef(t, openAPIContainsOperationID(operations, operationID), "canonical OpenAPI missing manual auth operation %s", operationID)
		_, ok := generated[operationID]
		assert.Falsef(t, ok, "manual auth operation %s should not be APIGenerated yet", operationID)
	}
}

func openAPIContainsOperationID(operations map[string]openAPICoreOperation, operationID string) bool {
	for _, operation := range operations {
		if operation.OperationID == operationID {
			return true
		}
	}
	return false
}
