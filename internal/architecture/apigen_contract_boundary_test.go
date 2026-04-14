package architecture

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	internalapi "github.com/Yacobolo/quackstack/internal/api"
)

func TestAPIGenContractBoundary_ComputeHandlersAvoidAdHocFallbacks(t *testing.T) {
	t.Parallel()

	content, err := os.ReadFile(filepath.Join("..", "api", "handler_compute.go"))
	require.NoError(t, err)

	source := string(content)
	require.NotContains(t, source, "return CreateComputeEndpoint400JSONResponse{badRequestErrorResponse(err)}, nil")
	require.NotContains(t, source, "return CreateComputeAssignment400JSONResponse{badRequestErrorResponse(err)}, nil")
	require.Contains(t, source, `respondDomainErrorForOperation[GenCreateComputeEndpointResponse]("createComputeEndpoint"`)
	require.Contains(t, source, `respondDomainErrorForOperation[GenCreateComputeAssignmentResponse]("createComputeAssignment"`)
	require.Contains(t, source, `respondDomainErrorForOperation[GenListComputeEndpointsResponse]("listComputeEndpoints"`)
}

func TestAPIGenContractBoundary_NoManualOperationsRemain(t *testing.T) {
	t.Parallel()

	contracts := internalapi.GetAPIGenOperationContracts()
	require.NotEmpty(t, contracts)

	remainingManual := make([]string, 0)
	for operationID, contract := range contracts {
		if contract.Manual {
			remainingManual = append(remainingManual, operationID)
		}
	}

	require.Emptyf(t, remainingManual, "expected full APIGen migration with no remaining manual operations, found: %s", strings.Join(remainingManual, ", "))
}

func TestAPIGenContractBoundary_AllHandlersUseOperationAwareMapping(t *testing.T) {
	t.Parallel()

	handlerPaths, err := filepath.Glob(filepath.Join("..", "api", "handler*.go"))
	require.NoError(t, err)
	sort.Strings(handlerPaths)
	require.NotEmpty(t, handlerPaths)

	skipFiles := map[string]struct{}{
		filepath.Join("..", "api", "handler_auth_custom.go"): {},
	}

	for _, handlerPath := range handlerPaths {
		if _, skip := skipFiles[handlerPath]; skip {
			continue
		}
		content, err := os.ReadFile(handlerPath)
		require.NoError(t, err)
		require.NotContainsf(t, string(content), `respondDomainError[`, "expected %s to use operation-aware contract mapping only", handlerPath)
	}
}
