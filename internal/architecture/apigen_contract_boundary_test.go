package architecture

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	internalapi "duck-demo/internal/api"
)

func TestAPIGenContractBoundary_ComputeHandlersAvoidAdHocFallbacks(t *testing.T) {
	t.Parallel()

	content, err := os.ReadFile(filepath.Join("..", "api", "handler_compute.go"))
	require.NoError(t, err)

	source := string(content)
	require.NotContains(t, source, "return CreateComputeEndpoint400JSONResponse{badRequestErrorResponse(err)}, nil")
	require.NotContains(t, source, "return CreateComputeAssignment400JSONResponse{badRequestErrorResponse(err)}, nil")
	require.True(t, strings.Contains(source, `respondDomainErrorForOperation[GenCreateComputeEndpointResponse]("createComputeEndpoint"`))
	require.True(t, strings.Contains(source, `respondDomainErrorForOperation[GenCreateComputeAssignmentResponse]("createComputeAssignment"`))
	require.True(t, strings.Contains(source, `respondDomainErrorForOperation[GenListComputeEndpointsResponse]("listComputeEndpoints"`))
}

func TestAPIGenContractBoundary_GrantAndProductAndGovernanceHandlersUseOperationAwareMapping(t *testing.T) {
	t.Parallel()

	securityBody, err := os.ReadFile(filepath.Join("..", "api", "handler_security.go"))
	require.NoError(t, err)
	securitySource := string(securityBody)
	require.True(t, strings.Contains(securitySource, `respondDomainErrorForOperation[GenListGrantsResponse]("listGrants"`))
	require.True(t, strings.Contains(securitySource, `respondDomainErrorForOperation[GenCreateGrantResponse]("createGrant"`))

	productsBody, err := os.ReadFile(filepath.Join("..", "api", "handler_products.go"))
	require.NoError(t, err)
	productsSource := string(productsBody)
	require.NotContains(t, productsSource, `respondDomainError[`)
	for _, snippet := range []string{
		`respondDomainErrorForOperation[GenListProductDomainsResponse]("listProductDomains"`,
		`respondDomainErrorForOperation[GenCreateProductDomainResponse]("createProductDomain"`,
		`respondDomainErrorForOperation[GenListProductTeamsResponse]("listProductTeams"`,
		`respondDomainErrorForOperation[GenCreateProductTeamResponse]("createProductTeam"`,
		`respondDomainErrorForOperation[GenListDataProductsResponse]("listDataProducts"`,
		`respondDomainErrorForOperation[GenCreateDataProductResponse]("createDataProduct"`,
		`respondDomainErrorForOperation[GenCreateDataProductDependencyResponse]("createDataProductDependency"`,
		`respondDomainErrorForOperation[GenCreateDataProductSubscriptionResponse]("createDataProductSubscription"`,
	} {
		require.True(t, strings.Contains(productsSource, snippet), "expected handler_products.go to contain %q", snippet)
	}

	governanceBody, err := os.ReadFile(filepath.Join("..", "api", "handler_governance.go"))
	require.NoError(t, err)
	governanceSource := string(governanceBody)
	require.True(t, strings.Contains(governanceSource, `respondDomainErrorForOperation[GenListTagAssignmentsResponse]("listTagAssignments"`))
	require.True(t, strings.Contains(governanceSource, `respondDomainErrorForOperation[GenCreateTagAssignmentResponse]("createTagAssignment"`))
}

func TestAPIGenContractBoundary_StorageHandlersUseOperationAwareMapping(t *testing.T) {
	t.Parallel()

	storageBody, err := os.ReadFile(filepath.Join("..", "api", "handler_storage.go"))
	require.NoError(t, err)
	storageSource := string(storageBody)
	require.NotContains(t, storageSource, `respondDomainError[`)
	for _, snippet := range []string{
		`respondDomainErrorForOperation[GenListStorageCredentialsResponse]("listStorageCredentials"`,
		`respondDomainErrorForOperation[GenCreateStorageCredentialResponse]("createStorageCredential"`,
		`respondDomainErrorForOperation[GenListExternalLocationsResponse]("listExternalLocations"`,
		`respondDomainErrorForOperation[GenCreateExternalLocationResponse]("createExternalLocation"`,
		`respondDomainErrorForOperation[GenListVolumesResponse]("listVolumes"`,
		`respondDomainErrorForOperation[GenCreateVolumeResponse]("createVolume"`,
	} {
		require.True(t, strings.Contains(storageSource, snippet), "expected handler_storage.go to contain %q", snippet)
	}
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
