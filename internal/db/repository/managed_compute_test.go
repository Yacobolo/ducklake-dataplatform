package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "github.com/Yacobolo/quackstack/internal/db"
	"github.com/Yacobolo/quackstack/internal/db/crypto"
	"github.com/Yacobolo/quackstack/internal/domain"
)

func setupManagedComputeRepos(t *testing.T) (*ComputeClusterTemplateRepo, *ManagedComputeClusterRepo, *ComputeEndpointRepo) {
	t.Helper()

	writeDB, _ := internaldb.OpenTestSQLite(t)
	enc, err := crypto.NewEncryptor("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	require.NoError(t, err)

	return NewComputeClusterTemplateRepo(writeDB), NewManagedComputeClusterRepo(writeDB), NewComputeEndpointRepo(writeDB, enc)
}

func TestComputeClusterTemplateRepo_CRUD(t *testing.T) {
	templateRepo, _, _ := setupManagedComputeRepos(t)
	ctx := context.Background()

	created, err := templateRepo.Create(ctx, &domain.ComputeClusterTemplate{
		Name:                   "standard-azure",
		Provider:               domain.ComputeProviderAzure,
		WorkloadClass:          domain.ComputeEndpointWorkloadMixed,
		Size:                   "MEDIUM",
		MinReplicas:            1,
		MaxReplicas:            4,
		IdleAutoStopSeconds:    900,
		ScalingPolicy:          "queue_depth",
		StorageProfile:         "hot",
		ResultRetentionSeconds: 3600,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, created.ID)
	assert.False(t, created.CreatedAt.IsZero())

	got, err := templateRepo.GetByName(ctx, "standard-azure")
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)
	assert.Equal(t, domain.ComputeProviderAzure, got.Provider)

	items, total, err := templateRepo.List(ctx, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, items, 1)

	require.NoError(t, templateRepo.Delete(ctx, created.ID))
	_, err = templateRepo.GetByID(ctx, created.ID)
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestManagedComputeClusterRepo_CRUD(t *testing.T) {
	templateRepo, clusterRepo, endpointRepo := setupManagedComputeRepos(t)
	ctx := context.Background()

	template, err := templateRepo.Create(ctx, &domain.ComputeClusterTemplate{
		Name:          "burst",
		Provider:      domain.ComputeProviderAzure,
		WorkloadClass: domain.ComputeEndpointWorkloadHeavy,
		Size:          "LARGE",
		MinReplicas:   1,
		MaxReplicas:   8,
	})
	require.NoError(t, err)

	endpoint, err := endpointRepo.Create(ctx, &domain.ComputeEndpoint{
		Name:      "burst-endpoint",
		URL:       "grpcs://burst.compute.example:9444",
		Type:      "REMOTE",
		AuthToken: "top-secret",
		Owner:     "admin",
	})
	require.NoError(t, err)

	cluster, err := clusterRepo.Create(ctx, &domain.ManagedComputeCluster{
		Name:          "burst-eastus",
		TemplateID:    template.ID,
		EndpointID:    endpoint.ID,
		Provider:      domain.ComputeProviderAzure,
		ExternalID:    "aks-burst-eastus",
		DesiredState:  domain.ManagedClusterDesiredRunning,
		ObservedState: domain.ManagedClusterObservedStarting,
		MinReplicas:   1,
		MaxReplicas:   8,
		EndpointURL:   &endpoint.URL,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, cluster.ID)

	gotByEndpoint, err := clusterRepo.GetByEndpointID(ctx, endpoint.ID)
	require.NoError(t, err)
	assert.Equal(t, cluster.ID, gotByEndpoint.ID)

	require.NoError(t, clusterRepo.UpdateDesiredState(ctx, cluster.ID, domain.ManagedClusterDesiredDraining))
	require.NoError(t, clusterRepo.UpdateObservedState(ctx, cluster.ID, domain.ManagedClusterObservedReady, &endpoint.URL, nil))

	got, err := clusterRepo.GetByID(ctx, cluster.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.ManagedClusterDesiredDraining, got.DesiredState)
	assert.True(t, got.IsDraining)
	assert.Equal(t, domain.ManagedClusterObservedReady, got.ObservedState)

	items, total, err := clusterRepo.List(ctx, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, items, 1)

	require.NoError(t, clusterRepo.Delete(ctx, cluster.ID))
	_, err = clusterRepo.GetByID(ctx, cluster.ID)
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}
