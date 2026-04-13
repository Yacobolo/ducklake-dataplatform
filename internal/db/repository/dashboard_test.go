package repository

import (
	"context"
	"testing"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupDashboardRepo(t *testing.T) *DashboardRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewDashboardRepo(writeDB)
}

func TestDashboardRepo_CreateStoresDefaultComputePolicy(t *testing.T) {
	repo := setupDashboardRepo(t)
	ctx := context.Background()

	created, err := repo.Create(ctx, &domain.Dashboard{
		Name:        "Revenue Dashboard",
		Description: "Executive metrics",
		Owner:       "alice",
	})
	require.NoError(t, err)

	compute := created.Compute.Normalize()
	assert.Equal(t, domain.ComputeModeAuto, compute.Mode)
	assert.Empty(t, compute.EndpointName)
	assert.False(t, compute.FallbackLocal)

	loaded, err := repo.GetByID(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, compute, loaded.Compute.Normalize())
}

func TestDashboardRepo_CreateAndUpdateRoundTripsComputePolicy(t *testing.T) {
	repo := setupDashboardRepo(t)
	ctx := context.Background()

	created, err := repo.Create(ctx, &domain.Dashboard{
		Name:        "Revenue Dashboard",
		Description: "Executive metrics",
		Owner:       "alice",
		Compute: domain.DashboardComputePolicy{
			Mode:          domain.ComputeModeSharedEndpoint,
			EndpointName:  "analytics-xl",
			FallbackLocal: true,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, domain.ComputeModeSharedEndpoint, created.Compute.Normalize().Mode)
	assert.Equal(t, "analytics-xl", created.Compute.Normalize().EndpointName)
	assert.True(t, created.Compute.Normalize().FallbackLocal)

	updated, err := repo.Update(ctx, created.ID, domain.UpdateDashboardRequest{
		Compute: &domain.DashboardComputePolicy{
			Mode: domain.ComputeModeByocLocal,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, domain.ComputeModeByocLocal, updated.Compute.Normalize().Mode)
	assert.Empty(t, updated.Compute.Normalize().EndpointName)
	assert.False(t, updated.Compute.Normalize().FallbackLocal)

	loaded, err := repo.GetByID(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.ComputeModeByocLocal, loaded.Compute.Normalize().Mode)
	assert.Empty(t, loaded.Compute.Normalize().EndpointName)
	assert.False(t, loaded.Compute.Normalize().FallbackLocal)
}

func TestDashboardRepo_UpdateRejectsInvalidComputePolicy(t *testing.T) {
	repo := setupDashboardRepo(t)
	ctx := context.Background()

	created, err := repo.Create(ctx, &domain.Dashboard{
		Name:        "Revenue Dashboard",
		Description: "Executive metrics",
		Owner:       "alice",
	})
	require.NoError(t, err)

	_, err = repo.Update(ctx, created.ID, domain.UpdateDashboardRequest{
		Compute: &domain.DashboardComputePolicy{
			Mode: domain.ComputeModeSharedEndpoint,
		},
	})
	require.Error(t, err)
	var validationErr *domain.ValidationError
	assert.ErrorAs(t, err, &validationErr)
}
