package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/crypto"
	"duck-demo/internal/domain"
)

func setupComputeEndpointRepo(t *testing.T) *ComputeEndpointRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	enc, err := crypto.NewEncryptor("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef") // 64 hex chars = 32-byte key
	require.NoError(t, err)
	return NewComputeEndpointRepo(writeDB, enc)
}

func TestComputeEndpoint_CreateAndGet(t *testing.T) {
	repo := setupComputeEndpointRepo(t)
	ctx := context.Background()

	t.Run("create_and_get_by_id", func(t *testing.T) {
		ep := &domain.ComputeEndpoint{
			Name:      "analytics-xl",
			URL:       "https://compute-1.example.com:9443",
			Type:      "REMOTE",
			Size:      "LARGE",
			AuthToken: "super-secret-token",
			Owner:     "admin",
		}

		created, err := repo.Create(ctx, ep)
		require.NoError(t, err)
		require.NotNil(t, created)

		assert.NotEmpty(t, created.ID)
		assert.NotEmpty(t, created.ExternalID)
		assert.Equal(t, "analytics-xl", created.Name)
		assert.Equal(t, "https://compute-1.example.com:9443", created.URL)
		assert.Equal(t, "REMOTE", created.Type)
		assert.Equal(t, "INACTIVE", created.Status)
		assert.Equal(t, "LARGE", created.Size)
		assert.Equal(t, "super-secret-token", created.AuthToken) // decrypted on read
		assert.Equal(t, "admin", created.Owner)
		assert.False(t, created.CreatedAt.IsZero())
		assert.False(t, created.UpdatedAt.IsZero())

		got, err := repo.GetByID(ctx, created.ID)
		require.NoError(t, err)
		assert.Equal(t, created.ID, got.ID)
		assert.Equal(t, "super-secret-token", got.AuthToken)
	})

	t.Run("create_and_get_by_name", func(t *testing.T) {
		ep := &domain.ComputeEndpoint{
			Name:      "dev-local",
			URL:       "http://localhost:9443",
			Type:      "LOCAL",
			AuthToken: "local-token",
			Owner:     "admin",
		}
		created, err := repo.Create(ctx, ep)
		require.NoError(t, err)

		got, err := repo.GetByName(ctx, "dev-local")
		require.NoError(t, err)
		assert.Equal(t, created.ID, got.ID)
		assert.Equal(t, "LOCAL", got.Type)
	})

	t.Run("create_with_max_memory", func(t *testing.T) {
		mem := int64(64)
		ep := &domain.ComputeEndpoint{
			Name:        "big-mem",
			URL:         "https://big.example.com:9443",
			Type:        "REMOTE",
			MaxMemoryGB: &mem,
			AuthToken:   "token",
			Owner:       "admin",
		}
		created, err := repo.Create(ctx, ep)
		require.NoError(t, err)
		require.NotNil(t, created.MaxMemoryGB)
		assert.Equal(t, int64(64), *created.MaxMemoryGB)
	})

	t.Run("duplicate_name_conflict", func(t *testing.T) {
		ep := &domain.ComputeEndpoint{
			Name:      "analytics-xl", // already exists
			URL:       "https://other.example.com",
			Type:      "REMOTE",
			AuthToken: "token",
			Owner:     "admin",
		}
		_, err := repo.Create(ctx, ep)
		require.Error(t, err)
		var conflict *domain.ConflictError
		assert.ErrorAs(t, err, &conflict)
	})

	t.Run("get_nonexistent_id", func(t *testing.T) {
		_, err := repo.GetByID(ctx, "99999")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("get_nonexistent_name", func(t *testing.T) {
		_, err := repo.GetByName(ctx, "nonexistent")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

func TestComputeEndpoint_List(t *testing.T) {
	repo := setupComputeEndpointRepo(t)
	ctx := context.Background()

	// Create 3 endpoints
	for _, name := range []string{"alpha", "beta", "gamma"} {
		_, err := repo.Create(ctx, &domain.ComputeEndpoint{
			Name: name, URL: "https://" + name + ".example.com", Type: "REMOTE",
			AuthToken: "tok", Owner: "admin",
		})
		require.NoError(t, err)
	}

	t.Run("list_all", func(t *testing.T) {
		eps, total, err := repo.List(ctx, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, eps, 3)
		// Ordered by name
		assert.Equal(t, "alpha", eps[0].Name)
		assert.Equal(t, "beta", eps[1].Name)
		assert.Equal(t, "gamma", eps[2].Name)
	})

	t.Run("list_paginated", func(t *testing.T) {
		page := domain.PageRequest{MaxResults: 2}
		eps, total, err := repo.List(ctx, page)
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, eps, 2)
	})
}

func TestComputeEndpoint_Update(t *testing.T) {
	repo := setupComputeEndpointRepo(t)
	ctx := context.Background()

	ep, err := repo.Create(ctx, &domain.ComputeEndpoint{
		Name: "to-update", URL: "https://old.example.com", Type: "REMOTE",
		Size: "SMALL", AuthToken: "old-token", Owner: "admin",
	})
	require.NoError(t, err)

	t.Run("update_url", func(t *testing.T) {
		newURL := "https://new.example.com:9443"
		updated, err := repo.Update(ctx, ep.ID, domain.UpdateComputeEndpointRequest{URL: &newURL})
		require.NoError(t, err)
		assert.Equal(t, "https://new.example.com:9443", updated.URL)
		assert.Equal(t, "SMALL", updated.Size) // unchanged
	})

	t.Run("update_auth_token", func(t *testing.T) {
		newToken := "new-secret"
		updated, err := repo.Update(ctx, ep.ID, domain.UpdateComputeEndpointRequest{AuthToken: &newToken})
		require.NoError(t, err)
		assert.Equal(t, "new-secret", updated.AuthToken) // decrypted
	})

	t.Run("update_size_and_memory", func(t *testing.T) {
		newSize := "LARGE"
		newMem := int64(128)
		updated, err := repo.Update(ctx, ep.ID, domain.UpdateComputeEndpointRequest{Size: &newSize, MaxMemoryGB: &newMem})
		require.NoError(t, err)
		assert.Equal(t, "LARGE", updated.Size)
		require.NotNil(t, updated.MaxMemoryGB)
		assert.Equal(t, int64(128), *updated.MaxMemoryGB)
	})

	t.Run("update_nonexistent", func(t *testing.T) {
		url := "https://x.com"
		_, err := repo.Update(ctx, "99999", domain.UpdateComputeEndpointRequest{URL: &url})
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

func TestComputeEndpoint_Delete(t *testing.T) {
	repo := setupComputeEndpointRepo(t)
	ctx := context.Background()

	ep, err := repo.Create(ctx, &domain.ComputeEndpoint{
		Name: "to-delete", URL: "https://del.example.com", Type: "REMOTE",
		AuthToken: "tok", Owner: "admin",
	})
	require.NoError(t, err)

	err = repo.Delete(ctx, ep.ID)
	require.NoError(t, err)

	_, err = repo.GetByID(ctx, ep.ID)
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestComputeEndpoint_UpdateStatus(t *testing.T) {
	repo := setupComputeEndpointRepo(t)
	ctx := context.Background()

	ep, err := repo.Create(ctx, &domain.ComputeEndpoint{
		Name: "status-test", URL: "https://status.example.com", Type: "REMOTE",
		AuthToken: "tok", Owner: "admin",
	})
	require.NoError(t, err)
	assert.Equal(t, "INACTIVE", ep.Status)

	err = repo.UpdateStatus(ctx, ep.ID, "ACTIVE")
	require.NoError(t, err)

	got, err := repo.GetByID(ctx, ep.ID)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", got.Status)
}

func TestComputeAssignment_CRUD(t *testing.T) {
	repo := setupComputeEndpointRepo(t)
	ctx := context.Background()

	ep, err := repo.Create(ctx, &domain.ComputeEndpoint{
		Name: "assign-test", URL: "https://assign.example.com", Type: "REMOTE",
		AuthToken: "tok", Owner: "admin",
	})
	require.NoError(t, err)

	// Mark as ACTIVE for GetDefaultForPrincipal to work
	require.NoError(t, repo.UpdateStatus(ctx, ep.ID, "ACTIVE"))

	t.Run("assign_and_list", func(t *testing.T) {
		a, err := repo.Assign(ctx, &domain.ComputeAssignment{
			PrincipalID:   "1",
			PrincipalType: "user",
			EndpointID:    ep.ID,
			IsDefault:     true,
			FallbackLocal: false,
		})
		require.NoError(t, err)
		require.NotNil(t, a)
		assert.NotEmpty(t, a.ID)
		assert.Equal(t, "1", a.PrincipalID)
		assert.Equal(t, "user", a.PrincipalType)
		assert.Equal(t, ep.ID, a.EndpointID)
		assert.True(t, a.IsDefault)
		assert.False(t, a.FallbackLocal)

		assignments, total, err := repo.ListAssignments(ctx, ep.ID, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Len(t, assignments, 1)
	})

	t.Run("duplicate_assignment_conflict", func(t *testing.T) {
		_, err := repo.Assign(ctx, &domain.ComputeAssignment{
			PrincipalID:   "1",
			PrincipalType: "user",
			EndpointID:    ep.ID,
			IsDefault:     true,
		})
		require.Error(t, err)
		var conflict *domain.ConflictError
		assert.ErrorAs(t, err, &conflict)
	})

	t.Run("get_default_for_principal", func(t *testing.T) {
		got, err := repo.GetDefaultForPrincipal(ctx, "1", "user")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, ep.ID, got.ID)
		assert.Equal(t, "assign-test", got.Name)
	})

	t.Run("get_default_nonexistent_principal", func(t *testing.T) {
		_, err := repo.GetDefaultForPrincipal(ctx, "999", "user")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("get_assignments_for_principal", func(t *testing.T) {
		eps, err := repo.GetAssignmentsForPrincipal(ctx, "1", "user")
		require.NoError(t, err)
		require.Len(t, eps, 1)
		assert.Equal(t, "assign-test", eps[0].Name)
	})

	t.Run("get_assignments_nonexistent_principal", func(t *testing.T) {
		eps, err := repo.GetAssignmentsForPrincipal(ctx, "999", "user")
		require.NoError(t, err)
		assert.Empty(t, eps)
	})

	t.Run("unassign", func(t *testing.T) {
		// List to get assignment ID
		assignments, _, err := repo.ListAssignments(ctx, ep.ID, domain.PageRequest{})
		require.NoError(t, err)
		require.Len(t, assignments, 1)

		err = repo.Unassign(ctx, assignments[0].ID)
		require.NoError(t, err)

		assignments, total, err := repo.ListAssignments(ctx, ep.ID, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(0), total)
		assert.Empty(t, assignments)
	})
}

func TestComputeAssignment_CascadeDelete(t *testing.T) {
	repo := setupComputeEndpointRepo(t)
	ctx := context.Background()

	ep, err := repo.Create(ctx, &domain.ComputeEndpoint{
		Name: "cascade-test", URL: "https://cascade.example.com", Type: "REMOTE",
		AuthToken: "tok", Owner: "admin",
	})
	require.NoError(t, err)

	_, err = repo.Assign(ctx, &domain.ComputeAssignment{
		PrincipalID: "1", PrincipalType: "user", EndpointID: ep.ID, IsDefault: true,
	})
	require.NoError(t, err)

	_, err = repo.Assign(ctx, &domain.ComputeAssignment{
		PrincipalID: "2", PrincipalType: "group", EndpointID: ep.ID, IsDefault: true,
	})
	require.NoError(t, err)

	// Delete endpoint — assignments should cascade
	err = repo.Delete(ctx, ep.ID)
	require.NoError(t, err)

	// Verify assignments are gone
	assignments, total, err := repo.ListAssignments(ctx, ep.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, assignments)
}
