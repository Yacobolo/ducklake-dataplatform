package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

func setupModelRepo(t *testing.T) *ModelRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewModelRepo(writeDB)
}

func TestModelRepo_Create(t *testing.T) {
	repo := setupModelRepo(t)
	ctx := context.Background()

	m := &domain.Model{
		ProjectName:     "sales",
		Name:            "stg_orders",
		SQL:             "SELECT order_id, customer_id FROM raw_data.orders",
		Materialization: "TABLE",
		Description:     "Staged orders",
		Owner:           "data-team",
		Tags:            []string{"finance", "staging"},
		DependsOn:       []string{"sales.raw_orders"},
		Config:          domain.ModelConfig{},
		CreatedBy:       "admin",
	}

	created, err := repo.Create(ctx, m)
	require.NoError(t, err)
	require.NotNil(t, created)

	assert.NotEmpty(t, created.ID)
	assert.Equal(t, "sales", created.ProjectName)
	assert.Equal(t, "stg_orders", created.Name)
	assert.Equal(t, "SELECT order_id, customer_id FROM raw_data.orders", created.SQL)
	assert.Equal(t, "TABLE", created.Materialization)
	assert.Equal(t, "Staged orders", created.Description)
	assert.Equal(t, "data-team", created.Owner)
	assert.Equal(t, []string{"finance", "staging"}, created.Tags)
	assert.Equal(t, []string{"sales.raw_orders"}, created.DependsOn)
	assert.Equal(t, "admin", created.CreatedBy)
	assert.False(t, created.CreatedAt.IsZero())
	assert.False(t, created.UpdatedAt.IsZero())
}

func TestModelRepo_GetByName(t *testing.T) {
	repo := setupModelRepo(t)
	ctx := context.Background()

	m := &domain.Model{
		ProjectName:     "sales",
		Name:            "stg_orders",
		SQL:             "SELECT 1",
		Materialization: "VIEW",
		Tags:            []string{},
		DependsOn:       []string{},
		CreatedBy:       "admin",
	}

	created, err := repo.Create(ctx, m)
	require.NoError(t, err)

	got, err := repo.GetByName(ctx, "sales", "stg_orders")
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)
	assert.Equal(t, "sales", got.ProjectName)
	assert.Equal(t, "stg_orders", got.Name)
	assert.Equal(t, "VIEW", got.Materialization)
}

func TestModelRepo_GetByName_NotFound(t *testing.T) {
	repo := setupModelRepo(t)
	ctx := context.Background()

	_, err := repo.GetByName(ctx, "nonexistent-project", "nonexistent-model")
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestModelRepo_List(t *testing.T) {
	repo := setupModelRepo(t)
	ctx := context.Background()

	models := []struct {
		project string
		name    string
	}{
		{"sales", "stg_orders"},
		{"sales", "fct_orders"},
		{"warehouse", "dim_products"},
	}

	for _, m := range models {
		_, err := repo.Create(ctx, &domain.Model{
			ProjectName:     m.project,
			Name:            m.name,
			SQL:             "SELECT 1",
			Materialization: "VIEW",
			Tags:            []string{},
			DependsOn:       []string{},
			CreatedBy:       "admin",
		})
		require.NoError(t, err)
	}

	t.Run("list_all_projects", func(t *testing.T) {
		results, total, err := repo.List(ctx, nil, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, results, 3)
	})

	t.Run("filter_by_project", func(t *testing.T) {
		project := "sales"
		results, total, err := repo.List(ctx, &project, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		assert.Len(t, results, 2)
		for _, r := range results {
			assert.Equal(t, "sales", r.ProjectName)
		}
	})

	t.Run("pagination", func(t *testing.T) {
		results, total, err := repo.List(ctx, nil, domain.PageRequest{MaxResults: 2})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, results, 2)
	})
}

func TestModelRepo_Update(t *testing.T) {
	repo := setupModelRepo(t)
	ctx := context.Background()

	created, err := repo.Create(ctx, &domain.Model{
		ProjectName:     "sales",
		Name:            "stg_orders",
		SQL:             "SELECT 1",
		Materialization: "VIEW",
		Description:     "original",
		Tags:            []string{"v1"},
		DependsOn:       []string{},
		CreatedBy:       "admin",
	})
	require.NoError(t, err)

	t.Run("update_sql_and_materialization", func(t *testing.T) {
		newSQL := "SELECT 2"
		newMat := "TABLE"
		updated, err := repo.Update(ctx, created.ID, domain.UpdateModelRequest{
			SQL:             &newSQL,
			Materialization: &newMat,
		})
		require.NoError(t, err)
		assert.Equal(t, "SELECT 2", updated.SQL)
		assert.Equal(t, "TABLE", updated.Materialization)
		assert.Equal(t, "original", updated.Description) // unchanged
	})

	t.Run("update_description", func(t *testing.T) {
		newDesc := "updated description"
		updated, err := repo.Update(ctx, created.ID, domain.UpdateModelRequest{
			Description: &newDesc,
		})
		require.NoError(t, err)
		assert.Equal(t, "updated description", updated.Description)
	})

	t.Run("update_tags", func(t *testing.T) {
		updated, err := repo.Update(ctx, created.ID, domain.UpdateModelRequest{
			Tags: []string{"v2", "production"},
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"v2", "production"}, updated.Tags)
	})

	t.Run("update_nonexistent", func(t *testing.T) {
		desc := "x"
		_, err := repo.Update(ctx, "nonexistent-id", domain.UpdateModelRequest{Description: &desc})
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

func TestModelRepo_Delete(t *testing.T) {
	repo := setupModelRepo(t)
	ctx := context.Background()

	created, err := repo.Create(ctx, &domain.Model{
		ProjectName:     "sales",
		Name:            "to_delete",
		SQL:             "SELECT 1",
		Materialization: "VIEW",
		Tags:            []string{},
		DependsOn:       []string{},
		CreatedBy:       "admin",
	})
	require.NoError(t, err)

	err = repo.Delete(ctx, created.ID)
	require.NoError(t, err)

	_, err = repo.GetByID(ctx, created.ID)
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestModelRepo_ListAll(t *testing.T) {
	repo := setupModelRepo(t)
	ctx := context.Background()

	for _, name := range []string{"alpha", "beta", "gamma"} {
		_, err := repo.Create(ctx, &domain.Model{
			ProjectName:     "analytics",
			Name:            name,
			SQL:             "SELECT 1",
			Materialization: "VIEW",
			Tags:            []string{},
			DependsOn:       []string{},
			CreatedBy:       "admin",
		})
		require.NoError(t, err)
	}

	all, err := repo.ListAll(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 3)
}

func TestModelRepo_UpdateDependencies(t *testing.T) {
	repo := setupModelRepo(t)
	ctx := context.Background()

	created, err := repo.Create(ctx, &domain.Model{
		ProjectName:     "sales",
		Name:            "fct_orders",
		SQL:             "SELECT 1",
		Materialization: "VIEW",
		Tags:            []string{},
		DependsOn:       []string{},
		CreatedBy:       "admin",
	})
	require.NoError(t, err)
	assert.Empty(t, created.DependsOn)

	err = repo.UpdateDependencies(ctx, created.ID, []string{"sales.stg_orders", "sales.stg_customers"})
	require.NoError(t, err)

	got, err := repo.GetByID(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"sales.stg_orders", "sales.stg_customers"}, got.DependsOn)
}

func TestModelRepo_DuplicateConflict(t *testing.T) {
	repo := setupModelRepo(t)
	ctx := context.Background()

	m := &domain.Model{
		ProjectName:     "sales",
		Name:            "stg_orders",
		SQL:             "SELECT 1",
		Materialization: "VIEW",
		Tags:            []string{},
		DependsOn:       []string{},
		CreatedBy:       "admin",
	}

	_, err := repo.Create(ctx, m)
	require.NoError(t, err)

	_, err = repo.Create(ctx, &domain.Model{
		ProjectName:     "sales",
		Name:            "stg_orders",
		SQL:             "SELECT 2",
		Materialization: "TABLE",
		Tags:            []string{},
		DependsOn:       []string{},
		CreatedBy:       "other",
	})
	require.Error(t, err)
	var conflict *domain.ConflictError
	assert.ErrorAs(t, err, &conflict)
}
