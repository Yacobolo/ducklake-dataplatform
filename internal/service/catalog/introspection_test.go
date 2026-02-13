package catalog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"
)

type mockIntrospectionRepo = testutil.MockIntrospectionRepo

func TestIntrospectionService_ListSchemas(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		expected := []domain.Schema{
			{ID: "s-1", Name: "public"},
			{ID: "s-2", Name: "analytics"},
		}
		repo := &mockIntrospectionRepo{
			ListSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.Schema, int64, error) {
				return expected, 2, nil
			},
		}
		svc := NewIntrospectionService(repo)

		schemas, total, err := svc.ListSchemas(context.Background(), domain.PageRequest{})
		require.NoError(t, err)
		assert.Len(t, schemas, 2)
		assert.Equal(t, int64(2), total)
		assert.Equal(t, "public", schemas[0].Name)
		assert.Equal(t, "analytics", schemas[1].Name)
	})

	t.Run("empty_result", func(t *testing.T) {
		repo := &mockIntrospectionRepo{
			ListSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.Schema, int64, error) {
				return []domain.Schema{}, 0, nil
			},
		}
		svc := NewIntrospectionService(repo)

		schemas, total, err := svc.ListSchemas(context.Background(), domain.PageRequest{})
		require.NoError(t, err)
		assert.Empty(t, schemas)
		assert.Equal(t, int64(0), total)
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockIntrospectionRepo{
			ListSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.Schema, int64, error) {
				return nil, 0, errTest
			},
		}
		svc := NewIntrospectionService(repo)

		_, _, err := svc.ListSchemas(context.Background(), domain.PageRequest{})
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})
}

func TestIntrospectionService_ListTables(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		expected := []domain.Table{
			{ID: "t-1", SchemaID: "s-1", Name: "users"},
			{ID: "t-2", SchemaID: "s-1", Name: "orders"},
		}
		repo := &mockIntrospectionRepo{
			ListTablesFn: func(_ context.Context, schemaID string, _ domain.PageRequest) ([]domain.Table, int64, error) {
				assert.Equal(t, "s-1", schemaID)
				return expected, 2, nil
			},
		}
		svc := NewIntrospectionService(repo)

		tables, total, err := svc.ListTables(context.Background(), "s-1", domain.PageRequest{})
		require.NoError(t, err)
		assert.Len(t, tables, 2)
		assert.Equal(t, int64(2), total)
		assert.Equal(t, "users", tables[0].Name)
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockIntrospectionRepo{
			ListTablesFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.Table, int64, error) {
				return nil, 0, errTest
			},
		}
		svc := NewIntrospectionService(repo)

		_, _, err := svc.ListTables(context.Background(), "s-1", domain.PageRequest{})
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})
}

func TestIntrospectionService_ListColumns(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		expected := []domain.Column{
			{ID: "c-1", TableID: "t-1", Name: "id", Type: "INTEGER"},
			{ID: "c-2", TableID: "t-1", Name: "email", Type: "VARCHAR"},
		}
		repo := &mockIntrospectionRepo{
			ListColumnsFn: func(_ context.Context, tableID string, _ domain.PageRequest) ([]domain.Column, int64, error) {
				assert.Equal(t, "t-1", tableID)
				return expected, 2, nil
			},
		}
		svc := NewIntrospectionService(repo)

		columns, total, err := svc.ListColumns(context.Background(), "t-1", domain.PageRequest{})
		require.NoError(t, err)
		assert.Len(t, columns, 2)
		assert.Equal(t, int64(2), total)
		assert.Equal(t, "id", columns[0].Name)
		assert.Equal(t, "INTEGER", columns[0].Type)
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockIntrospectionRepo{
			ListColumnsFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.Column, int64, error) {
				return nil, 0, errTest
			},
		}
		svc := NewIntrospectionService(repo)

		_, _, err := svc.ListColumns(context.Background(), "t-1", domain.PageRequest{})
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})
}
