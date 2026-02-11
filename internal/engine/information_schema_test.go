package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === Mock CatalogRepository for engine package ===

type mockEngineCatalog struct {
	listSchemasFn func(ctx context.Context, page domain.PageRequest) ([]domain.SchemaDetail, int64, error)
	listTablesFn  func(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.TableDetail, int64, error)
	listColumnsFn func(ctx context.Context, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, int64, error)
}

func (m *mockEngineCatalog) GetCatalogInfo(_ context.Context) (*domain.CatalogInfo, error) {
	panic("unexpected call")
}
func (m *mockEngineCatalog) GetMetastoreSummary(_ context.Context) (*domain.MetastoreSummary, error) {
	panic("unexpected call")
}
func (m *mockEngineCatalog) CreateSchema(_ context.Context, _, _, _ string) (*domain.SchemaDetail, error) {
	panic("unexpected call")
}
func (m *mockEngineCatalog) GetSchema(_ context.Context, _ string) (*domain.SchemaDetail, error) {
	panic("unexpected call")
}
func (m *mockEngineCatalog) ListSchemas(ctx context.Context, page domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
	if m.listSchemasFn != nil {
		return m.listSchemasFn(ctx, page)
	}
	panic("unexpected call to ListSchemas")
}
func (m *mockEngineCatalog) UpdateSchema(_ context.Context, _ string, _ *string, _ map[string]string) (*domain.SchemaDetail, error) {
	panic("unexpected call")
}
func (m *mockEngineCatalog) DeleteSchema(_ context.Context, _ string, _ bool) error {
	panic("unexpected call")
}
func (m *mockEngineCatalog) CreateTable(_ context.Context, _ string, _ domain.CreateTableRequest, _ string) (*domain.TableDetail, error) {
	panic("unexpected call")
}
func (m *mockEngineCatalog) GetTable(_ context.Context, _, _ string) (*domain.TableDetail, error) {
	panic("unexpected call")
}
func (m *mockEngineCatalog) ListTables(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.TableDetail, int64, error) {
	if m.listTablesFn != nil {
		return m.listTablesFn(ctx, schemaName, page)
	}
	panic("unexpected call to ListTables")
}
func (m *mockEngineCatalog) DeleteTable(_ context.Context, _, _ string) error {
	panic("unexpected call")
}
func (m *mockEngineCatalog) ListColumns(ctx context.Context, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, int64, error) {
	if m.listColumnsFn != nil {
		return m.listColumnsFn(ctx, schemaName, tableName, page)
	}
	panic("unexpected call to ListColumns")
}
func (m *mockEngineCatalog) UpdateTable(_ context.Context, _, _ string, _ *string, _ map[string]string, _ *string) (*domain.TableDetail, error) {
	panic("unexpected call")
}
func (m *mockEngineCatalog) UpdateCatalog(_ context.Context, _ *string) (*domain.CatalogInfo, error) {
	panic("unexpected call")
}
func (m *mockEngineCatalog) UpdateColumn(_ context.Context, _, _, _ string, _ *string, _ map[string]string) (*domain.ColumnDetail, error) {
	panic("unexpected call")
}
func (m *mockEngineCatalog) SetSchemaStoragePath(_ context.Context, _ int64, _ string) error {
	return nil
}
func (m *mockEngineCatalog) CreateExternalTable(_ context.Context, _ string, _ domain.CreateTableRequest, _ string) (*domain.TableDetail, error) {
	panic("unexpected call")
}

var _ domain.CatalogRepository = (*mockEngineCatalog)(nil)

// === IsInformationSchemaQuery ===

func TestIsInformationSchemaQuery(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"schemata", "SELECT * FROM information_schema.schemata", true},
		{"tables_uppercase", "SELECT * FROM INFORMATION_SCHEMA.TABLES", true},
		{"columns_mixed", "SELECT column_name FROM information_schema.columns WHERE table_name = 'foo'", true},
		{"no_match", "SELECT * FROM users", false},
		{"partial_no_dot", "SELECT * FROM information_schema_foo", false},
		{"empty_string", "", false},
		{"contains_in_where", "SELECT 1 WHERE table_name IN (SELECT table_name FROM information_schema.tables)", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := IsInformationSchemaQuery(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// === BuildSchemataRows ===

func TestBuildSchemataRows(t *testing.T) {
	t.Run("two_schemas", func(t *testing.T) {
		catalog := &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return []domain.SchemaDetail{
					{SchemaID: 1, Name: "main", CatalogName: "lake", Owner: "admin"},
					{SchemaID: 2, Name: "staging", CatalogName: "lake", Owner: "etl"},
				}, 2, nil
			},
		}
		provider := NewInformationSchemaProvider(catalog)

		rows, columns, err := provider.BuildSchemataRows(context.Background())

		require.NoError(t, err)
		assert.Equal(t, []string{"catalog_name", "schema_name", "schema_owner", "default_character_set_catalog"}, columns)
		require.Len(t, rows, 2)
		assert.Equal(t, "lake", rows[0][0])
		assert.Equal(t, "main", rows[0][1])
		assert.Equal(t, "admin", rows[0][2])
		assert.Equal(t, "staging", rows[1][1])
	})

	t.Run("empty", func(t *testing.T) {
		catalog := &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return []domain.SchemaDetail{}, 0, nil
			},
		}
		provider := NewInformationSchemaProvider(catalog)

		rows, columns, err := provider.BuildSchemataRows(context.Background())

		require.NoError(t, err)
		assert.NotEmpty(t, columns, "columns should always be returned")
		assert.Empty(t, rows)
	})

	t.Run("list_error", func(t *testing.T) {
		catalog := &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return nil, 0, errInfoTest
			},
		}
		provider := NewInformationSchemaProvider(catalog)

		_, _, err := provider.BuildSchemataRows(context.Background())

		require.Error(t, err)
		assert.Contains(t, err.Error(), "list schemas")
	})
}

// === BuildTablesRows ===

func TestBuildTablesRows(t *testing.T) {
	t.Run("tables_across_schemas", func(t *testing.T) {
		catalog := &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return []domain.SchemaDetail{
					{SchemaID: 1, Name: "main", CatalogName: "lake"},
					{SchemaID: 2, Name: "staging", CatalogName: "lake"},
				}, 2, nil
			},
			listTablesFn: func(_ context.Context, schemaName string, _ domain.PageRequest) ([]domain.TableDetail, int64, error) {
				switch schemaName {
				case "main":
					return []domain.TableDetail{{Name: "orders", TableType: "MANAGED"}}, 1, nil
				case "staging":
					return []domain.TableDetail{{Name: "raw_events", TableType: "MANAGED"}}, 1, nil
				}
				return nil, 0, nil
			},
		}
		provider := NewInformationSchemaProvider(catalog)

		rows, columns, err := provider.BuildTablesRows(context.Background())

		require.NoError(t, err)
		assert.Equal(t, []string{"table_catalog", "table_schema", "table_name", "table_type"}, columns)
		require.Len(t, rows, 2)
		assert.Equal(t, "main", rows[0][1])
		assert.Equal(t, "orders", rows[0][2])
		assert.Equal(t, "staging", rows[1][1])
		assert.Equal(t, "raw_events", rows[1][2])
	})

	t.Run("schema_list_error", func(t *testing.T) {
		catalog := &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return nil, 0, errInfoTest
			},
		}
		provider := NewInformationSchemaProvider(catalog)

		_, _, err := provider.BuildTablesRows(context.Background())

		require.Error(t, err)
	})

	t.Run("table_list_error_continues", func(t *testing.T) {
		catalog := &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return []domain.SchemaDetail{
					{SchemaID: 1, Name: "broken", CatalogName: "lake"},
					{SchemaID: 2, Name: "good", CatalogName: "lake"},
				}, 2, nil
			},
			listTablesFn: func(_ context.Context, schemaName string, _ domain.PageRequest) ([]domain.TableDetail, int64, error) {
				if schemaName == "broken" {
					return nil, 0, errInfoTest
				}
				return []domain.TableDetail{{Name: "t1", TableType: "MANAGED"}}, 1, nil
			},
		}
		provider := NewInformationSchemaProvider(catalog)

		rows, _, err := provider.BuildTablesRows(context.Background())

		require.NoError(t, err)
		require.Len(t, rows, 1, "should skip broken schema and continue")
		assert.Equal(t, "t1", rows[0][2])
	})
}

// === BuildColumnsRows ===

func TestBuildColumnsRows(t *testing.T) {
	t.Run("columns_present", func(t *testing.T) {
		catalog := &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return []domain.SchemaDetail{
					{SchemaID: 1, Name: "main", CatalogName: "lake"},
				}, 1, nil
			},
			listTablesFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.TableDetail, int64, error) {
				return []domain.TableDetail{{Name: "orders"}}, 1, nil
			},
			listColumnsFn: func(_ context.Context, _, _ string, _ domain.PageRequest) ([]domain.ColumnDetail, int64, error) {
				return []domain.ColumnDetail{
					{Name: "id", Type: "INTEGER", Position: 0},
					{Name: "amount", Type: "DOUBLE", Position: 1},
				}, 2, nil
			},
		}
		provider := NewInformationSchemaProvider(catalog)

		rows, columns, err := provider.BuildColumnsRows(context.Background())

		require.NoError(t, err)
		assert.Equal(t, []string{"table_catalog", "table_schema", "table_name", "column_name", "ordinal_position", "data_type"}, columns)
		require.Len(t, rows, 2)
		assert.Equal(t, "id", rows[0][3])
		assert.Equal(t, 0, rows[0][4])
		assert.Equal(t, "INTEGER", rows[0][5])
		assert.Equal(t, "amount", rows[1][3])
		assert.Equal(t, 1, rows[1][4])
	})

	t.Run("column_list_error_continues", func(t *testing.T) {
		catalog := &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return []domain.SchemaDetail{
					{SchemaID: 1, Name: "main", CatalogName: "lake"},
				}, 1, nil
			},
			listTablesFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.TableDetail, int64, error) {
				return []domain.TableDetail{
					{Name: "broken_table"},
					{Name: "good_table"},
				}, 2, nil
			},
			listColumnsFn: func(_ context.Context, _, tableName string, _ domain.PageRequest) ([]domain.ColumnDetail, int64, error) {
				if tableName == "broken_table" {
					return nil, 0, errInfoTest
				}
				return []domain.ColumnDetail{
					{Name: "col1", Type: "VARCHAR", Position: 0},
				}, 1, nil
			},
		}
		provider := NewInformationSchemaProvider(catalog)

		rows, _, err := provider.BuildColumnsRows(context.Background())

		require.NoError(t, err)
		require.Len(t, rows, 1, "should skip broken table and continue")
		assert.Equal(t, "col1", rows[0][3])
	})
}

// errInfoTest is a sentinel error for information_schema tests.
var errInfoTest = assert.AnError
