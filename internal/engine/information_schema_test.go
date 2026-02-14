package engine

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
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
func (m *mockEngineCatalog) SetSchemaStoragePath(_ context.Context, _ string, _ string) error {
	return nil
}
func (m *mockEngineCatalog) CreateExternalTable(_ context.Context, _ string, _ domain.CreateTableRequest, _ string) (*domain.TableDetail, error) {
	panic("unexpected call")
}

var _ domain.CatalogRepository = (*mockEngineCatalog)(nil)

// === Mock CatalogRepoFactory ===

type mockCatalogRepoFactory struct {
	catalogs map[string]domain.CatalogRepository
}

func (f *mockCatalogRepoFactory) ForCatalog(_ context.Context, name string) (domain.CatalogRepository, error) {
	if repo, ok := f.catalogs[name]; ok {
		return repo, nil
	}
	return nil, fmt.Errorf("unexpected catalog: %s", name)
}

// === Mock CatalogRegistrationRepository (lister) ===

type mockCatalogLister struct {
	registrations []domain.CatalogRegistration
}

func (m *mockCatalogLister) Create(_ context.Context, _ *domain.CatalogRegistration) (*domain.CatalogRegistration, error) {
	panic("unexpected call")
}
func (m *mockCatalogLister) GetByID(_ context.Context, _ string) (*domain.CatalogRegistration, error) {
	panic("unexpected call")
}
func (m *mockCatalogLister) GetByName(_ context.Context, _ string) (*domain.CatalogRegistration, error) {
	panic("unexpected call")
}
func (m *mockCatalogLister) List(_ context.Context, _ domain.PageRequest) ([]domain.CatalogRegistration, int64, error) {
	return m.registrations, int64(len(m.registrations)), nil
}
func (m *mockCatalogLister) Update(_ context.Context, _ string, _ domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error) {
	panic("unexpected call")
}
func (m *mockCatalogLister) Delete(_ context.Context, _ string) error {
	panic("unexpected call")
}
func (m *mockCatalogLister) UpdateStatus(_ context.Context, _ string, _ domain.CatalogStatus, _ string) error {
	panic("unexpected call")
}
func (m *mockCatalogLister) GetDefault(_ context.Context) (*domain.CatalogRegistration, error) {
	panic("unexpected call")
}
func (m *mockCatalogLister) SetDefault(_ context.Context, _ string) error {
	panic("unexpected call")
}

var _ domain.CatalogRegistrationRepository = (*mockCatalogLister)(nil)

// newTestProvider creates an InformationSchemaProvider for testing with a single catalog.
func newTestProvider(catalogName string, repo domain.CatalogRepository) *InformationSchemaProvider {
	factory := &mockCatalogRepoFactory{
		catalogs: map[string]domain.CatalogRepository{catalogName: repo},
	}
	lister := &mockCatalogLister{
		registrations: []domain.CatalogRegistration{
			{Name: catalogName, Status: domain.CatalogStatusActive},
		},
	}
	return NewInformationSchemaProvider(factory, lister)
}

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
					{SchemaID: "1", Name: "main", CatalogName: "lake", Owner: "admin"},
					{SchemaID: "2", Name: "staging", CatalogName: "lake", Owner: "etl"},
				}, 2, nil
			},
		}
		provider := newTestProvider("lake", catalog)

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
		provider := newTestProvider("lake", catalog)

		rows, columns, err := provider.BuildSchemataRows(context.Background())

		require.NoError(t, err)
		assert.NotEmpty(t, columns, "columns should always be returned")
		assert.Empty(t, rows)
	})

	t.Run("list_error_skips_catalog", func(t *testing.T) {
		catalog := &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return nil, 0, errInfoTest
			},
		}
		provider := newTestProvider("lake", catalog)

		rows, _, err := provider.BuildSchemataRows(context.Background())

		// Errors in a single catalog are skipped, not propagated
		require.NoError(t, err)
		assert.Empty(t, rows)
	})

	t.Run("multi_catalog", func(t *testing.T) {
		lakeCatalog := &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return []domain.SchemaDetail{
					{SchemaID: "1", Name: "main", CatalogName: "lake", Owner: "admin"},
				}, 1, nil
			},
		}
		analyticsCatalog := &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return []domain.SchemaDetail{
					{SchemaID: "1", Name: "raw", CatalogName: "analytics", Owner: "data_eng"},
				}, 1, nil
			},
		}
		factory := &mockCatalogRepoFactory{
			catalogs: map[string]domain.CatalogRepository{
				"lake":      lakeCatalog,
				"analytics": analyticsCatalog,
			},
		}
		lister := &mockCatalogLister{
			registrations: []domain.CatalogRegistration{
				{Name: "lake", Status: domain.CatalogStatusActive},
				{Name: "analytics", Status: domain.CatalogStatusActive},
			},
		}
		provider := NewInformationSchemaProvider(factory, lister)

		rows, _, err := provider.BuildSchemataRows(context.Background())

		require.NoError(t, err)
		require.Len(t, rows, 2)
		// Verify both catalogs are represented
		catalogNames := make(map[string]bool)
		for _, row := range rows {
			catalogNames[row[0].(string)] = true
		}
		assert.True(t, catalogNames["lake"])
		assert.True(t, catalogNames["analytics"])
	})
}

// === BuildTablesRows ===

func TestBuildTablesRows(t *testing.T) {
	t.Run("tables_across_schemas", func(t *testing.T) {
		catalog := &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return []domain.SchemaDetail{
					{SchemaID: "1", Name: "main", CatalogName: "lake"},
					{SchemaID: "2", Name: "staging", CatalogName: "lake"},
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
		provider := newTestProvider("lake", catalog)

		rows, columns, err := provider.BuildTablesRows(context.Background())

		require.NoError(t, err)
		assert.Equal(t, []string{"table_catalog", "table_schema", "table_name", "table_type"}, columns)
		require.Len(t, rows, 2)
		assert.Equal(t, "main", rows[0][1])
		assert.Equal(t, "orders", rows[0][2])
		assert.Equal(t, "staging", rows[1][1])
		assert.Equal(t, "raw_events", rows[1][2])
	})

	t.Run("schema_list_error_skips_catalog", func(t *testing.T) {
		catalog := &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return nil, 0, errInfoTest
			},
		}
		provider := newTestProvider("lake", catalog)

		rows, _, err := provider.BuildTablesRows(context.Background())

		require.NoError(t, err)
		assert.Empty(t, rows)
	})

	t.Run("table_list_error_continues", func(t *testing.T) {
		catalog := &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return []domain.SchemaDetail{
					{SchemaID: "1", Name: "broken", CatalogName: "lake"},
					{SchemaID: "2", Name: "good", CatalogName: "lake"},
				}, 2, nil
			},
			listTablesFn: func(_ context.Context, schemaName string, _ domain.PageRequest) ([]domain.TableDetail, int64, error) {
				if schemaName == "broken" {
					return nil, 0, errInfoTest
				}
				return []domain.TableDetail{{Name: "t1", TableType: "MANAGED"}}, 1, nil
			},
		}
		provider := newTestProvider("lake", catalog)

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
					{SchemaID: "1", Name: "main", CatalogName: "lake"},
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
		provider := newTestProvider("lake", catalog)

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
					{SchemaID: "1", Name: "main", CatalogName: "lake"},
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
		provider := newTestProvider("lake", catalog)

		rows, _, err := provider.BuildColumnsRows(context.Background())

		require.NoError(t, err)
		require.Len(t, rows, 1, "should skip broken table and continue")
		assert.Equal(t, "col1", rows[0][3])
	})
}

func TestInformationSchema_ConcurrentQueries(t *testing.T) {
	catalog := &mockEngineCatalog{
		listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
			return []domain.SchemaDetail{
				{SchemaID: "1", Name: "main", CatalogName: "lake", Owner: "admin"},
			}, 1, nil
		},
		listTablesFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.TableDetail, int64, error) {
			return []domain.TableDetail{
				{Name: "orders", TableType: "MANAGED"},
				{Name: "users", TableType: "MANAGED"},
			}, 2, nil
		},
	}

	provider := newTestProvider("lake", catalog)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const goroutines = 10
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			rows, err := provider.HandleQuery(context.Background(), db, "admin", "SELECT * FROM information_schema.tables")
			if err != nil {
				errs <- err
				return
			}
			defer rows.Close() //nolint:errcheck

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				errs <- fmt.Errorf("rows iteration: %w", err)
				return
			}
			if count != 2 {
				errs <- fmt.Errorf("expected 2 rows, got %d", count)
				return
			}
			errs <- nil
		}()
	}

	for i := 0; i < goroutines; i++ {
		if err := <-errs; err != nil {
			t.Errorf("goroutine error: %v", err)
		}
	}
}

// === Mock AuthorizationService for RBAC filtering ===

type mockAuthzService struct {
	checkPrivilegeFn func(ctx context.Context, principalName, securableType, securableID, privilege string) (bool, error)
}

func (m *mockAuthzService) LookupTableID(_ context.Context, _ string) (string, string, bool, error) {
	return "", "", false, nil
}
func (m *mockAuthzService) CheckPrivilege(ctx context.Context, principalName, securableType, securableID, privilege string) (bool, error) {
	if m.checkPrivilegeFn != nil {
		return m.checkPrivilegeFn(ctx, principalName, securableType, securableID, privilege)
	}
	return false, nil
}
func (m *mockAuthzService) GetEffectiveRowFilters(_ context.Context, _ string, _ string) ([]string, error) {
	return nil, nil
}
func (m *mockAuthzService) GetEffectiveColumnMasks(_ context.Context, _ string, _ string) (map[string]string, error) {
	return nil, nil
}
func (m *mockAuthzService) GetTableColumnNames(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}

var _ domain.AuthorizationService = (*mockAuthzService)(nil)

// === Issue #38: RBAC filtering on information_schema ===

func TestHandleQuery_RBACFiltersSchemata(t *testing.T) {
	catalog := &mockEngineCatalog{
		listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
			return []domain.SchemaDetail{
				{SchemaID: "s1", Name: "public", CatalogName: "lake", Owner: "admin"},
				{SchemaID: "s2", Name: "secret", CatalogName: "lake", Owner: "admin"},
			}, 2, nil
		},
	}

	// Add GetSchema to the mock
	catalogWithGetSchema := &mockEngineCatalogWithGetSchema{
		mockEngineCatalog: catalog,
		getSchemaFn: func(_ context.Context, name string) (*domain.SchemaDetail, error) {
			switch name {
			case "public":
				return &domain.SchemaDetail{SchemaID: "s1", Name: "public", CatalogName: "lake"}, nil
			case "secret":
				return &domain.SchemaDetail{SchemaID: "s2", Name: "secret", CatalogName: "lake"}, nil
			}
			return nil, fmt.Errorf("not found")
		},
	}

	factory := &mockCatalogRepoFactory{
		catalogs: map[string]domain.CatalogRepository{"lake": catalogWithGetSchema},
	}
	lister := &mockCatalogLister{
		registrations: []domain.CatalogRegistration{
			{Name: "lake", Status: domain.CatalogStatusActive},
		},
	}
	authz := &mockAuthzService{
		checkPrivilegeFn: func(_ context.Context, principalName, securableType, securableID, privilege string) (bool, error) {
			// admin sees everything
			if principalName == "admin" {
				return true, nil
			}
			// analyst only has USAGE on schema s1 (public)
			if principalName == "analyst" && securableType == "schema" && securableID == "s1" {
				return true, nil
			}
			return false, nil
		},
	}

	provider := NewInformationSchemaProvider(factory, lister)
	provider.SetAuthorizationService(authz)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	t.Run("admin_sees_all_schemas", func(t *testing.T) {
		rows, err := provider.HandleQuery(context.Background(), db, "admin", "SELECT * FROM information_schema.schemata")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck

		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, 2, count, "admin should see all schemas")
	})

	t.Run("analyst_sees_only_public", func(t *testing.T) {
		rows, err := provider.HandleQuery(context.Background(), db, "analyst", "SELECT * FROM information_schema.schemata")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck

		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, 1, count, "analyst should see only public schema")
	})

	t.Run("no_access_sees_nothing", func(t *testing.T) {
		rows, err := provider.HandleQuery(context.Background(), db, "nobody", "SELECT * FROM information_schema.schemata")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck

		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, 0, count, "no_access user should see no schemas")
	})
}

func TestHandleQuery_RBACFiltersTables(t *testing.T) {
	catalog := &mockEngineCatalogWithGetTable{
		mockEngineCatalog: &mockEngineCatalog{
			listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return []domain.SchemaDetail{
					{SchemaID: "s1", Name: "main", CatalogName: "lake"},
				}, 1, nil
			},
			listTablesFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.TableDetail, int64, error) {
				return []domain.TableDetail{
					{TableID: "t1", Name: "public_table", TableType: "MANAGED"},
					{TableID: "t2", Name: "secret_table", TableType: "MANAGED"},
				}, 2, nil
			},
		},
		getTableFn: func(_ context.Context, _, tableName string) (*domain.TableDetail, error) {
			switch tableName {
			case "public_table":
				return &domain.TableDetail{TableID: "t1", Name: "public_table"}, nil
			case "secret_table":
				return &domain.TableDetail{TableID: "t2", Name: "secret_table"}, nil
			}
			return nil, fmt.Errorf("not found")
		},
	}

	factory := &mockCatalogRepoFactory{
		catalogs: map[string]domain.CatalogRepository{"lake": catalog},
	}
	lister := &mockCatalogLister{
		registrations: []domain.CatalogRegistration{
			{Name: "lake", Status: domain.CatalogStatusActive},
		},
	}
	authz := &mockAuthzService{
		checkPrivilegeFn: func(_ context.Context, principalName, securableType, securableID, _ string) (bool, error) {
			if principalName == "admin" {
				return true, nil
			}
			// analyst has SELECT on t1 only
			if principalName == "analyst" && securableType == "table" && securableID == "t1" {
				return true, nil
			}
			return false, nil
		},
	}

	provider := NewInformationSchemaProvider(factory, lister)
	provider.SetAuthorizationService(authz)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	t.Run("admin_sees_all_tables", func(t *testing.T) {
		rows, err := provider.HandleQuery(context.Background(), db, "admin", "SELECT * FROM information_schema.tables")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck

		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, 2, count)
	})

	t.Run("analyst_sees_only_public_table", func(t *testing.T) {
		rows, err := provider.HandleQuery(context.Background(), db, "analyst", "SELECT * FROM information_schema.tables")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck

		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, 1, count)
	})
}

// === Issue #39: SQL injection prevention ===

func TestHandleQuery_NoSQLInjection(t *testing.T) {
	// The fix for Issue #39 ensures user SQL is never executed against the temp table.
	// This test verifies that even if the user provides SQL that would be dangerous
	// if executed, only the materialized rows are returned.
	catalog := &mockEngineCatalog{
		listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
			return []domain.SchemaDetail{
				{SchemaID: "1", Name: "main", CatalogName: "lake", Owner: "admin"},
			}, 1, nil
		},
	}
	provider := newTestProvider("lake", catalog)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Previously this would have executed the user's SQL with string replacement.
	// Now it should just return the materialized rows regardless of the SQL content.
	rows, err := provider.HandleQuery(context.Background(), db, "admin", "SELECT read_parquet('evil.parquet') FROM information_schema.schemata")
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck

	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	// Should return the 1 schema row, not execute read_parquet
	assert.Equal(t, 1, count, "should return materialized rows, not execute user SQL")
}

// === Issue #41: Connection leak prevention ===

func TestHandleQuery_NoConnectionLeak(t *testing.T) {
	// This test verifies that calling HandleQuery and closing rows doesn't
	// accumulate pinned connections. We run many queries in sequence.
	catalog := &mockEngineCatalog{
		listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
			return []domain.SchemaDetail{
				{SchemaID: "1", Name: "main", CatalogName: "lake", Owner: "admin"},
			}, 1, nil
		},
	}
	provider := newTestProvider("lake", catalog)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Run many queries and close them. If there's a conn leak, we'll eventually
	// run out of connections or see errors.
	for i := 0; i < 100; i++ {
		rows, err := provider.HandleQuery(context.Background(), db, "admin", "SELECT * FROM information_schema.schemata")
		require.NoError(t, err, "query %d should not fail", i)

		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		defer rows.Close() //nolint:errcheck
		assert.Equal(t, 1, count, "query %d should return 1 row", i)
	}
}

func TestHandleQuery_EmptyResult(t *testing.T) {
	catalog := &mockEngineCatalog{
		listSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
			return []domain.SchemaDetail{}, 0, nil
		},
	}
	provider := newTestProvider("lake", catalog)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	rows, err := provider.HandleQuery(context.Background(), db, "admin", "SELECT * FROM information_schema.schemata")
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck

	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 0, count)
}

// === Extended mock types for RBAC tests ===

// mockEngineCatalogWithGetSchema extends mockEngineCatalog with GetSchema support.
type mockEngineCatalogWithGetSchema struct {
	*mockEngineCatalog
	getSchemaFn func(ctx context.Context, name string) (*domain.SchemaDetail, error)
}

func (m *mockEngineCatalogWithGetSchema) GetSchema(ctx context.Context, name string) (*domain.SchemaDetail, error) {
	if m.getSchemaFn != nil {
		return m.getSchemaFn(ctx, name)
	}
	panic("unexpected call to GetSchema")
}

// mockEngineCatalogWithGetTable extends mockEngineCatalog with GetTable support.
type mockEngineCatalogWithGetTable struct {
	*mockEngineCatalog
	getTableFn func(ctx context.Context, schemaName, tableName string) (*domain.TableDetail, error)
}

func (m *mockEngineCatalogWithGetTable) GetTable(ctx context.Context, schemaName, tableName string) (*domain.TableDetail, error) {
	if m.getTableFn != nil {
		return m.getTableFn(ctx, schemaName, tableName)
	}
	panic("unexpected call to GetTable")
}

// errInfoTest is a sentinel error for information_schema tests.
var errInfoTest = assert.AnError
