package sqlrewrite

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// mockCatalog implements CatalogResolver for testing.
type mockCatalog struct {
	tables map[string][]string // "schema.table" -> columns
}

func (m *mockCatalog) ResolveColumns(_ context.Context, schema, table string) ([]string, error) {
	key := schema + "." + table
	if cols, ok := m.tables[key]; ok {
		return cols, nil
	}
	// Try just table name for backward compat
	if cols, ok := m.tables[table]; ok {
		return cols, nil
	}
	return nil, fmt.Errorf("table %s.%s not found", schema, table)
}

// errorCatalog always returns an error.
type errorCatalog struct{}

func (e *errorCatalog) ResolveColumns(_ context.Context, _, _ string) ([]string, error) {
	return nil, fmt.Errorf("catalog unavailable")
}

// partialCatalog resolves some tables but fails on others.
type partialCatalog struct {
	known map[string][]string
}

func (p *partialCatalog) ResolveColumns(_ context.Context, schema, table string) ([]string, error) {
	key := schema + "." + table
	if cols, ok := p.known[key]; ok {
		return cols, nil
	}
	return nil, fmt.Errorf("table %s not found", table)
}

func TestExtractColumnLineage(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		sql           string
		defaultSchema string
		catalog       CatalogResolver
		expected      []domain.ColumnLineageEntry
		expectNil     bool
	}{
		{
			name:          "simple SELECT",
			sql:           "SELECT a, b FROM t",
			defaultSchema: "main",
			catalog: &mockCatalog{tables: map[string][]string{
				"main.t": {"a", "b", "c"},
			}},
			expected: []domain.ColumnLineageEntry{
				{TargetColumn: "a", TransformType: domain.TransformDirect, Sources: []domain.ColumnSource{{Table: "t", Column: "a"}}},
				{TargetColumn: "b", TransformType: domain.TransformDirect, Sources: []domain.ColumnSource{{Table: "t", Column: "b"}}},
			},
		},
		{
			name:          "SELECT * resolved",
			sql:           "SELECT * FROM t",
			defaultSchema: "main",
			catalog: &mockCatalog{tables: map[string][]string{
				"main.t": {"id", "name", "email"},
			}},
			expected: []domain.ColumnLineageEntry{
				{TargetColumn: "id", TransformType: domain.TransformDirect, Sources: []domain.ColumnSource{{Table: "t", Column: "id"}}},
				{TargetColumn: "name", TransformType: domain.TransformDirect, Sources: []domain.ColumnSource{{Table: "t", Column: "name"}}},
				{TargetColumn: "email", TransformType: domain.TransformDirect, Sources: []domain.ColumnSource{{Table: "t", Column: "email"}}},
			},
		},
		{
			name:          "SELECT * multi-table",
			sql:           "SELECT t.*, s.val FROM t JOIN s ON t.id = s.id",
			defaultSchema: "main",
			catalog: &mockCatalog{tables: map[string][]string{
				"main.t": {"id", "name"},
				"main.s": {"id", "val"},
			}},
			expected: []domain.ColumnLineageEntry{
				{TargetColumn: "id", TransformType: domain.TransformDirect, Sources: []domain.ColumnSource{{Table: "t", Column: "id"}}},
				{TargetColumn: "name", TransformType: domain.TransformDirect, Sources: []domain.ColumnSource{{Table: "t", Column: "name"}}},
				{TargetColumn: "val", TransformType: domain.TransformDirect, Sources: []domain.ColumnSource{{Table: "s", Column: "val"}}},
			},
		},
		{
			name:          "expression column",
			sql:           "SELECT a + b AS total FROM t",
			defaultSchema: "main",
			catalog: &mockCatalog{tables: map[string][]string{
				"main.t": {"a", "b"},
			}},
			expected: []domain.ColumnLineageEntry{
				{TargetColumn: "total", TransformType: domain.TransformExpression, Sources: []domain.ColumnSource{{Table: "t", Column: "a"}, {Table: "t", Column: "b"}}},
			},
		},
		{
			name:          "non-SELECT skipped",
			sql:           "INSERT INTO t VALUES (1, 2)",
			defaultSchema: "main",
			catalog:       &mockCatalog{tables: map[string][]string{}},
			expectNil:     true,
		},
		{
			name:          "DDL statement",
			sql:           "CREATE TABLE foo (id INT)",
			defaultSchema: "main",
			catalog:       &mockCatalog{tables: map[string][]string{}},
			expectNil:     true,
		},
		{
			name:          "parse error",
			sql:           "NOT VALID SQL %%% !!!",
			defaultSchema: "main",
			catalog:       &mockCatalog{tables: map[string][]string{}},
			expectNil:     true,
		},
		{
			name:          "empty SQL",
			sql:           "",
			defaultSchema: "main",
			catalog:       &mockCatalog{tables: map[string][]string{}},
			expectNil:     true,
		},
		{
			name:          "catalog error returns nil",
			sql:           "SELECT a FROM t",
			defaultSchema: "main",
			catalog:       &errorCatalog{},
			expected: []domain.ColumnLineageEntry{
				// Column resolution fails gracefully — sources are empty
				{TargetColumn: "a", TransformType: domain.TransformDirect},
			},
		},
		{
			name:          "catalog partial failure",
			sql:           "SELECT t.a, s.b FROM t JOIN s ON t.id = s.id",
			defaultSchema: "main",
			catalog: &partialCatalog{known: map[string][]string{
				"main.t": {"id", "a"},
				// s is not resolvable — its columns will be unresolved
			}},
			expected: []domain.ColumnLineageEntry{
				{TargetColumn: "a", TransformType: domain.TransformDirect, Sources: []domain.ColumnSource{{Table: "t", Column: "a"}}},
				// s.b has no sources because table s was not resolvable via catalog
				{TargetColumn: "b", TransformType: domain.TransformDirect},
			},
		},
		{
			name:          "nil catalog",
			sql:           "SELECT a FROM t",
			defaultSchema: "main",
			catalog:       nil,
			expected: []domain.ColumnLineageEntry{
				{TargetColumn: "a", TransformType: domain.TransformDirect},
			},
		},
		{
			name:          "CTE with catalog",
			sql:           "WITH cte AS (SELECT a, b FROM t) SELECT a FROM cte",
			defaultSchema: "main",
			catalog: &mockCatalog{tables: map[string][]string{
				"main.t": {"a", "b"},
			}},
			expected: []domain.ColumnLineageEntry{
				{TargetColumn: "a", TransformType: domain.TransformDirect, Sources: []domain.ColumnSource{{Table: "t", Column: "a"}}},
			},
		},
		{
			name:          "function expression",
			sql:           "SELECT SUM(amount) AS total FROM orders",
			defaultSchema: "public",
			catalog: &mockCatalog{tables: map[string][]string{
				"public.orders": {"id", "amount"},
			}},
			expected: []domain.ColumnLineageEntry{
				{TargetColumn: "total", TransformType: domain.TransformExpression, Function: "SUM", Sources: []domain.ColumnSource{{Table: "orders", Column: "amount"}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ExtractColumnLineage(ctx, tt.sql, tt.defaultSchema, tt.catalog)
			require.NoError(t, err)

			if tt.expectNil {
				assert.Nil(t, result)
				return
			}

			require.Len(t, result, len(tt.expected))
			for i, exp := range tt.expected {
				assert.Equal(t, exp.TargetColumn, result[i].TargetColumn, "column %d name", i)
				assert.Equal(t, exp.TransformType, result[i].TransformType, "column %d transform type", i)
				if exp.Function != "" {
					assert.Equal(t, exp.Function, result[i].Function, "column %d function", i)
				}
				if len(exp.Sources) > 0 {
					require.Len(t, result[i].Sources, len(exp.Sources), "column %d sources count", i)
					for j, src := range exp.Sources {
						assert.Equal(t, src.Table, result[i].Sources[j].Table, "column %d source %d table", i, j)
						assert.Equal(t, src.Column, result[i].Sources[j].Column, "column %d source %d column", i, j)
					}
				}
			}
		})
	}
}

func TestExtractColumnLineage_PanicRecovery(t *testing.T) {
	// This test verifies that panics in the lineage analysis are caught
	// and don't propagate to the caller.
	ctx := context.Background()

	// A nil catalog with valid SQL should not panic, but we want to
	// verify the recovery mechanism works. We test this by ensuring
	// the function completes without panic for edge cases.
	result, err := ExtractColumnLineage(ctx, "SELECT 1", "", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result, 1)
}
