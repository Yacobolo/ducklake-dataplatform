package query

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
	"duck-demo/internal/sqlrewrite"
)

// Compile-time check that CatalogAdapter implements CatalogResolver.
var _ sqlrewrite.CatalogResolver = (*CatalogAdapter)(nil)

// CatalogAdapter wraps an IntrospectionRepository to implement
// sqlrewrite.CatalogResolver for column lineage analysis.
type CatalogAdapter struct {
	repo domain.IntrospectionRepository
}

// NewCatalogAdapter creates a new CatalogAdapter.
func NewCatalogAdapter(repo domain.IntrospectionRepository) *CatalogAdapter {
	return &CatalogAdapter{repo: repo}
}

// ResolveColumns returns the ordered column names for a given table.
// If the table is not found, an error is returned.
func (a *CatalogAdapter) ResolveColumns(ctx context.Context, schema, table string) ([]string, error) {
	if a.repo == nil {
		return nil, fmt.Errorf("introspection repository not available")
	}

	// Look up the table by name
	t, err := a.repo.GetTableByName(ctx, table)
	if err != nil {
		return nil, fmt.Errorf("resolve table %s.%s: %w", schema, table, err)
	}
	if t == nil {
		return nil, fmt.Errorf("table %s.%s not found", schema, table)
	}

	// Fetch columns (use a large page to get all columns)
	cols, _, err := a.repo.ListColumns(ctx, t.ID, domain.PageRequest{MaxResults: 1000})
	if err != nil {
		return nil, fmt.Errorf("list columns for %s.%s: %w", schema, table, err)
	}

	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names, nil
}
