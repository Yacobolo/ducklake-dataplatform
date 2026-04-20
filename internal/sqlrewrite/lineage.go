package sqlrewrite

import (
	"context"
	"fmt"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/duckdbsql"
)

// CatalogResolver provides column metadata for tables referenced in a query.
// It is used by ExtractColumnLineage to resolve SELECT * and perform accurate
// column lineage analysis.
type CatalogResolver interface {
	ResolveColumns(ctx context.Context, schema, table string) ([]string, error)
}

// ExtractColumnLineage parses a SQL statement, resolves table columns via the
// catalog, and returns column-level lineage entries for each output column.
// Only SELECT statements are analyzed; other types return nil, nil.
// This is best-effort: any failure returns nil, nil without error.
func ExtractColumnLineage(ctx context.Context, sql string, defaultSchema string, catalog CatalogResolver) (result []domain.ColumnLineageEntry, err error) {
	// Panic safety — never crash the query path
	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = fmt.Errorf("column lineage panic: %v", r)
		}
	}()

	if sql == "" {
		return nil, nil
	}

	stmt, err := duckdbsql.Parse(sql)
	if err != nil {
		return nil, nil
	}

	// Only analyze SELECT statements
	if duckdbsql.Classify(stmt) != duckdbsql.StmtTypeSelect {
		return nil, nil
	}

	// Collect table names and resolve columns via catalog
	schemaInfo := buildSchemaInfo(ctx, stmt, defaultSchema, catalog)

	// Analyze column lineage
	cols, err := duckdbsql.AnalyzeColumnLineage(stmt, schemaInfo)
	if err != nil {
		return nil, nil
	}

	// Convert AST-level types to domain types
	return convertToDomain(cols), nil
}

// buildSchemaInfo collects all table references from the AST and resolves their
// columns via the catalog. Tables that fail resolution are silently skipped.
func buildSchemaInfo(ctx context.Context, stmt duckdbsql.Stmt, defaultSchema string, catalog CatalogResolver) duckdbsql.SchemaInfo {
	if catalog == nil {
		return nil
	}

	refs := duckdbsql.CollectTableRefs(stmt)
	if len(refs) == 0 {
		return nil
	}

	info := make(duckdbsql.SchemaInfo, len(refs))
	for _, ref := range refs {
		// Skip internal sentinel entries (e.g., __func__read_csv)
		if strings.HasPrefix(ref.Name, "__func__") {
			continue
		}

		schema := defaultSchema
		if strings.TrimSpace(ref.Schema) != "" {
			schema = strings.TrimSpace(ref.Schema)
		}
		table := strings.TrimSpace(ref.Name)
		if table == "" {
			continue
		}

		cols, err := catalog.ResolveColumns(ctx, schema, table)
		if err != nil || len(cols) == 0 {
			continue
		}

		// Store with both keys so SchemaInfo lookup works for both
		// qualified and unqualified references
		info[strings.ToLower(table)] = cols
		if schema != "" {
			info[strings.ToLower(schema)+"."+strings.ToLower(table)] = cols
		}
	}

	if len(info) == 0 {
		return nil
	}
	return info
}

// convertToDomain maps AST-level column lineage to domain types.
func convertToDomain(cols []duckdbsql.ColumnLineage) []domain.ColumnLineageEntry {
	if len(cols) == 0 {
		return nil
	}

	entries := make([]domain.ColumnLineageEntry, len(cols))
	for i, col := range cols {
		var sources []domain.ColumnSource
		for _, src := range col.Sources {
			sources = append(sources, domain.ColumnSource{
				Schema: src.Schema,
				Table:  src.Table,
				Column: src.Column,
			})
		}
		entries[i] = domain.ColumnLineageEntry{
			TargetColumn:  col.Name,
			TransformType: domain.TransformType(col.TransformType),
			Function:      col.Function,
			Sources:       sources,
		}
	}
	return entries
}
