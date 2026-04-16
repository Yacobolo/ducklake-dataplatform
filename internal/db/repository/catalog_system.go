package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
)

func (r *CatalogRepo) getSystemSchema(ctx context.Context, name string) (*domain.SchemaDetail, error) {
	switch {
	case domain.IsAppSystemSchema(name):
		if r.controlDB == nil {
			return nil, domain.ErrNotFound("schema %q not found", name)
		}
		return systemSchemaDetail(domain.AppSystemSchemaName, r.catalogName), nil
	case domain.IsDuckLakeSystemSchema(r.catalogName, name):
		return r.getDuckLakeSystemSchema(ctx, name)
	default:
		return nil, domain.ErrNotFound("schema %q not found", name)
	}
}

func (r *CatalogRepo) getDuckLakeSystemSchema(ctx context.Context, name string) (*domain.SchemaDetail, error) {
	if r.duckDB == nil {
		return nil, domain.ErrNotFound("schema %q not found", name)
	}

	var schemaName string
	err := r.duckDB.QueryRowContext(ctx,
		`SELECT schema_name
		 FROM information_schema.schemata
		 WHERE catalog_name = ? AND lower(schema_name) = lower(?)`,
		r.catalogName, name,
	).Scan(&schemaName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrNotFound("schema %q not found", name)
	}
	if err != nil {
		return nil, fmt.Errorf("lookup system schema %q: %w", name, err)
	}

	return systemSchemaDetail(schemaName, r.catalogName), nil
}

func (r *CatalogRepo) listSystemSchemas(ctx context.Context) ([]domain.SchemaDetail, error) {
	schemas := make([]domain.SchemaDetail, 0, 2)

	if r.controlDB != nil {
		schemas = append(schemas, *systemSchemaDetail(domain.AppSystemSchemaName, r.catalogName))
	}

	if r.duckDB != nil {
		rows, err := r.duckDB.QueryContext(ctx,
			`SELECT schema_name
			 FROM information_schema.schemata
			 WHERE catalog_name = ? AND lower(schema_name) = lower(?)
			 ORDER BY schema_name`,
			r.catalogName, domain.DuckLakeMetadataSchemaName(r.catalogName),
		)
		if err != nil {
			return nil, fmt.Errorf("list system schemas for catalog %q: %w", r.catalogName, err)
		}
		defer rows.Close() //nolint:errcheck

		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, err
			}
			schemas = append(schemas, *systemSchemaDetail(name, r.catalogName))
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	sort.Slice(schemas, func(i, j int) bool {
		return strings.ToLower(schemas[i].Name) < strings.ToLower(schemas[j].Name)
	})
	return schemas, nil
}

func systemSchemaDetail(schemaName, catalogName string) *domain.SchemaDetail {
	return &domain.SchemaDetail{
		SchemaID:    domain.SystemSchemaObjectID(schemaName),
		Name:        schemaName,
		CatalogName: catalogName,
		Owner:       domain.SystemPrincipalName,
		Properties: map[string]string{
			"read_only":     "true",
			"system_schema": "true",
		},
	}
}

func (r *CatalogRepo) getSystemTable(ctx context.Context, schemaName, tableName string) (*domain.TableDetail, error) {
	switch {
	case domain.IsAppSystemSchema(schemaName):
		return r.getAppSystemTable(ctx, schemaName, tableName)
	case domain.IsDuckLakeSystemSchema(r.catalogName, schemaName):
		return r.getDuckLakeSystemTable(ctx, schemaName, tableName)
	default:
		return nil, domain.ErrNotFound("table %q not found in schema %q", tableName, schemaName)
	}
}

func (r *CatalogRepo) getDuckLakeSystemTable(ctx context.Context, schemaName, tableName string) (*domain.TableDetail, error) {
	if r.duckDB == nil {
		return nil, domain.ErrNotFound("table %q not found in schema %q", tableName, schemaName)
	}

	var name string
	err := r.duckDB.QueryRowContext(ctx,
		`SELECT table_name
		 FROM information_schema.tables
		 WHERE table_catalog = ? AND table_schema = ? AND lower(table_name) = lower(?) AND table_type = 'BASE TABLE'`,
		r.catalogName, schemaName, tableName,
	).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrNotFound("table %q not found in schema %q", tableName, schemaName)
	}
	if err != nil {
		return nil, fmt.Errorf("lookup system table %q.%q: %w", schemaName, tableName, err)
	}

	columns, err := r.listDuckLakeSystemColumns(ctx, schemaName, name, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return nil, err
	}

	return systemTableDetail(schemaName, name, r.catalogName, columns), nil
}

func (r *CatalogRepo) listSystemTables(ctx context.Context, schemaName string) ([]domain.TableDetail, error) {
	switch {
	case domain.IsAppSystemSchema(schemaName):
		return r.listAppSystemTables(ctx, schemaName)
	case domain.IsDuckLakeSystemSchema(r.catalogName, schemaName):
		return r.listDuckLakeSystemTables(ctx, schemaName)
	default:
		return nil, nil
	}
}

func (r *CatalogRepo) listDuckLakeSystemTables(ctx context.Context, schemaName string) ([]domain.TableDetail, error) {
	if r.duckDB == nil {
		return nil, nil
	}

	rows, err := r.duckDB.QueryContext(ctx,
		`SELECT table_name
		 FROM information_schema.tables
		 WHERE table_catalog = ? AND table_schema = ? AND table_type = 'BASE TABLE'
		 ORDER BY table_name`,
		r.catalogName, schemaName,
	)
	if err != nil {
		return nil, fmt.Errorf("list system tables for %q.%q: %w", r.catalogName, schemaName, err)
	}
	defer rows.Close() //nolint:errcheck

	var tables []domain.TableDetail
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns, err := r.listDuckLakeSystemColumns(ctx, schemaName, name, domain.PageRequest{MaxResults: domain.MaxMaxResults})
		if err != nil {
			return nil, err
		}
		tables = append(tables, *systemTableDetail(schemaName, name, r.catalogName, columns))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func (r *CatalogRepo) listSystemColumns(ctx context.Context, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, error) {
	switch {
	case domain.IsAppSystemSchema(schemaName):
		return r.listAppSystemColumns(ctx, schemaName, tableName, page)
	case domain.IsDuckLakeSystemSchema(r.catalogName, schemaName):
		return r.listDuckLakeSystemColumns(ctx, schemaName, tableName, page)
	default:
		return nil, domain.ErrNotFound("table %q not found in schema %q", tableName, schemaName)
	}
}

func (r *CatalogRepo) listDuckLakeSystemColumns(ctx context.Context, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, error) {
	if r.duckDB == nil {
		return nil, domain.ErrNotFound("table %q not found in schema %q", tableName, schemaName)
	}

	rows, err := r.duckDB.QueryContext(ctx,
		`SELECT column_name, data_type, ordinal_position, is_nullable
		 FROM information_schema.columns
		 WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
		 ORDER BY ordinal_position`,
		r.catalogName, schemaName, tableName,
	)
	if err != nil {
		return nil, fmt.Errorf("list system columns for %q.%q: %w", schemaName, tableName, err)
	}
	defer rows.Close() //nolint:errcheck

	var columns []domain.ColumnDetail
	for rows.Next() {
		var (
			name       string
			dataType   string
			position   int
			isNullable string
		)
		if err := rows.Scan(&name, &dataType, &position, &isNullable); err != nil {
			return nil, err
		}
		columns = append(columns, domain.ColumnDetail{
			Name:     name,
			Type:     dataType,
			Position: position - 1,
			Nullable: strings.EqualFold(isNullable, "YES"),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	offset := page.Offset()
	if offset >= len(columns) {
		return []domain.ColumnDetail{}, nil
	}
	end := offset + page.Limit()
	if end > len(columns) {
		end = len(columns)
	}
	return columns[offset:end], nil
}

func (r *CatalogRepo) getAppSystemTable(ctx context.Context, schemaName, tableName string) (*domain.TableDetail, error) {
	name, err := r.lookupAppSystemTableName(ctx, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	columns, err := r.listAppSystemColumns(ctx, schemaName, name, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return nil, err
	}
	return systemTableDetail(schemaName, name, r.catalogName, columns), nil
}

func (r *CatalogRepo) listAppSystemTables(ctx context.Context, schemaName string) ([]domain.TableDetail, error) {
	if r.controlDB == nil {
		return nil, nil
	}

	rows, err := r.controlDB.QueryContext(ctx,
		`SELECT name
		 FROM sqlite_master
		 WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
		 ORDER BY name`,
	)
	if err != nil {
		return nil, fmt.Errorf("list app system tables: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	var tableNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	tables := make([]domain.TableDetail, 0, len(tableNames))
	for _, name := range tableNames {
		columns, err := r.listAppSystemColumns(ctx, schemaName, name, domain.PageRequest{MaxResults: domain.MaxMaxResults})
		if err != nil {
			return nil, err
		}
		tables = append(tables, *systemTableDetail(schemaName, name, r.catalogName, columns))
	}
	return tables, nil
}

func (r *CatalogRepo) listAppSystemColumns(ctx context.Context, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, error) {
	name, err := r.lookupAppSystemTableName(ctx, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	rows, err := r.controlDB.QueryContext(ctx,
		`SELECT name, type, cid, "notnull"
		 FROM pragma_table_info(?)
		 ORDER BY cid`,
		name,
	)
	if err != nil {
		return nil, fmt.Errorf("list app system columns for %q.%q: %w", schemaName, tableName, err)
	}
	defer rows.Close() //nolint:errcheck

	var columns []domain.ColumnDetail
	for rows.Next() {
		var (
			name     string
			dataType string
			position int
			notNull  int
		)
		if err := rows.Scan(&name, &dataType, &position, &notNull); err != nil {
			return nil, err
		}
		columns = append(columns, domain.ColumnDetail{
			Name:     name,
			Type:     dataType,
			Position: position,
			Nullable: notNull == 0,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	offset := page.Offset()
	if offset >= len(columns) {
		return []domain.ColumnDetail{}, nil
	}
	end := offset + page.Limit()
	if end > len(columns) {
		end = len(columns)
	}
	return columns[offset:end], nil
}

func (r *CatalogRepo) lookupAppSystemTableName(ctx context.Context, schemaName, tableName string) (string, error) {
	if r.controlDB == nil {
		return "", domain.ErrNotFound("table %q not found in schema %q", tableName, schemaName)
	}

	var name string
	err := r.controlDB.QueryRowContext(ctx,
		`SELECT name
		 FROM sqlite_master
		 WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND lower(name) = lower(?)`,
		tableName,
	).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", domain.ErrNotFound("table %q not found in schema %q", tableName, schemaName)
	}
	if err != nil {
		return "", fmt.Errorf("lookup app system table %q.%q: %w", schemaName, tableName, err)
	}
	return name, nil
}

func systemTableDetail(schemaName, tableName, catalogName string, columns []domain.ColumnDetail) *domain.TableDetail {
	return &domain.TableDetail{
		TableID:     domain.SystemTableObjectID(schemaName, tableName),
		Name:        tableName,
		SchemaName:  schemaName,
		CatalogName: catalogName,
		TableType:   domain.TableTypeSystem,
		Columns:     columns,
		Owner:       domain.SystemPrincipalName,
		Properties: map[string]string{
			"read_only":    "true",
			"system_table": "true",
		},
	}
}

func mergeSchemas(managed, system []domain.SchemaDetail) []domain.SchemaDetail {
	if len(system) == 0 {
		return managed
	}

	seen := make(map[string]struct{}, len(managed))
	for i := range managed {
		seen[strings.ToLower(managed[i].Name)] = struct{}{}
	}
	for i := range system {
		if _, ok := seen[strings.ToLower(system[i].Name)]; ok {
			continue
		}
		managed = append(managed, system[i])
	}
	sort.Slice(managed, func(i, j int) bool {
		return strings.ToLower(managed[i].Name) < strings.ToLower(managed[j].Name)
	})
	return managed
}
