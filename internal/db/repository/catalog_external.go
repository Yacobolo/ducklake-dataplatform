package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/ddl"
	"duck-demo/internal/domain"
)

// CreateExternalTable creates an external table backed by a DuckDB VIEW.
func (r *CatalogRepo) CreateExternalTable(ctx context.Context, schemaName string, req domain.CreateTableRequest, owner string) (*domain.TableDetail, error) {
	if err := ddl.ValidateIdentifier(schemaName); err != nil {
		return nil, domain.ErrValidation("%s", err.Error())
	}
	if err := ddl.ValidateIdentifier(req.Name); err != nil {
		return nil, domain.ErrValidation("%s", err.Error())
	}
	if req.SourcePath == "" {
		return nil, domain.ErrValidation("source_path is required for external tables")
	}

	fileFormat := req.FileFormat
	if fileFormat == "" {
		fileFormat = "parquet"
	}

	// Discover columns if not provided
	columns := req.Columns
	if len(columns) == 0 {
		discovered, err := r.discoverColumns(ctx, req.SourcePath, fileFormat)
		if err != nil {
			return nil, fmt.Errorf("discover columns: %w", err)
		}
		columns = discovered
	}

	// Create the VIEW on DuckDB
	viewSQL, err := ddl.CreateExternalTableView(r.catalogName, schemaName, req.Name, req.SourcePath, fileFormat)
	if err != nil {
		return nil, domain.ErrValidation("%s", err.Error())
	}
	if _, err := r.duckDB.ExecContext(ctx, viewSQL); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "already exists") {
			return nil, domain.ErrConflict("table %q already exists in schema %q", req.Name, schemaName)
		}
		return nil, fmt.Errorf("create external table view: %w", err)
	}

	// Build domain columns for persistence
	domainCols := make([]domain.ExternalTableColumn, len(columns))
	for i, c := range columns {
		domainCols[i] = domain.ExternalTableColumn{
			ColumnName: c.Name,
			ColumnType: c.Type,
			Position:   i,
		}
	}

	// Persist metadata in SQLite
	et, err := r.extRepo.Create(ctx, &domain.ExternalTableRecord{
		SchemaName:   schemaName,
		TableName:    req.Name,
		FileFormat:   fileFormat,
		SourcePath:   req.SourcePath,
		LocationName: req.LocationName,
		Comment:      req.Comment,
		Owner:        owner,
		Columns:      domainCols,
	})
	if err != nil {
		// Best-effort: drop the VIEW we just created
		dropSQL, _ := ddl.DropView(r.catalogName, schemaName, req.Name)
		if dropSQL != "" {
			_, _ = r.duckDB.ExecContext(ctx, dropSQL)
		}
		return nil, err
	}

	// Store catalog metadata
	securableName := schemaName + "." + req.Name
	if req.Comment != "" || owner != "" {
		_ = r.q.InsertOrReplaceCatalogMetadata(ctx, dbstore.InsertOrReplaceCatalogMetadataParams{
			SecurableType: "table",
			SecurableName: securableName,
			Comment:       sql.NullString{String: req.Comment, Valid: req.Comment != ""},
			Owner:         sql.NullString{String: owner, Valid: owner != ""},
		})
	}

	return r.externalTableToDetail(et, schemaName), nil
}

// externalTableToDetail converts an ExternalTableRecord to a TableDetail.
func (r *CatalogRepo) externalTableToDetail(et *domain.ExternalTableRecord, schemaName string) *domain.TableDetail {
	cols := make([]domain.ColumnDetail, len(et.Columns))
	for i, c := range et.Columns {
		cols[i] = domain.ColumnDetail{
			Name:     c.ColumnName,
			Type:     c.ColumnType,
			Position: c.Position,
		}
	}
	return &domain.TableDetail{
		TableID:      et.ID,
		Name:         et.TableName,
		SchemaName:   schemaName,
		CatalogName:  r.catalogName,
		TableType:    domain.TableTypeExternal,
		Columns:      cols,
		Comment:      et.Comment,
		Owner:        et.Owner,
		CreatedAt:    et.CreatedAt,
		UpdatedAt:    et.UpdatedAt,
		SourcePath:   et.SourcePath,
		FileFormat:   et.FileFormat,
		LocationName: et.LocationName,
	}
}

// deleteExternalTable drops the VIEW and soft-deletes external table metadata.
func (r *CatalogRepo) deleteExternalTable(ctx context.Context, schemaName, tableName string, et *domain.ExternalTableRecord) error {
	// Drop the VIEW on DuckDB
	dropSQL, err := ddl.DropView(r.catalogName, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("build DDL: %w", err)
	}
	if _, err := r.duckDB.ExecContext(ctx, dropSQL); err != nil {
		return fmt.Errorf("drop external table view: %w", err)
	}

	// Soft-delete metadata
	if err := r.extRepo.Delete(ctx, schemaName, tableName); err != nil {
		return fmt.Errorf("soft-delete external table: %w", err)
	}

	// Soft-delete catalog metadata
	securableName := schemaName + "." + tableName
	_ = r.q.SoftDeleteCatalogMetadata(ctx, dbstore.SoftDeleteCatalogMetadataParams{
		SecurableType: "table",
		SecurableName: securableName,
	})

	// Cascade governance cleanup using effective table ID
	tableID := et.ID
	_ = r.q.DeleteRowFiltersByTable(ctx, tableID)
	_ = r.q.DeleteColumnMasksByTable(ctx, tableID)
	_ = r.q.DeleteTagAssignmentsBySecurableTypes(ctx, dbstore.DeleteTagAssignmentsBySecurableTypesParams{
		SecurableType:   "table",
		SecurableType_2: "column",
		SecurableID:     tableID,
	})
	_ = r.q.DeleteColumnMetadataByTable(ctx, securableName)
	_ = r.q.DeleteTableStatistics(ctx, securableName)
	_ = r.q.DeleteLineageByTable(ctx, dbstore.DeleteLineageByTableParams{
		SourceTable: securableName,
		TargetTable: sql.NullString{String: securableName, Valid: true},
	})

	return nil
}

// discoverColumns runs a DESCRIBE query on DuckDB to discover column metadata.
func (r *CatalogRepo) discoverColumns(ctx context.Context, sourcePath, fileFormat string) ([]domain.CreateColumnDef, error) {
	descSQL, err := ddl.DiscoverColumnsSQL(sourcePath, fileFormat)
	if err != nil {
		return nil, err
	}

	rows, err := r.duckDB.QueryContext(ctx, descSQL)
	if err != nil {
		return nil, fmt.Errorf("discover columns: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	var columns []domain.CreateColumnDef
	colNames, _ := rows.Columns()
	for rows.Next() {
		// DESCRIBE returns: column_name, column_type, null, key, default, extra
		vals := make([]interface{}, len(colNames))
		ptrs := make([]interface{}, len(colNames))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan describe row: %w", err)
		}
		// First two columns are column_name and column_type
		name := fmt.Sprintf("%v", vals[0])
		colType := fmt.Sprintf("%v", vals[1])
		columns = append(columns, domain.CreateColumnDef{Name: name, Type: colType})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns discovered from source")
	}
	return columns, nil
}
