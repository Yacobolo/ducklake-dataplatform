package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/ddl"
	"duck-demo/internal/domain"
)

// CreateTable creates a new table via DuckDB DDL and reads it back.
func (r *CatalogRepo) CreateTable(ctx context.Context, schemaName string, req domain.CreateTableRequest, owner string) (*domain.TableDetail, error) {
	if err := ddl.ValidateIdentifier(schemaName); err != nil {
		return nil, domain.ErrValidation("%s", err.Error())
	}
	if err := ddl.ValidateIdentifier(req.Name); err != nil {
		return nil, domain.ErrValidation("%s", err.Error())
	}
	if len(req.Columns) == 0 {
		return nil, domain.ErrValidation("at least one column is required")
	}

	// Build column definitions via ddl package (validates names + types)
	cols := make([]ddl.ColumnDef, len(req.Columns))
	for i, c := range req.Columns {
		cols[i] = ddl.ColumnDef{Name: c.Name, Type: c.Type}
	}

	stmt, err := ddl.CreateTable(r.catalogName, schemaName, req.Name, cols)
	if err != nil {
		return nil, domain.ErrValidation("%s", err.Error())
	}
	if _, err := r.duckDB.ExecContext(ctx, stmt); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "already exists") {
			return nil, domain.ErrConflict("table %q already exists in schema %q", req.Name, schemaName)
		}
		if strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "does not exist") {
			return nil, domain.ErrNotFound("schema %q not found", schemaName)
		}
		return nil, fmt.Errorf("create table: %w", err)
	}
	r.refreshMetaDB(ctx)

	// Store metadata via sqlc
	securableName := schemaName + "." + req.Name
	if req.Comment != "" || owner != "" {
		_ = r.q.InsertOrReplaceCatalogMetadata(ctx, dbstore.InsertOrReplaceCatalogMetadataParams{
			SecurableType: "table",
			SecurableName: securableName,
			Comment:       sql.NullString{String: req.Comment, Valid: req.Comment != ""},
			Owner:         sql.NullString{String: owner, Valid: owner != ""},
		})
	}

	return r.GetTable(ctx, schemaName, req.Name)
}

// GetTable reads a table by schema and table name, including columns.
// NOTE: ducklake_schema and ducklake_table are not managed by sqlc.
func (r *CatalogRepo) GetTable(ctx context.Context, schemaName, tableName string) (*domain.TableDetail, error) {
	// First get the schema_id and schema path
	var schemaID int64
	var schemaPath sql.NullString
	err := r.metaDB.QueryRowContext(ctx,
		`SELECT schema_id, path FROM ducklake_schema WHERE schema_name = ? AND end_snapshot IS NULL`, schemaName).
		Scan(&schemaID, &schemaPath)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrNotFound("schema %q not found", schemaName)
	}
	if err != nil {
		return nil, err
	}

	var t domain.TableDetail
	var tableID int64
	var tablePath sql.NullString
	var tablePathIsRelative sql.NullInt64
	err = r.metaDB.QueryRowContext(ctx,
		`SELECT table_id, table_name, path, path_is_relative FROM ducklake_table WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL`,
		schemaID, tableName).
		Scan(&tableID, &t.Name, &tablePath, &tablePathIsRelative)
	if errors.Is(err, sql.ErrNoRows) {
		// Fall back to external tables
		if r.extRepo != nil {
			et, extErr := r.extRepo.GetByName(ctx, schemaName, tableName)
			if extErr == nil {
				detail := r.externalTableToDetail(et, schemaName)
				r.enrichTableMetadata(ctx, detail)
				return detail, nil
			}
		}
		return nil, domain.ErrNotFound("table %q not found in schema %q", tableName, schemaName)
	}
	if err != nil {
		return nil, err
	}
	t.TableID = domain.DuckLakeIDToString(tableID)

	t.SchemaName = schemaName
	t.CatalogName = r.catalogName
	t.TableType = "MANAGED"

	// Resolve storage path for MANAGED tables
	t.StoragePath = r.resolveStoragePath(ctx, schemaPath, tablePath, tablePathIsRelative)

	// Load columns (ducklake_column — not managed by sqlc)
	cols, err := r.loadColumns(ctx, t.TableID)
	if err != nil {
		return nil, err
	}
	t.Columns = cols

	// Enrich columns with metadata via sqlc
	securableName := schemaName + "." + tableName
	for i := range t.Columns {
		r.enrichColumnMetadata(ctx, securableName, &t.Columns[i])
	}

	// Join with catalog_metadata via sqlc
	r.enrichTableMetadata(ctx, &t)

	return &t, nil
}

// ListTables returns a paginated list of tables in a schema.
// NOTE: ducklake_schema and ducklake_table are not managed by sqlc.
func (r *CatalogRepo) ListTables(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.TableDetail, int64, error) {
	// First get the schema_id
	schemaID, err := r.resolveSchemaID(ctx, schemaName)
	if err != nil {
		return nil, 0, err
	}

	var total int64
	if err := r.metaDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ducklake_table WHERE schema_id = ? AND end_snapshot IS NULL`, schemaID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.metaDB.QueryContext(ctx,
		`SELECT table_id, table_name FROM ducklake_table WHERE schema_id = ? AND end_snapshot IS NULL ORDER BY table_name LIMIT ? OFFSET ?`,
		schemaID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close() //nolint:errcheck

	var tables []domain.TableDetail
	for rows.Next() {
		var t domain.TableDetail
		var tblID int64
		if err := rows.Scan(&tblID, &t.Name); err != nil {
			return nil, 0, err
		}
		t.TableID = domain.DuckLakeIDToString(tblID)
		t.SchemaName = schemaName
		t.CatalogName = r.catalogName
		t.TableType = "MANAGED"
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	for i := range tables {
		r.enrichTableMetadata(ctx, &tables[i])
	}

	// Append external tables (v1: simple append, not merged pagination)
	if r.extRepo != nil {
		extTables, extTotal, extErr := r.extRepo.List(ctx, schemaName, page)
		if extErr == nil {
			for _, et := range extTables {
				detail := r.externalTableToDetail(&et, schemaName)
				r.enrichTableMetadata(ctx, detail)
				tables = append(tables, *detail)
			}
			total += extTotal
		}
	}

	return tables, total, nil
}

// DeleteTable drops a table via DuckDB DDL and cascades governance cleanup.
func (r *CatalogRepo) DeleteTable(ctx context.Context, schemaName, tableName string) error {
	if err := ddl.ValidateIdentifier(schemaName); err != nil {
		return domain.ErrValidation("%s", err.Error())
	}
	if err := ddl.ValidateIdentifier(tableName); err != nil {
		return domain.ErrValidation("%s", err.Error())
	}

	// Check if this is an external table first
	if r.extRepo != nil {
		et, extErr := r.extRepo.GetByName(ctx, schemaName, tableName)
		if extErr == nil {
			return r.deleteExternalTable(ctx, schemaName, tableName, et)
		}
	}

	// Verify table exists and capture table ID for governance cleanup
	tbl, err := r.GetTable(ctx, schemaName, tableName)
	if err != nil {
		return err
	}

	stmt, err := ddl.DropTable(r.catalogName, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("build DDL: %w", err)
	}
	if _, err := r.duckDB.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}
	r.refreshMetaDB(ctx)

	// Run all cascade cleanup in a transaction to maintain consistency.
	tx, err := r.metaDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin cascade cleanup tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	qtx := r.q.WithTx(tx)
	securableName := schemaName + "." + tableName

	// Soft-delete table metadata
	if err := qtx.SoftDeleteCatalogMetadata(ctx, dbstore.SoftDeleteCatalogMetadataParams{
		SecurableType: "table",
		SecurableName: securableName,
	}); err != nil {
		r.logger.Warn("cascade cleanup: soft-delete table metadata", "table", securableName, "error", err)
	}

	// Cascade: remove row filters, column masks, and tag assignments
	r.cascadeDeleteTableGrants(ctx, qtx, tbl.TableID, "table")

	// Cascade: remove column metadata
	if err := qtx.DeleteColumnMetadataByTable(ctx, securableName); err != nil {
		r.logger.Warn("cascade cleanup: delete column metadata", "table", securableName, "error", err)
	}

	// Cascade: remove table statistics
	if err := qtx.DeleteTableStatistics(ctx, securableName); err != nil {
		r.logger.Warn("cascade cleanup: delete table statistics", "table", securableName, "error", err)
	}

	// Cascade: remove lineage edges referencing this table
	qualifiedName := schemaName + "." + tableName
	if err := qtx.DeleteLineageByTable(ctx, dbstore.DeleteLineageByTableParams{
		SourceTable: qualifiedName,
		TargetTable: sql.NullString{String: qualifiedName, Valid: true},
	}); err != nil {
		r.logger.Warn("cascade cleanup: delete lineage", "table", securableName, "error", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit cascade cleanup: %w", err)
	}

	return nil
}

// ListColumns returns a paginated list of columns for a table.
// NOTE: ducklake_schema, ducklake_table, ducklake_column are not managed by sqlc.
func (r *CatalogRepo) ListColumns(ctx context.Context, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, int64, error) {
	// First resolve schema_id
	schemaID, err := r.resolveSchemaID(ctx, schemaName)
	if err != nil {
		return nil, 0, err
	}

	var tableID int64
	err = r.metaDB.QueryRowContext(ctx,
		`SELECT table_id FROM ducklake_table WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL`,
		schemaID, tableName).Scan(&tableID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, domain.ErrNotFound("table %q not found in schema %q", tableName, schemaName)
	}
	if err != nil {
		return nil, 0, err
	}

	var total int64
	if err := r.metaDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ducklake_column WHERE table_id = ? AND end_snapshot IS NULL`, tableID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.metaDB.QueryContext(ctx,
		`SELECT column_name, column_type, column_id, COALESCE(nulls_allowed, 1) FROM ducklake_column WHERE table_id = ? AND end_snapshot IS NULL ORDER BY column_id LIMIT ? OFFSET ?`,
		tableID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close() //nolint:errcheck

	var columns []domain.ColumnDetail
	pos := page.Offset()
	for rows.Next() {
		var c domain.ColumnDetail
		var colID int64
		var nullsAllowed int64
		if err := rows.Scan(&c.Name, &c.Type, &colID, &nullsAllowed); err != nil {
			return nil, 0, err
		}
		c.Position = pos
		c.Nullable = nullsAllowed != 0
		pos++
		columns = append(columns, c)
	}
	securableName := schemaName + "." + tableName
	for i := range columns {
		r.enrichColumnMetadata(ctx, securableName, &columns[i])
	}
	return columns, total, rows.Err()
}

// UpdateTable updates table metadata (comment, properties, owner).
func (r *CatalogRepo) UpdateTable(ctx context.Context, schemaName, tableName string, comment *string, props map[string]string, owner *string) (*domain.TableDetail, error) {
	// Verify table exists
	_, err := r.GetTable(ctx, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	securableName := schemaName + "." + tableName
	var propsJSON sql.NullString
	if props != nil {
		b, _ := json.Marshal(props)
		propsJSON = sql.NullString{String: string(b), Valid: true}
	}

	err = r.q.UpsertCatalogMetadata(ctx, dbstore.UpsertCatalogMetadataParams{
		SecurableType: "table",
		SecurableName: securableName,
		Comment:       sql.NullString{String: ptrToStr(comment), Valid: comment != nil},
		Properties:    propsJSON,
		Owner:         sql.NullString{String: ptrToStr(owner), Valid: owner != nil},
	})
	if err != nil {
		return nil, fmt.Errorf("update table metadata: %w", err)
	}

	return r.GetTable(ctx, schemaName, tableName)
}

// UpdateCatalog updates catalog-level metadata (comment).
func (r *CatalogRepo) UpdateCatalog(ctx context.Context, comment *string) (*domain.CatalogInfo, error) {
	err := r.q.UpsertCatalogMetadata(ctx, dbstore.UpsertCatalogMetadataParams{
		SecurableType: "catalog",
		SecurableName: r.catalogName,
		Comment:       sql.NullString{String: ptrToStr(comment), Valid: comment != nil},
		Properties:    sql.NullString{},
		Owner:         sql.NullString{},
	})
	if err != nil {
		return nil, fmt.Errorf("update catalog metadata: %w", err)
	}
	return r.GetCatalogInfo(ctx)
}

// UpdateColumn updates column metadata (comment, properties).
func (r *CatalogRepo) UpdateColumn(ctx context.Context, schemaName, tableName, columnName string, comment *string, props map[string]string) (*domain.ColumnDetail, error) {
	// Verify table exists
	tbl, err := r.GetTable(ctx, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	// Verify column exists
	found := false
	for _, c := range tbl.Columns {
		if c.Name == columnName {
			found = true
			break
		}
	}
	if !found {
		return nil, domain.ErrNotFound("column %q not found in table %q.%q", columnName, schemaName, tableName)
	}

	securableName := schemaName + "." + tableName
	var propsJSON sql.NullString
	if props != nil {
		b, _ := json.Marshal(props)
		propsJSON = sql.NullString{String: string(b), Valid: true}
	}

	err = r.q.UpsertColumnMetadata(ctx, dbstore.UpsertColumnMetadataParams{
		TableSecurableName: securableName,
		ColumnName:         columnName,
		Comment:            sql.NullString{String: ptrToStr(comment), Valid: comment != nil},
		Properties:         propsJSON,
	})
	if err != nil {
		return nil, fmt.Errorf("update column metadata: %w", err)
	}

	// Return the updated column
	for _, c := range tbl.Columns {
		if c.Name == columnName {
			r.enrichColumnMetadata(ctx, securableName, &c)
			return &c, nil
		}
	}
	return nil, domain.ErrNotFound("column %q not found", columnName)
}
