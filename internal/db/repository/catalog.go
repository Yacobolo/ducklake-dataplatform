package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/ddl"
	"duck-demo/internal/domain"
)

// CatalogRepo implements domain.CatalogRepository using the DuckLake SQLite
// metastore (metaDB) for reads and the DuckDB connection (duckDB) for DDL.
type CatalogRepo struct {
	metaDB  *sql.DB
	duckDB  *sql.DB
	q       *dbstore.Queries // sqlc queries for application-owned tables
	extRepo *ExternalTableRepo
	logger  *slog.Logger
}

// NewCatalogRepo creates a new CatalogRepo.
func NewCatalogRepo(metaDB, duckDB *sql.DB, extRepo *ExternalTableRepo, logger *slog.Logger) *CatalogRepo {
	return &CatalogRepo{metaDB: metaDB, duckDB: duckDB, q: dbstore.New(metaDB), extRepo: extRepo, logger: logger}
}

// refreshMetaDB forces metaDB to see the latest WAL changes written by
// DuckLake's internal SQLite handle after DDL operations (CREATE/DROP
// SCHEMA/TABLE). Without this, metaDB may read a stale WAL snapshot and
// miss rows that DuckLake just inserted or updated.
//
// The approach: cycle the pool's idle connections so the next query opens a
// fresh SQLite read snapshot that includes the DuckLake WAL entries.
func (r *CatalogRepo) refreshMetaDB(_ context.Context) {
	cur := r.metaDB.Stats().MaxOpenConnections
	r.metaDB.SetMaxIdleConns(0)
	if cur > 0 {
		r.metaDB.SetMaxIdleConns(cur)
	} else {
		r.metaDB.SetMaxIdleConns(2) // Go default
	}
}

// GetCatalogInfo returns information about the single "lake" catalog.
func (r *CatalogRepo) GetCatalogInfo(ctx context.Context) (*domain.CatalogInfo, error) {
	info := &domain.CatalogInfo{
		Name: "lake",
	}

	// Try to read comment from catalog_metadata via sqlc
	row, err := r.q.GetCatalogMetadata(ctx, dbstore.GetCatalogMetadataParams{
		SecurableType: "catalog",
		SecurableName: "lake",
	})
	if err == nil {
		if row.Comment.Valid {
			info.Comment = row.Comment.String
		}
		info.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", row.CreatedAt)
		info.UpdatedAt, _ = time.Parse("2006-01-02 15:04:05", row.UpdatedAt)
	}

	return info, nil
}

// GetMetastoreSummary returns high-level info about the DuckLake metastore.
// NOTE: These queries hit ducklake_* tables (not managed by sqlc).
func (r *CatalogRepo) GetMetastoreSummary(ctx context.Context) (*domain.MetastoreSummary, error) {
	summary := &domain.MetastoreSummary{
		CatalogName:    "lake",
		MetastoreType:  "DuckLake (SQLite)",
		StorageBackend: "S3",
	}

	// Read data_path (ducklake_metadata — not managed by sqlc)
	var dataPath sql.NullString
	_ = r.metaDB.QueryRowContext(ctx,
		`SELECT value FROM ducklake_metadata WHERE key = 'data_path'`).Scan(&dataPath)
	if dataPath.Valid {
		summary.DataPath = dataPath.String
	}

	// Count schemas (ducklake_schema — not managed by sqlc)
	_ = r.metaDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ducklake_schema WHERE end_snapshot IS NULL`).Scan(&summary.SchemaCount)

	// Count tables (ducklake_table — not managed by sqlc)
	_ = r.metaDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ducklake_table WHERE end_snapshot IS NULL`).Scan(&summary.TableCount)

	return summary, nil
}

// CreateSchema creates a new schema via DuckDB DDL and reads it back.
func (r *CatalogRepo) CreateSchema(ctx context.Context, name, comment, owner string) (*domain.SchemaDetail, error) {
	if err := ddl.ValidateIdentifier(name); err != nil {
		return nil, domain.ErrValidation("%s", err.Error())
	}

	// Check if schema already exists (DuckDB silently succeeds on duplicate CREATE SCHEMA)
	if _, err := r.GetSchema(ctx, name); err == nil {
		return nil, domain.ErrConflict("schema %q already exists", name)
	}

	stmt, err := ddl.CreateSchema(name)
	if err != nil {
		return nil, fmt.Errorf("build DDL: %w", err)
	}
	if _, err := r.duckDB.ExecContext(ctx, stmt); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "already exists") {
			return nil, domain.ErrConflict("schema %q already exists", name)
		}
		return nil, fmt.Errorf("create schema: %w", err)
	}
	r.refreshMetaDB(ctx)

	// Store metadata via sqlc
	if comment != "" || owner != "" {
		_ = r.q.InsertOrReplaceCatalogMetadata(ctx, dbstore.InsertOrReplaceCatalogMetadataParams{
			SecurableType: "schema",
			SecurableName: name,
			Comment:       sql.NullString{String: comment, Valid: comment != ""},
			Owner:         sql.NullString{String: owner, Valid: owner != ""},
		})
	}

	return r.GetSchema(ctx, name)
}

// GetSchema reads a schema by name from the DuckLake metastore.
// NOTE: ducklake_schema is not managed by sqlc.
func (r *CatalogRepo) GetSchema(ctx context.Context, name string) (*domain.SchemaDetail, error) {
	var s domain.SchemaDetail
	err := r.metaDB.QueryRowContext(ctx,
		`SELECT schema_id, schema_name FROM ducklake_schema WHERE schema_name = ? AND end_snapshot IS NULL`, name).
		Scan(&s.SchemaID, &s.Name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrNotFound("schema %q not found", name)
	}
	if err != nil {
		return nil, err
	}
	s.CatalogName = "lake"

	// Join with catalog_metadata via sqlc
	r.enrichSchemaMetadata(ctx, &s)

	return &s, nil
}

// ListSchemas returns a paginated list of schemas.
// NOTE: ducklake_schema is not managed by sqlc.
func (r *CatalogRepo) ListSchemas(ctx context.Context, page domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
	var total int64
	if err := r.metaDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ducklake_schema WHERE end_snapshot IS NULL`).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.metaDB.QueryContext(ctx,
		`SELECT schema_id, schema_name FROM ducklake_schema WHERE end_snapshot IS NULL ORDER BY schema_name LIMIT ? OFFSET ?`,
		page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close() //nolint:errcheck

	var schemas []domain.SchemaDetail
	for rows.Next() {
		var s domain.SchemaDetail
		if err := rows.Scan(&s.SchemaID, &s.Name); err != nil {
			return nil, 0, err
		}
		s.CatalogName = "lake"
		schemas = append(schemas, s)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	for i := range schemas {
		r.enrichSchemaMetadata(ctx, &schemas[i])
	}
	return schemas, total, nil
}

// UpdateSchema updates schema metadata (comment, properties).
func (r *CatalogRepo) UpdateSchema(ctx context.Context, name string, comment *string, props map[string]string) (*domain.SchemaDetail, error) {
	// Verify schema exists
	_, err := r.GetSchema(ctx, name)
	if err != nil {
		return nil, err
	}

	var propsJSON sql.NullString
	if props != nil {
		b, _ := json.Marshal(props)
		propsJSON = sql.NullString{String: string(b), Valid: true}
	}

	err = r.q.UpsertCatalogMetadata(ctx, dbstore.UpsertCatalogMetadataParams{
		SecurableType: "schema",
		SecurableName: name,
		Comment:       sql.NullString{String: ptrToStr(comment), Valid: comment != nil},
		Properties:    propsJSON,
		Owner:         sql.NullString{},
	})
	if err != nil {
		return nil, fmt.Errorf("update schema metadata: %w", err)
	}

	return r.GetSchema(ctx, name)
}

// DeleteSchema drops a schema via DuckDB DDL and cascades governance cleanup.
func (r *CatalogRepo) DeleteSchema(ctx context.Context, name string, force bool) error {
	if err := ddl.ValidateIdentifier(name); err != nil {
		return domain.ErrValidation("%s", err.Error())
	}

	// Verify schema exists and capture schema ID
	schema, err := r.GetSchema(ctx, name)
	if err != nil {
		return err
	}

	// If force, gather table IDs in this schema for governance cleanup before DDL
	// NOTE: ducklake_table is not managed by sqlc
	var tableIDs []int64
	if force {
		rows, err := r.metaDB.QueryContext(ctx,
			`SELECT table_id FROM ducklake_table WHERE schema_id = ? AND end_snapshot IS NULL`, schema.SchemaID)
		if err == nil {
			defer rows.Close() //nolint:errcheck
			for rows.Next() {
				var tid int64
				if err := rows.Scan(&tid); err == nil {
					tableIDs = append(tableIDs, tid)
				}
			}
			_ = rows.Err() // best-effort scan for cascade cleanup
		}
	}

	// Drop external table VIEWs and soft-delete their metadata before schema DDL
	if r.extRepo != nil {
		extTables, extErr := r.extRepo.ListAll(ctx)
		if extErr == nil {
			for _, et := range extTables {
				if et.SchemaName == name {
					dropViewSQL, _ := ddl.DropView(et.SchemaName, et.TableName)
					if dropViewSQL != "" {
						_, _ = r.duckDB.ExecContext(ctx, dropViewSQL)
					}
				}
			}
		}
		_ = r.extRepo.DeleteBySchema(ctx, name)
	}

	stmt, err := ddl.DropSchema(name, force)
	if err != nil {
		return fmt.Errorf("build DDL: %w", err)
	}

	if _, err := r.duckDB.ExecContext(ctx, stmt); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "not empty") || strings.Contains(errMsg, "depends on") {
			return domain.ErrConflict("schema %q is not empty; use force=true to cascade delete", name)
		}
		return fmt.Errorf("drop schema: %w", err)
	}
	r.refreshMetaDB(ctx)

	// Run all cascade cleanup in a transaction to maintain consistency.
	tx, err := r.metaDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin cascade cleanup tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	qtx := r.q.WithTx(tx)

	// Soft-delete schema metadata
	if err := qtx.SoftDeleteCatalogMetadata(ctx, dbstore.SoftDeleteCatalogMetadataParams{
		SecurableType: "schema",
		SecurableName: name,
	}); err != nil {
		r.logger.Warn("cascade cleanup: soft-delete schema metadata", "schema", name, "error", err)
	}
	// Soft-delete table metadata for tables in this schema
	if err := qtx.SoftDeleteCatalogMetadataByPattern(ctx, dbstore.SoftDeleteCatalogMetadataByPatternParams{
		SecurableType: "table",
		SecurableName: name + ".%",
	}); err != nil {
		r.logger.Warn("cascade cleanup: soft-delete table metadata", "schema", name, "error", err)
	}

	// Cascade: remove tag assignments for the schema
	if err := qtx.DeleteTagAssignmentsBySecurable(ctx, dbstore.DeleteTagAssignmentsBySecurableParams{
		SecurableType: "schema",
		SecurableID:   schema.SchemaID,
	}); err != nil {
		r.logger.Warn("cascade cleanup: delete schema tag assignments", "schema", name, "error", err)
	}

	// Cascade: clean up governance records for all tables in this schema
	for _, tid := range tableIDs {
		if err := qtx.DeleteRowFiltersByTable(ctx, tid); err != nil {
			r.logger.Warn("cascade cleanup: delete row filters", "table_id", tid, "error", err)
		}
		if err := qtx.DeleteColumnMasksByTable(ctx, tid); err != nil {
			r.logger.Warn("cascade cleanup: delete column masks", "table_id", tid, "error", err)
		}
		if err := qtx.DeleteTagAssignmentsBySecurableTypes(ctx, dbstore.DeleteTagAssignmentsBySecurableTypesParams{
			SecurableType:   "table",
			SecurableType_2: "column",
			SecurableID:     tid,
		}); err != nil {
			r.logger.Warn("cascade cleanup: delete tag assignments", "table_id", tid, "error", err)
		}
	}

	// Cascade: remove column metadata and table statistics
	if err := qtx.DeleteColumnMetadataByTablePattern(ctx, name+".%"); err != nil {
		r.logger.Warn("cascade cleanup: delete column metadata", "schema", name, "error", err)
	}
	if err := qtx.DeleteTableStatisticsByPattern(ctx, name+".%"); err != nil {
		r.logger.Warn("cascade cleanup: delete table statistics", "schema", name, "error", err)
	}

	// Cascade: remove lineage edges referencing tables in this schema
	if err := qtx.DeleteLineageByTablePattern(ctx, dbstore.DeleteLineageByTablePatternParams{
		SourceTable: name + ".%",
		TargetTable: sql.NullString{String: name + ".%", Valid: true},
	}); err != nil {
		r.logger.Warn("cascade cleanup: delete lineage", "schema", name, "error", err)
	}

	// Cascade: remove views in this schema
	if err := qtx.DeleteViewsBySchema(ctx, schema.SchemaID); err != nil {
		r.logger.Warn("cascade cleanup: delete views", "schema", name, "error", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit cascade cleanup: %w", err)
	}

	return nil
}

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

	stmt, err := ddl.CreateTable(schemaName, req.Name, cols)
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
	var tablePath sql.NullString
	var tablePathIsRelative sql.NullInt64
	err = r.metaDB.QueryRowContext(ctx,
		`SELECT table_id, table_name, path, path_is_relative FROM ducklake_table WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL`,
		schemaID, tableName).
		Scan(&t.TableID, &t.Name, &tablePath, &tablePathIsRelative)
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

	t.SchemaName = schemaName
	t.CatalogName = "lake"
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
	var schemaID int64
	err := r.metaDB.QueryRowContext(ctx,
		`SELECT schema_id FROM ducklake_schema WHERE schema_name = ? AND end_snapshot IS NULL`, schemaName).
		Scan(&schemaID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, domain.ErrNotFound("schema %q not found", schemaName)
	}
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
		if err := rows.Scan(&t.TableID, &t.Name); err != nil {
			return nil, 0, err
		}
		t.SchemaName = schemaName
		t.CatalogName = "lake"
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

	stmt, err := ddl.DropTable(schemaName, tableName)
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

	// Cascade: remove row filters and their bindings
	if err := qtx.DeleteRowFiltersByTable(ctx, tbl.TableID); err != nil {
		r.logger.Warn("cascade cleanup: delete row filters", "table", securableName, "error", err)
	}

	// Cascade: remove column masks and their bindings
	if err := qtx.DeleteColumnMasksByTable(ctx, tbl.TableID); err != nil {
		r.logger.Warn("cascade cleanup: delete column masks", "table", securableName, "error", err)
	}

	// Cascade: remove tag assignments for this table and its columns
	if err := qtx.DeleteTagAssignmentsBySecurableTypes(ctx, dbstore.DeleteTagAssignmentsBySecurableTypesParams{
		SecurableType:   "table",
		SecurableType_2: "column",
		SecurableID:     tbl.TableID,
	}); err != nil {
		r.logger.Warn("cascade cleanup: delete tag assignments", "table", securableName, "error", err)
	}

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
	// First resolve table_id
	var schemaID int64
	err := r.metaDB.QueryRowContext(ctx,
		`SELECT schema_id FROM ducklake_schema WHERE schema_name = ? AND end_snapshot IS NULL`, schemaName).
		Scan(&schemaID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, domain.ErrNotFound("schema %q not found", schemaName)
	}
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
		SecurableName: "lake",
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

// SetSchemaStoragePath sets the storage path for a schema in DuckLake's metadata.
// This allows per-schema data paths pointing to different external locations.
// NOTE: ducklake_schema is not managed by sqlc.
func (r *CatalogRepo) SetSchemaStoragePath(ctx context.Context, schemaID int64, path string) error {
	_, err := r.metaDB.ExecContext(ctx,
		`UPDATE ducklake_schema SET path = ?, path_is_relative = 0 WHERE schema_id = ?`,
		path, schemaID)
	if err != nil {
		return fmt.Errorf("set schema storage path: %w", err)
	}
	return nil
}

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
	viewSQL, err := ddl.CreateExternalTableView(schemaName, req.Name, req.SourcePath, fileFormat)
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
		dropSQL, _ := ddl.DropView(schemaName, req.Name)
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
		TableID:      et.EffectiveTableID(),
		Name:         et.TableName,
		SchemaName:   schemaName,
		CatalogName:  "lake",
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
	dropSQL, err := ddl.DropView(schemaName, tableName)
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
	tableID := et.EffectiveTableID()
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

// --- helpers ---

// resolveStoragePath computes the effective storage path for a MANAGED table
// by combining the global data_path, optional schema path, and optional table path.
// DuckLake path resolution: if table has its own path, use it (prepend data_path if relative).
// If not, use the schema's path. If neither, use data_path directly.
func (r *CatalogRepo) resolveStoragePath(ctx context.Context, schemaPath, tablePath sql.NullString, tablePathIsRelative sql.NullInt64) string {
	// Read global data_path
	var dataPath string
	_ = r.metaDB.QueryRowContext(ctx,
		`SELECT value FROM ducklake_metadata WHERE key = 'data_path'`).Scan(&dataPath)

	// If table has its own path, use that
	if tablePath.Valid && tablePath.String != "" {
		if tablePathIsRelative.Valid && tablePathIsRelative.Int64 != 0 {
			return dataPath + tablePath.String
		}
		return tablePath.String
	}

	// If schema has a path, use that
	if schemaPath.Valid && schemaPath.String != "" {
		return schemaPath.String
	}

	// Fall back to global data_path
	return dataPath
}

// loadColumns reads columns from ducklake_column (not managed by sqlc).
func (r *CatalogRepo) loadColumns(ctx context.Context, tableID int64) ([]domain.ColumnDetail, error) {
	rows, err := r.metaDB.QueryContext(ctx,
		`SELECT column_name, column_type, column_id, COALESCE(nulls_allowed, 1) FROM ducklake_column WHERE table_id = ? AND end_snapshot IS NULL ORDER BY column_id`,
		tableID)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var cols []domain.ColumnDetail
	pos := 0
	for rows.Next() {
		var c domain.ColumnDetail
		var colID int64
		var nullsAllowed int64
		if err := rows.Scan(&c.Name, &c.Type, &colID, &nullsAllowed); err != nil {
			return nil, err
		}
		c.Position = pos
		c.Nullable = nullsAllowed != 0
		pos++
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

// enrichSchemaMetadata reads catalog_metadata for a schema via sqlc.
func (r *CatalogRepo) enrichSchemaMetadata(ctx context.Context, s *domain.SchemaDetail) {
	row, err := r.q.GetCatalogMetadata(ctx, dbstore.GetCatalogMetadataParams{
		SecurableType: "schema",
		SecurableName: s.Name,
	})
	if err != nil {
		return
	}
	if row.Comment.Valid {
		s.Comment = row.Comment.String
	}
	if row.Owner.Valid {
		s.Owner = row.Owner.String
	}
	if row.Properties.Valid {
		_ = json.Unmarshal([]byte(row.Properties.String), &s.Properties)
	}
	s.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", row.CreatedAt)
	s.UpdatedAt, _ = time.Parse("2006-01-02 15:04:05", row.UpdatedAt)
	if row.DeletedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.DeletedAt.String)
		s.DeletedAt = &t
	}
}

// enrichTableMetadata reads catalog_metadata for a table via sqlc.
func (r *CatalogRepo) enrichTableMetadata(ctx context.Context, t *domain.TableDetail) {
	securableName := t.SchemaName + "." + t.Name
	row, err := r.q.GetCatalogMetadata(ctx, dbstore.GetCatalogMetadataParams{
		SecurableType: "table",
		SecurableName: securableName,
	})
	if err != nil {
		return
	}
	if row.Comment.Valid {
		t.Comment = row.Comment.String
	}
	if row.Owner.Valid {
		t.Owner = row.Owner.String
	}
	if row.Properties.Valid {
		_ = json.Unmarshal([]byte(row.Properties.String), &t.Properties)
	}
	t.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", row.CreatedAt)
	t.UpdatedAt, _ = time.Parse("2006-01-02 15:04:05", row.UpdatedAt)
	if row.DeletedAt.Valid {
		dt, _ := time.Parse("2006-01-02 15:04:05", row.DeletedAt.String)
		t.DeletedAt = &dt
	}
}

// enrichColumnMetadata reads column_metadata via sqlc.
func (r *CatalogRepo) enrichColumnMetadata(ctx context.Context, tableSecurableName string, c *domain.ColumnDetail) {
	row, err := r.q.GetColumnMetadata(ctx, dbstore.GetColumnMetadataParams{
		TableSecurableName: tableSecurableName,
		ColumnName:         c.Name,
	})
	if err != nil {
		return
	}
	if row.Comment.Valid {
		c.Comment = row.Comment.String
	}
	if row.Properties.Valid {
		_ = json.Unmarshal([]byte(row.Properties.String), &c.Properties)
	}
}

func ptrToStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
