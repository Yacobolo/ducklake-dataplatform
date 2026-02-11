package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"duck-demo/internal/domain"
)

// identifierRe allows alphanumeric + underscores, starting with a letter or underscore.
var identifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func validateIdentifier(name string) error {
	if name == "" {
		return domain.ErrValidation("name is required")
	}
	if len(name) > 128 {
		return domain.ErrValidation("name must be at most 128 characters")
	}
	if !identifierRe.MatchString(name) {
		return domain.ErrValidation("name must match [a-zA-Z_][a-zA-Z0-9_]*")
	}
	return nil
}

// CatalogRepo implements domain.CatalogRepository using the DuckLake SQLite
// metastore (metaDB) for reads and the DuckDB connection (duckDB) for DDL.
type CatalogRepo struct {
	metaDB *sql.DB
	duckDB *sql.DB
}

func NewCatalogRepo(metaDB, duckDB *sql.DB) *CatalogRepo {
	return &CatalogRepo{metaDB: metaDB, duckDB: duckDB}
}

// GetCatalogInfo returns information about the single "lake" catalog.
func (r *CatalogRepo) GetCatalogInfo(ctx context.Context) (*domain.CatalogInfo, error) {
	info := &domain.CatalogInfo{
		Name: "lake",
	}

	// Try to read comment from catalog_metadata
	var comment sql.NullString
	var createdAt, updatedAt sql.NullString
	err := r.metaDB.QueryRowContext(ctx,
		`SELECT comment, created_at, updated_at FROM catalog_metadata WHERE securable_type = 'catalog' AND securable_name = 'lake'`).
		Scan(&comment, &createdAt, &updatedAt)
	if err == nil {
		if comment.Valid {
			info.Comment = comment.String
		}
		if createdAt.Valid {
			info.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", createdAt.String)
		}
		if updatedAt.Valid {
			info.UpdatedAt, _ = time.Parse("2006-01-02 15:04:05", updatedAt.String)
		}
	}

	return info, nil
}

// GetMetastoreSummary returns high-level info about the DuckLake metastore.
func (r *CatalogRepo) GetMetastoreSummary(ctx context.Context) (*domain.MetastoreSummary, error) {
	summary := &domain.MetastoreSummary{
		CatalogName:    "lake",
		MetastoreType:  "DuckLake (SQLite)",
		StorageBackend: "S3",
	}

	// Read data_path
	var dataPath sql.NullString
	_ = r.metaDB.QueryRowContext(ctx,
		`SELECT value FROM ducklake_metadata WHERE key = 'data_path'`).Scan(&dataPath)
	if dataPath.Valid {
		summary.DataPath = dataPath.String
	}

	// Count schemas
	_ = r.metaDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ducklake_schema WHERE end_snapshot IS NULL`).Scan(&summary.SchemaCount)

	// Count tables
	_ = r.metaDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ducklake_table WHERE end_snapshot IS NULL`).Scan(&summary.TableCount)

	return summary, nil
}

// CreateSchema creates a new schema via DuckDB DDL and reads it back.
func (r *CatalogRepo) CreateSchema(ctx context.Context, name, comment, owner string) (*domain.SchemaDetail, error) {
	if err := validateIdentifier(name); err != nil {
		return nil, err
	}

	// Check if schema already exists (DuckDB silently succeeds on duplicate CREATE SCHEMA)
	if _, err := r.GetSchema(ctx, name); err == nil {
		return nil, domain.ErrConflict("schema %q already exists", name)
	}

	ddl := fmt.Sprintf(`CREATE SCHEMA lake."%s"`, name)
	if _, err := r.duckDB.ExecContext(ctx, ddl); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "already exists") {
			return nil, domain.ErrConflict("schema %q already exists", name)
		}
		return nil, fmt.Errorf("create schema: %w", err)
	}

	// Store metadata
	if comment != "" || owner != "" {
		_, _ = r.metaDB.ExecContext(ctx,
			`INSERT OR REPLACE INTO catalog_metadata (securable_type, securable_name, comment, owner) VALUES ('schema', ?, ?, ?)`,
			name, comment, owner)
	}

	return r.GetSchema(ctx, name)
}

// GetSchema reads a schema by name from the DuckLake metastore.
func (r *CatalogRepo) GetSchema(ctx context.Context, name string) (*domain.SchemaDetail, error) {
	var s domain.SchemaDetail
	err := r.metaDB.QueryRowContext(ctx,
		`SELECT schema_id, schema_name FROM ducklake_schema WHERE schema_name = ? AND end_snapshot IS NULL`, name).
		Scan(&s.SchemaID, &s.Name)
	if err == sql.ErrNoRows {
		return nil, domain.ErrNotFound("schema %q not found", name)
	}
	if err != nil {
		return nil, err
	}
	s.CatalogName = "lake"

	// Join with catalog_metadata
	r.enrichSchemaMetadata(ctx, &s)

	return &s, nil
}

// ListSchemas returns a paginated list of schemas.
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
	defer rows.Close()

	var schemas []domain.SchemaDetail
	for rows.Next() {
		var s domain.SchemaDetail
		if err := rows.Scan(&s.SchemaID, &s.Name); err != nil {
			return nil, 0, err
		}
		s.CatalogName = "lake"
		r.enrichSchemaMetadata(ctx, &s)
		schemas = append(schemas, s)
	}
	return schemas, total, rows.Err()
}

// UpdateSchema updates schema metadata (comment, properties).
func (r *CatalogRepo) UpdateSchema(ctx context.Context, name string, comment *string, props map[string]string) (*domain.SchemaDetail, error) {
	// Verify schema exists
	_, err := r.GetSchema(ctx, name)
	if err != nil {
		return nil, err
	}

	var propsJSON *string
	if props != nil {
		b, _ := json.Marshal(props)
		s := string(b)
		propsJSON = &s
	}

	_, err = r.metaDB.ExecContext(ctx,
		`INSERT INTO catalog_metadata (securable_type, securable_name, comment, properties)
		 VALUES ('schema', ?, ?, ?)
		 ON CONFLICT(securable_type, securable_name)
		 DO UPDATE SET comment = COALESCE(excluded.comment, comment),
		               properties = COALESCE(excluded.properties, properties),
		               updated_at = datetime('now')`,
		name, comment, propsJSON)
	if err != nil {
		return nil, fmt.Errorf("update schema metadata: %w", err)
	}

	return r.GetSchema(ctx, name)
}

// DeleteSchema drops a schema via DuckDB DDL and cascades governance cleanup.
func (r *CatalogRepo) DeleteSchema(ctx context.Context, name string, force bool) error {
	if err := validateIdentifier(name); err != nil {
		return err
	}

	// Verify schema exists and capture schema ID
	schema, err := r.GetSchema(ctx, name)
	if err != nil {
		return err
	}

	// If force, gather table IDs in this schema for governance cleanup before DDL
	var tableIDs []int64
	if force {
		rows, err := r.metaDB.QueryContext(ctx,
			`SELECT table_id FROM ducklake_table WHERE schema_id = ? AND end_snapshot IS NULL`, schema.SchemaID)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var tid int64
				if err := rows.Scan(&tid); err == nil {
					tableIDs = append(tableIDs, tid)
				}
			}
		}
	}

	ddl := fmt.Sprintf(`DROP SCHEMA lake."%s"`, name)
	if force {
		ddl += " CASCADE"
	}

	if _, err := r.duckDB.ExecContext(ctx, ddl); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "not empty") || strings.Contains(errMsg, "depends on") {
			return domain.ErrConflict("schema %q is not empty; use force=true to cascade delete", name)
		}
		return fmt.Errorf("drop schema: %w", err)
	}

	// Clean up schema metadata
	_, _ = r.metaDB.ExecContext(ctx,
		`DELETE FROM catalog_metadata WHERE securable_type = 'schema' AND securable_name = ?`, name)
	// Clean up table metadata for tables that were in this schema
	_, _ = r.metaDB.ExecContext(ctx,
		`DELETE FROM catalog_metadata WHERE securable_type = 'table' AND securable_name LIKE ?`, name+".%")

	// Cascade: remove tag assignments for the schema
	_, _ = r.metaDB.ExecContext(ctx,
		`DELETE FROM tag_assignments WHERE securable_type = 'schema' AND securable_id = ?`, schema.SchemaID)

	// Cascade: clean up governance records for all tables in this schema
	for _, tid := range tableIDs {
		_, _ = r.metaDB.ExecContext(ctx, `DELETE FROM row_filters WHERE table_id = ?`, tid)
		_, _ = r.metaDB.ExecContext(ctx, `DELETE FROM column_masks WHERE table_id = ?`, tid)
		_, _ = r.metaDB.ExecContext(ctx, `DELETE FROM tag_assignments WHERE securable_type IN ('table', 'column') AND securable_id = ?`, tid)
	}

	// Cascade: remove column metadata and table statistics for tables in schema
	_, _ = r.metaDB.ExecContext(ctx,
		`DELETE FROM column_metadata WHERE table_securable_name LIKE ?`, name+".%")
	_, _ = r.metaDB.ExecContext(ctx,
		`DELETE FROM table_statistics WHERE table_securable_name LIKE ?`, name+".%")

	// Cascade: remove lineage edges referencing tables in this schema
	_, _ = r.metaDB.ExecContext(ctx,
		`DELETE FROM lineage_edges WHERE source_table LIKE ? OR target_table LIKE ?`, name+".%", name+".%")

	// Cascade: remove views in this schema
	_, _ = r.metaDB.ExecContext(ctx,
		`DELETE FROM views WHERE schema_id = ?`, schema.SchemaID)

	return nil
}

// CreateTable creates a new table via DuckDB DDL and reads it back.
func (r *CatalogRepo) CreateTable(ctx context.Context, schemaName string, req domain.CreateTableRequest, owner string) (*domain.TableDetail, error) {
	if err := validateIdentifier(schemaName); err != nil {
		return nil, err
	}
	if err := validateIdentifier(req.Name); err != nil {
		return nil, err
	}
	if len(req.Columns) == 0 {
		return nil, domain.ErrValidation("at least one column is required")
	}

	// Build column definitions
	var colDefs []string
	for _, c := range req.Columns {
		if err := validateIdentifier(c.Name); err != nil {
			return nil, domain.ErrValidation("invalid column name: %s", err.Error())
		}
		colDefs = append(colDefs, fmt.Sprintf(`"%s" %s`, c.Name, c.Type))
	}

	ddl := fmt.Sprintf(`CREATE TABLE lake."%s"."%s" (%s)`, schemaName, req.Name, strings.Join(colDefs, ", "))
	if _, err := r.duckDB.ExecContext(ctx, ddl); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "already exists") {
			return nil, domain.ErrConflict("table %q already exists in schema %q", req.Name, schemaName)
		}
		if strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "does not exist") {
			return nil, domain.ErrNotFound("schema %q not found", schemaName)
		}
		return nil, fmt.Errorf("create table: %w", err)
	}

	// Store metadata
	securableName := schemaName + "." + req.Name
	if req.Comment != "" || owner != "" {
		_, _ = r.metaDB.ExecContext(ctx,
			`INSERT OR REPLACE INTO catalog_metadata (securable_type, securable_name, comment, owner) VALUES ('table', ?, ?, ?)`,
			securableName, req.Comment, owner)
	}

	return r.GetTable(ctx, schemaName, req.Name)
}

// GetTable reads a table by schema and table name, including columns.
func (r *CatalogRepo) GetTable(ctx context.Context, schemaName, tableName string) (*domain.TableDetail, error) {
	// First get the schema_id
	var schemaID int64
	err := r.metaDB.QueryRowContext(ctx,
		`SELECT schema_id FROM ducklake_schema WHERE schema_name = ? AND end_snapshot IS NULL`, schemaName).
		Scan(&schemaID)
	if err == sql.ErrNoRows {
		return nil, domain.ErrNotFound("schema %q not found", schemaName)
	}
	if err != nil {
		return nil, err
	}

	var t domain.TableDetail
	err = r.metaDB.QueryRowContext(ctx,
		`SELECT table_id, table_name FROM ducklake_table WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL`,
		schemaID, tableName).
		Scan(&t.TableID, &t.Name)
	if err == sql.ErrNoRows {
		return nil, domain.ErrNotFound("table %q not found in schema %q", tableName, schemaName)
	}
	if err != nil {
		return nil, err
	}

	t.SchemaName = schemaName
	t.CatalogName = "lake"
	t.TableType = "MANAGED"

	// Load columns
	cols, err := r.loadColumns(ctx, t.TableID)
	if err != nil {
		return nil, err
	}
	t.Columns = cols

	// Enrich columns with metadata
	securableName := schemaName + "." + tableName
	for i := range t.Columns {
		r.enrichColumnMetadata(ctx, securableName, &t.Columns[i])
	}

	// Join with catalog_metadata
	r.enrichTableMetadata(ctx, &t)

	return &t, nil
}

// ListTables returns a paginated list of tables in a schema.
func (r *CatalogRepo) ListTables(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.TableDetail, int64, error) {
	// First get the schema_id
	var schemaID int64
	err := r.metaDB.QueryRowContext(ctx,
		`SELECT schema_id FROM ducklake_schema WHERE schema_name = ? AND end_snapshot IS NULL`, schemaName).
		Scan(&schemaID)
	if err == sql.ErrNoRows {
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
	defer rows.Close()

	var tables []domain.TableDetail
	for rows.Next() {
		var t domain.TableDetail
		if err := rows.Scan(&t.TableID, &t.Name); err != nil {
			return nil, 0, err
		}
		t.SchemaName = schemaName
		t.CatalogName = "lake"
		t.TableType = "MANAGED"
		r.enrichTableMetadata(ctx, &t)
		tables = append(tables, t)
	}
	return tables, total, rows.Err()
}

// DeleteTable drops a table via DuckDB DDL and cascades governance cleanup.
func (r *CatalogRepo) DeleteTable(ctx context.Context, schemaName, tableName string) error {
	if err := validateIdentifier(schemaName); err != nil {
		return err
	}
	if err := validateIdentifier(tableName); err != nil {
		return err
	}

	// Verify table exists and capture table ID for governance cleanup
	tbl, err := r.GetTable(ctx, schemaName, tableName)
	if err != nil {
		return err
	}

	ddl := fmt.Sprintf(`DROP TABLE lake."%s"."%s"`, schemaName, tableName)
	if _, err := r.duckDB.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}

	// Clean up metadata
	securableName := schemaName + "." + tableName
	_, _ = r.metaDB.ExecContext(ctx,
		`DELETE FROM catalog_metadata WHERE securable_type = 'table' AND securable_name = ?`, securableName)

	// Cascade: remove row filters and their bindings (bindings cascade via FK)
	_, _ = r.metaDB.ExecContext(ctx,
		`DELETE FROM row_filters WHERE table_id = ?`, tbl.TableID)

	// Cascade: remove column masks and their bindings (bindings cascade via FK)
	_, _ = r.metaDB.ExecContext(ctx,
		`DELETE FROM column_masks WHERE table_id = ?`, tbl.TableID)

	// Cascade: remove tag assignments for this table and its columns
	_, _ = r.metaDB.ExecContext(ctx,
		`DELETE FROM tag_assignments WHERE securable_type IN ('table', 'column') AND securable_id = ?`, tbl.TableID)

	// Cascade: remove column metadata
	_, _ = r.metaDB.ExecContext(ctx,
		`DELETE FROM column_metadata WHERE table_securable_name = ?`, securableName)

	// Cascade: remove table statistics
	_, _ = r.metaDB.ExecContext(ctx,
		`DELETE FROM table_statistics WHERE table_securable_name = ?`, securableName)

	// Cascade: remove lineage edges referencing this table
	qualifiedName := schemaName + "." + tableName
	_, _ = r.metaDB.ExecContext(ctx,
		`DELETE FROM lineage_edges WHERE source_table = ? OR target_table = ?`, qualifiedName, qualifiedName)

	return nil
}

// ListColumns returns a paginated list of columns for a table.
func (r *CatalogRepo) ListColumns(ctx context.Context, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, int64, error) {
	// First resolve table_id
	var schemaID int64
	err := r.metaDB.QueryRowContext(ctx,
		`SELECT schema_id FROM ducklake_schema WHERE schema_name = ? AND end_snapshot IS NULL`, schemaName).
		Scan(&schemaID)
	if err == sql.ErrNoRows {
		return nil, 0, domain.ErrNotFound("schema %q not found", schemaName)
	}
	if err != nil {
		return nil, 0, err
	}

	var tableID int64
	err = r.metaDB.QueryRowContext(ctx,
		`SELECT table_id FROM ducklake_table WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL`,
		schemaID, tableName).Scan(&tableID)
	if err == sql.ErrNoRows {
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
		`SELECT column_name, column_type, column_id FROM ducklake_column WHERE table_id = ? AND end_snapshot IS NULL ORDER BY column_id LIMIT ? OFFSET ?`,
		tableID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var columns []domain.ColumnDetail
	pos := page.Offset()
	for rows.Next() {
		var c domain.ColumnDetail
		var colID int64
		if err := rows.Scan(&c.Name, &c.Type, &colID); err != nil {
			return nil, 0, err
		}
		c.Position = pos
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
	var propsJSON *string
	if props != nil {
		b, _ := json.Marshal(props)
		s := string(b)
		propsJSON = &s
	}

	_, err = r.metaDB.ExecContext(ctx,
		`INSERT INTO catalog_metadata (securable_type, securable_name, comment, properties, owner)
		 VALUES ('table', ?, ?, ?, ?)
		 ON CONFLICT(securable_type, securable_name)
		 DO UPDATE SET comment = COALESCE(excluded.comment, comment),
		               properties = COALESCE(excluded.properties, properties),
		               owner = COALESCE(excluded.owner, owner),
		               updated_at = datetime('now')`,
		securableName, comment, propsJSON, owner)
	if err != nil {
		return nil, fmt.Errorf("update table metadata: %w", err)
	}

	return r.GetTable(ctx, schemaName, tableName)
}

// UpdateCatalog updates catalog-level metadata (comment).
func (r *CatalogRepo) UpdateCatalog(ctx context.Context, comment *string) (*domain.CatalogInfo, error) {
	_, err := r.metaDB.ExecContext(ctx,
		`INSERT INTO catalog_metadata (securable_type, securable_name, comment)
		 VALUES ('catalog', 'lake', ?)
		 ON CONFLICT(securable_type, securable_name)
		 DO UPDATE SET comment = COALESCE(excluded.comment, comment),
		               updated_at = datetime('now')`,
		comment)
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
	var propsJSON *string
	if props != nil {
		b, _ := json.Marshal(props)
		s := string(b)
		propsJSON = &s
	}

	_, err = r.metaDB.ExecContext(ctx,
		`INSERT INTO column_metadata (table_securable_name, column_name, comment, properties)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(table_securable_name, column_name)
		 DO UPDATE SET comment = COALESCE(excluded.comment, comment),
		               properties = COALESCE(excluded.properties, properties),
		               updated_at = datetime('now')`,
		securableName, columnName, comment, propsJSON)
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
func (r *CatalogRepo) SetSchemaStoragePath(ctx context.Context, schemaID int64, path string) error {
	_, err := r.metaDB.ExecContext(ctx,
		`UPDATE ducklake_schema SET path = ?, path_is_relative = 0 WHERE schema_id = ?`,
		path, schemaID)
	if err != nil {
		return fmt.Errorf("set schema storage path: %w", err)
	}
	return nil
}

// --- helpers ---

func (r *CatalogRepo) loadColumns(ctx context.Context, tableID int64) ([]domain.ColumnDetail, error) {
	rows, err := r.metaDB.QueryContext(ctx,
		`SELECT column_name, column_type, column_id FROM ducklake_column WHERE table_id = ? AND end_snapshot IS NULL ORDER BY column_id`,
		tableID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []domain.ColumnDetail
	pos := 0
	for rows.Next() {
		var c domain.ColumnDetail
		var colID int64
		if err := rows.Scan(&c.Name, &c.Type, &colID); err != nil {
			return nil, err
		}
		c.Position = pos
		pos++
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

func (r *CatalogRepo) enrichSchemaMetadata(ctx context.Context, s *domain.SchemaDetail) {
	var comment, properties, owner sql.NullString
	var createdAt, updatedAt sql.NullString
	err := r.metaDB.QueryRowContext(ctx,
		`SELECT comment, properties, owner, created_at, updated_at FROM catalog_metadata WHERE securable_type = 'schema' AND securable_name = ?`,
		s.Name).Scan(&comment, &properties, &owner, &createdAt, &updatedAt)
	if err != nil {
		return
	}
	if comment.Valid {
		s.Comment = comment.String
	}
	if owner.Valid {
		s.Owner = owner.String
	}
	if properties.Valid {
		_ = json.Unmarshal([]byte(properties.String), &s.Properties)
	}
	if createdAt.Valid {
		s.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", createdAt.String)
	}
	if updatedAt.Valid {
		s.UpdatedAt, _ = time.Parse("2006-01-02 15:04:05", updatedAt.String)
	}
}

func (r *CatalogRepo) enrichTableMetadata(ctx context.Context, t *domain.TableDetail) {
	securableName := t.SchemaName + "." + t.Name
	var comment, properties, owner sql.NullString
	var createdAt, updatedAt sql.NullString
	err := r.metaDB.QueryRowContext(ctx,
		`SELECT comment, properties, owner, created_at, updated_at FROM catalog_metadata WHERE securable_type = 'table' AND securable_name = ?`,
		securableName).Scan(&comment, &properties, &owner, &createdAt, &updatedAt)
	if err != nil {
		return
	}
	if comment.Valid {
		t.Comment = comment.String
	}
	if owner.Valid {
		t.Owner = owner.String
	}
	if properties.Valid {
		_ = json.Unmarshal([]byte(properties.String), &t.Properties)
	}
	if createdAt.Valid {
		t.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", createdAt.String)
	}
	if updatedAt.Valid {
		t.UpdatedAt, _ = time.Parse("2006-01-02 15:04:05", updatedAt.String)
	}
}

func (r *CatalogRepo) enrichColumnMetadata(ctx context.Context, tableSecurableName string, c *domain.ColumnDetail) {
	var comment, properties sql.NullString
	err := r.metaDB.QueryRowContext(ctx,
		`SELECT comment, properties FROM column_metadata WHERE table_securable_name = ? AND column_name = ?`,
		tableSecurableName, c.Name).Scan(&comment, &properties)
	if err != nil {
		return
	}
	if comment.Valid {
		c.Comment = comment.String
	}
	if properties.Valid {
		_ = json.Unmarshal([]byte(properties.String), &c.Properties)
	}
}
