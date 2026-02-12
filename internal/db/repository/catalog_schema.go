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

// CreateSchema creates a new schema via DuckDB DDL and reads it back.
func (r *CatalogRepo) CreateSchema(ctx context.Context, name, comment, owner string) (*domain.SchemaDetail, error) {
	if err := ddl.ValidateIdentifier(name); err != nil {
		return nil, domain.ErrValidation("%s", err.Error())
	}

	// Check if schema already exists (DuckDB silently succeeds on duplicate CREATE SCHEMA)
	if _, err := r.GetSchema(ctx, name); err == nil {
		return nil, domain.ErrConflict("schema %q already exists", name)
	}

	stmt, err := ddl.CreateSchema(r.catalogName, name)
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
	s.CatalogName = r.catalogName

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
		s.CatalogName = r.catalogName
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
					dropViewSQL, _ := ddl.DropView(r.catalogName, et.SchemaName, et.TableName)
					if dropViewSQL != "" {
						_, _ = r.duckDB.ExecContext(ctx, dropViewSQL)
					}
				}
			}
		}
		_ = r.extRepo.DeleteBySchema(ctx, name)
	}

	stmt, err := ddl.DropSchema(r.catalogName, name, force)
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
		r.cascadeDeleteTableGrants(ctx, qtx, tid, "table_id")
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
