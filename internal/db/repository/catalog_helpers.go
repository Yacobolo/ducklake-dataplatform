package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// resolveSchemaID looks up the ducklake_schema row by name and returns its ID.
func (r *CatalogRepo) resolveSchemaID(ctx context.Context, schemaName string) (int64, error) {
	var schemaID int64
	err := r.metaDB.QueryRowContext(ctx,
		`SELECT schema_id FROM ducklake_schema WHERE schema_name = ? AND end_snapshot IS NULL`, schemaName).
		Scan(&schemaID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, domain.ErrNotFound("schema %q not found", schemaName)
	}
	if err != nil {
		return 0, err
	}
	return schemaID, nil
}

// cascadeDeleteTableGrants removes row filters, column masks, and tag
// assignments for a single table. It logs warnings on failure but does not
// return errors (best-effort cascade cleanup).
func (r *CatalogRepo) cascadeDeleteTableGrants(ctx context.Context, qtx *dbstore.Queries, tableID string, logLabel string) {
	if err := qtx.DeleteRowFiltersByTable(ctx, tableID); err != nil {
		r.logger.Warn("cascade cleanup: delete row filters", logLabel, tableID, "error", err)
	}
	if err := qtx.DeleteColumnMasksByTable(ctx, tableID); err != nil {
		r.logger.Warn("cascade cleanup: delete column masks", logLabel, tableID, "error", err)
	}
	if err := qtx.DeleteTagAssignmentsBySecurableTypes(ctx, dbstore.DeleteTagAssignmentsBySecurableTypesParams{
		SecurableType:   "table",
		SecurableType_2: "column",
		SecurableID:     tableID,
	}); err != nil {
		r.logger.Warn("cascade cleanup: delete tag assignments", logLabel, tableID, "error", err)
	}
}

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
func (r *CatalogRepo) loadColumns(ctx context.Context, tableID string) ([]domain.ColumnDetail, error) {
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
	if t, err := time.Parse("2006-01-02 15:04:05", row.CreatedAt); err == nil {
		s.CreatedAt = t
	} else {
		slog.Default().Warn("failed to parse schema created_at", "value", row.CreatedAt, "error", err)
	}
	if t, err := time.Parse("2006-01-02 15:04:05", row.UpdatedAt); err == nil {
		s.UpdatedAt = t
	} else {
		slog.Default().Warn("failed to parse schema updated_at", "value", row.UpdatedAt, "error", err)
	}
	if row.DeletedAt.Valid {
		if t, err := time.Parse("2006-01-02 15:04:05", row.DeletedAt.String); err == nil {
			s.DeletedAt = &t
		} else {
			slog.Default().Warn("failed to parse schema deleted_at", "value", row.DeletedAt.String, "error", err)
		}
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
	if ct, err := time.Parse("2006-01-02 15:04:05", row.CreatedAt); err == nil {
		t.CreatedAt = ct
	} else {
		slog.Default().Warn("failed to parse table created_at", "value", row.CreatedAt, "error", err)
	}
	if ut, err := time.Parse("2006-01-02 15:04:05", row.UpdatedAt); err == nil {
		t.UpdatedAt = ut
	} else {
		slog.Default().Warn("failed to parse table updated_at", "value", row.UpdatedAt, "error", err)
	}
	if row.DeletedAt.Valid {
		if dt, err := time.Parse("2006-01-02 15:04:05", row.DeletedAt.String); err == nil {
			t.DeletedAt = &dt
		} else {
			slog.Default().Warn("failed to parse table deleted_at", "value", row.DeletedAt.String, "error", err)
		}
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
