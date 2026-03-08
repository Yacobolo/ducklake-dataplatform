package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

// ListCatalogHistory returns recent snapshot-aware history entries for schemas, tables, and columns.
func (r *CatalogRepo) ListCatalogHistory(ctx context.Context, filter domain.CatalogHistoryFilter) ([]domain.CatalogHistoryEntry, error) {
	queries := make([]string, 0, 3)
	args := make([]any, 0, 12)

	include := func(entityType string) bool {
		return filter.EntityType == "" || filter.EntityType == entityType
	}

	if include("schema") {
		q, qArgs := schemaHistoryQuery(filter)
		queries = append(queries, q)
		args = append(args, qArgs...)
	}
	if include("table") {
		q, qArgs := tableHistoryQuery(filter)
		queries = append(queries, q)
		args = append(args, qArgs...)
	}
	if include("column") {
		q, qArgs := columnHistoryQuery(filter)
		queries = append(queries, q)
		args = append(args, qArgs...)
	}

	if len(queries) == 0 {
		return []domain.CatalogHistoryEntry{}, nil
	}

	query := strings.Join(queries, " UNION ALL ") + `
		ORDER BY latest_snapshot_id DESC, entity_type ASC, schema_name ASC, table_name ASC, column_name ASC
		LIMIT ?`
	args = append(args, filter.Limit)

	rows, err := r.metaDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query catalog history: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	entries := make([]domain.CatalogHistoryEntry, 0)
	for rows.Next() {
		entry, err := scanCatalogHistoryEntry(rows)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate catalog history: %w", err)
	}

	return entries, nil
}

func schemaHistoryQuery(filter domain.CatalogHistoryFilter) (string, []any) {
	args := make([]any, 0, 1)
	where := make([]string, 0, 1)
	if filter.SchemaName != "" {
		where = append(where, "s.schema_name = ?")
		args = append(args, filter.SchemaName)
	}
	query := `
		SELECT
			'schema' AS entity_type,
			s.schema_name AS schema_name,
			NULL AS table_name,
			NULL AS column_name,
			s.schema_name AS object_name,
			CAST(s.schema_id AS TEXT) AS object_id,
			s.begin_snapshot AS begin_snapshot_id,
			s.end_snapshot AS end_snapshot_id,
			COALESCE(s.end_snapshot, s.begin_snapshot) AS latest_snapshot_id,
			CASE WHEN s.end_snapshot IS NULL THEN 1 ELSE 0 END AS is_active,
			CASE WHEN s.end_snapshot IS NOT NULL THEN 1 ELSE 0 END AS has_history
		FROM ducklake_schema s`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	return query, args
}

func tableHistoryQuery(filter domain.CatalogHistoryFilter) (string, []any) {
	args := make([]any, 0, 2)
	where := make([]string, 0, 2)
	if filter.SchemaName != "" {
		where = append(where, "s.schema_name = ?")
		args = append(args, filter.SchemaName)
	}
	if filter.TableName != "" {
		where = append(where, "t.table_name = ?")
		args = append(args, filter.TableName)
	}
	query := `
		SELECT
			'table' AS entity_type,
			COALESCE(s.schema_name, '') AS schema_name,
			t.table_name AS table_name,
			NULL AS column_name,
			COALESCE(s.schema_name || '.', '') || t.table_name AS object_name,
			CAST(t.table_id AS TEXT) AS object_id,
			t.begin_snapshot AS begin_snapshot_id,
			t.end_snapshot AS end_snapshot_id,
			COALESCE(t.end_snapshot, t.begin_snapshot) AS latest_snapshot_id,
			CASE WHEN t.end_snapshot IS NULL THEN 1 ELSE 0 END AS is_active,
			CASE WHEN t.end_snapshot IS NOT NULL THEN 1 ELSE 0 END AS has_history
		FROM ducklake_table t
		LEFT JOIN ducklake_schema s ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	return query, args
}

func columnHistoryQuery(filter domain.CatalogHistoryFilter) (string, []any) {
	args := make([]any, 0, 2)
	where := make([]string, 0, 2)
	if filter.SchemaName != "" {
		where = append(where, "s.schema_name = ?")
		args = append(args, filter.SchemaName)
	}
	if filter.TableName != "" {
		where = append(where, "t.table_name = ?")
		args = append(args, filter.TableName)
	}
	query := `
		SELECT
			'column' AS entity_type,
			COALESCE(s.schema_name, '') AS schema_name,
			COALESCE(t.table_name, '') AS table_name,
			c.column_name AS column_name,
			COALESCE(s.schema_name || '.', '') || COALESCE(t.table_name || '.', '') || c.column_name AS object_name,
			CAST(c.column_id AS TEXT) AS object_id,
			c.begin_snapshot AS begin_snapshot_id,
			c.end_snapshot AS end_snapshot_id,
			COALESCE(c.end_snapshot, c.begin_snapshot) AS latest_snapshot_id,
			CASE WHEN c.end_snapshot IS NULL THEN 1 ELSE 0 END AS is_active,
			CASE WHEN c.end_snapshot IS NOT NULL THEN 1 ELSE 0 END AS has_history
		FROM ducklake_column c
		LEFT JOIN ducklake_table t ON t.table_id = c.table_id AND t.end_snapshot IS NULL
		LEFT JOIN ducklake_schema s ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	return query, args
}

func scanCatalogHistoryEntry(scanner interface{ Scan(dest ...any) error }) (domain.CatalogHistoryEntry, error) {
	var entry domain.CatalogHistoryEntry
	var schemaName sql.NullString
	var tableName sql.NullString
	var columnName sql.NullString
	var beginSnapshot sql.NullInt64
	var endSnapshot sql.NullInt64
	var latestSnapshot sql.NullInt64
	var isActive int64
	var hasHistory int64

	if err := scanner.Scan(
		&entry.EntityType,
		&schemaName,
		&tableName,
		&columnName,
		&entry.ObjectName,
		&entry.ObjectID,
		&beginSnapshot,
		&endSnapshot,
		&latestSnapshot,
		&isActive,
		&hasHistory,
	); err != nil {
		return domain.CatalogHistoryEntry{}, fmt.Errorf("scan catalog history entry: %w", err)
	}
	entry.SchemaName = schemaName.String
	entry.TableName = tableName.String
	entry.ColumnName = columnName.String
	entry.BeginSnapshotID = nullableInt64Ptr(beginSnapshot)
	entry.EndSnapshotID = nullableInt64Ptr(endSnapshot)
	entry.LatestSnapshotID = nullableInt64Ptr(latestSnapshot)
	entry.IsActive = isActive != 0
	entry.HasHistory = hasHistory != 0
	return entry, nil
}

func nullableInt64Ptr(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}
	value := v.Int64
	return &value
}
