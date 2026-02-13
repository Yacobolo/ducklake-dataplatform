package repository

import (
	"context"
	"database/sql"
	"fmt"

	"duck-demo/internal/domain"
)

// MetastoreRepo implements domain.MetastoreQuerier using a SQLite connection
// to the DuckLake metastore.
type MetastoreRepo struct {
	db *sql.DB
}

// NewMetastoreRepo creates a new MetastoreRepo.
func NewMetastoreRepo(db *sql.DB) *MetastoreRepo {
	return &MetastoreRepo{db: db}
}

var _ domain.MetastoreQuerier = (*MetastoreRepo)(nil)

// ReadDataPath returns the data_path value from the DuckLake metadata table.
func (r *MetastoreRepo) ReadDataPath(ctx context.Context) (string, error) {
	var dataPath string
	err := r.db.QueryRowContext(ctx,
		`SELECT value FROM ducklake_metadata WHERE key = 'data_path'`).Scan(&dataPath)
	if err != nil {
		return "", fmt.Errorf("read data_path from ducklake_metadata: %w", err)
	}
	return dataPath, nil
}

// ReadSchemaPath returns the storage path for the given schema, or empty if none is set.
func (r *MetastoreRepo) ReadSchemaPath(ctx context.Context, schemaName string) (string, error) {
	var schemaPath string
	err := r.db.QueryRowContext(ctx,
		`SELECT path FROM ducklake_schema WHERE schema_name = ? AND path IS NOT NULL AND path != ''`,
		schemaName).Scan(&schemaPath)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read schema path: %w", err)
	}
	return schemaPath, nil
}

// ListDataFiles returns the file paths and relative flags for active data files of the given table.
func (r *MetastoreRepo) ListDataFiles(ctx context.Context, tableID string) ([]string, []bool, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT path, path_is_relative FROM ducklake_data_file
		 WHERE table_id = ? AND end_snapshot IS NULL`, tableID)
	if err != nil {
		return nil, nil, fmt.Errorf("query ducklake_data_file: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var paths []string
	var isRelative []bool
	for rows.Next() {
		var path string
		var rel bool
		if err := rows.Scan(&path, &rel); err != nil {
			return nil, nil, err
		}
		paths = append(paths, path)
		isRelative = append(isRelative, rel)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return paths, isRelative, nil
}
