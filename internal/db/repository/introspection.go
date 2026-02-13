package repository

import (
	"context"
	"database/sql"
	"errors"

	"duck-demo/internal/domain"
)

// IntrospectionRepo queries DuckLake metadata tables directly.
type IntrospectionRepo struct {
	db *sql.DB
}

// NewIntrospectionRepo creates a new IntrospectionRepo.
func NewIntrospectionRepo(db *sql.DB) *IntrospectionRepo {
	return &IntrospectionRepo{db: db}
}

// ListSchemas returns a paginated list of schemas from the DuckLake metastore.
func (r *IntrospectionRepo) ListSchemas(ctx context.Context, page domain.PageRequest) ([]domain.Schema, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ducklake_schema WHERE end_snapshot IS NULL`).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.db.QueryContext(ctx,
		`SELECT schema_id, schema_name FROM ducklake_schema WHERE end_snapshot IS NULL ORDER BY schema_name LIMIT ? OFFSET ?`,
		page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close() //nolint:errcheck

	var schemas []domain.Schema
	for rows.Next() {
		var s domain.Schema
		var schemaID int64
		if err := rows.Scan(&schemaID, &s.Name); err != nil {
			return nil, 0, err
		}
		s.ID = domain.DuckLakeIDToString(schemaID)
		schemas = append(schemas, s)
	}
	return schemas, total, rows.Err()
}

// ListTables returns a paginated list of tables in a schema.
func (r *IntrospectionRepo) ListTables(ctx context.Context, schemaID string, page domain.PageRequest) ([]domain.Table, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ducklake_table WHERE schema_id = ? AND end_snapshot IS NULL`,
		schemaID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.db.QueryContext(ctx,
		`SELECT table_id, schema_id, table_name FROM ducklake_table WHERE schema_id = ? AND end_snapshot IS NULL ORDER BY table_name LIMIT ? OFFSET ?`,
		schemaID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close() //nolint:errcheck

	var tables []domain.Table
	for rows.Next() {
		var t domain.Table
		var tID, sID int64
		if err := rows.Scan(&tID, &sID, &t.Name); err != nil {
			return nil, 0, err
		}
		t.ID = domain.DuckLakeIDToString(tID)
		t.SchemaID = domain.DuckLakeIDToString(sID)
		tables = append(tables, t)
	}
	return tables, total, rows.Err()
}

// GetTable returns a table by its ID.
func (r *IntrospectionRepo) GetTable(ctx context.Context, tableID string) (*domain.Table, error) {
	var t domain.Table
	var tID, sID int64
	err := r.db.QueryRowContext(ctx,
		`SELECT table_id, schema_id, table_name FROM ducklake_table WHERE table_id = ? AND end_snapshot IS NULL`, tableID).
		Scan(&tID, &sID, &t.Name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, &domain.NotFoundError{Message: "table not found"}
	}
	if err != nil {
		return nil, err
	}
	t.ID = domain.DuckLakeIDToString(tID)
	t.SchemaID = domain.DuckLakeIDToString(sID)
	return &t, nil
}

// ListColumns returns a paginated list of columns for a table.
func (r *IntrospectionRepo) ListColumns(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.Column, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ducklake_column WHERE table_id = ? AND end_snapshot IS NULL`,
		tableID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.db.QueryContext(ctx,
		`SELECT column_id, table_id, column_name, column_type FROM ducklake_column WHERE table_id = ? AND end_snapshot IS NULL ORDER BY column_id LIMIT ? OFFSET ?`,
		tableID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close() //nolint:errcheck

	var columns []domain.Column
	for rows.Next() {
		var c domain.Column
		var cID, tblID int64
		if err := rows.Scan(&cID, &tblID, &c.Name, &c.Type); err != nil {
			return nil, 0, err
		}
		c.ID = domain.DuckLakeIDToString(cID)
		c.TableID = domain.DuckLakeIDToString(tblID)
		columns = append(columns, c)
	}
	return columns, total, rows.Err()
}

// GetTableByName returns a table by its name.
func (r *IntrospectionRepo) GetTableByName(ctx context.Context, tableName string) (*domain.Table, error) {
	var t domain.Table
	var tID, sID int64
	err := r.db.QueryRowContext(ctx,
		`SELECT table_id, schema_id, table_name FROM ducklake_table WHERE table_name = ? AND end_snapshot IS NULL`, tableName).
		Scan(&tID, &sID, &t.Name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, &domain.NotFoundError{Message: "table not found"}
	}
	if err != nil {
		return nil, err
	}
	t.ID = domain.DuckLakeIDToString(tID)
	t.SchemaID = domain.DuckLakeIDToString(sID)
	return &t, nil
}

// GetSchemaByName returns a schema by its name.
func (r *IntrospectionRepo) GetSchemaByName(ctx context.Context, schemaName string) (*domain.Schema, error) {
	var s domain.Schema
	var schemaID int64
	err := r.db.QueryRowContext(ctx,
		`SELECT schema_id, schema_name FROM ducklake_schema WHERE schema_name = ? AND end_snapshot IS NULL`, schemaName).
		Scan(&schemaID, &s.Name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, &domain.NotFoundError{Message: "schema not found"}
	}
	if err != nil {
		return nil, err
	}
	s.ID = domain.DuckLakeIDToString(schemaID)
	return &s, nil
}
