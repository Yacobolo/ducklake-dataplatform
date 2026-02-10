package repository

import (
	"context"
	"database/sql"

	"duck-demo/internal/domain"
)

// IntrospectionRepo queries DuckLake metadata tables directly.
type IntrospectionRepo struct {
	db *sql.DB
}

func NewIntrospectionRepo(db *sql.DB) *IntrospectionRepo {
	return &IntrospectionRepo{db: db}
}

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
	defer rows.Close()

	var schemas []domain.Schema
	for rows.Next() {
		var s domain.Schema
		if err := rows.Scan(&s.ID, &s.Name); err != nil {
			return nil, 0, err
		}
		schemas = append(schemas, s)
	}
	return schemas, total, rows.Err()
}

func (r *IntrospectionRepo) ListTables(ctx context.Context, schemaID int64, page domain.PageRequest) ([]domain.Table, int64, error) {
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
	defer rows.Close()

	var tables []domain.Table
	for rows.Next() {
		var t domain.Table
		if err := rows.Scan(&t.ID, &t.SchemaID, &t.Name); err != nil {
			return nil, 0, err
		}
		tables = append(tables, t)
	}
	return tables, total, rows.Err()
}

func (r *IntrospectionRepo) GetTable(ctx context.Context, tableID int64) (*domain.Table, error) {
	var t domain.Table
	err := r.db.QueryRowContext(ctx,
		`SELECT table_id, schema_id, table_name FROM ducklake_table WHERE table_id = ? AND end_snapshot IS NULL`, tableID).
		Scan(&t.ID, &t.SchemaID, &t.Name)
	if err == sql.ErrNoRows {
		return nil, &domain.NotFoundError{Message: "table not found"}
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (r *IntrospectionRepo) ListColumns(ctx context.Context, tableID int64, page domain.PageRequest) ([]domain.Column, int64, error) {
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
	defer rows.Close()

	var columns []domain.Column
	for rows.Next() {
		var c domain.Column
		if err := rows.Scan(&c.ID, &c.TableID, &c.Name, &c.Type); err != nil {
			return nil, 0, err
		}
		columns = append(columns, c)
	}
	return columns, total, rows.Err()
}

func (r *IntrospectionRepo) GetTableByName(ctx context.Context, tableName string) (*domain.Table, error) {
	var t domain.Table
	err := r.db.QueryRowContext(ctx,
		`SELECT table_id, schema_id, table_name FROM ducklake_table WHERE table_name = ? AND end_snapshot IS NULL`, tableName).
		Scan(&t.ID, &t.SchemaID, &t.Name)
	if err == sql.ErrNoRows {
		return nil, &domain.NotFoundError{Message: "table not found"}
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (r *IntrospectionRepo) GetSchemaByName(ctx context.Context, schemaName string) (*domain.Schema, error) {
	var s domain.Schema
	err := r.db.QueryRowContext(ctx,
		`SELECT schema_id, schema_name FROM ducklake_schema WHERE schema_name = ? AND end_snapshot IS NULL`, schemaName).
		Scan(&s.ID, &s.Name)
	if err == sql.ErrNoRows {
		return nil, &domain.NotFoundError{Message: "schema not found"}
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}
