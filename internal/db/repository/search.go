package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

type SearchRepo struct {
	db *sql.DB
}

func NewSearchRepo(db *sql.DB) *SearchRepo {
	return &SearchRepo{db: db}
}

func (r *SearchRepo) Search(ctx context.Context, query string, objectType *string, maxResults int, offset int) ([]domain.SearchResult, int64, error) {
	likePattern := "%" + strings.ToLower(query) + "%"

	var unions []string
	var args []interface{}

	// Search schemas
	if objectType == nil || *objectType == "schema" {
		unions = append(unions, `
			SELECT 'schema' as type, ds.schema_name as name, NULL as schema_name, NULL as table_name,
				cm.comment as comment, 'name' as match_field
			FROM ducklake_schema ds
			LEFT JOIN catalog_metadata cm ON cm.securable_type = 'schema' AND cm.securable_name = ds.schema_name
			WHERE LOWER(ds.schema_name) LIKE ?`)
		args = append(args, likePattern)
	}

	// Search tables
	if objectType == nil || *objectType == "table" {
		unions = append(unions, `
			SELECT 'table' as type, dt.table_name as name, ds.schema_name as schema_name, NULL as table_name,
				cm.comment as comment, 'name' as match_field
			FROM ducklake_table dt
			JOIN ducklake_schema ds ON dt.schema_id = ds.schema_id
			LEFT JOIN catalog_metadata cm ON cm.securable_type = 'table' AND cm.securable_name = ds.schema_name || '.' || dt.table_name
			WHERE LOWER(dt.table_name) LIKE ?`)
		args = append(args, likePattern)
	}

	// Search columns
	if objectType == nil || *objectType == "column" {
		unions = append(unions, `
			SELECT 'column' as type, dc.column_name as name, ds.schema_name as schema_name, dt.table_name as table_name,
				NULL as comment, 'name' as match_field
			FROM ducklake_column dc
			JOIN ducklake_table dt ON dc.table_id = dt.table_id
			JOIN ducklake_schema ds ON dt.schema_id = ds.schema_id
			WHERE LOWER(dc.column_name) LIKE ?`)
		args = append(args, likePattern)
	}

	if len(unions) == 0 {
		return nil, 0, nil
	}

	fullQuery := strings.Join(unions, " UNION ALL ") + fmt.Sprintf(" LIMIT %d OFFSET %d", maxResults, offset)

	rows, err := r.db.QueryContext(ctx, fullQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("search query: %w", err)
	}
	defer rows.Close()

	var results []domain.SearchResult
	for rows.Next() {
		var sr domain.SearchResult
		var schemaName, tableName, comment sql.NullString
		if err := rows.Scan(&sr.Type, &sr.Name, &schemaName, &tableName, &comment, &sr.MatchField); err != nil {
			return nil, 0, fmt.Errorf("scan search result: %w", err)
		}
		if schemaName.Valid {
			sr.SchemaName = &schemaName.String
		}
		if tableName.Valid {
			sr.TableName = &tableName.String
		}
		if comment.Valid {
			sr.Comment = &comment.String
		}
		results = append(results, sr)
	}

	// Count query
	countQuery := "SELECT COUNT(*) FROM (" + strings.Join(unions, " UNION ALL ") + ")"
	var total int64
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("search count: %w", err)
	}

	return results, total, nil
}
