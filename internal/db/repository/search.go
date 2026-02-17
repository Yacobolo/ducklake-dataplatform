package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

// SearchRepo implements domain.SearchRepository using SQLite.
// It requires two DB connections:
//   - metaDB: the catalog's DuckLake metastore (ducklake_schema, ducklake_table, ducklake_column)
//   - controlDB: the control plane DB (catalog_metadata, column_metadata, tags, tag_assignments)
//
// When metaDB == controlDB (legacy single-DB mode), all queries run against one DB.
type SearchRepo struct {
	metaDB    *sql.DB
	controlDB *sql.DB
}

// NewSearchRepo creates a new SearchRepo.
// Both metaDB and controlDB are required. In single-DB mode, pass the same *sql.DB for both.
func NewSearchRepo(metaDB, controlDB *sql.DB) *SearchRepo {
	return &SearchRepo{metaDB: metaDB, controlDB: controlDB}
}

// Search performs a full-text search across schemas, tables, columns, and macros.
// Name-based searches query the catalog metastore (ducklake_* tables).
// Comment/tag/property searches query the control plane (catalog_metadata, tags).
// Results from both sources are merged and deduplicated.
func (r *SearchRepo) Search(ctx context.Context, query string, objectType *string, maxResults int, offset int) ([]domain.SearchResult, int64, error) {
	return r.searchMultiDB(ctx, query, objectType, maxResults, offset)
}

// searchMultiDB performs a search when ducklake tables and governance tables
// live in separate databases. Name-based searches query the metastore;
// comment/tag/property searches query the control plane.
func (r *SearchRepo) searchMultiDB(ctx context.Context, query string, objectType *string, maxResults int, offset int) ([]domain.SearchResult, int64, error) {
	likePattern := "%" + strings.ToLower(query) + "%"

	// Phase 1: Name-based search from metastore (ducklake_* tables)
	nameResults, err := r.searchNamesMeta(ctx, likePattern, objectType)
	if err != nil {
		return nil, 0, fmt.Errorf("search names from metastore: %w", err)
	}

	// Phase 2: Comment/property search from control plane (catalog_metadata)
	govResults, err := r.searchGovernanceControl(ctx, likePattern, objectType)
	if err != nil {
		return nil, 0, fmt.Errorf("search governance from control plane: %w", err)
	}

	// Merge and deduplicate (name match takes priority)
	all := mergeSearchResults(nameResults, govResults)
	total := int64(len(all))

	// Apply pagination
	if offset >= len(all) {
		return nil, total, nil
	}
	end := offset + maxResults
	if end > len(all) {
		end = len(all)
	}

	return all[offset:end], total, nil
}

// searchNamesMeta searches ducklake_* tables for name matches.
func (r *SearchRepo) searchNamesMeta(ctx context.Context, likePattern string, objectType *string) ([]domain.SearchResult, error) {
	var unions []string
	var args []interface{}

	if objectType == nil || *objectType == "schema" {
		unions = append(unions, `
			SELECT 'schema' as type, ds.schema_name as name, NULL as schema_name, NULL as table_name,
				NULL as comment, 'name' as match_field
			FROM ducklake_schema ds
			WHERE ds.end_snapshot IS NULL AND LOWER(ds.schema_name) LIKE ?`)
		args = append(args, likePattern)
	}

	if objectType == nil || *objectType == "table" {
		unions = append(unions, `
			SELECT 'table' as type, dt.table_name as name, ds.schema_name as schema_name, NULL as table_name,
				NULL as comment, 'name' as match_field
			FROM ducklake_table dt
			JOIN ducklake_schema ds ON dt.schema_id = ds.schema_id AND ds.end_snapshot IS NULL
			WHERE dt.end_snapshot IS NULL AND LOWER(dt.table_name) LIKE ?`)
		args = append(args, likePattern)
	}

	if objectType == nil || *objectType == "column" {
		unions = append(unions, `
			SELECT 'column' as type, dc.column_name as name, ds.schema_name as schema_name, dt.table_name as table_name,
				NULL as comment, 'name' as match_field
			FROM ducklake_column dc
			JOIN ducklake_table dt ON dc.table_id = dt.table_id AND dt.end_snapshot IS NULL
			JOIN ducklake_schema ds ON dt.schema_id = ds.schema_id AND ds.end_snapshot IS NULL
			WHERE dc.end_snapshot IS NULL AND LOWER(dc.column_name) LIKE ?`)
		args = append(args, likePattern)
	}

	if objectType == nil || *objectType == "macro" {
		unions = append(unions, `
			SELECT 'macro' as type, m.name as name, NULL as schema_name, NULL as table_name,
				m.description as comment, 'name' as match_field
			FROM macros m
			WHERE LOWER(m.name) LIKE ?`)
		args = append(args, likePattern)
	}

	if len(unions) == 0 {
		return nil, nil
	}

	return r.execSearchQuery(ctx, r.metaDB, strings.Join(unions, " UNION ALL "), args)
}

// searchGovernanceControl searches the control plane for comment/property/tag matches.
func (r *SearchRepo) searchGovernanceControl(ctx context.Context, likePattern string, objectType *string) ([]domain.SearchResult, error) {
	var unions []string
	var args []interface{}

	// Schemas by comment
	if objectType == nil || *objectType == "schema" {
		unions = append(unions, `
			SELECT 'schema' as type, cm.securable_name as name, NULL as schema_name, NULL as table_name,
				cm.comment as comment, 'comment' as match_field
			FROM catalog_metadata cm
			WHERE cm.securable_type = 'schema' AND cm.deleted_at IS NULL
			AND LOWER(cm.comment) LIKE ?`)
		args = append(args, likePattern)

		// Schemas by property
		unions = append(unions, `
			SELECT 'schema' as type, cm.securable_name as name, NULL as schema_name, NULL as table_name,
				cm.comment as comment, 'property' as match_field
			FROM catalog_metadata cm
			WHERE cm.securable_type = 'schema' AND cm.deleted_at IS NULL
			AND LOWER(cm.properties) LIKE ? AND (cm.comment IS NULL OR LOWER(cm.comment) NOT LIKE ?)`)
		args = append(args, likePattern, likePattern)
	}

	if objectType == nil || *objectType == "macro" {
		unions = append(unions, `
			SELECT 'macro' as type, m.name as name, NULL as schema_name, NULL as table_name,
				m.description as comment, 'comment' as match_field
			FROM macros m
			WHERE LOWER(m.description) LIKE ?`)
		args = append(args, likePattern)

		unions = append(unions, `
			SELECT 'macro' as type, m.name as name, NULL as schema_name, NULL as table_name,
				m.description as comment, 'property' as match_field
			FROM macros m
			WHERE LOWER(m.properties) LIKE ? AND (m.description IS NULL OR LOWER(m.description) NOT LIKE ?)`)
		args = append(args, likePattern, likePattern)

		unions = append(unions, `
			SELECT 'macro' as type, m.name as name, NULL as schema_name, NULL as table_name,
				m.description as comment, 'tag' as match_field
			FROM macros m
			JOIN tag_assignments ta ON ta.securable_type = 'macro' AND ta.securable_id = m.id
			JOIN tags t ON t.id = ta.tag_id
			WHERE (LOWER(t.key) LIKE ? OR LOWER(COALESCE(t.value, '')) LIKE ?)`)
		args = append(args, likePattern, likePattern)
	}

	// Tables by comment
	if objectType == nil || *objectType == "table" {
		unions = append(unions, `
			SELECT 'table' as type,
				SUBSTR(cm.securable_name, INSTR(cm.securable_name, '.') + 1) as name,
				SUBSTR(cm.securable_name, 1, INSTR(cm.securable_name, '.') - 1) as schema_name,
				NULL as table_name,
				cm.comment as comment, 'comment' as match_field
			FROM catalog_metadata cm
			WHERE cm.securable_type = 'table' AND cm.deleted_at IS NULL
			AND LOWER(cm.comment) LIKE ?`)
		args = append(args, likePattern)

		// Tables by property
		unions = append(unions, `
			SELECT 'table' as type,
				SUBSTR(cm.securable_name, INSTR(cm.securable_name, '.') + 1) as name,
				SUBSTR(cm.securable_name, 1, INSTR(cm.securable_name, '.') - 1) as schema_name,
				NULL as table_name,
				cm.comment as comment, 'property' as match_field
			FROM catalog_metadata cm
			WHERE cm.securable_type = 'table' AND cm.deleted_at IS NULL
			AND LOWER(cm.properties) LIKE ? AND (cm.comment IS NULL OR LOWER(cm.comment) NOT LIKE ?)`)
		args = append(args, likePattern, likePattern)
	}

	// Columns by comment
	if objectType == nil || *objectType == "column" {
		unions = append(unions, `
			SELECT 'column' as type, colm.column_name as name,
				SUBSTR(colm.table_securable_name, 1, INSTR(colm.table_securable_name, '.') - 1) as schema_name,
				SUBSTR(colm.table_securable_name, INSTR(colm.table_securable_name, '.') + 1) as table_name,
				colm.comment as comment, 'comment' as match_field
			FROM column_metadata colm
			WHERE LOWER(colm.comment) LIKE ?`)
		args = append(args, likePattern)
	}

	// Tags (all types) — tags are in control plane.
	// When both DB handles point to the same SQLite DB, enrich tag matches with
	// object names via ducklake_* tables.
	if r.metaDB == r.controlDB {
		if objectType == nil || *objectType == "schema" {
			unions = append(unions, `
				SELECT 'schema' as type, ds.schema_name as name, NULL as schema_name, NULL as table_name,
					cm.comment as comment, 'tag' as match_field
				FROM tag_assignments ta
				JOIN tags t ON t.id = ta.tag_id
				JOIN ducklake_schema ds ON ta.securable_type = 'schema' AND ta.securable_id = CAST(ds.schema_id AS TEXT)
				LEFT JOIN catalog_metadata cm ON cm.securable_type = 'schema' AND cm.securable_name = ds.schema_name AND cm.deleted_at IS NULL
				WHERE ds.end_snapshot IS NULL AND (LOWER(t.key) LIKE ? OR LOWER(COALESCE(t.value, '')) LIKE ?)`)
			args = append(args, likePattern, likePattern)
		}

		if objectType == nil || *objectType == "table" {
			unions = append(unions, `
				SELECT 'table' as type, dt.table_name as name, ds.schema_name as schema_name, NULL as table_name,
					cm.comment as comment, 'tag' as match_field
				FROM tag_assignments ta
				JOIN tags t ON t.id = ta.tag_id
				JOIN ducklake_table dt ON ta.securable_type = 'table' AND ta.securable_id = CAST(dt.table_id AS TEXT)
				JOIN ducklake_schema ds ON dt.schema_id = ds.schema_id AND ds.end_snapshot IS NULL
				LEFT JOIN catalog_metadata cm ON cm.securable_type = 'table' AND cm.securable_name = ds.schema_name || '.' || dt.table_name AND cm.deleted_at IS NULL
				WHERE dt.end_snapshot IS NULL AND (LOWER(t.key) LIKE ? OR LOWER(COALESCE(t.value, '')) LIKE ?)`)
			args = append(args, likePattern, likePattern)
		}

		if objectType == nil || *objectType == "column" {
			unions = append(unions, `
				SELECT 'column' as type, dc.column_name as name, ds.schema_name as schema_name, dt.table_name as table_name,
					colm.comment as comment, 'tag' as match_field
				FROM tag_assignments ta
				JOIN tags t ON t.id = ta.tag_id
				JOIN ducklake_table dt ON ta.securable_type = 'column' AND ta.securable_id = CAST(dt.table_id AS TEXT)
				JOIN ducklake_schema ds ON dt.schema_id = ds.schema_id AND ds.end_snapshot IS NULL
				JOIN ducklake_column dc ON dc.table_id = dt.table_id AND dc.column_name = ta.column_name AND dc.end_snapshot IS NULL
				LEFT JOIN column_metadata colm ON colm.table_securable_name = ds.schema_name || '.' || dt.table_name AND colm.column_name = dc.column_name
				WHERE dt.end_snapshot IS NULL AND (LOWER(t.key) LIKE ? OR LOWER(COALESCE(t.value, '')) LIKE ?)`)
			args = append(args, likePattern, likePattern)
		}
	}

	if len(unions) == 0 {
		return nil, nil
	}

	return r.execSearchQuery(ctx, r.controlDB, strings.Join(unions, " UNION ALL "), args)
}

// execSearchQuery runs a search SQL query and scans results.
func (r *SearchRepo) execSearchQuery(ctx context.Context, db *sql.DB, query string, args []interface{}) ([]domain.SearchResult, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("search query: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	var results []domain.SearchResult
	for rows.Next() {
		var sr domain.SearchResult
		var schemaName, tableName, comment sql.NullString
		if err := rows.Scan(&sr.Type, &sr.Name, &schemaName, &tableName, &comment, &sr.MatchField); err != nil {
			return nil, fmt.Errorf("scan search result: %w", err)
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate search results: %w", err)
	}
	return results, nil
}

// mergeSearchResults merges name results and governance results, deduplicating
// by (type, name, schema_name, table_name). Name matches take priority.
func mergeSearchResults(nameResults, govResults []domain.SearchResult) []domain.SearchResult {
	type key struct {
		Type       string
		Name       string
		SchemaName string
		TableName  string
	}

	seen := make(map[key]bool)
	var merged []domain.SearchResult

	// Add name results first (higher priority)
	for _, r := range nameResults {
		k := key{Type: r.Type, Name: r.Name}
		if r.SchemaName != nil {
			k.SchemaName = *r.SchemaName
		}
		if r.TableName != nil {
			k.TableName = *r.TableName
		}
		seen[k] = true
		merged = append(merged, r)
	}

	// Add governance results that weren't already matched by name
	for _, r := range govResults {
		k := key{Type: r.Type, Name: r.Name}
		if r.SchemaName != nil {
			k.SchemaName = *r.SchemaName
		}
		if r.TableName != nil {
			k.TableName = *r.TableName
		}
		if !seen[k] {
			seen[k] = true
			merged = append(merged, r)
		}
	}

	return merged
}
