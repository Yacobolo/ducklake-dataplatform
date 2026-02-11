package engine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"duck-demo/internal/domain"
)

// InformationSchemaProvider builds virtual information_schema views from the
// catalog metadata. It intercepts queries to information_schema.* tables
// and returns results from the SQLite metastore rather than DuckDB's own
// information_schema.
type InformationSchemaProvider struct {
	catalog domain.CatalogRepository
}

// NewInformationSchemaProvider creates a new provider.
func NewInformationSchemaProvider(catalog domain.CatalogRepository) *InformationSchemaProvider {
	return &InformationSchemaProvider{catalog: catalog}
}

// IsInformationSchemaQuery checks if the SQL references information_schema tables.
func IsInformationSchemaQuery(sqlQuery string) bool {
	lower := strings.ToLower(sqlQuery)
	return strings.Contains(lower, "information_schema.")
}

// BuildSchemataRows returns rows for information_schema.schemata.
func (p *InformationSchemaProvider) BuildSchemataRows(ctx context.Context) ([][]interface{}, []string, error) {
	page := domain.PageRequest{MaxResults: 1000}
	schemas, _, err := p.catalog.ListSchemas(ctx, page)
	if err != nil {
		return nil, nil, fmt.Errorf("list schemas: %w", err)
	}

	columns := []string{"catalog_name", "schema_name", "schema_owner", "default_character_set_catalog"}
	var rows [][]interface{}
	for _, s := range schemas {
		rows = append(rows, []interface{}{s.CatalogName, s.Name, s.Owner, nil})
	}
	return rows, columns, nil
}

// BuildTablesRows returns rows for information_schema.tables.
func (p *InformationSchemaProvider) BuildTablesRows(ctx context.Context) ([][]interface{}, []string, error) {
	page := domain.PageRequest{MaxResults: 1000}
	schemas, _, err := p.catalog.ListSchemas(ctx, page)
	if err != nil {
		return nil, nil, fmt.Errorf("list schemas: %w", err)
	}

	columns := []string{"table_catalog", "table_schema", "table_name", "table_type"}
	var rows [][]interface{}
	for _, s := range schemas {
		tables, _, err := p.catalog.ListTables(ctx, s.Name, page)
		if err != nil {
			continue
		}
		for _, t := range tables {
			rows = append(rows, []interface{}{s.CatalogName, s.Name, t.Name, t.TableType})
		}
	}
	return rows, columns, nil
}

// BuildColumnsRows returns rows for information_schema.columns.
func (p *InformationSchemaProvider) BuildColumnsRows(ctx context.Context) ([][]interface{}, []string, error) {
	page := domain.PageRequest{MaxResults: 1000}
	schemas, _, err := p.catalog.ListSchemas(ctx, page)
	if err != nil {
		return nil, nil, fmt.Errorf("list schemas: %w", err)
	}

	columns := []string{"table_catalog", "table_schema", "table_name", "column_name", "ordinal_position", "data_type"}
	var rows [][]interface{}
	for _, s := range schemas {
		tables, _, err := p.catalog.ListTables(ctx, s.Name, page)
		if err != nil {
			continue
		}
		for _, t := range tables {
			cols, _, err := p.catalog.ListColumns(ctx, s.Name, t.Name, page)
			if err != nil {
				continue
			}
			for _, c := range cols {
				rows = append(rows, []interface{}{s.CatalogName, s.Name, t.Name, c.Name, c.Position, c.Type})
			}
		}
	}
	return rows, columns, nil
}

// HandleQuery intercepts information_schema queries and returns virtual results.
// It creates a temporary DuckDB table with the metadata and then runs the original
// query against it.
func (p *InformationSchemaProvider) HandleQuery(ctx context.Context, db *sql.DB, sqlQuery string) (*sql.Rows, error) {
	lower := strings.ToLower(sqlQuery)

	// Create the appropriate virtual table
	if strings.Contains(lower, "information_schema.schemata") {
		return p.queryVirtualTable(ctx, db, sqlQuery, "schemata")
	}
	if strings.Contains(lower, "information_schema.tables") {
		return p.queryVirtualTable(ctx, db, sqlQuery, "tables")
	}
	if strings.Contains(lower, "information_schema.columns") {
		return p.queryVirtualTable(ctx, db, sqlQuery, "columns")
	}

	return nil, fmt.Errorf("unsupported information_schema table")
}

func (p *InformationSchemaProvider) queryVirtualTable(ctx context.Context, db *sql.DB, sqlQuery, table string) (*sql.Rows, error) {
	var dataRows [][]interface{}
	var columns []string
	var err error

	switch table {
	case "schemata":
		dataRows, columns, err = p.BuildSchemataRows(ctx)
	case "tables":
		dataRows, columns, err = p.BuildTablesRows(ctx)
	case "columns":
		dataRows, columns, err = p.BuildColumnsRows(ctx)
	default:
		return nil, fmt.Errorf("unsupported table: %s", table)
	}
	if err != nil {
		return nil, err
	}

	// Pin a single connection for the entire temp table lifecycle (CREATE, INSERT,
	// SELECT) to avoid DuckDB connection pool issues — temp tables are per-connection.
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}

	// Build CREATE TEMP TABLE + INSERT statements with unique name to avoid
	// race conditions between concurrent information_schema queries.
	tempName := fmt.Sprintf("__info_schema_%s_%s", table, randomSuffix())

	// Build column defs
	colDefs := make([]string, len(columns))
	for i, c := range columns {
		colDefs[i] = fmt.Sprintf("%s VARCHAR", c)
	}
	createSQL := fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s)", tempName, strings.Join(colDefs, ", "))
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		conn.Close()
		return nil, fmt.Errorf("create temp table: %w", err)
	}

	// Insert rows
	if len(dataRows) > 0 {
		placeholders := make([]string, len(columns))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		insertSQL := fmt.Sprintf("INSERT INTO %s VALUES (%s)", tempName, strings.Join(placeholders, ", "))
		for _, row := range dataRows {
			if _, err := conn.ExecContext(ctx, insertSQL, row...); err != nil {
				conn.Close()
				return nil, fmt.Errorf("insert row: %w", err)
			}
		}
	}

	// Rewrite the query to use the temp table (case-insensitive).
	rewritten := replaceAllCaseInsensitive(sqlQuery, "information_schema."+table, tempName)

	rows, err := conn.QueryContext(ctx, rewritten)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("execute information_schema query: %w", err)
	}

	// The *sql.Rows keeps the connection pinned until rows.Close().
	// The temp table lives on this connection and will be cleaned up when
	// the connection is eventually closed or reused.
	return rows, nil
}

// replaceAllCaseInsensitive replaces all occurrences of old (case-insensitive)
// with the replacement string. This ensures mixed-case references like
// "Information_Schema.Tables" are handled correctly.
func replaceAllCaseInsensitive(s, old, replacement string) string {
	re := regexp.MustCompile("(?i)" + regexp.QuoteMeta(old))
	return re.ReplaceAllLiteralString(s, replacement)
}

// randomSuffix generates a short random hex string for unique temp table names.
func randomSuffix() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}
