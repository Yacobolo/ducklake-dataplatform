package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/sqlrewrite"
)

// CatalogRepoFactory creates per-catalog CatalogRepository instances.
// Matches the interface from repository.CatalogRepoFactory.
type CatalogRepoFactory interface {
	ForCatalog(ctx context.Context, catalogName string) (domain.CatalogRepository, error)
}

// InformationSchemaProvider builds virtual information_schema views from the
// catalog metadata. It intercepts queries to information_schema.* tables
// and returns results aggregated across ALL active catalogs, not just one.
type InformationSchemaProvider struct {
	factory       CatalogRepoFactory
	catalogLister domain.CatalogRegistrationRepository
	catalog       domain.AuthorizationService
}

// NewInformationSchemaProvider creates a new provider.
// factory provides per-catalog CatalogRepository instances.
// catalogLister provides the list of all registered catalogs.
// Use SetAuthorizationService to enable RBAC filtering on information_schema results.
func NewInformationSchemaProvider(factory CatalogRepoFactory, catalogLister domain.CatalogRegistrationRepository) *InformationSchemaProvider {
	return &InformationSchemaProvider{
		factory:       factory,
		catalogLister: catalogLister,
	}
}

// SetAuthorizationService configures the authorization service used to filter
// information_schema results based on the caller's RBAC grants. When nil,
// all rows are visible (no filtering).
func (p *InformationSchemaProvider) SetAuthorizationService(catalog domain.AuthorizationService) {
	p.catalog = catalog
}

// IsInformationSchemaQuery checks if the SQL references information_schema tables.
func IsInformationSchemaQuery(sqlQuery string) bool {
	refs, err := sqlrewrite.ExtractTableRefs(sqlQuery)
	if err != nil || len(refs) == 0 {
		return false
	}

	for _, ref := range refs {
		if !strings.EqualFold(ref.Schema, "information_schema") {
			return false
		}
		if !isSupportedInformationSchemaTable(ref.Name) {
			return false
		}
	}

	return true
}

// activeCatalogs returns all ACTIVE catalog names. If the catalog lister is nil
// or returns an error, falls back to an empty list (graceful degradation).
func (p *InformationSchemaProvider) activeCatalogs(ctx context.Context) []string {
	if p.catalogLister == nil {
		return nil
	}

	page := domain.PageRequest{MaxResults: 10000}
	catalogs, _, err := p.catalogLister.List(ctx, page)
	if err != nil {
		return nil
	}

	var names []string
	for _, c := range catalogs {
		if c.Status == domain.CatalogStatusActive {
			names = append(names, c.Name)
		}
	}
	return names
}

// BuildSchemataRows returns rows for information_schema.schemata across all active catalogs.
func (p *InformationSchemaProvider) BuildSchemataRows(ctx context.Context) ([][]interface{}, []string, error) {
	columns := []string{"catalog_name", "schema_name", "schema_owner", "default_character_set_catalog"}
	var rows [][]interface{}

	for _, catalogName := range p.activeCatalogs(ctx) {
		repo, err := p.factory.ForCatalog(ctx, catalogName)
		if err != nil {
			continue // skip catalogs that fail
		}
		page := domain.PageRequest{MaxResults: 1000}
		schemas, _, err := repo.ListSchemas(ctx, page)
		if err != nil {
			continue // skip catalogs that fail
		}
		for _, s := range schemas {
			rows = append(rows, []interface{}{s.CatalogName, s.Name, s.Owner, nil})
		}
	}
	return rows, columns, nil
}

// BuildTablesRows returns rows for information_schema.tables across all active catalogs.
func (p *InformationSchemaProvider) BuildTablesRows(ctx context.Context) ([][]interface{}, []string, error) {
	columns := []string{"table_catalog", "table_schema", "table_name", "table_type"}
	var rows [][]interface{}

	for _, catalogName := range p.activeCatalogs(ctx) {
		repo, err := p.factory.ForCatalog(ctx, catalogName)
		if err != nil {
			continue
		}
		page := domain.PageRequest{MaxResults: 1000}
		schemas, _, err := repo.ListSchemas(ctx, page)
		if err != nil {
			continue
		}
		for _, s := range schemas {
			tables, _, err := repo.ListTables(ctx, s.Name, page)
			if err != nil {
				continue
			}
			for _, t := range tables {
				rows = append(rows, []interface{}{s.CatalogName, s.Name, t.Name, t.TableType})
			}
		}
	}
	return rows, columns, nil
}

// BuildColumnsRows returns rows for information_schema.columns across all active catalogs.
func (p *InformationSchemaProvider) BuildColumnsRows(ctx context.Context) ([][]interface{}, []string, error) {
	columns := []string{"table_catalog", "table_schema", "table_name", "column_name", "ordinal_position", "data_type"}
	var rows [][]interface{}

	for _, catalogName := range p.activeCatalogs(ctx) {
		repo, err := p.factory.ForCatalog(ctx, catalogName)
		if err != nil {
			continue
		}
		page := domain.PageRequest{MaxResults: 1000}
		schemas, _, err := repo.ListSchemas(ctx, page)
		if err != nil {
			continue
		}
		for _, s := range schemas {
			tables, _, err := repo.ListTables(ctx, s.Name, page)
			if err != nil {
				continue
			}
			for _, t := range tables {
				cols, _, err := repo.ListColumns(ctx, s.Name, t.Name, page)
				if err != nil {
					continue
				}
				for _, c := range cols {
					rows = append(rows, []interface{}{s.CatalogName, s.Name, t.Name, c.Name, c.Position, c.Type})
				}
			}
		}
	}
	return rows, columns, nil
}

// HandleQuery intercepts information_schema queries and returns virtual results.
// principalName is used to filter results based on RBAC grants so that users
// only see metadata for objects they have access to.
func (p *InformationSchemaProvider) HandleQuery(ctx context.Context, db *sql.DB, principalName, sqlQuery string) (*sql.Rows, error) {
	refs, err := sqlrewrite.ExtractTableRefs(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("parse information_schema query: %w", err)
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("unsupported information_schema query")
	}

	tableName := ""
	for _, ref := range refs {
		if !strings.EqualFold(ref.Schema, "information_schema") {
			return nil, fmt.Errorf("unsupported information_schema query")
		}
		if !isSupportedInformationSchemaTable(ref.Name) {
			return nil, fmt.Errorf("unsupported information_schema table")
		}
		if tableName == "" {
			tableName = strings.ToLower(ref.Name)
			continue
		}
		if !strings.EqualFold(tableName, ref.Name) {
			return nil, fmt.Errorf("unsupported information_schema query")
		}
	}

	return p.queryVirtualTable(ctx, db, principalName, tableName)
}

func isSupportedInformationSchemaTable(tableName string) bool {
	switch strings.ToLower(tableName) {
	case "schemata", "tables", "columns":
		return true
	default:
		return false
	}
}

func (p *InformationSchemaProvider) queryVirtualTable(ctx context.Context, db *sql.DB, principalName, table string) (*sql.Rows, error) {
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

	// Filter rows based on the caller's RBAC grants so that users only see
	// metadata for objects they have access to (Issue #38).
	if p.catalog != nil {
		dataRows = p.filterRowsByPrivilege(ctx, principalName, table, dataRows)
	}

	// Materialize metadata rows as *sql.Rows using a pool-level VALUES query.
	// This avoids both the SQL injection risk (user SQL is never executed) and
	// the connection leak from pinned *sql.Conn (no pinned connection needed).
	return materializeAsRows(ctx, db, columns, dataRows)
}

// filterRowsByPrivilege filters information_schema rows based on the caller's grants.
// For schemata: filter by USAGE on schema.
// For tables: filter by SELECT on table (or any privilege).
// For columns: filter by SELECT on parent table.
// Admin users see everything (CheckPrivilege handles admin bypass).
func (p *InformationSchemaProvider) filterRowsByPrivilege(ctx context.Context, principalName, table string, rows [][]interface{}) [][]interface{} {
	var filtered [][]interface{}
	for _, row := range rows {
		if p.isRowVisible(ctx, principalName, table, row) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

// isRowVisible checks whether a single information_schema row is visible to the principal.
func (p *InformationSchemaProvider) isRowVisible(ctx context.Context, principalName, table string, row []interface{}) bool {
	switch table {
	case "schemata":
		// row: [catalog_name, schema_name, schema_owner, default_character_set_catalog]
		if len(row) < 2 {
			return false
		}
		schemaName, _ := row[1].(string)
		// Look up the schema ID via the catalog repo and check USAGE privilege.
		catalogName, _ := row[0].(string)
		repo, err := p.factory.ForCatalog(ctx, catalogName)
		if err != nil {
			return false
		}
		schema, err := repo.GetSchema(ctx, schemaName)
		if err != nil {
			return false
		}
		allowed, err := p.catalog.CheckPrivilege(ctx, principalName, domain.SecurableSchema, strconv.FormatInt(schema.SchemaID, 10), domain.PrivUsage)
		if err != nil {
			return false
		}
		return allowed

	case "tables":
		// row: [table_catalog, table_schema, table_name, table_type]
		if len(row) < 3 {
			return false
		}
		catalogName, _ := row[0].(string)
		schemaName, _ := row[1].(string)
		tableName, _ := row[2].(string)
		repo, err := p.factory.ForCatalog(ctx, catalogName)
		if err != nil {
			return false
		}
		tbl, err := repo.GetTable(ctx, schemaName, tableName)
		if err != nil {
			return false
		}
		allowed, err := p.catalog.CheckPrivilege(ctx, principalName, domain.SecurableTable, strconv.FormatInt(tbl.TableID, 10), domain.PrivSelect)
		if err != nil {
			return false
		}
		return allowed

	case "columns":
		// row: [table_catalog, table_schema, table_name, column_name, ordinal_position, data_type]
		if len(row) < 3 {
			return false
		}
		catalogName, _ := row[0].(string)
		schemaName, _ := row[1].(string)
		tableName, _ := row[2].(string)
		repo, err := p.factory.ForCatalog(ctx, catalogName)
		if err != nil {
			return false
		}
		tbl, err := repo.GetTable(ctx, schemaName, tableName)
		if err != nil {
			return false
		}
		allowed, err := p.catalog.CheckPrivilege(ctx, principalName, domain.SecurableTable, strconv.FormatInt(tbl.TableID, 10), domain.PrivSelect)
		if err != nil {
			return false
		}
		return allowed
	}
	return false
}

// materializeAsRows converts in-memory data rows into a *sql.Rows by running
// a parameterized VALUES query against the connection pool. No temp tables or
// pinned connections are needed, so there is no connection leak risk.
func materializeAsRows(ctx context.Context, db *sql.DB, columns []string, dataRows [][]interface{}) (*sql.Rows, error) {
	if len(dataRows) == 0 {
		// Return an empty result set with the correct columns.
		colDefs := make([]string, len(columns))
		for i, c := range columns {
			colDefs[i] = fmt.Sprintf("NULL::%s AS %s", "VARCHAR", c)
		}
		return db.QueryContext(ctx, fmt.Sprintf("SELECT %s WHERE false", strings.Join(colDefs, ", ")))
	}

	// Build a VALUES clause with parameter placeholders.
	numCols := len(columns)
	placeholders := make([]string, numCols)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	rowPlaceholder := "(" + strings.Join(placeholders, ", ") + ")"

	rowPlaceholders := make([]string, len(dataRows))
	var args []interface{}
	for i, row := range dataRows {
		rowPlaceholders[i] = rowPlaceholder
		args = append(args, row...)
	}

	// Build column aliases for the VALUES clause.
	colAliases := make([]string, numCols)
	copy(colAliases, columns)

	query := fmt.Sprintf(
		"SELECT * FROM (VALUES %s) AS __t(%s)",
		strings.Join(rowPlaceholders, ", "),
		strings.Join(colAliases, ", "),
	)

	return db.QueryContext(ctx, query, args...)
}
