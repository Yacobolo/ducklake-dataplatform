package catalog

import (
	"context"
	"database/sql"
	"fmt"

	dbstore "duck-demo/db/catalog"
)

// Privilege constants matching the Databricks/Hive model.
const (
	PrivSelect        = "SELECT"
	PrivInsert        = "INSERT"
	PrivUpdate        = "UPDATE"
	PrivDelete        = "DELETE"
	PrivUsage         = "USAGE"
	PrivCreateTable   = "CREATE_TABLE"
	PrivCreateSchema  = "CREATE_SCHEMA"
	PrivAllPrivileges = "ALL_PRIVILEGES"
)

// Securable type constants.
const (
	SecurableCatalog = "catalog"
	SecurableSchema  = "schema"
	SecurableTable   = "table"
)

// CatalogID is the sentinel securable_id for catalog-level grants.
const CatalogID int64 = 0

// CatalogService provides permission checking on top of the sqlc-generated queries.
// It reads from the SQLite metastore that also holds DuckLake metadata.
type CatalogService struct {
	db      *sql.DB
	queries *dbstore.Queries
}

// NewCatalogService creates a new CatalogService backed by the given SQLite connection.
func NewCatalogService(db *sql.DB) *CatalogService {
	return &CatalogService{
		db:      db,
		queries: dbstore.New(db),
	}
}

// Queries returns the underlying sqlc Queries for direct CRUD operations.
func (s *CatalogService) Queries() *dbstore.Queries {
	return s.queries
}

// DB returns the underlying database connection.
func (s *CatalogService) DB() *sql.DB {
	return s.db
}

// resolveGroupIDs returns the set of group IDs a principal belongs to,
// including nested groups (transitive closure).
func (s *CatalogService) resolveGroupIDs(ctx context.Context, principalID int64) ([]int64, error) {
	visited := map[int64]bool{}
	queue := []int64{principalID}
	// Seed: get groups where the user is a direct member
	memberType := "user"

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		groups, err := s.queries.GetGroupsForMember(ctx, dbstore.GetGroupsForMemberParams{
			MemberType: memberType,
			MemberID:   current,
		})
		if err != nil {
			return nil, fmt.Errorf("resolve groups for %d: %w", current, err)
		}

		for _, g := range groups {
			if !visited[g.ID] {
				visited[g.ID] = true
				queue = append(queue, g.ID)
			}
		}
		// After first iteration, subsequent items in queue are groups
		memberType = "group"
	}

	ids := make([]int64, 0, len(visited))
	for id := range visited {
		ids = append(ids, id)
	}
	return ids, nil
}

// LookupTableID resolves a table name to its DuckLake table_id and schema_id
// by querying the ducklake_table/ducklake_schema tables directly.
func (s *CatalogService) LookupTableID(ctx context.Context, tableName string) (tableID, schemaID int64, err error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT t.table_id, t.schema_id FROM ducklake_table t
		 WHERE t.table_name = ? AND t.end_snapshot IS NULL`, tableName)
	err = row.Scan(&tableID, &schemaID)
	if err == sql.ErrNoRows {
		return 0, 0, fmt.Errorf("table %q not found in catalog", tableName)
	}
	return
}

// LookupSchemaID resolves a schema name to its DuckLake schema_id.
func (s *CatalogService) LookupSchemaID(ctx context.Context, schemaName string) (int64, error) {
	var schemaID int64
	row := s.db.QueryRowContext(ctx,
		`SELECT schema_id FROM ducklake_schema
		 WHERE schema_name = ? AND end_snapshot IS NULL`, schemaName)
	err := row.Scan(&schemaID)
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("schema %q not found in catalog", schemaName)
	}
	return schemaID, err
}

// hasGrant checks if any of the given identities has a specific grant.
func (s *CatalogService) hasGrant(ctx context.Context, principalID int64, groupIDs []int64, securableType string, securableID int64, privilege string) (bool, error) {
	// Check direct user grant (exact privilege OR ALL_PRIVILEGES)
	cnt, err := s.queries.CheckDirectGrantAny(ctx, dbstore.CheckDirectGrantAnyParams{
		PrincipalID:   principalID,
		PrincipalType: "user",
		SecurableType: securableType,
		SecurableID:   securableID,
		Privilege:     privilege,
	})
	if err != nil {
		return false, err
	}
	if cnt > 0 {
		return true, nil
	}

	// Check group grants
	for _, gid := range groupIDs {
		cnt, err := s.queries.CheckDirectGrantAny(ctx, dbstore.CheckDirectGrantAnyParams{
			PrincipalID:   gid,
			PrincipalType: "group",
			SecurableType: securableType,
			SecurableID:   securableID,
			Privilege:     privilege,
		})
		if err != nil {
			return false, err
		}
		if cnt > 0 {
			return true, nil
		}
	}
	return false, nil
}

// CheckPrivilege determines whether the named principal has the given privilege
// on the specified securable. It implements the Databricks-style permission model:
//  1. Admin bypass
//  2. USAGE gate on parent schema (for table-level checks)
//  3. Walk up hierarchy: table → schema → catalog
//  4. ALL_PRIVILEGES expansion
func (s *CatalogService) CheckPrivilege(ctx context.Context, principalName string, securableType string, securableID int64, privilege string) (bool, error) {
	// Resolve principal
	principal, err := s.queries.GetPrincipalByName(ctx, principalName)
	if err == sql.ErrNoRows {
		return false, fmt.Errorf("principal %q not found", principalName)
	}
	if err != nil {
		return false, err
	}

	// Admin bypass
	if principal.IsAdmin != 0 {
		return true, nil
	}

	// Resolve group memberships
	groupIDs, err := s.resolveGroupIDs(ctx, principal.ID)
	if err != nil {
		return false, err
	}

	return s.checkPrivilegeForIdentities(ctx, principal.ID, groupIDs, securableType, securableID, privilege)
}

func (s *CatalogService) checkPrivilegeForIdentities(ctx context.Context, principalID int64, groupIDs []int64, securableType string, securableID int64, privilege string) (bool, error) {
	switch securableType {
	case SecurableTable:
		return s.checkTablePrivilege(ctx, principalID, groupIDs, securableID, privilege)
	case SecurableSchema:
		return s.checkSchemaPrivilege(ctx, principalID, groupIDs, securableID, privilege)
	case SecurableCatalog:
		return s.hasGrant(ctx, principalID, groupIDs, SecurableCatalog, CatalogID, privilege)
	default:
		return false, fmt.Errorf("unknown securable type: %s", securableType)
	}
}

func (s *CatalogService) checkTablePrivilege(ctx context.Context, principalID int64, groupIDs []int64, tableID int64, privilege string) (bool, error) {
	// Get schema_id for the table (from DuckLake metadata)
	var schemaID int64
	row := s.db.QueryRowContext(ctx,
		`SELECT schema_id FROM ducklake_table WHERE table_id = ? AND end_snapshot IS NULL`, tableID)
	if err := row.Scan(&schemaID); err != nil {
		return false, fmt.Errorf("lookup schema for table %d: %w", tableID, err)
	}

	// USAGE gate: must have USAGE on the schema
	hasUsage, err := s.checkSchemaPrivilege(ctx, principalID, groupIDs, schemaID, PrivUsage)
	if err != nil {
		return false, err
	}
	if !hasUsage {
		return false, nil
	}

	// Check grant on the table itself
	ok, err := s.hasGrant(ctx, principalID, groupIDs, SecurableTable, tableID, privilege)
	if err != nil || ok {
		return ok, err
	}

	// Inherit from schema
	ok, err = s.hasGrant(ctx, principalID, groupIDs, SecurableSchema, schemaID, privilege)
	if err != nil || ok {
		return ok, err
	}

	// Inherit from catalog
	return s.hasGrant(ctx, principalID, groupIDs, SecurableCatalog, CatalogID, privilege)
}

func (s *CatalogService) checkSchemaPrivilege(ctx context.Context, principalID int64, groupIDs []int64, schemaID int64, privilege string) (bool, error) {
	// Check grant on the schema
	ok, err := s.hasGrant(ctx, principalID, groupIDs, SecurableSchema, schemaID, privilege)
	if err != nil || ok {
		return ok, err
	}

	// Inherit from catalog
	return s.hasGrant(ctx, principalID, groupIDs, SecurableCatalog, CatalogID, privilege)
}

// GetEffectiveRowFilter returns the SQL filter expression for a table if the
// principal (or any of their groups) is bound to a row filter for that table.
// Returns nil if no filter applies.
func (s *CatalogService) GetEffectiveRowFilter(ctx context.Context, principalName string, tableID int64) (*string, error) {
	principal, err := s.queries.GetPrincipalByName(ctx, principalName)
	if err != nil {
		return nil, err
	}

	// Admin bypass: no row filters
	if principal.IsAdmin != 0 {
		return nil, nil
	}

	// Check direct user binding
	rf, err := s.queries.GetRowFilterForTableAndPrincipal(ctx, dbstore.GetRowFilterForTableAndPrincipalParams{
		TableID:       tableID,
		PrincipalID:   principal.ID,
		PrincipalType: "user",
	})
	if err == nil {
		return &rf.FilterSql, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	// Check group bindings
	groupIDs, err := s.resolveGroupIDs(ctx, principal.ID)
	if err != nil {
		return nil, err
	}

	for _, gid := range groupIDs {
		rf, err := s.queries.GetRowFilterForTableAndPrincipal(ctx, dbstore.GetRowFilterForTableAndPrincipalParams{
			TableID:       tableID,
			PrincipalID:   gid,
			PrincipalType: "group",
		})
		if err == nil {
			return &rf.FilterSql, nil
		}
		if err != sql.ErrNoRows {
			return nil, err
		}
	}

	return nil, nil
}

// GetEffectiveColumnMasks returns a map of column_name → mask_expression for
// columns the principal should see masked on the given table.
// Only columns where see_original=0 are included.
func (s *CatalogService) GetEffectiveColumnMasks(ctx context.Context, principalName string, tableID int64) (map[string]string, error) {
	principal, err := s.queries.GetPrincipalByName(ctx, principalName)
	if err != nil {
		return nil, err
	}

	// Admin bypass: no masks
	if principal.IsAdmin != 0 {
		return nil, nil
	}

	masks := map[string]string{}

	// Check direct user bindings
	userMasks, err := s.queries.GetColumnMaskForTableAndPrincipal(ctx, dbstore.GetColumnMaskForTableAndPrincipalParams{
		TableID:       tableID,
		PrincipalID:   principal.ID,
		PrincipalType: "user",
	})
	if err != nil {
		return nil, err
	}
	for _, m := range userMasks {
		if m.SeeOriginal == 0 {
			masks[m.ColumnName] = m.MaskExpression
		}
	}

	// Check group bindings
	groupIDs, err := s.resolveGroupIDs(ctx, principal.ID)
	if err != nil {
		return nil, err
	}
	for _, gid := range groupIDs {
		groupMasks, err := s.queries.GetColumnMaskForTableAndPrincipal(ctx, dbstore.GetColumnMaskForTableAndPrincipalParams{
			TableID:       tableID,
			PrincipalID:   gid,
			PrincipalType: "group",
		})
		if err != nil {
			return nil, err
		}
		for _, m := range groupMasks {
			// If user has see_original via direct binding, don't override with mask
			if _, alreadyUnmasked := masks[m.ColumnName]; alreadyUnmasked {
				continue
			}
			if m.SeeOriginal == 0 {
				masks[m.ColumnName] = m.MaskExpression
			}
		}
	}

	if len(masks) == 0 {
		return nil, nil
	}
	return masks, nil
}
