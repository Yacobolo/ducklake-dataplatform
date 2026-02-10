package service

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
)

// Re-export domain constants for backward compatibility during migration.
const (
	PrivSelect        = domain.PrivSelect
	PrivInsert        = domain.PrivInsert
	PrivUpdate        = domain.PrivUpdate
	PrivDelete        = domain.PrivDelete
	PrivUsage         = domain.PrivUsage
	PrivCreateTable   = domain.PrivCreateTable
	PrivCreateSchema  = domain.PrivCreateSchema
	PrivAllPrivileges = domain.PrivAllPrivileges

	SecurableCatalog = domain.SecurableCatalog
	SecurableSchema  = domain.SecurableSchema
	SecurableTable   = domain.SecurableTable

	CatalogID = domain.CatalogID
)

// AuthorizationService provides permission checking using domain repository interfaces.
// It implements the domain.AuthorizationService interface.
type AuthorizationService struct {
	principals    domain.PrincipalRepository
	groups        domain.GroupRepository
	grants        domain.GrantRepository
	rowFilters    domain.RowFilterRepository
	columnMasks   domain.ColumnMaskRepository
	introspection domain.IntrospectionRepository
}

// NewAuthorizationService creates a new AuthorizationService backed by domain repositories.
func NewAuthorizationService(
	principals domain.PrincipalRepository,
	groups domain.GroupRepository,
	grants domain.GrantRepository,
	rowFilters domain.RowFilterRepository,
	columnMasks domain.ColumnMaskRepository,
	introspection domain.IntrospectionRepository,
) *AuthorizationService {
	return &AuthorizationService{
		principals:    principals,
		groups:        groups,
		grants:        grants,
		rowFilters:    rowFilters,
		columnMasks:   columnMasks,
		introspection: introspection,
	}
}

// resolveGroupIDs returns the set of group IDs a principal belongs to,
// including nested groups (transitive closure).
func (s *AuthorizationService) resolveGroupIDs(ctx context.Context, principalID int64) ([]int64, error) {
	visited := map[int64]bool{}
	queue := []int64{principalID}
	memberType := "user"

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		groups, err := s.groups.GetGroupsForMember(ctx, memberType, current)
		if err != nil {
			return nil, fmt.Errorf("resolve groups for %d: %w", current, err)
		}

		for _, g := range groups {
			if !visited[g.ID] {
				visited[g.ID] = true
				queue = append(queue, g.ID)
			}
		}
		memberType = "group"
	}

	ids := make([]int64, 0, len(visited))
	for id := range visited {
		ids = append(ids, id)
	}
	return ids, nil
}

// LookupTableID resolves a table name to its DuckLake table_id and schema_id.
func (s *AuthorizationService) LookupTableID(ctx context.Context, tableName string) (tableID, schemaID int64, err error) {
	t, err := s.introspection.GetTableByName(ctx, tableName)
	if err != nil {
		return 0, 0, fmt.Errorf("table %q not found in catalog", tableName)
	}
	return t.ID, t.SchemaID, nil
}

// LookupSchemaID resolves a schema name to its DuckLake schema_id.
func (s *AuthorizationService) LookupSchemaID(ctx context.Context, schemaName string) (int64, error) {
	sch, err := s.introspection.GetSchemaByName(ctx, schemaName)
	if err != nil {
		return 0, fmt.Errorf("schema %q not found in catalog", schemaName)
	}
	return sch.ID, nil
}

// hasGrant checks if any of the given identities has a specific grant.
func (s *AuthorizationService) hasGrant(ctx context.Context, principalID int64, groupIDs []int64, securableType string, securableID int64, privilege string) (bool, error) {
	// Check direct user grant
	ok, err := s.grants.HasPrivilege(ctx, principalID, "user", securableType, securableID, privilege)
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}

	// Check group grants
	for _, gid := range groupIDs {
		ok, err := s.grants.HasPrivilege(ctx, gid, "group", securableType, securableID, privilege)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

// CheckPrivilege determines whether the named principal has the given privilege
// on the specified securable. It implements the Databricks-style permission model:
//  1. Admin bypass
//  2. USAGE gate on parent schema (for table-level checks)
//  3. Walk up hierarchy: table -> schema -> catalog
//  4. ALL_PRIVILEGES expansion
func (s *AuthorizationService) CheckPrivilege(ctx context.Context, principalName string, securableType string, securableID int64, privilege string) (bool, error) {
	principal, err := s.principals.GetByName(ctx, principalName)
	if err != nil {
		return false, fmt.Errorf("principal %q not found", principalName)
	}

	// Admin bypass
	if principal.IsAdmin {
		return true, nil
	}

	// Resolve group memberships
	groupIDs, err := s.resolveGroupIDs(ctx, principal.ID)
	if err != nil {
		return false, err
	}

	return s.checkPrivilegeForIdentities(ctx, principal.ID, groupIDs, securableType, securableID, privilege)
}

func (s *AuthorizationService) checkPrivilegeForIdentities(ctx context.Context, principalID int64, groupIDs []int64, securableType string, securableID int64, privilege string) (bool, error) {
	switch securableType {
	case domain.SecurableTable:
		return s.checkTablePrivilege(ctx, principalID, groupIDs, securableID, privilege)
	case domain.SecurableSchema:
		return s.checkSchemaPrivilege(ctx, principalID, groupIDs, securableID, privilege)
	case domain.SecurableCatalog:
		return s.hasGrant(ctx, principalID, groupIDs, domain.SecurableCatalog, domain.CatalogID, privilege)
	default:
		return false, fmt.Errorf("unknown securable type: %s", securableType)
	}
}

func (s *AuthorizationService) checkTablePrivilege(ctx context.Context, principalID int64, groupIDs []int64, tableID int64, privilege string) (bool, error) {
	// Get schema_id for the table
	table, err := s.introspection.GetTable(ctx, tableID)
	if err != nil {
		return false, fmt.Errorf("lookup schema for table %d: %w", tableID, err)
	}
	schemaID := table.SchemaID

	// USAGE gate: must have USAGE on the schema
	hasUsage, err := s.checkSchemaPrivilege(ctx, principalID, groupIDs, schemaID, domain.PrivUsage)
	if err != nil {
		return false, err
	}
	if !hasUsage {
		return false, nil
	}

	// Check grant on the table itself
	ok, err := s.hasGrant(ctx, principalID, groupIDs, domain.SecurableTable, tableID, privilege)
	if err != nil || ok {
		return ok, err
	}

	// Inherit from schema
	ok, err = s.hasGrant(ctx, principalID, groupIDs, domain.SecurableSchema, schemaID, privilege)
	if err != nil || ok {
		return ok, err
	}

	// Inherit from catalog
	return s.hasGrant(ctx, principalID, groupIDs, domain.SecurableCatalog, domain.CatalogID, privilege)
}

func (s *AuthorizationService) checkSchemaPrivilege(ctx context.Context, principalID int64, groupIDs []int64, schemaID int64, privilege string) (bool, error) {
	ok, err := s.hasGrant(ctx, principalID, groupIDs, domain.SecurableSchema, schemaID, privilege)
	if err != nil || ok {
		return ok, err
	}
	return s.hasGrant(ctx, principalID, groupIDs, domain.SecurableCatalog, domain.CatalogID, privilege)
}

// GetEffectiveRowFilters returns all SQL filter expressions for a table that
// apply to the principal (or any of their groups). Returns nil if no filters apply.
func (s *AuthorizationService) GetEffectiveRowFilters(ctx context.Context, principalName string, tableID int64) ([]string, error) {
	principal, err := s.principals.GetByName(ctx, principalName)
	if err != nil {
		return nil, err
	}

	// Admin bypass
	if principal.IsAdmin {
		return nil, nil
	}

	seen := map[int64]bool{}
	var filters []string

	// Check direct user bindings
	userFilters, err := s.rowFilters.GetForTableAndPrincipal(ctx, tableID, principal.ID, "user")
	if err != nil {
		return nil, err
	}
	for _, rf := range userFilters {
		if !seen[rf.ID] {
			seen[rf.ID] = true
			filters = append(filters, rf.FilterSQL)
		}
	}

	// Check group bindings
	groupIDs, err := s.resolveGroupIDs(ctx, principal.ID)
	if err != nil {
		return nil, err
	}

	for _, gid := range groupIDs {
		groupFilters, err := s.rowFilters.GetForTableAndPrincipal(ctx, tableID, gid, "group")
		if err != nil {
			return nil, err
		}
		for _, rf := range groupFilters {
			if !seen[rf.ID] {
				seen[rf.ID] = true
				filters = append(filters, rf.FilterSQL)
			}
		}
	}

	if len(filters) == 0 {
		return nil, nil
	}
	return filters, nil
}

// GetEffectiveColumnMasks returns a map of column_name -> mask_expression for
// columns the principal should see masked on the given table.
func (s *AuthorizationService) GetEffectiveColumnMasks(ctx context.Context, principalName string, tableID int64) (map[string]string, error) {
	principal, err := s.principals.GetByName(ctx, principalName)
	if err != nil {
		return nil, err
	}

	// Admin bypass
	if principal.IsAdmin {
		return nil, nil
	}

	masks := map[string]string{}

	// Check direct user bindings
	userMasks, err := s.columnMasks.GetForTableAndPrincipal(ctx, tableID, principal.ID, "user")
	if err != nil {
		return nil, err
	}
	for _, m := range userMasks {
		if !m.SeeOriginal {
			masks[m.ColumnName] = m.MaskExpression
		}
	}

	// Check group bindings
	groupIDs, err := s.resolveGroupIDs(ctx, principal.ID)
	if err != nil {
		return nil, err
	}
	for _, gid := range groupIDs {
		groupMasks, err := s.columnMasks.GetForTableAndPrincipal(ctx, tableID, gid, "group")
		if err != nil {
			return nil, err
		}
		for _, m := range groupMasks {
			if _, alreadyUnmasked := masks[m.ColumnName]; alreadyUnmasked {
				continue
			}
			if !m.SeeOriginal {
				masks[m.ColumnName] = m.MaskExpression
			}
		}
	}

	if len(masks) == 0 {
		return nil, nil
	}
	return masks, nil
}
