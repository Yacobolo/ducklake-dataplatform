package security

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
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
	extTableRepo  domain.ExternalTableRepository
}

// NewAuthorizationService creates a new AuthorizationService backed by domain repositories.
func NewAuthorizationService(
	principals domain.PrincipalRepository,
	groups domain.GroupRepository,
	grants domain.GrantRepository,
	rowFilters domain.RowFilterRepository,
	columnMasks domain.ColumnMaskRepository,
	introspection domain.IntrospectionRepository,
	extTableRepo domain.ExternalTableRepository,
) *AuthorizationService {
	return &AuthorizationService{
		principals:    principals,
		groups:        groups,
		grants:        grants,
		rowFilters:    rowFilters,
		columnMasks:   columnMasks,
		introspection: introspection,
		extTableRepo:  extTableRepo,
	}
}

// resolveGroupIDs returns the set of group IDs a principal belongs to,
// including nested groups (transitive closure).
func (s *AuthorizationService) resolveGroupIDs(ctx context.Context, principalID string) ([]string, error) {
	visited := map[string]bool{}
	queue := []string{principalID}
	memberType := "user"

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		groups, err := s.groups.GetGroupsForMember(ctx, memberType, current)
		if err != nil {
			return nil, fmt.Errorf("resolve groups for %s: %w", current, err)
		}

		for _, g := range groups {
			if !visited[g.ID] {
				visited[g.ID] = true
				queue = append(queue, g.ID)
			}
		}
		memberType = "group"
	}

	ids := make([]string, 0, len(visited))
	for id := range visited {
		ids = append(ids, id)
	}
	return ids, nil
}

// LookupTableID resolves a table name to its table_id and schema_id.
// For external tables, isExternal is true.
func (s *AuthorizationService) LookupTableID(ctx context.Context, tableName string) (tableID, schemaID string, isExternal bool, err error) {
	catalogName, schemaName, bareTableName := splitTableReference(tableName)
	_ = catalogName // catalog is not currently used by metastore lookups.

	if schemaName != "" {
		t, lookupErr := s.lookupManagedTableBySchema(ctx, schemaName, bareTableName)
		if lookupErr == nil {
			return t.ID, t.SchemaID, false, nil
		}

		if s.extTableRepo != nil {
			et, extErr := s.extTableRepo.GetByName(ctx, schemaName, bareTableName)
			if extErr == nil {
				sch, schErr := s.introspection.GetSchemaByName(ctx, et.SchemaName)
				if schErr == nil {
					return et.ID, sch.ID, true, nil
				}
				return et.ID, "", true, nil
			}
		}

		return "", "", false, fmt.Errorf("table %q not found in schema %q", bareTableName, schemaName)
	}

	// Try DuckLake first
	t, err := s.introspection.GetTableByName(ctx, bareTableName)
	if err == nil {
		return t.ID, t.SchemaID, false, nil
	}
	var notFoundErr *domain.NotFoundError
	if err != nil && !errors.As(err, &notFoundErr) {
		return "", "", false, fmt.Errorf("lookup table %q: %w", bareTableName, err)
	}

	// Fall back to external tables
	if s.extTableRepo != nil {
		et, extErr := s.extTableRepo.GetByTableName(ctx, bareTableName)
		if extErr == nil {
			// Resolve schema ID via introspection
			sch, schErr := s.introspection.GetSchemaByName(ctx, et.SchemaName)
			if schErr == nil {
				return et.ID, sch.ID, true, nil
			}
			// Schema exists in external table metadata but not in DuckLake;
			// return the external table ID with empty schema ID
			return et.ID, "", true, nil
		}
	}

	return "", "", false, fmt.Errorf("table %q not found in catalog", bareTableName)
}

func (s *AuthorizationService) lookupManagedTableBySchema(ctx context.Context, schemaName, tableName string) (*domain.Table, error) {
	sch, err := s.introspection.GetSchemaByName(ctx, schemaName)
	if err != nil {
		return nil, fmt.Errorf("schema %q not found", schemaName)
	}

	tables, _, err := s.introspection.ListTables(ctx, sch.ID, domain.PageRequest{MaxResults: 10000})
	if err != nil {
		return nil, fmt.Errorf("list tables for schema %q: %w", schemaName, err)
	}
	for _, table := range tables {
		if table.Name == tableName {
			matched := table
			return &matched, nil
		}
	}

	return nil, fmt.Errorf("table %q not found in schema %q", tableName, schemaName)
}

func splitTableReference(name string) (catalog, schema, table string) {
	parts := strings.Split(name, ".")
	switch len(parts) {
	case 0:
		return "", "", ""
	case 1:
		return "", "", parts[0]
	case 2:
		return "", parts[0], parts[1]
	default:
		return parts[len(parts)-3], parts[len(parts)-2], parts[len(parts)-1]
	}
}

// LookupSchemaID resolves a schema name to its DuckLake schema_id.
func (s *AuthorizationService) LookupSchemaID(ctx context.Context, schemaName string) (string, error) {
	sch, err := s.introspection.GetSchemaByName(ctx, schemaName)
	if err != nil {
		return "", fmt.Errorf("schema %q not found in catalog", schemaName)
	}
	return sch.ID, nil
}

// hasGrant checks if any of the given identities has a specific grant.
func (s *AuthorizationService) hasGrant(ctx context.Context, principalID string, groupIDs []string, securableType string, securableID string, privilege string) (bool, error) {
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

	// If we didn't find the specific privilege, also check for ALL_PRIVILEGES
	if privilege != domain.PrivAllPrivileges {
		return s.hasGrant(ctx, principalID, groupIDs, securableType, securableID, domain.PrivAllPrivileges)
	}

	return false, nil
}

// CheckPrivilege determines whether the named principal has the given privilege
// on the specified securable. It implements the Databricks-style permission model:
//  1. Admin bypass
//  2. USAGE gate on parent schema (for table-level checks)
//  3. Walk up hierarchy: table -> schema -> catalog
//  4. ALL_PRIVILEGES expansion
func (s *AuthorizationService) CheckPrivilege(ctx context.Context, principalName string, securableType string, securableID string, privilege string) (bool, error) {
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

func (s *AuthorizationService) checkPrivilegeForIdentities(ctx context.Context, principalID string, groupIDs []string, securableType string, securableID string, privilege string) (bool, error) {
	switch securableType {
	case domain.SecurableTable:
		return s.checkTablePrivilege(ctx, principalID, groupIDs, securableID, privilege)
	case domain.SecurableSchema:
		return s.checkSchemaPrivilege(ctx, principalID, groupIDs, securableID, privilege)
	case domain.SecurableCatalog:
		return s.hasGrant(ctx, principalID, groupIDs, domain.SecurableCatalog, domain.CatalogID, privilege)
	case domain.SecurableExternalLocation, domain.SecurableStorageCredential, domain.SecurableVolume:
		return s.checkCatalogScopedPrivilege(ctx, principalID, groupIDs, securableType, securableID, privilege)
	default:
		return false, fmt.Errorf("unknown securable type: %s", securableType)
	}
}

func (s *AuthorizationService) checkTablePrivilege(ctx context.Context, principalID string, groupIDs []string, tableID string, privilege string) (bool, error) {
	var schemaID string

	// Try managed table first
	switch table, err := s.introspection.GetTable(ctx, tableID); {
	case err == nil:
		schemaID = table.SchemaID
	case s.extTableRepo != nil:
		// Try external table
		et, extErr := s.extTableRepo.GetByID(ctx, tableID)
		if extErr != nil {
			return false, fmt.Errorf("lookup table %s: %w", tableID, err)
		}
		sch, schErr := s.introspection.GetSchemaByName(ctx, et.SchemaName)
		if schErr != nil {
			return false, fmt.Errorf("lookup schema %q for external table: %w", et.SchemaName, schErr)
		}
		schemaID = sch.ID
	default:
		return false, fmt.Errorf("lookup table %s: %w", tableID, err)
	}

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

func (s *AuthorizationService) checkSchemaPrivilege(ctx context.Context, principalID string, groupIDs []string, schemaID string, privilege string) (bool, error) {
	ok, err := s.hasGrant(ctx, principalID, groupIDs, domain.SecurableSchema, schemaID, privilege)
	if err != nil || ok {
		return ok, err
	}
	return s.hasGrant(ctx, principalID, groupIDs, domain.SecurableCatalog, domain.CatalogID, privilege)
}

// checkCatalogScopedPrivilege checks a privilege on a catalog-scoped securable
// (external_location, storage_credential, volume). These inherit from catalog.
func (s *AuthorizationService) checkCatalogScopedPrivilege(ctx context.Context, principalID string, groupIDs []string, securableType string, securableID string, privilege string) (bool, error) {
	// Check direct grant on the securable itself
	ok, err := s.hasGrant(ctx, principalID, groupIDs, securableType, securableID, privilege)
	if err != nil || ok {
		return ok, err
	}
	// Inherit from catalog
	return s.hasGrant(ctx, principalID, groupIDs, domain.SecurableCatalog, domain.CatalogID, privilege)
}

// GetEffectiveRowFilters returns all SQL filter expressions for a table that
// apply to the principal (or any of their groups). Returns nil if no filters apply.
func (s *AuthorizationService) GetEffectiveRowFilters(ctx context.Context, principalName string, tableID string) ([]string, error) {
	principal, err := s.principals.GetByName(ctx, principalName)
	if err != nil {
		return nil, err
	}

	// Admin bypass
	if principal.IsAdmin {
		return nil, nil
	}

	seen := map[string]bool{}
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
func (s *AuthorizationService) GetEffectiveColumnMasks(ctx context.Context, principalName string, tableID string) (map[string]string, error) {
	principal, err := s.principals.GetByName(ctx, principalName)
	if err != nil {
		return nil, err
	}

	// Admin bypass
	if principal.IsAdmin {
		return nil, nil
	}

	masks := map[string]string{}
	exempted := map[string]bool{}

	// Check direct user bindings
	userMasks, err := s.columnMasks.GetForTableAndPrincipal(ctx, tableID, principal.ID, "user")
	if err != nil {
		return nil, err
	}
	for _, m := range userMasks {
		key := strings.ToLower(m.ColumnName)
		if m.SeeOriginal {
			exempted[key] = true
		} else {
			masks[key] = m.MaskExpression
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
			key := strings.ToLower(m.ColumnName)
			if exempted[key] {
				continue
			}
			if _, alreadyUnmasked := masks[key]; alreadyUnmasked {
				continue
			}
			if !m.SeeOriginal {
				masks[key] = m.MaskExpression
			}
		}
	}

	if len(masks) == 0 {
		return nil, nil
	}
	return masks, nil
}

// GetTableColumnNames returns the ordered list of column names for a table.
// This is used by the engine to expand SELECT * before applying column masks.
func (s *AuthorizationService) GetTableColumnNames(ctx context.Context, tableID string) ([]string, error) {
	cols, _, err := s.introspection.ListColumns(ctx, tableID, domain.PageRequest{MaxResults: 10000})
	if err != nil {
		return nil, fmt.Errorf("list columns for table %s: %w", tableID, err)
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names, nil
}
