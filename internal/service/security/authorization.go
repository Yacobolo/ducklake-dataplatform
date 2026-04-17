package security

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Yacobolo/quackstack/internal/domain"
)

const syntheticViewIDPrefix = "__view__:"

// AuthorizationService provides permission checking using domain repository interfaces.
// It implements the domain.AuthorizationService interface.
type AuthorizationService struct {
	principals               domain.PrincipalRepository
	groups                   domain.GroupRepository
	grants                   domain.GrantRepository
	rowFilters               domain.RowFilterRepository
	columnMasks              domain.ColumnMaskRepository
	introspection            domain.IntrospectionRepository
	extTableRepo             domain.ExternalTableRepository
	viewRepo                 domain.ViewRepository
	lookupCatalogTable       func(ctx context.Context, catalogName, schemaName, tableName string) (*domain.TableDetail, error)
	lookupCatalogColumns     func(ctx context.Context, catalogName, tableID string) ([]string, error)
	lookupCatalogSchema      func(ctx context.Context, catalogName, schemaName string) (*domain.SchemaDetail, error)
	lookupCatalogView        func(ctx context.Context, catalogName, schemaName, viewName string) (*domain.ViewDetail, error)
	lookupDefaultTable       func(ctx context.Context, schemaName, tableName string) (*domain.TableDetail, error)
	lookupDefaultView        func(ctx context.Context, schemaName, viewName string) (*domain.ViewDetail, error)
	lookupDefaultTableByID   func(ctx context.Context, tableID string) (*domain.Table, error)
	lookupDefaultSchema      func(ctx context.Context, schemaName string) (*domain.Schema, error)
	lookupDefaultCatalogName func(ctx context.Context) (string, error)
	cacheMu                  sync.RWMutex
	privilegeCache           map[string]bool
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
		principals:     principals,
		groups:         groups,
		grants:         grants,
		rowFilters:     rowFilters,
		columnMasks:    columnMasks,
		introspection:  introspection,
		extTableRepo:   extTableRepo,
		privilegeCache: make(map[string]bool),
	}
}

// InvalidatePrivilegeCache clears memoized privilege decisions.
func (s *AuthorizationService) InvalidatePrivilegeCache() {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.privilegeCache = make(map[string]bool)
}

// SetCatalogTableLookup configures catalog-aware table lookup for three-part
// table references (catalog.schema.table).
func (s *AuthorizationService) SetCatalogTableLookup(lookup func(ctx context.Context, catalogName, schemaName, tableName string) (*domain.TableDetail, error)) {
	s.lookupCatalogTable = lookup
}

// SetCatalogColumnLookup configures attached-catalog column lookup by raw table ID.
func (s *AuthorizationService) SetCatalogColumnLookup(lookup func(ctx context.Context, catalogName, tableID string) ([]string, error)) {
	s.lookupCatalogColumns = lookup
}

// SetCatalogSchemaLookup configures catalog-aware schema lookup for attached catalogs.
func (s *AuthorizationService) SetCatalogSchemaLookup(lookup func(ctx context.Context, catalogName, schemaName string) (*domain.SchemaDetail, error)) {
	s.lookupCatalogSchema = lookup
}

// SetCatalogViewLookup configures catalog-aware view lookup for three-part
// object references (catalog.schema.view).
func (s *AuthorizationService) SetCatalogViewLookup(lookup func(ctx context.Context, catalogName, schemaName, viewName string) (*domain.ViewDetail, error)) {
	s.lookupCatalogView = lookup
}

// SetDefaultCatalogTableLookup configures default-catalog table lookup for
// two-part references (schema.table) when the local metastore cannot resolve
// the object.
func (s *AuthorizationService) SetDefaultCatalogTableLookup(lookup func(ctx context.Context, schemaName, tableName string) (*domain.TableDetail, error)) {
	s.lookupDefaultTable = lookup
}

// SetDefaultCatalogViewLookup configures default-catalog view lookup for
// two-part references (schema.view) when the local metastore cannot resolve
// the object.
func (s *AuthorizationService) SetDefaultCatalogViewLookup(lookup func(ctx context.Context, schemaName, viewName string) (*domain.ViewDetail, error)) {
	s.lookupDefaultView = lookup
}

// SetDefaultCatalogSchemaLookup configures default-catalog schema lookup when
// the local metastore cannot resolve the schema.
func (s *AuthorizationService) SetDefaultCatalogSchemaLookup(lookup func(ctx context.Context, schemaName string) (*domain.Schema, error)) {
	s.lookupDefaultSchema = lookup
}

// SetDefaultCatalogNameLookup configures a callback that returns the current
// default catalog name for two-part system table references.
func (s *AuthorizationService) SetDefaultCatalogNameLookup(lookup func(ctx context.Context) (string, error)) {
	s.lookupDefaultCatalogName = lookup
}

// SetDefaultCatalogTableByIDLookup configures table lookup by ID in the
// default catalog for privilege inheritance checks.
func (s *AuthorizationService) SetDefaultCatalogTableByIDLookup(lookup func(ctx context.Context, tableID string) (*domain.Table, error)) {
	s.lookupDefaultTableByID = lookup
}

// SetViewRepository configures direct view lookup support.
func (s *AuthorizationService) SetViewRepository(repo domain.ViewRepository) {
	s.viewRepo = repo
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
	ids = append(ids, domain.AllAuthenticatedGroupID)
	return ids, nil
}

// LookupTableID resolves a table name to its table_id and schema_id.
// For external tables, isExternal is true.
func (s *AuthorizationService) LookupTableID(ctx context.Context, tableName string) (tableID, schemaID string, isExternal bool, err error) {
	catalogName, schemaName, bareTableName := splitTableReference(tableName)

	if catalogName != "" && schemaName != "" && s.lookupCatalogTable != nil {
		tbl, lookupErr := s.lookupCatalogTable(ctx, catalogName, schemaName, bareTableName)
		if lookupErr == nil {
			isExternal := strings.EqualFold(tbl.TableType, domain.TableTypeExternal)
			if strings.EqualFold(tbl.TableType, domain.TableTypeSystem) {
				rawSchemaID := domain.SystemSchemaObjectID(schemaName)
				return domain.SyntheticCatalogTableID(catalogName, rawSchemaID, domain.SystemTableObjectID(schemaName, bareTableName)), domain.SyntheticCatalogSchemaID(catalogName, rawSchemaID), false, nil
			}

			if s.lookupCatalogSchema != nil {
				if schema, schemaErr := s.lookupCatalogSchema(ctx, catalogName, schemaName); schemaErr == nil {
					if !isExternal {
						return domain.SyntheticCatalogTableID(catalogName, schema.SchemaID, tbl.TableID), domain.SyntheticCatalogSchemaID(catalogName, schema.SchemaID), false, nil
					}
					return tbl.TableID, domain.SyntheticCatalogSchemaID(catalogName, schema.SchemaID), true, nil
				}
			}

			return tbl.TableID, "", isExternal, nil
		}

		var notFoundErr *domain.NotFoundError
		if !errors.As(lookupErr, &notFoundErr) {
			return "", "", false, fmt.Errorf("lookup table %q in catalog %q: %w", bareTableName, catalogName, lookupErr)
		}
	}

	if catalogName != "" && schemaName != "" && s.lookupCatalogView != nil {
		view, lookupErr := s.lookupCatalogView(ctx, catalogName, schemaName, bareTableName)
		if lookupErr == nil {
			tableID, rawSchemaID, isExternal, err := resolvedViewIdentity(view, bareTableName)
			if err != nil {
				return "", "", false, err
			}
			if rawSchemaID == "" {
				return tableID, "", isExternal, nil
			}
			return syntheticViewID(domain.SyntheticCatalogSchemaID(catalogName, rawSchemaID), bareTableName), domain.SyntheticCatalogSchemaID(catalogName, rawSchemaID), isExternal, nil
		}

		var notFoundErr *domain.NotFoundError
		if !errors.As(lookupErr, &notFoundErr) {
			return "", "", false, fmt.Errorf("lookup view %q in catalog %q: %w", bareTableName, catalogName, lookupErr)
		}
	}

	if schemaName != "" {
		t, lookupErr := s.lookupManagedTableBySchema(ctx, schemaName, bareTableName)
		if lookupErr == nil {
			return t.ID, t.SchemaID, false, nil
		}

		if s.lookupDefaultTable != nil {
			tbl, defaultErr := s.lookupDefaultTable(ctx, schemaName, bareTableName)
			if defaultErr == nil {
				if strings.EqualFold(tbl.TableType, domain.TableTypeSystem) {
					rawSchemaID := domain.SystemSchemaObjectID(schemaName)
					rawTableID := domain.SystemTableObjectID(schemaName, bareTableName)
					if s.lookupDefaultCatalogName != nil {
						defaultCatalogName, catalogErr := s.lookupDefaultCatalogName(ctx)
						if catalogErr != nil {
							return "", "", false, fmt.Errorf("lookup default catalog for system table %q: %w", bareTableName, catalogErr)
						}
						if strings.TrimSpace(defaultCatalogName) != "" {
							return domain.SyntheticCatalogTableID(defaultCatalogName, rawSchemaID, rawTableID), domain.SyntheticCatalogSchemaID(defaultCatalogName, rawSchemaID), false, nil
						}
					}
					return rawTableID, rawSchemaID, false, nil
				}
				if sch, schErr := s.lookupSchemaByName(ctx, schemaName); schErr == nil {
					return tbl.TableID, sch.ID, strings.EqualFold(tbl.TableType, domain.TableTypeExternal), nil
				}
				return tbl.TableID, "", strings.EqualFold(tbl.TableType, domain.TableTypeExternal), nil
			}

			var notFoundErr *domain.NotFoundError
			if !errors.As(defaultErr, &notFoundErr) {
				return "", "", false, fmt.Errorf("lookup table %q in default catalog schema %q: %w", bareTableName, schemaName, defaultErr)
			}
		}

		view, viewErr := s.lookupViewBySchema(ctx, schemaName, bareTableName)
		if viewErr == nil {
			return resolvedViewIdentity(view, bareTableName)
		}
		var viewNotFoundErr *domain.NotFoundError
		if viewErr != nil && !errors.As(viewErr, &viewNotFoundErr) {
			return "", "", false, fmt.Errorf("lookup view %q in schema %q: %w", bareTableName, schemaName, viewErr)
		}

		if s.lookupDefaultView != nil {
			view, defaultErr := s.lookupDefaultView(ctx, schemaName, bareTableName)
			if defaultErr == nil {
				return resolvedViewIdentity(view, bareTableName)
			}

			var notFoundErr *domain.NotFoundError
			if !errors.As(defaultErr, &notFoundErr) {
				return "", "", false, fmt.Errorf("lookup view %q in default catalog schema %q: %w", bareTableName, schemaName, defaultErr)
			}
		}

		if s.extTableRepo != nil {
			et, extErr := s.extTableRepo.GetByName(ctx, schemaName, bareTableName)
			if extErr == nil {
				sch, schErr := s.lookupSchemaByName(ctx, et.SchemaName)
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

	view, viewErr := s.lookupViewByName(ctx, bareTableName)
	if viewErr == nil {
		return resolvedViewIdentity(view, bareTableName)
	}
	var viewNotFoundErr *domain.NotFoundError
	if viewErr != nil && !errors.As(viewErr, &viewNotFoundErr) {
		return "", "", false, fmt.Errorf("lookup view %q: %w", bareTableName, viewErr)
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

func (s *AuthorizationService) lookupViewBySchema(ctx context.Context, schemaName, viewName string) (*domain.ViewDetail, error) {
	if s.viewRepo == nil {
		return nil, domain.ErrNotFound("view %q not found in schema %q", viewName, schemaName)
	}

	schema, err := s.introspection.GetSchemaByName(ctx, schemaName)
	if err != nil {
		return nil, domain.ErrNotFound("schema %q not found", schemaName)
	}

	view, err := s.viewRepo.GetByName(ctx, schema.ID, viewName)
	if err != nil {
		return nil, err
	}
	return view, nil
}

func (s *AuthorizationService) lookupViewByName(ctx context.Context, viewName string) (*domain.ViewDetail, error) {
	if s.viewRepo == nil {
		return nil, domain.ErrNotFound("view %q not found", viewName)
	}

	schemas, _, err := s.introspection.ListSchemas(ctx, domain.PageRequest{MaxResults: 10000})
	if err != nil {
		return nil, fmt.Errorf("list schemas: %w", err)
	}

	var match *domain.ViewDetail
	for _, schema := range schemas {
		view, viewErr := s.viewRepo.GetByName(ctx, schema.ID, viewName)
		if viewErr == nil {
			if match != nil {
				return nil, fmt.Errorf("ambiguous view name %q", viewName)
			}
			match = view
			continue
		}

		var notFoundErr *domain.NotFoundError
		if !errors.As(viewErr, &notFoundErr) {
			return nil, viewErr
		}
	}

	if match == nil {
		return nil, domain.ErrNotFound("view %q not found", viewName)
	}
	return match, nil
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
	sch, err := s.lookupSchemaByName(ctx, schemaName)
	if err != nil {
		return "", fmt.Errorf("schema %q not found in catalog", schemaName)
	}
	return sch.ID, nil
}

func (s *AuthorizationService) lookupSchemaByName(ctx context.Context, schemaName string) (*domain.Schema, error) {
	schema, err := s.introspection.GetSchemaByName(ctx, schemaName)
	if err == nil {
		return schema, nil
	}

	var notFoundErr *domain.NotFoundError
	if err != nil && !errors.As(err, &notFoundErr) {
		return nil, err
	}

	if s.lookupDefaultSchema != nil {
		return s.lookupDefaultSchema(ctx, schemaName)
	}

	return nil, err
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
	cacheKey := principalName + "|" + securableType + "|" + securableID + "|" + privilege
	s.cacheMu.RLock()
	if cached, ok := s.privilegeCache[cacheKey]; ok {
		s.cacheMu.RUnlock()
		return cached, nil
	}
	s.cacheMu.RUnlock()

	principal, err := s.principals.GetByName(ctx, principalName)
	if err != nil {
		return false, fmt.Errorf("principal %q not found", principalName)
	}

	// Admin bypass
	if principal.IsAdmin {
		s.cacheMu.Lock()
		s.privilegeCache[cacheKey] = true
		s.cacheMu.Unlock()
		return true, nil
	}

	// Resolve group memberships
	groupIDs, err := s.resolveGroupIDs(ctx, principal.ID)
	if err != nil {
		return false, err
	}

	allowed, err := s.checkPrivilegeForIdentities(ctx, principal.ID, groupIDs, securableType, securableID, privilege)
	if err != nil {
		return false, err
	}
	s.cacheMu.Lock()
	s.privilegeCache[cacheKey] = allowed
	s.cacheMu.Unlock()
	return allowed, nil
}

func (s *AuthorizationService) checkPrivilegeForIdentities(ctx context.Context, principalID string, groupIDs []string, securableType string, securableID string, privilege string) (bool, error) {
	switch securableType {
	case domain.SecurableTable:
		return s.checkTablePrivilege(ctx, principalID, groupIDs, securableID, privilege)
	case domain.SecurableSchema:
		return s.checkSchemaPrivilege(ctx, principalID, groupIDs, securableID, privilege)
	case domain.SecurableCatalog:
		return s.checkCatalogPrivilege(ctx, principalID, groupIDs, securableID, privilege)
	case domain.SecurableFolder, domain.SecurablePipeline:
		return s.hasGrant(ctx, principalID, groupIDs, securableType, securableID, privilege)
	case domain.SecurableExternalLocation, domain.SecurableStorageCredential, domain.SecurableVolume, domain.SecurableComputeEndpoint:
		return s.checkCatalogScopedPrivilege(ctx, principalID, groupIDs, securableType, securableID, privilege)
	default:
		return false, fmt.Errorf("unknown securable type: %s", securableType)
	}
}

func (s *AuthorizationService) checkTablePrivilege(ctx context.Context, principalID string, groupIDs []string, tableID string, privilege string) (bool, error) {
	var (
		schemaID       string
		schemaResolved bool
	)

	if domain.IsSystemTableObjectID(tableID) {
		return false, nil
	}

	if parsedSchemaID, ok := schemaIDFromSyntheticViewID(tableID); ok {
		schemaID = parsedSchemaID
		schemaResolved = true
	}
	if !schemaResolved {
		if catalogName, parsedRawSchemaID, parsedRawTableID, ok := domain.ParseSyntheticCatalogTableID(tableID); ok {
			if domain.IsSystemTableObjectID(parsedRawTableID) {
				return false, nil
			}
			schemaID = domain.SyntheticCatalogSchemaID(catalogName, parsedRawSchemaID)
			schemaResolved = true
		}
	}

	// Try managed table first.
	if !schemaResolved {
		table, err := s.introspection.GetTable(ctx, tableID)
		if err == nil {
			schemaID = table.SchemaID
			schemaResolved = true
		}
		if !schemaResolved && s.lookupDefaultTableByID != nil {
			table, defaultErr := s.lookupDefaultTableByID(ctx, tableID)
			if defaultErr == nil {
				schemaID = table.SchemaID
				schemaResolved = true
			}
		}
	}

	// Fall back to external table lookup.
	if !schemaResolved && s.extTableRepo != nil {
		et, extErr := s.extTableRepo.GetByID(ctx, tableID)
		if extErr == nil {
			sch, schErr := s.introspection.GetSchemaByName(ctx, et.SchemaName)
			if schErr == nil {
				schemaID = sch.ID
				schemaResolved = true
			}
		}
	}

	// Fall back to view lookup by ID.
	if !schemaResolved && s.viewRepo != nil {
		view, viewErr := s.viewRepo.GetByID(ctx, tableID)
		if viewErr == nil {
			schemaID = view.SchemaID
			schemaResolved = true
		}
	}

	if !schemaResolved {
		return false, nil
	}

	// USE_SCHEMA gate: principal must be able to use the parent schema.
	hasUseSchema, err := s.checkSchemaPrivilege(ctx, principalID, groupIDs, schemaID, domain.PrivUseSchema)
	if err != nil {
		return false, err
	}
	if !hasUseSchema {
		hasUseSchema, err = s.checkSchemaPrivilege(ctx, principalID, groupIDs, schemaID, "USAGE")
		if err != nil {
			return false, err
		}
	}
	if !hasUseSchema {
		return false, nil
	}

	// Check grant on the table itself
	ok, err := s.hasGrant(ctx, principalID, groupIDs, domain.SecurableTable, tableID, privilege)
	if err != nil || ok {
		return ok, err
	}

	if schemaResolved {
		// Inherit from schema
		ok, err = s.hasGrant(ctx, principalID, groupIDs, domain.SecurableSchema, schemaID, privilege)
		if err != nil || ok {
			return ok, err
		}
	}

	// Inherit from catalog
	return s.checkCatalogPrivilege(ctx, principalID, groupIDs, catalogIDForSchema(schemaID), privilege)
}

func resolvedViewIdentity(view *domain.ViewDetail, fallbackName string) (tableID, schemaID string, isExternal bool, err error) {
	if view == nil {
		return "", "", false, domain.ErrNotFound("view %q not found", fallbackName)
	}
	if view.SchemaID == "" {
		return "", "", false, fmt.Errorf("view %q missing schema id", fallbackName)
	}
	if view.ID == "" {
		return syntheticViewID(view.SchemaID, fallbackName), view.SchemaID, false, nil
	}
	return view.ID, view.SchemaID, false, nil
}

func syntheticViewID(schemaID, viewName string) string {
	return syntheticViewIDPrefix + schemaID + "|" + strings.ToLower(strings.TrimSpace(viewName))
}

func schemaIDFromSyntheticViewID(id string) (string, bool) {
	if !strings.HasPrefix(id, syntheticViewIDPrefix) {
		return "", false
	}
	rest := strings.TrimPrefix(id, syntheticViewIDPrefix)
	separator := "|"
	if !strings.Contains(rest, separator) {
		separator = ":"
	}
	parts := strings.SplitN(rest, separator, 2)
	if len(parts) != 2 || parts[0] == "" {
		return "", false
	}
	return parts[0], true
}

func (s *AuthorizationService) checkSchemaPrivilege(ctx context.Context, principalID string, groupIDs []string, schemaID string, privilege string) (bool, error) {
	if domain.IsSystemSchemaObjectID(schemaID) {
		return false, nil
	}
	if _, parsedRawSchemaID, ok := domain.ParseSyntheticCatalogSchemaID(schemaID); ok && domain.IsSystemSchemaObjectID(parsedRawSchemaID) {
		return false, nil
	}
	ok, err := s.hasGrant(ctx, principalID, groupIDs, domain.SecurableSchema, schemaID, privilege)
	if err != nil || ok {
		return ok, err
	}
	return s.checkCatalogPrivilege(ctx, principalID, groupIDs, catalogIDForSchema(schemaID), privilege)
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
	return s.checkCatalogPrivilege(ctx, principalID, groupIDs, domain.CatalogID, privilege)
}

func (s *AuthorizationService) checkCatalogPrivilege(ctx context.Context, principalID string, groupIDs []string, catalogID string, privilege string) (bool, error) {
	candidates := []string{}
	if trimmed := strings.TrimSpace(catalogID); trimmed != "" {
		candidates = append(candidates, trimmed)
	}
	if len(candidates) == 0 || candidates[len(candidates)-1] != domain.CatalogID {
		candidates = append(candidates, domain.CatalogID)
	}
	for _, candidate := range candidates {
		ok, err := s.hasGrant(ctx, principalID, groupIDs, domain.SecurableCatalog, candidate, privilege)
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}

func catalogIDForSchema(schemaID string) string {
	if catalogName, _, ok := domain.ParseSyntheticCatalogSchemaID(schemaID); ok {
		return catalogName
	}
	return domain.CatalogID
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
	if domain.IsSystemTableObjectID(tableID) && s.lookupDefaultTable != nil {
		schemaName, tableName, parseOK := parseSystemTableObjectID(tableID)
		if !parseOK {
			return nil, fmt.Errorf("parse system table id %q", tableID)
		}
		tbl, err := s.lookupDefaultTable(ctx, schemaName, tableName)
		if err != nil {
			return nil, fmt.Errorf("lookup system table %s: %w", tableID, err)
		}
		names := make([]string, len(tbl.Columns))
		for i := range tbl.Columns {
			names[i] = tbl.Columns[i].Name
		}
		return names, nil
	}

	rawTableID := tableID
	if catalogName, _, parsedRawTableID, ok := domain.ParseSyntheticCatalogTableID(tableID); ok {
		rawTableID = parsedRawTableID
		if domain.IsSystemTableObjectID(rawTableID) && s.lookupCatalogTable != nil {
			schemaName, tableName, parseOK := parseSystemTableObjectID(rawTableID)
			if !parseOK {
				return nil, fmt.Errorf("parse system table id %q", tableID)
			}
			tbl, err := s.lookupCatalogTable(ctx, catalogName, schemaName, tableName)
			if err != nil {
				return nil, fmt.Errorf("lookup system table %s: %w", tableID, err)
			}
			names := make([]string, len(tbl.Columns))
			for i := range tbl.Columns {
				names[i] = tbl.Columns[i].Name
			}
			return names, nil
		}
		if s.lookupCatalogColumns != nil {
			names, err := s.lookupCatalogColumns(ctx, catalogName, rawTableID)
			if err != nil {
				return nil, fmt.Errorf("list columns for table %s: %w", tableID, err)
			}
			return names, nil
		}
	}
	cols, _, err := s.introspection.ListColumns(ctx, rawTableID, domain.PageRequest{MaxResults: 10000})
	if err != nil {
		return nil, fmt.Errorf("list columns for table %s: %w", tableID, err)
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names, nil
}

func parseSystemTableObjectID(id string) (schemaName, tableName string, ok bool) {
	if !domain.IsSystemTableObjectID(id) {
		return "", "", false
	}
	rest := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(id)), "__system__:")
	parts := strings.SplitN(rest, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}
