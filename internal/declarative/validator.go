package declarative

import (
	"fmt"
	"strings"
)

// ValidationError represents a single validation problem.
type ValidationError struct {
	Path    string // e.g. "security/principals.yaml" or "principal[analyst1]"
	Message string
}

func (e ValidationError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("%s: %s", e.Path, e.Message)
	}
	return e.Message
}

// Valid principal types.
var validPrincipalTypes = map[string]bool{
	"user":              true,
	"service_principal": true,
}

// Valid member types within a group.
var validMemberTypes = map[string]bool{
	"user":  true,
	"group": true,
}

// Valid grant principal types.
var validGrantPrincipalTypes = map[string]bool{
	"user":  true,
	"group": true,
}

// Valid securable types for grants.
var validSecurableTypes = map[string]bool{
	"catalog":            true,
	"schema":             true,
	"table":              true,
	"external_location":  true,
	"storage_credential": true,
	"volume":             true,
}

// Valid privilege names.
var validPrivileges = map[string]bool{
	"SELECT":                    true,
	"INSERT":                    true,
	"UPDATE":                    true,
	"DELETE":                    true,
	"USAGE":                     true,
	"CREATE_TABLE":              true,
	"CREATE_SCHEMA":             true,
	"ALL_PRIVILEGES":            true,
	"CREATE_EXTERNAL_LOCATION":  true,
	"CREATE_STORAGE_CREDENTIAL": true,
	"CREATE_VOLUME":             true,
	"READ_VOLUME":               true,
	"WRITE_VOLUME":              true,
	"READ_FILES":                true,
	"WRITE_FILES":               true,
	"MANAGE_COMPUTE":            true,
	"MANAGE_PIPELINES":          true,
}

// Valid metastore types.
var validMetastoreTypes = map[string]bool{
	"sqlite":   true,
	"postgres": true,
}

// Valid table types.
var validTableTypes = map[string]bool{
	"MANAGED":  true,
	"EXTERNAL": true,
}

// Valid volume types.
var validVolumeTypes = map[string]bool{
	"MANAGED":  true,
	"EXTERNAL": true,
}

// Valid credential types.
var validCredentialTypes = map[string]bool{
	"S3":    true,
	"AZURE": true,
	"GCS":   true,
}

// Valid compute endpoint types.
var validComputeTypes = map[string]bool{
	"LOCAL":  true,
	"REMOTE": true,
}

// Valid notebook cell types.
var validCellTypes = map[string]bool{
	"sql":      true,
	"markdown": true,
}

// Valid tag assignment securable types.
var validTagSecurableTypes = map[string]bool{
	"schema": true,
	"table":  true,
	"column": true,
}

// Validate checks the DesiredState for structural correctness and referential integrity.
// It returns a list of all validation errors (does not stop at first error).
func Validate(state *DesiredState) []ValidationError {
	var errs []ValidationError

	// Build lookup sets for referential integrity checks.
	principalNames := make(map[string]bool, len(state.Principals))
	for _, p := range state.Principals {
		principalNames[p.Name] = true
	}

	groupNames := make(map[string]bool, len(state.Groups))
	for _, g := range state.Groups {
		groupNames[g.Name] = true
	}

	catalogNames := make(map[string]bool, len(state.Catalogs))
	for _, c := range state.Catalogs {
		catalogNames[c.CatalogName] = true
	}

	schemaKeys := make(map[string]bool, len(state.Schemas))
	for _, s := range state.Schemas {
		schemaKeys[s.CatalogName+"."+s.SchemaName] = true
	}

	tableKeys := make(map[string]bool, len(state.Tables))
	// Also build column lookup: tableKey -> set of column names.
	tableColumns := make(map[string]map[string]bool, len(state.Tables))
	for _, t := range state.Tables {
		key := t.CatalogName + "." + t.SchemaName + "." + t.TableName
		tableKeys[key] = true
		if len(t.Spec.Columns) > 0 {
			cols := make(map[string]bool, len(t.Spec.Columns))
			for _, c := range t.Spec.Columns {
				cols[c.Name] = true
			}
			tableColumns[key] = cols
		}
	}

	viewKeys := make(map[string]bool, len(state.Views))
	for _, v := range state.Views {
		viewKeys[v.CatalogName+"."+v.SchemaName+"."+v.ViewName] = true
	}

	volumeKeys := make(map[string]bool, len(state.Volumes))
	for _, v := range state.Volumes {
		volumeKeys[v.CatalogName+"."+v.SchemaName+"."+v.VolumeName] = true
	}

	credentialNames := make(map[string]bool, len(state.StorageCredentials))
	for _, c := range state.StorageCredentials {
		credentialNames[c.Name] = true
	}

	locationNames := make(map[string]bool, len(state.ExternalLocations))
	for _, l := range state.ExternalLocations {
		locationNames[l.Name] = true
	}

	endpointNames := make(map[string]bool, len(state.ComputeEndpoints))
	for _, e := range state.ComputeEndpoints {
		endpointNames[e.Name] = true
	}

	notebookNames := make(map[string]bool, len(state.Notebooks))
	for _, n := range state.Notebooks {
		notebookNames[n.Name] = true
	}

	tagKeys := make(map[string]bool, len(state.Tags))
	for _, t := range state.Tags {
		tagKeys[formatTagKey(t)] = true
	}

	// 1. Validate principals.
	validatePrincipals(state.Principals, &errs)

	// 2. Validate groups.
	validateGroups(state.Groups, principalNames, groupNames, &errs)

	// 3. Validate grants.
	validateGrants(state.Grants, principalNames, groupNames, catalogNames, schemaKeys, tableKeys, locationNames, credentialNames, volumeKeys, &errs)

	// 4. Validate catalogs.
	validateCatalogs(state.Catalogs, &errs)

	// 5. Validate schemas.
	validateSchemas(state.Schemas, catalogNames, &errs)

	// 6. Validate tables.
	validateTables(state.Tables, schemaKeys, &errs)

	// 7. Validate views.
	validateViews(state.Views, schemaKeys, &errs)

	// 8. Validate volumes.
	validateVolumes(state.Volumes, schemaKeys, &errs)

	// 9. Validate row filters.
	validateRowFilters(state.RowFilters, tableKeys, principalNames, groupNames, &errs)

	// 10. Validate column masks.
	validateColumnMasks(state.ColumnMasks, tableKeys, tableColumns, principalNames, groupNames, &errs)

	// 11. Validate tags.
	validateTags(state.Tags, &errs)

	// 12. Validate tag assignments.
	validateTagAssignments(state.TagAssignments, tagKeys, schemaKeys, tableKeys, tableColumns, &errs)

	// 13. Validate storage credentials.
	validateStorageCredentials(state.StorageCredentials, &errs)

	// 14. Validate external locations.
	validateExternalLocations(state.ExternalLocations, credentialNames, &errs)

	// 15. Validate compute endpoints.
	validateComputeEndpoints(state.ComputeEndpoints, &errs)

	// 16. Validate compute assignments.
	validateComputeAssignments(state.ComputeAssignments, endpointNames, principalNames, groupNames, &errs)

	// 17. Validate API keys.
	validateAPIKeys(state.APIKeys, principalNames, &errs)

	// 18. Validate notebooks.
	validateNotebooks(state.Notebooks, &errs)

	// 19. Validate pipelines.
	validatePipelines(state.Pipelines, notebookNames, endpointNames, &errs)

	// 20. Validate models.
	validateModels(state.Models, &errs)

	// 21. Validate macros.
	validateMacros(state.Macros, &errs)

	return errs
}

// addErr appends a formatted validation error.
func addErr(errs *[]ValidationError, path, msg string, args ...any) {
	*errs = append(*errs, ValidationError{
		Path:    path,
		Message: fmt.Sprintf(msg, args...),
	})
}

// principalOrGroupExists checks that a referenced principal exists given its type.
func principalOrGroupExists(name, ptype string, principals, groups map[string]bool) bool {
	switch ptype {
	case "user", "service_principal":
		return principals[name]
	case "group":
		return groups[name]
	default:
		return false
	}
}

// formatTagKey returns a canonical tag identifier.
func formatTagKey(t TagSpec) string {
	if t.Value != nil {
		return t.Key + ":" + *t.Value
	}
	return t.Key
}

// === Principals ===

func validatePrincipals(principals []PrincipalSpec, errs *[]ValidationError) {
	seen := make(map[string]bool, len(principals))
	for i, p := range principals {
		path := fmt.Sprintf("principal[%d]", i)
		if p.Name != "" {
			path = fmt.Sprintf("principal[%s]", p.Name)
		}
		if p.Name == "" {
			addErr(errs, path, "name is required")
		}
		if !validPrincipalTypes[p.Type] {
			addErr(errs, path, "type must be \"user\" or \"service_principal\", got %q", p.Type)
		}
		if p.Name != "" {
			if seen[p.Name] {
				addErr(errs, path, "duplicate principal name %q", p.Name)
			}
			seen[p.Name] = true
		}
	}
}

// === Groups ===

func validateGroups(groups []GroupSpec, principalNames, groupNames map[string]bool, errs *[]ValidationError) {
	seen := make(map[string]bool, len(groups))
	for i, g := range groups {
		path := fmt.Sprintf("group[%d]", i)
		if g.Name != "" {
			path = fmt.Sprintf("group[%s]", g.Name)
		}
		if g.Name == "" {
			addErr(errs, path, "name is required")
		}
		if g.Name != "" {
			if seen[g.Name] {
				addErr(errs, path, "duplicate group name %q", g.Name)
			}
			seen[g.Name] = true
		}
		for j, m := range g.Members {
			mpath := fmt.Sprintf("%s.members[%d]", path, j)
			if !validMemberTypes[m.Type] {
				addErr(errs, mpath, "member type must be \"user\" or \"group\", got %q", m.Type)
			}
			if m.Type == "user" && !principalNames[m.Name] {
				addErr(errs, mpath, "member %q references unknown principal", m.Name)
			}
			if m.Type == "group" && !groupNames[m.Name] {
				addErr(errs, mpath, "member %q references unknown group", m.Name)
			}
		}
	}

	// Detect circular group memberships.
	cycles := detectGroupCycles(groups)
	for _, cycle := range cycles {
		addErr(errs, "groups", "circular membership detected: %s", strings.Join(cycle, " -> "))
	}
}

// detectGroupCycles finds circular membership in the group graph using DFS.
func detectGroupCycles(groups []GroupSpec) [][]string {
	// Build adjacency: group name -> list of group-type member names.
	adj := make(map[string][]string, len(groups))
	for _, g := range groups {
		for _, m := range g.Members {
			if m.Type == "group" {
				adj[g.Name] = append(adj[g.Name], m.Name)
			}
		}
	}

	var cycles [][]string
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(groups))
	parent := make(map[string]string, len(groups))

	var dfs func(node string)
	dfs = func(node string) {
		color[node] = gray
		for _, next := range adj[node] {
			if color[next] == gray {
				// Back edge found — reconstruct cycle.
				cycle := []string{next, node}
				cur := node
				for cur != next {
					cur = parent[cur]
					if cur == "" {
						break
					}
					cycle = append(cycle, cur)
				}
				// Reverse to get natural order.
				for l, r := 0, len(cycle)-1; l < r; l, r = l+1, r-1 {
					cycle[l], cycle[r] = cycle[r], cycle[l]
				}
				cycles = append(cycles, cycle)
				return
			}
			if color[next] == white {
				parent[next] = node
				dfs(next)
			}
		}
		color[node] = black
	}

	for _, g := range groups {
		if color[g.Name] == white {
			dfs(g.Name)
		}
	}
	return cycles
}

// === Grants ===

func validateGrants(
	grants []GrantSpec,
	principalNames, groupNames, catalogNames, schemaKeys, tableKeys, locationNames, credentialNames, volumeKeys map[string]bool,
	errs *[]ValidationError,
) {
	seen := make(map[string]bool, len(grants))
	for i, g := range grants {
		path := fmt.Sprintf("grant[%d]", i)

		if g.Principal == "" {
			addErr(errs, path, "principal is required")
		}
		if !validGrantPrincipalTypes[g.PrincipalType] {
			addErr(errs, path, "principal_type must be \"user\" or \"group\", got %q", g.PrincipalType)
		}
		if g.PrincipalType == "user" && g.Principal != "" && !principalNames[g.Principal] {
			addErr(errs, path, "principal %q references unknown user", g.Principal)
		}
		if g.PrincipalType == "group" && g.Principal != "" && !groupNames[g.Principal] {
			addErr(errs, path, "principal %q references unknown group", g.Principal)
		}

		if !validSecurableTypes[g.SecurableType] {
			addErr(errs, path, "securable_type must be one of [catalog, schema, table, external_location, storage_credential, volume], got %q", g.SecurableType)
		}
		if g.Securable == "" {
			addErr(errs, path, "securable is required")
		}
		if !validPrivileges[g.Privilege] {
			addErr(errs, path, "unknown privilege %q", g.Privilege)
		}

		// Validate securable path format and existence.
		if g.Securable != "" && validSecurableTypes[g.SecurableType] {
			validateGrantSecurable(g, catalogNames, schemaKeys, tableKeys, locationNames, credentialNames, volumeKeys, path, errs)
		}

		// Duplicate detection.
		key := fmt.Sprintf("%s|%s|%s|%s|%s", g.Principal, g.PrincipalType, g.SecurableType, g.Securable, g.Privilege)
		if seen[key] {
			addErr(errs, path, "duplicate grant")
		}
		seen[key] = true
	}
}

func validateGrantSecurable(
	g GrantSpec,
	catalogNames, schemaKeys, tableKeys, locationNames, credentialNames, volumeKeys map[string]bool,
	path string, errs *[]ValidationError,
) {
	parts := strings.Split(g.Securable, ".")
	switch g.SecurableType {
	case "catalog":
		if len(parts) != 1 {
			addErr(errs, path, "catalog securable must be a single name, got %q", g.Securable)
		} else if !catalogNames[g.Securable] {
			addErr(errs, path, "securable references unknown catalog %q", g.Securable)
		}
	case "schema":
		if len(parts) != 2 {
			addErr(errs, path, "schema securable must be \"catalog.schema\", got %q", g.Securable)
		} else if !schemaKeys[g.Securable] {
			addErr(errs, path, "securable references unknown schema %q", g.Securable)
		}
	case "table":
		if len(parts) != 3 {
			addErr(errs, path, "table securable must be \"catalog.schema.table\", got %q", g.Securable)
		} else if !tableKeys[g.Securable] {
			addErr(errs, path, "securable references unknown table %q", g.Securable)
		}
	case "external_location":
		if len(parts) != 1 {
			addErr(errs, path, "external_location securable must be a single name, got %q", g.Securable)
		} else if !locationNames[g.Securable] {
			addErr(errs, path, "securable references unknown external location %q", g.Securable)
		}
	case "storage_credential":
		if len(parts) != 1 {
			addErr(errs, path, "storage_credential securable must be a single name, got %q", g.Securable)
		} else if !credentialNames[g.Securable] {
			addErr(errs, path, "securable references unknown storage credential %q", g.Securable)
		}
	case "volume":
		if len(parts) != 3 {
			addErr(errs, path, "volume securable must be \"catalog.schema.volume\", got %q", g.Securable)
		} else if !volumeKeys[g.Securable] {
			addErr(errs, path, "securable references unknown volume %q", g.Securable)
		}
	}
}

// === Catalogs ===

func validateCatalogs(catalogs []CatalogResource, errs *[]ValidationError) {
	seen := make(map[string]bool, len(catalogs))
	for i, c := range catalogs {
		path := fmt.Sprintf("catalog[%d]", i)
		if c.CatalogName != "" {
			path = fmt.Sprintf("catalog[%s]", c.CatalogName)
		}
		if c.CatalogName == "" {
			addErr(errs, path, "name is required")
		}
		if !validMetastoreTypes[c.Spec.MetastoreType] {
			addErr(errs, path, "metastore_type must be \"sqlite\" or \"postgres\", got %q", c.Spec.MetastoreType)
		}
		if c.Spec.DSN == "" {
			addErr(errs, path, "dsn is required")
		}
		if c.Spec.DataPath == "" {
			addErr(errs, path, "data_path is required")
		}
		if c.CatalogName != "" {
			if seen[c.CatalogName] {
				addErr(errs, path, "duplicate catalog name %q", c.CatalogName)
			}
			seen[c.CatalogName] = true
		}
	}
}

// === Schemas ===

func validateSchemas(schemas []SchemaResource, catalogNames map[string]bool, errs *[]ValidationError) {
	seen := make(map[string]bool, len(schemas))
	for i, s := range schemas {
		path := fmt.Sprintf("schema[%d]", i)
		if s.CatalogName != "" && s.SchemaName != "" {
			path = fmt.Sprintf("schema[%s.%s]", s.CatalogName, s.SchemaName)
		}
		if s.CatalogName == "" {
			addErr(errs, path, "catalog_name is required")
		}
		if s.SchemaName == "" {
			addErr(errs, path, "schema_name is required")
		}
		if s.CatalogName != "" && !catalogNames[s.CatalogName] {
			addErr(errs, path, "references unknown catalog %q", s.CatalogName)
		}
		key := s.CatalogName + "." + s.SchemaName
		if s.CatalogName != "" && s.SchemaName != "" {
			if seen[key] {
				addErr(errs, path, "duplicate schema %q", key)
			}
			seen[key] = true
		}
	}
}

// === Tables ===

func validateTables(tables []TableResource, schemaKeys map[string]bool, errs *[]ValidationError) {
	seen := make(map[string]bool, len(tables))
	for i, t := range tables {
		path := fmt.Sprintf("table[%d]", i)
		if t.CatalogName != "" && t.SchemaName != "" && t.TableName != "" {
			path = fmt.Sprintf("table[%s.%s.%s]", t.CatalogName, t.SchemaName, t.TableName)
		}
		if t.CatalogName == "" {
			addErr(errs, path, "catalog_name is required")
		}
		if t.SchemaName == "" {
			addErr(errs, path, "schema_name is required")
		}
		if t.TableName == "" {
			addErr(errs, path, "table_name is required")
		}

		schemaKey := t.CatalogName + "." + t.SchemaName
		if t.CatalogName != "" && t.SchemaName != "" && !schemaKeys[schemaKey] {
			addErr(errs, path, "references unknown schema %q", schemaKey)
		}

		if t.Spec.TableType != "" && !validTableTypes[t.Spec.TableType] {
			addErr(errs, path, "table_type must be \"MANAGED\" or \"EXTERNAL\", got %q", t.Spec.TableType)
		}
		if t.Spec.TableType == "EXTERNAL" {
			if t.Spec.SourcePath == "" {
				addErr(errs, path, "source_path is required for EXTERNAL tables")
			}
			if t.Spec.FileFormat == "" {
				addErr(errs, path, "file_format is required for EXTERNAL tables")
			}
		}

		// Validate columns.
		colSeen := make(map[string]bool, len(t.Spec.Columns))
		for j, col := range t.Spec.Columns {
			cpath := fmt.Sprintf("%s.columns[%d]", path, j)
			if col.Name == "" {
				addErr(errs, cpath, "column name is required")
			}
			if col.Type == "" {
				addErr(errs, cpath, "column type is required")
			}
			if col.Name != "" {
				if colSeen[col.Name] {
					addErr(errs, cpath, "duplicate column name %q", col.Name)
				}
				colSeen[col.Name] = true
			}
		}

		tableKey := t.CatalogName + "." + t.SchemaName + "." + t.TableName
		if t.CatalogName != "" && t.SchemaName != "" && t.TableName != "" {
			if seen[tableKey] {
				addErr(errs, path, "duplicate table %q", tableKey)
			}
			seen[tableKey] = true
		}
	}
}

// === Views ===

func validateViews(views []ViewResource, schemaKeys map[string]bool, errs *[]ValidationError) {
	seen := make(map[string]bool, len(views))
	for i, v := range views {
		path := fmt.Sprintf("view[%d]", i)
		if v.CatalogName != "" && v.SchemaName != "" && v.ViewName != "" {
			path = fmt.Sprintf("view[%s.%s.%s]", v.CatalogName, v.SchemaName, v.ViewName)
		}
		if v.CatalogName == "" {
			addErr(errs, path, "catalog_name is required")
		}
		if v.SchemaName == "" {
			addErr(errs, path, "schema_name is required")
		}
		if v.ViewName == "" {
			addErr(errs, path, "view_name is required")
		}

		schemaKey := v.CatalogName + "." + v.SchemaName
		if v.CatalogName != "" && v.SchemaName != "" && !schemaKeys[schemaKey] {
			addErr(errs, path, "references unknown schema %q", schemaKey)
		}

		if v.Spec.ViewDefinition == "" {
			addErr(errs, path, "view_definition is required")
		}

		viewKey := v.CatalogName + "." + v.SchemaName + "." + v.ViewName
		if v.CatalogName != "" && v.SchemaName != "" && v.ViewName != "" {
			if seen[viewKey] {
				addErr(errs, path, "duplicate view %q", viewKey)
			}
			seen[viewKey] = true
		}
	}
}

// === Volumes ===

func validateVolumes(volumes []VolumeResource, schemaKeys map[string]bool, errs *[]ValidationError) {
	seen := make(map[string]bool, len(volumes))
	for i, v := range volumes {
		path := fmt.Sprintf("volume[%d]", i)
		if v.CatalogName != "" && v.SchemaName != "" && v.VolumeName != "" {
			path = fmt.Sprintf("volume[%s.%s.%s]", v.CatalogName, v.SchemaName, v.VolumeName)
		}
		if v.CatalogName == "" {
			addErr(errs, path, "catalog_name is required")
		}
		if v.SchemaName == "" {
			addErr(errs, path, "schema_name is required")
		}
		if v.VolumeName == "" {
			addErr(errs, path, "volume_name is required")
		}

		schemaKey := v.CatalogName + "." + v.SchemaName
		if v.CatalogName != "" && v.SchemaName != "" && !schemaKeys[schemaKey] {
			addErr(errs, path, "references unknown schema %q", schemaKey)
		}

		if v.Spec.VolumeType != "" && !validVolumeTypes[v.Spec.VolumeType] {
			addErr(errs, path, "volume_type must be \"MANAGED\" or \"EXTERNAL\", got %q", v.Spec.VolumeType)
		}
		if v.Spec.VolumeType == "EXTERNAL" && v.Spec.StorageLocation == "" {
			addErr(errs, path, "storage_location is required for EXTERNAL volumes")
		}

		volKey := v.CatalogName + "." + v.SchemaName + "." + v.VolumeName
		if v.CatalogName != "" && v.SchemaName != "" && v.VolumeName != "" {
			if seen[volKey] {
				addErr(errs, path, "duplicate volume %q", volKey)
			}
			seen[volKey] = true
		}
	}
}

// === Row Filters ===

func validateRowFilters(
	rowFilters []RowFilterResource,
	tableKeys map[string]bool,
	principalNames, groupNames map[string]bool,
	errs *[]ValidationError,
) {
	for i, rf := range rowFilters {
		tableKey := rf.CatalogName + "." + rf.SchemaName + "." + rf.TableName
		path := fmt.Sprintf("row_filter[%s]", tableKey)

		if !tableKeys[tableKey] {
			addErr(errs, path, "references unknown table %q", tableKey)
		}

		filterSeen := make(map[string]bool, len(rf.Filters))
		for j, f := range rf.Filters {
			fpath := fmt.Sprintf("row_filter[%d].filter[%d]", i, j)
			if f.Name != "" {
				fpath = fmt.Sprintf("row_filter[%s].filter[%s]", tableKey, f.Name)
			}
			if f.Name == "" {
				addErr(errs, fpath, "filter name is required")
			}
			if f.FilterSQL == "" {
				addErr(errs, fpath, "filter_sql is required")
			}
			if f.Name != "" {
				if filterSeen[f.Name] {
					addErr(errs, fpath, "duplicate filter name %q within table %q", f.Name, tableKey)
				}
				filterSeen[f.Name] = true
			}

			for k, b := range f.Bindings {
				bpath := fmt.Sprintf("%s.bindings[%d]", fpath, k)
				if !validGrantPrincipalTypes[b.PrincipalType] {
					addErr(errs, bpath, "principal_type must be \"user\" or \"group\", got %q", b.PrincipalType)
				}
				if b.Principal == "" {
					addErr(errs, bpath, "principal is required")
				}
				if b.Principal != "" && !principalOrGroupExists(b.Principal, b.PrincipalType, principalNames, groupNames) {
					addErr(errs, bpath, "references unknown principal %q (type %q)", b.Principal, b.PrincipalType)
				}
			}
		}
	}
}

// === Column Masks ===

func validateColumnMasks(
	columnMasks []ColumnMaskResource,
	tableKeys map[string]bool,
	tableColumns map[string]map[string]bool,
	principalNames, groupNames map[string]bool,
	errs *[]ValidationError,
) {
	for i, cm := range columnMasks {
		tableKey := cm.CatalogName + "." + cm.SchemaName + "." + cm.TableName
		path := fmt.Sprintf("column_mask[%s]", tableKey)

		if !tableKeys[tableKey] {
			addErr(errs, path, "references unknown table %q", tableKey)
		}

		maskSeen := make(map[string]bool, len(cm.Masks))
		for j, m := range cm.Masks {
			mpath := fmt.Sprintf("column_mask[%d].mask[%d]", i, j)
			if m.Name != "" {
				mpath = fmt.Sprintf("column_mask[%s].mask[%s]", tableKey, m.Name)
			}
			if m.Name == "" {
				addErr(errs, mpath, "mask name is required")
			}
			if m.ColumnName == "" {
				addErr(errs, mpath, "column_name is required")
			}
			if m.MaskExpression == "" {
				addErr(errs, mpath, "mask_expression is required")
			}

			// Check column exists if table has columns defined.
			if m.ColumnName != "" && tableColumns[tableKey] != nil && !tableColumns[tableKey][m.ColumnName] {
				addErr(errs, mpath, "column %q not found in table %q", m.ColumnName, tableKey)
			}

			if m.Name != "" {
				if maskSeen[m.Name] {
					addErr(errs, mpath, "duplicate mask name %q within table %q", m.Name, tableKey)
				}
				maskSeen[m.Name] = true
			}

			for k, b := range m.Bindings {
				bpath := fmt.Sprintf("%s.bindings[%d]", mpath, k)
				if !validGrantPrincipalTypes[b.PrincipalType] {
					addErr(errs, bpath, "principal_type must be \"user\" or \"group\", got %q", b.PrincipalType)
				}
				if b.Principal == "" {
					addErr(errs, bpath, "principal is required")
				}
				if b.Principal != "" && !principalOrGroupExists(b.Principal, b.PrincipalType, principalNames, groupNames) {
					addErr(errs, bpath, "references unknown principal %q (type %q)", b.Principal, b.PrincipalType)
				}
			}
		}
	}
}

// === Tags ===

func validateTags(tags []TagSpec, errs *[]ValidationError) {
	seen := make(map[string]bool, len(tags))
	for i, t := range tags {
		path := fmt.Sprintf("tag[%d]", i)
		if t.Key != "" {
			path = fmt.Sprintf("tag[%s]", formatTagKey(t))
		}
		if t.Key == "" {
			addErr(errs, path, "key is required")
		}
		if t.Key != "" {
			k := formatTagKey(t)
			if seen[k] {
				addErr(errs, path, "duplicate tag %q", k)
			}
			seen[k] = true
		}
	}
}

// === Tag Assignments ===

func validateTagAssignments(
	assignments []TagAssignmentSpec,
	tagKeys map[string]bool,
	schemaKeys, tableKeys map[string]bool,
	tableColumns map[string]map[string]bool,
	errs *[]ValidationError,
) {
	for i, a := range assignments {
		path := fmt.Sprintf("tag_assignment[%d]", i)

		if a.Tag == "" {
			addErr(errs, path, "tag is required")
		}
		if a.Tag != "" && !tagKeys[a.Tag] {
			addErr(errs, path, "references unknown tag %q", a.Tag)
		}

		if !validTagSecurableTypes[a.SecurableType] {
			addErr(errs, path, "securable_type must be one of [schema, table, column], got %q", a.SecurableType)
		}

		if a.Securable == "" {
			addErr(errs, path, "securable is required")
		}

		// Validate securable existence.
		if a.Securable != "" && validTagSecurableTypes[a.SecurableType] {
			switch a.SecurableType {
			case "schema":
				if !schemaKeys[a.Securable] {
					addErr(errs, path, "references unknown schema %q", a.Securable)
				}
			case "table":
				if !tableKeys[a.Securable] {
					addErr(errs, path, "references unknown table %q", a.Securable)
				}
			case "column":
				if a.ColumnName == "" {
					addErr(errs, path, "column_name is required for column tag assignments")
				}
				if !tableKeys[a.Securable] {
					addErr(errs, path, "references unknown table %q", a.Securable)
				} else if a.ColumnName != "" && tableColumns[a.Securable] != nil && !tableColumns[a.Securable][a.ColumnName] {
					addErr(errs, path, "column %q not found in table %q", a.ColumnName, a.Securable)
				}
			}
		}
	}
}

// === Storage Credentials ===

func validateStorageCredentials(creds []StorageCredentialSpec, errs *[]ValidationError) {
	seen := make(map[string]bool, len(creds))
	for i, c := range creds {
		path := fmt.Sprintf("storage_credential[%d]", i)
		if c.Name != "" {
			path = fmt.Sprintf("storage_credential[%s]", c.Name)
		}
		if c.Name == "" {
			addErr(errs, path, "name is required")
		}
		if !validCredentialTypes[c.CredentialType] {
			addErr(errs, path, "credential_type must be \"S3\", \"AZURE\", or \"GCS\", got %q", c.CredentialType)
		}

		switch c.CredentialType {
		case "S3":
			if c.S3 == nil {
				addErr(errs, path, "s3 spec is required when credential_type is \"S3\"")
			} else {
				if c.S3.KeyIDFromEnv == "" {
					addErr(errs, path, "s3.key_id_from_env is required")
				}
				if c.S3.SecretFromEnv == "" {
					addErr(errs, path, "s3.secret_from_env is required")
				}
			}
		case "AZURE":
			if c.Azure == nil {
				addErr(errs, path, "azure spec is required when credential_type is \"AZURE\"")
			} else if c.Azure.AccountNameFromEnv == "" {
				addErr(errs, path, "azure.account_name_from_env is required")
			}
		case "GCS":
			if c.GCS == nil {
				addErr(errs, path, "gcs spec is required when credential_type is \"GCS\"")
			}
		}

		if c.Name != "" {
			if seen[c.Name] {
				addErr(errs, path, "duplicate storage credential name %q", c.Name)
			}
			seen[c.Name] = true
		}
	}
}

// === External Locations ===

func validateExternalLocations(locations []ExternalLocationSpec, credentialNames map[string]bool, errs *[]ValidationError) {
	seen := make(map[string]bool, len(locations))
	for i, l := range locations {
		path := fmt.Sprintf("external_location[%d]", i)
		if l.Name != "" {
			path = fmt.Sprintf("external_location[%s]", l.Name)
		}
		if l.Name == "" {
			addErr(errs, path, "name is required")
		}
		if l.URL == "" {
			addErr(errs, path, "url is required")
		}
		if l.CredentialName == "" {
			addErr(errs, path, "credential_name is required")
		}
		if l.CredentialName != "" && !credentialNames[l.CredentialName] {
			addErr(errs, path, "references unknown storage credential %q", l.CredentialName)
		}
		if l.Name != "" {
			if seen[l.Name] {
				addErr(errs, path, "duplicate external location name %q", l.Name)
			}
			seen[l.Name] = true
		}
	}
}

// === Compute Endpoints ===

func validateComputeEndpoints(endpoints []ComputeEndpointSpec, errs *[]ValidationError) {
	seen := make(map[string]bool, len(endpoints))
	for i, e := range endpoints {
		path := fmt.Sprintf("compute_endpoint[%d]", i)
		if e.Name != "" {
			path = fmt.Sprintf("compute_endpoint[%s]", e.Name)
		}
		if e.Name == "" {
			addErr(errs, path, "name is required")
		}
		if !validComputeTypes[e.Type] {
			addErr(errs, path, "type must be \"LOCAL\" or \"REMOTE\", got %q", e.Type)
		}
		if e.Type == "REMOTE" && e.URL == "" {
			addErr(errs, path, "url is required for REMOTE compute endpoints")
		}
		if e.Name != "" {
			if seen[e.Name] {
				addErr(errs, path, "duplicate compute endpoint name %q", e.Name)
			}
			seen[e.Name] = true
		}
	}
}

// === Compute Assignments ===

func validateComputeAssignments(
	assignments []ComputeAssignmentSpec,
	endpointNames, principalNames, groupNames map[string]bool,
	errs *[]ValidationError,
) {
	seen := make(map[string]bool, len(assignments))
	for i, a := range assignments {
		path := fmt.Sprintf("compute_assignment[%d]", i)

		if a.Endpoint == "" {
			addErr(errs, path, "endpoint is required")
		}
		if a.Endpoint != "" && !endpointNames[a.Endpoint] {
			addErr(errs, path, "references unknown compute endpoint %q", a.Endpoint)
		}
		if a.Principal == "" {
			addErr(errs, path, "principal is required")
		}
		if !validGrantPrincipalTypes[a.PrincipalType] {
			addErr(errs, path, "principal_type must be \"user\" or \"group\", got %q", a.PrincipalType)
		}
		if a.Principal != "" && !principalOrGroupExists(a.Principal, a.PrincipalType, principalNames, groupNames) {
			addErr(errs, path, "references unknown principal %q (type %q)", a.Principal, a.PrincipalType)
		}

		key := fmt.Sprintf("%s|%s|%s", a.Endpoint, a.Principal, a.PrincipalType)
		if seen[key] {
			addErr(errs, path, "duplicate compute assignment")
		}
		seen[key] = true
	}
}

// === API Keys ===

func validateAPIKeys(keys []APIKeySpec, principalNames map[string]bool, errs *[]ValidationError) {
	seen := make(map[string]bool, len(keys))
	for i, k := range keys {
		path := fmt.Sprintf("api_key[%d]", i)
		if k.Name != "" {
			path = fmt.Sprintf("api_key[%s]", k.Name)
		}
		if k.Name == "" {
			addErr(errs, path, "name is required")
		}
		if k.Principal == "" {
			addErr(errs, path, "principal is required")
		}
		if k.Principal != "" && !principalNames[k.Principal] {
			addErr(errs, path, "references unknown principal %q", k.Principal)
		}
		if k.Name != "" {
			if seen[k.Name] {
				addErr(errs, path, "duplicate API key name %q", k.Name)
			}
			seen[k.Name] = true
		}
	}
}

// === Notebooks ===

func validateNotebooks(notebooks []NotebookResource, errs *[]ValidationError) {
	seen := make(map[string]bool, len(notebooks))
	for i, n := range notebooks {
		path := fmt.Sprintf("notebook[%d]", i)
		if n.Name != "" {
			path = fmt.Sprintf("notebook[%s]", n.Name)
		}
		if n.Name == "" {
			addErr(errs, path, "name is required")
		}

		for j, c := range n.Spec.Cells {
			cpath := fmt.Sprintf("%s.cells[%d]", path, j)
			if !validCellTypes[c.Type] {
				addErr(errs, cpath, "cell type must be \"sql\" or \"markdown\", got %q", c.Type)
			}
			if c.Content == "" {
				addErr(errs, cpath, "content is required")
			}
		}

		if n.Name != "" {
			if seen[n.Name] {
				addErr(errs, path, "duplicate notebook name %q", n.Name)
			}
			seen[n.Name] = true
		}
	}
}

// === Pipelines ===

func validatePipelines(pipelines []PipelineResource, notebookNames, endpointNames map[string]bool, errs *[]ValidationError) {
	seen := make(map[string]bool, len(pipelines))
	for i, p := range pipelines {
		path := fmt.Sprintf("pipeline[%d]", i)
		if p.Name != "" {
			path = fmt.Sprintf("pipeline[%s]", p.Name)
		}
		if p.Name == "" {
			addErr(errs, path, "name is required")
		}

		// Build set of job names within this pipeline for depends_on validation.
		jobNames := make(map[string]bool, len(p.Spec.Jobs))
		for _, j := range p.Spec.Jobs {
			if j.Name != "" {
				jobNames[j.Name] = true
			}
		}

		jobSeen := make(map[string]bool, len(p.Spec.Jobs))
		for j, job := range p.Spec.Jobs {
			jpath := fmt.Sprintf("%s.job[%d]", path, j)
			if job.Name != "" {
				jpath = fmt.Sprintf("%s.job[%s]", path, job.Name)
			}
			if job.Name == "" {
				addErr(errs, jpath, "job name is required")
			}
			if job.Notebook == "" {
				addErr(errs, jpath, "notebook is required")
			}
			if job.Notebook != "" && !notebookNames[job.Notebook] {
				addErr(errs, jpath, "references unknown notebook %q", job.Notebook)
			}
			if job.ComputeEndpoint != "" && !endpointNames[job.ComputeEndpoint] {
				addErr(errs, jpath, "references unknown compute endpoint %q", job.ComputeEndpoint)
			}

			for _, dep := range job.DependsOn {
				if !jobNames[dep] {
					addErr(errs, jpath, "depends_on references unknown job %q in pipeline %q", dep, p.Name)
				}
			}

			if job.Name != "" {
				if jobSeen[job.Name] {
					addErr(errs, jpath, "duplicate job name %q within pipeline %q", job.Name, p.Name)
				}
				jobSeen[job.Name] = true
			}
		}

		// Detect circular dependencies among jobs.
		cycles := detectJobCycles(p.Spec.Jobs)
		for _, cycle := range cycles {
			addErr(errs, path, "circular job dependency detected: %s", strings.Join(cycle, " -> "))
		}

		if p.Name != "" {
			if seen[p.Name] {
				addErr(errs, path, "duplicate pipeline name %q", p.Name)
			}
			seen[p.Name] = true
		}
	}
}

// === Models ===

// Valid materialization types for models.
var validMaterializationTypes = map[string]bool{
	"VIEW":        true,
	"TABLE":       true,
	"INCREMENTAL": true,
	"EPHEMERAL":   true,
}

// Valid test types for model tests.
var validTestTypes = map[string]bool{
	"not_null":        true,
	"unique":          true,
	"accepted_values": true,
	"relationships":   true,
	"custom_sql":      true,
}

func validateModels(models []ModelResource, errs *[]ValidationError) {
	seen := make(map[string]bool, len(models))
	for i, m := range models {
		path := fmt.Sprintf("model[%d]", i)
		if m.ProjectName != "" && m.ModelName != "" {
			path = fmt.Sprintf("model[%s.%s]", m.ProjectName, m.ModelName)
		}
		if m.ProjectName == "" {
			addErr(errs, path, "project_name is required")
		}
		if m.ModelName == "" {
			addErr(errs, path, "model_name is required")
		}
		if m.Spec.SQL == "" {
			addErr(errs, path, "sql is required")
		}
		if m.Spec.Materialization != "" && !validMaterializationTypes[m.Spec.Materialization] {
			addErr(errs, path, "materialization must be one of [VIEW, TABLE, INCREMENTAL, EPHEMERAL], got %q", m.Spec.Materialization)
		}

		// Validate contract.
		if m.Spec.Contract != nil {
			validateModelContract(m.Spec.Contract, path, errs)
		}

		// Validate tests.
		validateModelTests(m.Spec.Tests, path, errs)

		key := m.ProjectName + "." + m.ModelName
		if m.ProjectName != "" && m.ModelName != "" {
			if seen[key] {
				addErr(errs, path, "duplicate model %q", key)
			}
			seen[key] = true
		}
	}
}

func validateModelContract(contract *ContractSpec, parentPath string, errs *[]ValidationError) {
	colSeen := make(map[string]bool, len(contract.Columns))
	for i, col := range contract.Columns {
		cpath := fmt.Sprintf("%s.contract.columns[%d]", parentPath, i)
		if col.Name == "" {
			addErr(errs, cpath, "column name is required")
		}
		if col.Type == "" {
			addErr(errs, cpath, "column type is required")
		}
		if col.Name != "" {
			if colSeen[col.Name] {
				addErr(errs, cpath, "duplicate contract column name %q", col.Name)
			}
			colSeen[col.Name] = true
		}
	}
}

func validateModelTests(tests []TestSpec, parentPath string, errs *[]ValidationError) {
	testSeen := make(map[string]bool, len(tests))
	for i, t := range tests {
		tpath := fmt.Sprintf("%s.tests[%d]", parentPath, i)
		if t.Name != "" {
			tpath = fmt.Sprintf("%s.tests[%s]", parentPath, t.Name)
		}
		if t.Name == "" {
			addErr(errs, tpath, "test name is required")
		}
		if !validTestTypes[t.Type] {
			addErr(errs, tpath, "test type must be one of [not_null, unique, accepted_values, relationships, custom_sql], got %q", t.Type)
		}

		switch t.Type {
		case "not_null", "unique":
			if t.Column == "" {
				addErr(errs, tpath, "column is required for %s tests", t.Type)
			}
		case "accepted_values":
			if t.Column == "" {
				addErr(errs, tpath, "column is required for accepted_values tests")
			}
			if len(t.Values) == 0 {
				addErr(errs, tpath, "values are required for accepted_values tests")
			}
		case "relationships":
			if t.Column == "" {
				addErr(errs, tpath, "column is required for relationships tests")
			}
			if t.ToModel == "" || t.ToColumn == "" {
				addErr(errs, tpath, "to_model and to_column are required for relationships tests")
			}
		case "custom_sql":
			if t.SQL == "" {
				addErr(errs, tpath, "sql is required for custom_sql tests")
			}
		}

		if t.Name != "" {
			if testSeen[t.Name] {
				addErr(errs, tpath, "duplicate test name %q", t.Name)
			}
			testSeen[t.Name] = true
		}
	}
}

// === Macros ===

// Valid macro types.
var validMacroTypes = map[string]bool{
	"SCALAR": true,
	"TABLE":  true,
	"":       true, // default to SCALAR
}

func validateMacros(macros []MacroResource, errs *[]ValidationError) {
	seen := make(map[string]bool, len(macros))
	for i, m := range macros {
		path := fmt.Sprintf("macro[%d]", i)
		if m.Name != "" {
			path = fmt.Sprintf("macro[%s]", m.Name)
		}
		if m.Name == "" {
			addErr(errs, path, "name is required")
		}
		if m.Spec.Body == "" {
			addErr(errs, path, "body is required")
		}
		if !validMacroTypes[m.Spec.MacroType] {
			addErr(errs, path, "macro_type must be \"SCALAR\" or \"TABLE\", got %q", m.Spec.MacroType)
		}
		if m.Name != "" {
			if seen[m.Name] {
				addErr(errs, path, "duplicate macro name %q", m.Name)
			}
			seen[m.Name] = true
		}
	}
}

// detectJobCycles finds circular dependencies in pipeline job graphs using DFS.
func detectJobCycles(jobs []PipelineJobSpec) [][]string {
	adj := make(map[string][]string, len(jobs))
	for _, j := range jobs {
		adj[j.Name] = append(adj[j.Name], j.DependsOn...)
	}

	var cycles [][]string
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(jobs))
	parent := make(map[string]string, len(jobs))

	var dfs func(node string)
	dfs = func(node string) {
		color[node] = gray
		for _, next := range adj[node] {
			if color[next] == gray {
				cycle := []string{next, node}
				cur := node
				for cur != next {
					cur = parent[cur]
					if cur == "" {
						break
					}
					cycle = append(cycle, cur)
				}
				for l, r := 0, len(cycle)-1; l < r; l, r = l+1, r-1 {
					cycle[l], cycle[r] = cycle[r], cycle[l]
				}
				cycles = append(cycles, cycle)
				return
			}
			if color[next] == white {
				parent[next] = node
				dfs(next)
			}
		}
		color[node] = black
	}

	for _, j := range jobs {
		if j.Name != "" && color[j.Name] == white {
			dfs(j.Name)
		}
	}
	return cycles
}
