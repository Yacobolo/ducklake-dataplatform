package declarative

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/duckdbsql"
)

// ValidationError represents a single validation problem.
type ValidationError struct {
	Path    string // e.g. "security/principals.cue" or "principal[analyst1]"
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

// Valid scope types for preset bindings.
var validBindingScopeTypes = map[string]bool{
	"catalog":            true,
	"schema":             true,
	"table":              true,
	"external_location":  true,
	"storage_credential": true,
	"volume":             true,
}

// Valid privilege names.
var validPrivileges = map[string]bool{
	"SELECT":                        true,
	"INSERT":                        true,
	"UPDATE":                        true,
	"DELETE":                        true,
	"USE_CATALOG":                   true,
	"USE_SCHEMA":                    true,
	"USAGE":                         true,
	"CREATE_TABLE":                  true,
	"CREATE_VIEW":                   true,
	"CREATE_SCHEMA":                 true,
	"MODIFY":                        true,
	"MANAGE":                        true,
	"APPLY_TAG":                     true,
	"MANAGE_TAGS":                   true,
	"MANAGE_POLICIES":               true,
	"ALL_PRIVILEGES":                true,
	"CREATE_EXTERNAL_LOCATION":      true,
	"CREATE_STORAGE_CREDENTIAL":     true,
	"CREATE_VOLUME":                 true,
	"READ_VOLUME":                   true,
	"WRITE_VOLUME":                  true,
	"READ_FILES":                    true,
	"WRITE_FILES":                   true,
	"MANAGE_COMPUTE":                true,
	"MANAGE_ASSET_DEFINITIONS":      true,
	"EXECUTE_ASSET_MATERIALIZATION": true,
	"MANAGE_ASSET_POLICIES":         true,
}

var allowedPrivilegesBySecurable = map[string]map[string]bool{
	"catalog": {
		"USE_CATALOG":                   true,
		"USAGE":                         true,
		"CREATE_SCHEMA":                 true,
		"CREATE_EXTERNAL_LOCATION":      true,
		"CREATE_STORAGE_CREDENTIAL":     true,
		"MANAGE_COMPUTE":                true,
		"MANAGE_ASSET_DEFINITIONS":      true,
		"EXECUTE_ASSET_MATERIALIZATION": true,
		"MANAGE_ASSET_POLICIES":         true,
		"MANAGE_TAGS":                   true,
		"MODIFY":                        true,
		"MANAGE":                        true,
		"ALL_PRIVILEGES":                true,
	},
	"schema": {
		"USE_CATALOG":    true,
		"USE_SCHEMA":     true,
		"USAGE":          true,
		"SELECT":         true,
		"INSERT":         true,
		"UPDATE":         true,
		"DELETE":         true,
		"CREATE_TABLE":   true,
		"CREATE_VIEW":    true,
		"CREATE_VOLUME":  true,
		"MODIFY":         true,
		"MANAGE":         true,
		"APPLY_TAG":      true,
		"MANAGE_TAGS":    true,
		"ALL_PRIVILEGES": true,
	},
	"table": {
		"SELECT":          true,
		"INSERT":          true,
		"UPDATE":          true,
		"DELETE":          true,
		"MODIFY":          true,
		"MANAGE":          true,
		"APPLY_TAG":       true,
		"MANAGE_POLICIES": true,
		"MANAGE_TAGS":     true,
		"ALL_PRIVILEGES":  true,
	},
	"volume": {
		"READ_VOLUME":    true,
		"WRITE_VOLUME":   true,
		"MODIFY":         true,
		"MANAGE":         true,
		"APPLY_TAG":      true,
		"MANAGE_TAGS":    true,
		"ALL_PRIVILEGES": true,
	},
	"external_location": {
		"READ_FILES":     true,
		"WRITE_FILES":    true,
		"MODIFY":         true,
		"MANAGE":         true,
		"APPLY_TAG":      true,
		"MANAGE_TAGS":    true,
		"ALL_PRIVILEGES": true,
	},
	"storage_credential": {
		"MODIFY":         true,
		"MANAGE":         true,
		"APPLY_TAG":      true,
		"MANAGE_TAGS":    true,
		"ALL_PRIVILEGES": true,
	},
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

var validCellRoles = map[string]bool{
	"transform": true,
	"output":    true,
	"test":      true,
	"markdown":  true,
}

var validNotebookTestSeverities = map[string]bool{
	"error": true,
	"warn":  true,
}

// Valid tag assignment securable types.
var validTagSecurableTypes = map[string]bool{
	"schema": true,
	"table":  true,
	"column": true,
}

var validWorkspaceKinds = map[string]bool{
	"personal": true,
	"shared":   true,
	"library":  true,
}

var validProjectKinds = map[string]bool{
	"personal": true,
	"shared":   true,
	"library":  true,
}

var validEnvironmentKinds = map[string]bool{
	"development": true,
	"staging":     true,
	"production":  true,
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

	domainNames := make(map[string]bool, len(state.Domains))
	for _, d := range state.Domains {
		domainNames[d.Name] = true
	}

	teamNames := make(map[string]bool, len(state.Teams))
	for _, team := range state.Teams {
		teamNames[team.Name] = true
	}

	productNames := make(map[string]bool, len(state.DataProducts))
	for _, product := range state.DataProducts {
		productNames[product.Slug] = true
	}

	workspaceNames := make(map[string]bool, len(state.Workspaces))
	for _, workspace := range state.Workspaces {
		workspaceNames[workspace.Name] = true
	}

	projectKeys := make(map[string]bool, len(state.Projects))
	for _, project := range state.Projects {
		key := projectRefKey(project.Spec.WorkspaceRef, project.Name)
		if key != "" {
			projectKeys[key] = true
		}
	}

	environmentKeys := make(map[string]bool, len(state.Environments))
	for _, environment := range state.Environments {
		key := environmentRefKeyFromProjectRef(environment.Spec.ProjectRef, environment.Name)
		if key != "" {
			environmentKeys[key] = true
		}
	}

	folderKeys := make(map[string]bool, len(state.Folders))
	for _, folder := range state.Folders {
		key := folderRefKey(folder.Spec.WorkspaceRef, folder.Spec.ParentFolderRef, folder.Name)
		if key != "" {
			folderKeys[key] = true
		}
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

	assetNames := make(map[string]bool, len(state.Assets))
	for _, asset := range state.Assets {
		assetNames[asset.Name] = true
	}

	notebookNames := make(map[string]bool, len(state.Notebooks))
	notebookCellNames := make(map[string]map[string]bool, len(state.Notebooks))
	for _, n := range state.Notebooks {
		notebookNames[n.Name] = true
		cellNames := make(map[string]bool, len(n.Spec.Cells))
		for _, cell := range n.Spec.Cells {
			cellName := strings.TrimSpace(cell.Name)
			if cellName != "" {
				cellNames[cellName] = true
			}
		}
		notebookCellNames[n.Name] = cellNames
	}

	tagKeys := make(map[string]bool, len(state.Tags))
	for _, t := range state.Tags {
		tagKeys[formatTagKey(t)] = true
	}

	macroNames := make(map[string]bool, len(state.Macros))
	for _, m := range state.Macros {
		if m.Name != "" {
			macroNames[m.Name] = true
		}
	}

	semanticModelKeys := make(map[string]bool, len(state.SemanticModels))
	for _, m := range state.SemanticModels {
		semanticModelKeys[m.ModelName] = true
	}

	presetNames := make(map[string]bool, len(state.PrivilegePresets))
	for _, p := range state.PrivilegePresets {
		presetNames[p.Name] = true
	}

	// 1. Validate principals.
	validatePrincipals(state.Principals, &errs)

	// 2. Validate groups.
	validateGroups(state.Groups, principalNames, groupNames, &errs)

	// 3. Validate grants.
	validateGrants(state.Grants, principalNames, groupNames, catalogNames, schemaKeys, tableKeys, locationNames, credentialNames, volumeKeys, &errs)

	// 4. Validate privilege presets.
	validatePrivilegePresets(state.PrivilegePresets, &errs)

	// 5. Validate preset bindings.
	validateBindings(state.Bindings, principalNames, groupNames, presetNames, catalogNames, schemaKeys, tableKeys, locationNames, credentialNames, volumeKeys, &errs)
	validateEffectiveBindingGrants(state.Grants, state.PrivilegePresets, state.Bindings, &errs)

	// 6. Validate catalogs.
	validateCatalogs(state.Catalogs, &errs)

	// 7. Validate schemas.
	validateSchemas(state.Schemas, catalogNames, &errs)

	// 8. Validate tables.
	validateTables(state.Tables, schemaKeys, &errs)

	// 9. Validate views.
	validateViews(state.Views, schemaKeys, &errs)

	// 10. Validate volumes.
	validateVolumes(state.Volumes, schemaKeys, &errs)

	// 11. Validate row filters.
	validateRowFilters(state.RowFilters, tableKeys, principalNames, groupNames, &errs)

	// 12. Validate column masks.
	validateColumnMasks(state.ColumnMasks, tableKeys, tableColumns, principalNames, groupNames, &errs)

	// 13. Validate tags.
	validateTags(state.Tags, &errs)

	// 14. Validate tag assignments.
	validateTagAssignments(state.TagAssignments, tagKeys, schemaKeys, tableKeys, tableColumns, &errs)

	// 15. Validate storage credentials.
	validateStorageCredentials(state.StorageCredentials, &errs)

	// 16. Validate external locations.
	validateExternalLocations(state.ExternalLocations, credentialNames, &errs)

	// 17. Validate compute endpoints.
	validateComputeEndpoints(state.ComputeEndpoints, &errs)

	// 18. Validate compute assignments.
	validateComputeAssignments(state.ComputeAssignments, endpointNames, principalNames, groupNames, &errs)

	// 18a. Validate compute routing defaults.
	validateComputeRoutingDefaults(state.ComputeDefaults, &errs)

	// 19. Validate API keys.
	validateAPIKeys(state.APIKeys, principalNames, &errs)

	// 20. Validate notebooks.
	validateWorkspaces(state.Workspaces, projectKeys, environmentKeys, &errs)
	validateFolders(state.Folders, workspaceNames, folderKeys, projectKeys, environmentKeys, &errs)
	validateProjects(state.Projects, workspaceNames, &errs)
	validateEnvironments(state.Environments, projectKeys, &errs)
	validateNotebooks(state.Notebooks, workspaceNames, folderKeys, projectKeys, environmentKeys, &errs)

	// 21. Validate product control-plane resources.
	validateDomains(state.Domains, &errs)
	validateTeams(state.Teams, domainNames, &errs)
	validateDataProducts(state.DataProducts, domainNames, teamNames, assetNames, semanticModelKeys, productNames, &errs)

	// 22. Validate orchestration resources.
	validateAssets(state.Assets, productNames, &errs)

	// 23. Validate macros.
	validateMacros(state.Macros, &errs)

	// 24. Validate models.
	validateModels(state.Models, macroNames, &errs)
	validateNotebookPublishTargets(state.Notebooks, state.Models, &errs)

	// 25. Validate semantic models.
	validateSemanticModels(state.SemanticModels, &errs)

	// 26. Validate dashboards.
	validateDashboards(state.Dashboards, notebookNames, notebookCellNames, semanticModelKeys, &errs)

	return errs
}

func projectRefKey(workspaceRef, projectName string) string {
	workspaceRef = strings.TrimSpace(workspaceRef)
	projectName = strings.TrimSpace(projectName)
	if workspaceRef == "" || projectName == "" {
		return ""
	}
	return workspaceRef + "/" + projectName
}

func environmentRefKeyFromProjectRef(projectRef, environmentName string) string {
	projectRef = strings.TrimSpace(projectRef)
	environmentName = strings.TrimSpace(environmentName)
	if projectRef == "" || environmentName == "" {
		return ""
	}
	return projectRef + "/" + environmentName
}

func folderRefKey(workspaceRef, parentFolderRef, folderName string) string {
	workspaceRef = strings.TrimSpace(workspaceRef)
	parentFolderRef = strings.TrimSpace(parentFolderRef)
	folderName = strings.TrimSpace(folderName)
	if workspaceRef == "" || folderName == "" {
		return ""
	}
	if parentFolderRef != "" {
		return parentFolderRef + "/" + folderName
	}
	return workspaceRef + "/" + folderName
}

func parseProjectRef(ref string) (string, string, bool) {
	parts := strings.Split(strings.TrimSpace(ref), "/")
	if len(parts) != 2 {
		return "", "", false
	}
	if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", false
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), true
}

func parseEnvironmentRef(ref string) (string, string, string, bool) {
	parts := strings.Split(strings.TrimSpace(ref), "/")
	if len(parts) != 3 {
		return "", "", "", false
	}
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return "", "", "", false
		}
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), strings.TrimSpace(parts[2]), true
}

func parseFolderRef(ref string) (string, []string, bool) {
	parts := strings.Split(strings.TrimSpace(ref), "/")
	if len(parts) < 2 {
		return "", nil, false
	}
	segments := make([]string, 0, len(parts)-1)
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return "", nil, false
		}
		if i == 0 {
			continue
		}
		segments = append(segments, part)
	}
	return strings.TrimSpace(parts[0]), segments, true
}

func workspaceNameFromFolderRef(ref string) string {
	workspace, _, ok := parseFolderRef(ref)
	if !ok {
		return ""
	}
	return workspace
}

func workspaceNameFromProjectRef(ref string) string {
	workspace, _, ok := parseProjectRef(ref)
	if !ok {
		return ""
	}
	return workspace
}

func workspaceNameFromEnvironmentRef(ref string) string {
	workspace, _, _, ok := parseEnvironmentRef(ref)
	if !ok {
		return ""
	}
	return workspace
}

func validateNotebookPublishTargets(notebooks []NotebookResource, models []ModelResource, errs *[]ValidationError) {
	modelKeys := make(map[string]bool, len(models))
	for _, m := range models {
		modelKeys[m.ProjectName+"."+m.ModelName] = true
	}

	for i, n := range notebooks {
		path := fmt.Sprintf("notebook[%d]", i)
		if n.Name != "" {
			path = fmt.Sprintf("notebook[%s]", n.Name)
		}
		if n.Spec.Publish == nil || n.Spec.Publish.Model == nil {
			continue
		}
		pm := n.Spec.Publish.Model
		key := pm.Project + "." + pm.Name
		if modelKeys[key] {
			addErr(errs, path, "publish.model target %q conflicts with standalone model resource; use notebook publish only", key)
		}
	}
}

func validateWorkspaces(workspaces []WorkspaceResource, projectKeys, environmentKeys map[string]bool, errs *[]ValidationError) {
	seen := make(map[string]bool, len(workspaces))
	for i, item := range workspaces {
		path := fmt.Sprintf("workspace[%d]", i)
		if item.Name != "" {
			path = fmt.Sprintf("workspace[%s]", item.Name)
		}
		if strings.TrimSpace(item.Name) == "" {
			addErr(errs, path, "name is required")
			continue
		}
		if seen[item.Name] {
			addErr(errs, path, "duplicate workspace name %q", item.Name)
		}
		seen[item.Name] = true

		kind := strings.ToLower(strings.TrimSpace(item.Spec.Kind))
		if !validWorkspaceKinds[kind] {
			addErr(errs, path, "kind must be one of [personal, shared, library], got %q", item.Spec.Kind)
		}
		switch kind {
		case "personal":
			if strings.TrimSpace(item.Spec.OwnerPrincipal) == "" {
				addErr(errs, path, "owner_principal is required for personal workspaces")
			}
			if strings.TrimSpace(item.Spec.OwnerTeamID) != "" {
				addErr(errs, path, "owner_team_id must be empty for personal workspaces")
			}
		case "shared", "library":
			if strings.TrimSpace(item.Spec.OwnerTeamID) == "" {
				addErr(errs, path, "owner_team_id is required for %s workspaces", kind)
			}
			if strings.TrimSpace(item.Spec.OwnerPrincipal) != "" {
				addErr(errs, path, "owner_principal must be empty for %s workspaces", kind)
			}
		}

		if strings.TrimSpace(item.Spec.DefaultProjectRef) != "" && !projectKeys[item.Spec.DefaultProjectRef] {
			addErr(errs, path, "default_project_ref references unknown project %q", item.Spec.DefaultProjectRef)
		}
		if strings.TrimSpace(item.Spec.DefaultEnvironmentRef) != "" {
			if !environmentKeys[item.Spec.DefaultEnvironmentRef] {
				addErr(errs, path, "default_environment_ref references unknown environment %q", item.Spec.DefaultEnvironmentRef)
			}
			if strings.TrimSpace(item.Spec.DefaultProjectRef) == "" {
				addErr(errs, path, "default_environment_ref requires default_project_ref")
			} else if !strings.HasPrefix(item.Spec.DefaultEnvironmentRef, item.Spec.DefaultProjectRef+"/") {
				addErr(errs, path, "default_environment_ref must belong to default_project_ref")
			}
		}
	}
}

func validateFolders(
	folders []FolderResource,
	workspaceNames, folderKeys, projectKeys, environmentKeys map[string]bool,
	errs *[]ValidationError,
) {
	seen := make(map[string]bool, len(folders))
	for i, item := range folders {
		path := fmt.Sprintf("folder[%d]", i)
		if item.Name != "" {
			path = fmt.Sprintf("folder[%s]", item.Name)
		}

		if strings.TrimSpace(item.Name) == "" {
			addErr(errs, path, "name is required")
			continue
		}
		if strings.TrimSpace(item.Spec.WorkspaceRef) == "" {
			addErr(errs, path, "workspace_ref is required")
		} else if !workspaceNames[item.Spec.WorkspaceRef] {
			addErr(errs, path, "workspace_ref references unknown workspace %q", item.Spec.WorkspaceRef)
		}

		if strings.TrimSpace(item.Spec.ParentFolderRef) != "" {
			parentWorkspace, _, ok := parseFolderRef(item.Spec.ParentFolderRef)
			if !ok {
				addErr(errs, path, "parent_folder_ref must be in the form <workspace>/<folder-path>")
			} else {
				if parentWorkspace != strings.TrimSpace(item.Spec.WorkspaceRef) {
					addErr(errs, path, "parent_folder_ref must be in the same workspace as workspace_ref")
				}
				if !folderKeys[item.Spec.ParentFolderRef] {
					addErr(errs, path, "parent_folder_ref references unknown folder %q", item.Spec.ParentFolderRef)
				}
			}
		}

		key := folderRefKey(item.Spec.WorkspaceRef, item.Spec.ParentFolderRef, item.Name)
		if key != "" {
			if seen[key] {
				addErr(errs, path, "duplicate folder key %q", key)
			}
			seen[key] = true
		}

		if strings.TrimSpace(item.Spec.DefaultProjectRef) != "" {
			if !projectKeys[item.Spec.DefaultProjectRef] {
				addErr(errs, path, "default_project_ref references unknown project %q", item.Spec.DefaultProjectRef)
			} else if workspaceNameFromProjectRef(item.Spec.DefaultProjectRef) != strings.TrimSpace(item.Spec.WorkspaceRef) {
				addErr(errs, path, "default_project_ref must belong to the same workspace")
			}
		}
		if strings.TrimSpace(item.Spec.DefaultEnvironmentRef) != "" {
			if !environmentKeys[item.Spec.DefaultEnvironmentRef] {
				addErr(errs, path, "default_environment_ref references unknown environment %q", item.Spec.DefaultEnvironmentRef)
			} else if workspaceNameFromEnvironmentRef(item.Spec.DefaultEnvironmentRef) != strings.TrimSpace(item.Spec.WorkspaceRef) {
				addErr(errs, path, "default_environment_ref must belong to the same workspace")
			}
			if strings.TrimSpace(item.Spec.DefaultProjectRef) == "" {
				addErr(errs, path, "default_environment_ref requires default_project_ref")
			} else if !strings.HasPrefix(item.Spec.DefaultEnvironmentRef, item.Spec.DefaultProjectRef+"/") {
				addErr(errs, path, "default_environment_ref must belong to default_project_ref")
			}
		}
	}
}

func validateProjects(projects []ProjectResource, workspaceNames map[string]bool, errs *[]ValidationError) {
	seen := make(map[string]bool, len(projects))
	for i, item := range projects {
		path := fmt.Sprintf("project[%d]", i)
		if item.Name != "" {
			path = fmt.Sprintf("project[%s]", item.Name)
		}

		if strings.TrimSpace(item.Name) == "" {
			addErr(errs, path, "name is required")
			continue
		}
		if strings.TrimSpace(item.Spec.WorkspaceRef) == "" {
			addErr(errs, path, "workspace_ref is required")
		} else if !workspaceNames[item.Spec.WorkspaceRef] {
			addErr(errs, path, "workspace_ref references unknown workspace %q", item.Spec.WorkspaceRef)
		}
		if !validProjectKinds[strings.ToLower(strings.TrimSpace(item.Spec.Kind))] {
			addErr(errs, path, "kind must be one of [personal, shared, library], got %q", item.Spec.Kind)
		}
		key := projectRefKey(item.Spec.WorkspaceRef, item.Name)
		if key != "" {
			if seen[key] {
				addErr(errs, path, "duplicate project key %q", key)
			}
			seen[key] = true
		}
	}
}

func validateEnvironments(environments []EnvironmentResource, projectKeys map[string]bool, errs *[]ValidationError) {
	seen := make(map[string]bool, len(environments))
	for i, item := range environments {
		path := fmt.Sprintf("environment[%d]", i)
		if item.Name != "" {
			path = fmt.Sprintf("environment[%s]", item.Name)
		}

		if strings.TrimSpace(item.Name) == "" {
			addErr(errs, path, "name is required")
			continue
		}
		if strings.TrimSpace(item.Spec.ProjectRef) == "" {
			addErr(errs, path, "project_ref is required")
		} else if !projectKeys[item.Spec.ProjectRef] {
			addErr(errs, path, "project_ref references unknown project %q", item.Spec.ProjectRef)
		}
		if !validEnvironmentKinds[strings.ToLower(strings.TrimSpace(item.Spec.Kind))] {
			addErr(errs, path, "kind must be one of [development, staging, production], got %q", item.Spec.Kind)
		}
		if strings.TrimSpace(item.Spec.TargetCatalog) == "" {
			addErr(errs, path, "target_catalog is required")
		}
		if strings.TrimSpace(item.Spec.TargetSchema) == "" {
			addErr(errs, path, "target_schema is required")
		}
		key := environmentRefKeyFromProjectRef(item.Spec.ProjectRef, item.Name)
		if key != "" {
			if seen[key] {
				addErr(errs, path, "duplicate environment key %q", key)
			}
			seen[key] = true
		}
	}
}

func validateDomains(domains []DomainResource, errs *[]ValidationError) {
	seen := make(map[string]bool, len(domains))
	for i, item := range domains {
		path := fmt.Sprintf("domain[%d]", i)
		if item.Name != "" {
			path = fmt.Sprintf("domain[%s]", item.Name)
		}
		if strings.TrimSpace(item.Name) == "" {
			addErr(errs, path, "name is required")
			continue
		}
		if seen[item.Name] {
			addErr(errs, path, "duplicate domain name %q", item.Name)
		}
		seen[item.Name] = true
	}
}

func validateTeams(teams []TeamResource, domainNames map[string]bool, errs *[]ValidationError) {
	seen := make(map[string]bool, len(teams))
	for i, item := range teams {
		path := fmt.Sprintf("team[%d]", i)
		if item.Name != "" {
			path = fmt.Sprintf("team[%s]", item.Name)
		}
		if strings.TrimSpace(item.Name) == "" {
			addErr(errs, path, "name is required")
			continue
		}
		if seen[item.Name] {
			addErr(errs, path, "duplicate team name %q", item.Name)
		}
		seen[item.Name] = true
		if strings.TrimSpace(item.Spec.DomainRef) == "" {
			addErr(errs, path, "domain_ref is required")
		} else if !domainNames[item.Spec.DomainRef] {
			addErr(errs, path, "domain_ref references unknown domain %q", item.Spec.DomainRef)
		}
	}
}

func validateDataProducts(
	products []DataProductResource,
	domainNames map[string]bool,
	teamNames map[string]bool,
	assetNames map[string]bool,
	semanticModelKeys map[string]bool,
	productNames map[string]bool,
	errs *[]ValidationError,
) {
	seen := make(map[string]bool, len(products))
	for i, item := range products {
		path := fmt.Sprintf("data_product[%d]", i)
		if item.Slug != "" {
			path = fmt.Sprintf("data_product[%s]", item.Slug)
		}
		if strings.TrimSpace(item.Slug) == "" {
			addErr(errs, path, "slug is required")
			continue
		}
		if seen[item.Slug] {
			addErr(errs, path, "duplicate data product slug %q", item.Slug)
		}
		seen[item.Slug] = true
		if strings.TrimSpace(item.Spec.DomainRef) == "" {
			addErr(errs, path, "domain_ref is required")
		} else if !domainNames[item.Spec.DomainRef] {
			addErr(errs, path, "domain_ref references unknown domain %q", item.Spec.DomainRef)
		}
		if strings.TrimSpace(item.Spec.OwnerTeamRef) == "" {
			addErr(errs, path, "owner_team_ref is required")
		} else if !teamNames[item.Spec.OwnerTeamRef] {
			addErr(errs, path, "owner_team_ref references unknown team %q", item.Spec.OwnerTeamRef)
		}
		if strings.TrimSpace(item.Spec.PublicationIntent) != "" {
			switch strings.ToUpper(strings.TrimSpace(item.Spec.PublicationIntent)) {
			case "DRAFT", "PUBLISHED":
			default:
				addErr(errs, path, "publication_intent must be DRAFT or PUBLISHED")
			}
		}
		for j, output := range item.Spec.Outputs {
			output = strings.TrimSpace(output)
			if output == "" {
				addErr(errs, fmt.Sprintf("%s.outputs[%d]", path, j), "output reference must not be blank")
				continue
			}
			if !assetNames[output] {
				addErr(errs, fmt.Sprintf("%s.outputs[%d]", path, j), "output references unknown asset %q", output)
			}
		}
		for j, entrypoint := range item.Spec.SemanticEntrypoints {
			entrypoint = strings.TrimSpace(entrypoint)
			if entrypoint == "" {
				addErr(errs, fmt.Sprintf("%s.semantic_entrypoints[%d]", path, j), "semantic_entrypoints entry must not be blank")
				continue
			}
			if !semanticModelKeys[entrypoint] {
				addErr(errs, fmt.Sprintf("%s.semantic_entrypoints[%d]", path, j), "semantic_entrypoints references unknown semantic model %q", entrypoint)
			}
		}
		for j, dep := range item.Spec.Dependencies {
			dep = strings.TrimSpace(dep)
			if dep == "" {
				addErr(errs, fmt.Sprintf("%s.dependencies[%d]", path, j), "dependency reference must not be blank")
				continue
			}
			if dep == item.Slug {
				addErr(errs, fmt.Sprintf("%s.dependencies[%d]", path, j), "dependency must not reference the product itself")
				continue
			}
			if !productNames[dep] {
				addErr(errs, fmt.Sprintf("%s.dependencies[%d]", path, j), "dependency references unknown data product %q", dep)
			}
		}
		validateProductVersions(path, item.Spec.Versions, assetNames, semanticModelKeys, errs)
	}
}

func validateProductVersions(path string, versions []DataProductVersionSpec, assetNames map[string]bool, semanticModelKeys map[string]bool, errs *[]ValidationError) {
	seen := make(map[int]bool, len(versions))
	for i, version := range versions {
		vpath := fmt.Sprintf("%s.versions[%d]", path, i)
		if version.Version <= 0 {
			addErr(errs, vpath, "version must be > 0")
		}
		if seen[version.Version] {
			addErr(errs, vpath, "duplicate version number %d", version.Version)
		}
		seen[version.Version] = true
		if strings.TrimSpace(version.ReleaseState) != "" {
			switch strings.ToUpper(strings.TrimSpace(version.ReleaseState)) {
			case "DRAFT", "PUBLISHED", "DEPRECATED", "RETIRED":
			default:
				addErr(errs, vpath, "release_state must be DRAFT, PUBLISHED, DEPRECATED, or RETIRED")
			}
		}
		if strings.TrimSpace(version.CompatibilityLevel) != "" {
			switch strings.ToUpper(strings.TrimSpace(version.CompatibilityLevel)) {
			case "BACKWARD_COMPATIBLE", "BREAKING":
			default:
				addErr(errs, vpath, "compatibility_level must be BACKWARD_COMPATIBLE or BREAKING")
			}
		}
		for j, output := range version.Outputs {
			output = strings.TrimSpace(output)
			if output == "" {
				addErr(errs, fmt.Sprintf("%s.outputs[%d]", vpath, j), "output reference must not be blank")
				continue
			}
			if !assetNames[output] {
				addErr(errs, fmt.Sprintf("%s.outputs[%d]", vpath, j), "output references unknown asset %q", output)
			}
		}
		for j, entrypoint := range version.SemanticEntrypoints {
			entrypoint = strings.TrimSpace(entrypoint)
			if entrypoint == "" {
				addErr(errs, fmt.Sprintf("%s.semantic_entrypoints[%d]", vpath, j), "semantic_entrypoints entry must not be blank")
				continue
			}
			if !semanticModelKeys[entrypoint] {
				addErr(errs, fmt.Sprintf("%s.semantic_entrypoints[%d]", vpath, j), "semantic_entrypoints references unknown semantic model %q", entrypoint)
			}
		}
	}
}

func validatePrivilegePresets(presets []PrivilegePresetSpec, errs *[]ValidationError) {
	seen := make(map[string]bool, len(presets))
	for i, p := range presets {
		path := fmt.Sprintf("privilege_preset[%d]", i)
		if p.Name != "" {
			path = fmt.Sprintf("privilege_preset[%s]", p.Name)
		}
		if p.Name == "" {
			addErr(errs, path, "name is required")
		}
		if p.Name != "" {
			if seen[p.Name] {
				addErr(errs, path, "duplicate preset name %q", p.Name)
			}
			seen[p.Name] = true
		}
		if len(p.Privileges) == 0 {
			addErr(errs, path, "privileges must contain at least one entry")
		}
		for j, priv := range p.Privileges {
			ppath := fmt.Sprintf("%s.privileges[%d]", path, j)
			if !validPrivileges[priv] {
				addErr(errs, ppath, "unknown privilege %q", priv)
			}
		}
	}
}

func validateBindings(
	bindings []BindingSpec,
	principalNames, groupNames, presetNames, catalogNames, schemaKeys, tableKeys, locationNames, credentialNames, volumeKeys map[string]bool,
	errs *[]ValidationError,
) {
	seen := make(map[string]bool, len(bindings))
	for i, b := range bindings {
		path := fmt.Sprintf("binding[%d]", i)

		if b.Principal == "" {
			addErr(errs, path, "principal is required")
		}
		if !validGrantPrincipalTypes[b.PrincipalType] {
			addErr(errs, path, "principal_type must be \"user\" or \"group\", got %q", b.PrincipalType)
		}
		if b.PrincipalType == "user" && b.Principal != "" && !principalNames[b.Principal] {
			addErr(errs, path, "principal %q references unknown user", b.Principal)
		}
		if b.PrincipalType == "group" && b.Principal != "" && !groupNames[b.Principal] {
			addErr(errs, path, "principal %q references unknown group", b.Principal)
		}

		if b.Preset == "" {
			addErr(errs, path, "preset is required")
		} else if !presetNames[b.Preset] {
			addErr(errs, path, "preset %q references unknown preset", b.Preset)
		}

		if !validBindingScopeTypes[b.ScopeType] {
			addErr(errs, path, "scope_type must be one of [catalog, schema, table, external_location, storage_credential, volume], got %q", b.ScopeType)
		}
		if b.Scope == "" {
			addErr(errs, path, "scope is required")
		}
		if b.Scope != "" && validBindingScopeTypes[b.ScopeType] {
			validateBindingScope(b.ScopeType, b.Scope, catalogNames, schemaKeys, tableKeys, locationNames, credentialNames, volumeKeys, path, errs)
		}

		key := fmt.Sprintf("%s|%s|%s|%s|%s", b.Principal, b.PrincipalType, b.Preset, b.ScopeType, b.Scope)
		if seen[key] {
			addErr(errs, path, "duplicate binding")
		}
		seen[key] = true
	}
}

func validateBindingScope(
	scopeType, scope string,
	catalogNames, schemaKeys, tableKeys, locationNames, credentialNames, volumeKeys map[string]bool,
	path string,
	errs *[]ValidationError,
) {
	parts := strings.Split(scope, ".")
	switch scopeType {
	case "catalog":
		if len(parts) != 1 {
			addErr(errs, path, "catalog scope must be a single name, got %q", scope)
		} else if !catalogNames[scope] {
			addErr(errs, path, "scope references unknown catalog %q", scope)
		}
	case "schema":
		if len(parts) != 2 {
			addErr(errs, path, "schema scope must be \"catalog.schema\", got %q", scope)
		} else if !schemaKeys[scope] {
			addErr(errs, path, "scope references unknown schema %q", scope)
		}
	case "table":
		if len(parts) != 3 {
			addErr(errs, path, "table scope must be \"catalog.schema.table\", got %q", scope)
		} else if !tableKeys[scope] {
			addErr(errs, path, "scope references unknown table %q", scope)
		}
	case "external_location":
		if len(parts) != 1 {
			addErr(errs, path, "external_location scope must be a single name, got %q", scope)
		} else if !locationNames[scope] {
			addErr(errs, path, "scope references unknown external location %q", scope)
		}
	case "storage_credential":
		if len(parts) != 1 {
			addErr(errs, path, "storage_credential scope must be a single name, got %q", scope)
		} else if !credentialNames[scope] {
			addErr(errs, path, "scope references unknown storage credential %q", scope)
		}
	case "volume":
		if len(parts) != 3 {
			addErr(errs, path, "volume scope must be \"catalog.schema.volume\", got %q", scope)
		} else if !volumeKeys[scope] {
			addErr(errs, path, "scope references unknown volume %q", scope)
		}
	}
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
		if strings.TrimSpace(p.Name) == "" {
			addErr(errs, path, "name must not be blank")
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
		if strings.TrimSpace(g.Name) == "" {
			addErr(errs, path, "name must not be blank")
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
		} else if validSecurableTypes[g.SecurableType] && !isPrivilegeAllowedOnSecurable(g.SecurableType, g.Privilege) {
			addErr(errs, path, "privilege %q is not allowed on securable_type %q", g.Privilege, g.SecurableType)
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

func validateEffectiveBindingGrants(explicit []GrantSpec, presets []PrivilegePresetSpec, bindings []BindingSpec, errs *[]ValidationError) {
	presetByName := make(map[string]PrivilegePresetSpec, len(presets))
	for _, p := range presets {
		presetByName[p.Name] = p
	}

	seen := make(map[string]bool, len(explicit)+(len(bindings)*2))
	for _, g := range explicit {
		seen[grantIdentityKey(g)] = true
	}

	for i, b := range bindings {
		preset, ok := presetByName[b.Preset]
		if !ok {
			continue
		}
		path := fmt.Sprintf("binding[%d]", i)
		for _, privilege := range preset.Privileges {
			if !isPrivilegeAllowedOnSecurable(b.ScopeType, privilege) {
				addErr(errs, path, "privilege %q is not allowed on scope_type %q", privilege, b.ScopeType)
			}

			key := grantIdentityKey(GrantSpec{
				Principal:     b.Principal,
				PrincipalType: b.PrincipalType,
				SecurableType: b.ScopeType,
				Securable:     b.Scope,
				Privilege:     privilege,
			})
			if seen[key] {
				addErr(errs, path, "duplicate effective grant generated for preset binding")
				continue
			}
			seen[key] = true
		}
	}
}

func isPrivilegeAllowedOnSecurable(securableType, privilege string) bool {
	allowed, ok := allowedPrivilegesBySecurable[securableType]
	if !ok {
		return false
	}
	return allowed[privilege]
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
			if m.MaskExpression != "" {
				if _, err := duckdbsql.ParseExpr(m.MaskExpression); err != nil {
					addErr(errs, mpath, "mask_expression must be valid SQL expression: %v", err)
				}
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
		if strings.TrimSpace(e.Name) == "" {
			addErr(errs, path, "name must not be blank")
		}
		if !validComputeTypes[e.Type] {
			addErr(errs, path, "type must be \"LOCAL\" or \"REMOTE\", got %q", e.Type)
		}
		if e.Type == "REMOTE" && e.URL == "" {
			addErr(errs, path, "url is required for REMOTE compute endpoints")
		}
		if e.SelectionPolicy != "" && e.SelectionPolicy != "ADMIN_ONLY" && e.SelectionPolicy != "ALLOWED_USERS" && e.SelectionPolicy != "SELF_SERVICE" {
			addErr(errs, path, "selection_policy must be ADMIN_ONLY, ALLOWED_USERS, or SELF_SERVICE")
		}
		if e.WorkloadClass != "" && e.WorkloadClass != "INTERACTIVE" && e.WorkloadClass != "SCHEDULED" && e.WorkloadClass != "HEAVY" && e.WorkloadClass != "MIXED" {
			addErr(errs, path, "workload_class must be INTERACTIVE, SCHEDULED, HEAVY, or MIXED")
		}
		if e.ReadinessStatus != "" && e.ReadinessStatus != "READY" && e.ReadinessStatus != "DEGRADED" && e.ReadinessStatus != "UNAVAILABLE" {
			addErr(errs, path, "readiness_status must be READY, DEGRADED, or UNAVAILABLE")
		}
		if e.MaxConcurrency != nil && *e.MaxConcurrency <= 0 {
			addErr(errs, path, "max_concurrency must be greater than zero")
		}
		if e.MaxResultSizeMB != nil && *e.MaxResultSizeMB <= 0 {
			addErr(errs, path, "max_result_size_mb must be greater than zero")
		}
		if e.Name != "" {
			if seen[e.Name] {
				addErr(errs, path, "duplicate compute endpoint name %q", e.Name)
			}
			seen[e.Name] = true
		}
	}
}

func validateComputeRoutingDefaults(defaults *ComputeRoutingDefaultsSpec, errs *[]ValidationError) {
	if defaults == nil {
		return
	}
	validModes := map[string]bool{
		"":                true,
		"AUTO":            true,
		"BYOC_LOCAL":      true,
		"SHARED_ENDPOINT": true,
	}
	if !validModes[defaults.InteractiveMode] {
		addErr(errs, "compute_defaults.interactive_mode", "interactive_mode must be AUTO, BYOC_LOCAL, or SHARED_ENDPOINT")
	}
	if !validModes[defaults.ScheduledMode] {
		addErr(errs, "compute_defaults.scheduled_mode", "scheduled_mode must be AUTO, BYOC_LOCAL, or SHARED_ENDPOINT")
	}
	if !validModes[defaults.NotebookMode] {
		addErr(errs, "compute_defaults.notebook_mode", "notebook_mode must be AUTO, BYOC_LOCAL, or SHARED_ENDPOINT")
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

func validateNotebooks(
	notebooks []NotebookResource,
	workspaceNames, folderKeys, projectKeys, environmentKeys map[string]bool,
	errs *[]ValidationError,
) {
	seen := make(map[string]bool, len(notebooks))
	for i, n := range notebooks {
		path := fmt.Sprintf("notebook[%d]", i)
		if n.Name != "" {
			path = fmt.Sprintf("notebook[%s]", n.Name)
		}
		if n.Name == "" {
			addErr(errs, path, "name is required")
		}
		if strings.TrimSpace(n.Name) == "" {
			addErr(errs, path, "name must not be blank")
		}
		if strings.TrimSpace(n.Spec.WorkspaceRef) != "" && !workspaceNames[n.Spec.WorkspaceRef] {
			addErr(errs, path, "workspace_ref references unknown workspace %q", n.Spec.WorkspaceRef)
		}
		if strings.TrimSpace(n.Spec.FolderRef) != "" {
			if !folderKeys[n.Spec.FolderRef] {
				addErr(errs, path, "folder_ref references unknown folder %q", n.Spec.FolderRef)
			}
		}
		if strings.TrimSpace(n.Spec.ProjectRef) != "" {
			if !projectKeys[n.Spec.ProjectRef] {
				addErr(errs, path, "project_ref references unknown project %q", n.Spec.ProjectRef)
			}
		}
		if strings.TrimSpace(n.Spec.EnvironmentRef) != "" {
			if !environmentKeys[n.Spec.EnvironmentRef] {
				addErr(errs, path, "environment_ref references unknown environment %q", n.Spec.EnvironmentRef)
			}
		}

		effectiveWorkspace := strings.TrimSpace(n.Spec.WorkspaceRef)
		if effectiveWorkspace == "" && strings.TrimSpace(n.Spec.FolderRef) != "" {
			effectiveWorkspace = workspaceNameFromFolderRef(n.Spec.FolderRef)
		}
		if effectiveWorkspace == "" && strings.TrimSpace(n.Spec.ProjectRef) != "" {
			effectiveWorkspace = workspaceNameFromProjectRef(n.Spec.ProjectRef)
		}
		if effectiveWorkspace == "" && strings.TrimSpace(n.Spec.EnvironmentRef) != "" {
			effectiveWorkspace = workspaceNameFromEnvironmentRef(n.Spec.EnvironmentRef)
		}
		if effectiveWorkspace != "" {
			if strings.TrimSpace(n.Spec.FolderRef) != "" && workspaceNameFromFolderRef(n.Spec.FolderRef) != effectiveWorkspace {
				addErr(errs, path, "folder_ref must belong to the same workspace as the notebook")
			}
			if strings.TrimSpace(n.Spec.ProjectRef) != "" && workspaceNameFromProjectRef(n.Spec.ProjectRef) != effectiveWorkspace {
				addErr(errs, path, "project_ref must belong to the same workspace as the notebook")
			}
			if strings.TrimSpace(n.Spec.EnvironmentRef) != "" && workspaceNameFromEnvironmentRef(n.Spec.EnvironmentRef) != effectiveWorkspace {
				addErr(errs, path, "environment_ref must belong to the same workspace as the notebook")
			}
		}
		if strings.TrimSpace(n.Spec.ProjectRef) != "" && strings.TrimSpace(n.Spec.EnvironmentRef) != "" {
			if !strings.HasPrefix(n.Spec.EnvironmentRef, n.Spec.ProjectRef+"/") {
				addErr(errs, path, "environment_ref must belong to project_ref")
			}
		}

		for j, c := range n.Spec.Cells {
			cpath := fmt.Sprintf("%s.cells[%d]", path, j)
			if !validCellTypes[c.Type] {
				addErr(errs, cpath, "cell type must be \"sql\" or \"markdown\", got %q", c.Type)
			}
			if c.Content == "" {
				addErr(errs, cpath, "content is required")
			}
			if c.Role != "" {
				if !validCellRoles[c.Role] {
					addErr(errs, cpath, "cell role must be one of [transform, output, test, markdown], got %q", c.Role)
				} else {
					switch c.Role {
					case "transform", "output", "test":
						if c.Type != "sql" {
							addErr(errs, cpath, "cell role %q requires type \"sql\"", c.Role)
						}
					case "markdown":
						if c.Type != "markdown" {
							addErr(errs, cpath, "cell role \"markdown\" requires type \"markdown\"")
						}
					}
				}
			}
			if c.Test != nil {
				if c.Role != "test" {
					addErr(errs, cpath, "test config is only allowed when role is \"test\"")
				}
				if c.Test.Severity != "" && !validNotebookTestSeverities[c.Test.Severity] {
					addErr(errs, cpath, "test severity must be \"error\" or \"warn\", got %q", c.Test.Severity)
				}
			}
			if c.VisualSpec != nil {
				if c.Type != "sql" {
					addErr(errs, cpath, "visual_spec is only allowed for sql cells")
				} else if err := c.VisualSpec.Validate(); err != nil {
					addErr(errs, cpath, "visual_spec is invalid: %v", err)
				}
			}
		}

		seenCellNames := make(map[string]bool)
		outputCount := 0
		for j, c := range n.Spec.Cells {
			cpath := fmt.Sprintf("%s.cells[%d]", path, j)
			if c.Name != "" {
				if seenCellNames[c.Name] {
					addErr(errs, cpath, "duplicate cell name %q", c.Name)
				}
				seenCellNames[c.Name] = true
			}
			if c.Role == "output" {
				outputCount++
			}
		}
		if outputCount > 1 {
			addErr(errs, path, "notebook has multiple output cells")
		}
		if n.Spec.Publish != nil && n.Spec.Publish.Model != nil {
			m := n.Spec.Publish.Model
			if strings.TrimSpace(m.Project) == "" {
				addErr(errs, path, "publish.model.project is required")
			}
			if strings.TrimSpace(m.Name) == "" {
				addErr(errs, path, "publish.model.name is required")
			}
			switch {
			case strings.TrimSpace(m.OutputCell) == "":
				addErr(errs, path, "publish.model.output_cell is required")
			case !seenCellNames[m.OutputCell]:
				addErr(errs, path, "publish.model.output_cell references unknown cell %q", m.OutputCell)
			default:
				isOutput := false
				for _, c := range n.Spec.Cells {
					if c.Name != m.OutputCell {
						continue
					}
					role := strings.TrimSpace(c.Role)
					if role == "" && c.Type == "sql" {
						role = "transform"
					}
					if role == "output" {
						isOutput = true
					}
					break
				}
				if !isOutput {
					addErr(errs, path, "publish.model.output_cell %q must reference a cell with role \"output\"", m.OutputCell)
				}
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

func validateAssets(assets []AssetResource, productNames map[string]bool, errs *[]ValidationError) {
	validPartitionDefinitionTypes := map[string]bool{
		"daily":   true,
		"hourly":  true,
		"static":  true,
		"dynamic": true,
	}

	seen := make(map[string]bool, len(assets))
	for i, a := range assets {
		path := fmt.Sprintf("asset[%d]", i)
		if a.Name != "" {
			path = fmt.Sprintf("asset[%s]", a.Name)
		}
		if strings.TrimSpace(a.Name) == "" {
			addErr(errs, path, "name is required")
			continue
		}
		if seen[a.Name] {
			addErr(errs, path, "duplicate asset name %q", a.Name)
		}
		seen[a.Name] = true
		if strings.TrimSpace(a.Spec.ProductRef) == "" {
			addErr(errs, path, "product_ref is required")
		} else if !productNames[a.Spec.ProductRef] {
			addErr(errs, path, "product_ref references unknown data product %q", a.Spec.ProductRef)
		}

		for j, dep := range a.Spec.DependsOn {
			if strings.TrimSpace(dep) == "" {
				addErr(errs, fmt.Sprintf("%s.depends_on[%d]", path, j), "depends_on entry must not be blank")
			}
		}

		if a.Spec.IOProfile != "" && strings.TrimSpace(a.Spec.IOProfile) == "" {
			addErr(errs, fmt.Sprintf("%s.io_profile", path), "io_profile must not be blank when provided")
		}

		if a.Spec.PartitionDefinition != nil {
			typeValue := strings.ToLower(strings.TrimSpace(a.Spec.PartitionDefinition.Type))
			if !validPartitionDefinitionTypes[typeValue] {
				addErr(errs, fmt.Sprintf("%s.partition_definition.type", path), "partition_definition.type must be one of [daily, hourly, static, dynamic], got %q", a.Spec.PartitionDefinition.Type)
			}

			if typeValue == "static" {
				hasStaticKeys := false
				for _, key := range a.Spec.PartitionDefinition.StaticKeys {
					if strings.TrimSpace(key) != "" {
						hasStaticKeys = true
						break
					}
				}
				if !hasStaticKeys {
					addErr(errs, fmt.Sprintf("%s.partition_definition.static_keys", path), "static partition requires at least one non-empty static_keys entry")
				}
			}

			if typeValue == "dynamic" && strings.TrimSpace(a.Spec.PartitionDefinition.DynamicGroup) == "" {
				addErr(errs, fmt.Sprintf("%s.partition_definition.dynamic_group", path), "dynamic partition requires dynamic_group")
			}
		}

		if a.Spec.AutoMaterializePolicy != nil {
			if a.Spec.AutoMaterializePolicy.MinIntervalSeconds != nil && *a.Spec.AutoMaterializePolicy.MinIntervalSeconds <= 0 {
				addErr(errs, fmt.Sprintf("%s.auto_materialize_policy.min_interval_seconds", path), "auto_materialize_policy.min_interval_seconds must be > 0")
			}
			if a.Spec.AutoMaterializePolicy.Mode != "" && strings.TrimSpace(a.Spec.AutoMaterializePolicy.Mode) == "" {
				addErr(errs, fmt.Sprintf("%s.auto_materialize_policy.mode", path), "auto_materialize_policy.mode must not be blank when provided")
			}
		}

		if a.Spec.FreshnessPolicy != nil {
			if a.Spec.FreshnessPolicy.MaxLagSeconds != nil && *a.Spec.FreshnessPolicy.MaxLagSeconds <= 0 {
				addErr(errs, fmt.Sprintf("%s.freshness_policy.max_lag_seconds", path), "freshness_policy.max_lag_seconds must be > 0")
			}
		}

		if a.Spec.MaterializationPolicy != nil && a.Spec.MaterializationPolicy.Mode != "" && strings.TrimSpace(a.Spec.MaterializationPolicy.Mode) == "" {
			addErr(errs, fmt.Sprintf("%s.materialization_policy.mode", path), "materialization_policy.mode must not be blank when provided")
		}

		if a.Spec.MaxLagSeconds != nil && *a.Spec.MaxLagSeconds <= 0 {
			addErr(errs, fmt.Sprintf("%s.max_lag_seconds", path), "max_lag_seconds must be > 0")
		}

		for j, check := range a.Spec.CheckDefinitions {
			cpath := fmt.Sprintf("%s.checks[%d]", path, j)
			if strings.TrimSpace(check.Name) == "" {
				addErr(errs, cpath, "check name is required")
			}
			if strings.TrimSpace(check.CheckType) == "" {
				addErr(errs, cpath, "check_type is required")
			}
		}
	}

	for _, a := range assets {
		path := fmt.Sprintf("asset[%s]", a.Name)
		for i, dep := range a.Spec.DependsOn {
			if !seen[dep] {
				addErr(errs, fmt.Sprintf("%s.depends_on[%d]", path, i), "depends_on references unknown asset %q", dep)
			}
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

// Valid incremental strategy values for model config.
var validIncrementalStrategies = map[string]bool{
	"":              true,
	"merge":         true,
	"delete_insert": true,
	"delete+insert": true,
}

// Valid on_schema_change values for model config.
var validOnSchemaChange = map[string]bool{
	"":       true,
	"ignore": true,
	"fail":   true,
}

type relationshipTestRef struct {
	path           string
	currentProject string
	toModel        string
	toColumn       string
}

func validateModels(models []ModelResource, macroNames map[string]bool, errs *[]ValidationError) {
	seen := make(map[string]bool, len(models))
	contractColumns := make(map[string]map[string]bool, len(models))
	relationshipChecks := make([]relationshipTestRef, 0)

	for _, m := range models {
		if m.ProjectName == "" || m.ModelName == "" || m.Spec.Contract == nil {
			continue
		}
		cols := make(map[string]bool, len(m.Spec.Contract.Columns))
		for _, col := range m.Spec.Contract.Columns {
			if col.Name != "" {
				cols[col.Name] = true
			}
		}
		if len(cols) > 0 {
			contractColumns[m.ProjectName+"."+m.ModelName] = cols
		}
	}

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
		if m.Spec.Config != nil {
			strategy := strings.ToLower(strings.TrimSpace(m.Spec.Config.IncrementalStrategy))
			if !validIncrementalStrategies[strategy] {
				addErr(errs, path, "config.incremental_strategy must be one of [merge, delete_insert, delete+insert], got %q", m.Spec.Config.IncrementalStrategy)
			}

			onSchemaChange := strings.ToLower(strings.TrimSpace(m.Spec.Config.OnSchemaChange))
			if !validOnSchemaChange[onSchemaChange] {
				addErr(errs, path, "config.on_schema_change must be one of [ignore, fail], got %q", m.Spec.Config.OnSchemaChange)
			}
		}

		// Validate contract.
		if m.Spec.Contract != nil {
			validateModelContract(m.Spec.Contract, path, errs)
		}

		// Validate tests.
		validateModelTests(m.Spec.Tests, path, errs)
		for j, test := range m.Spec.Tests {
			testPath := fmt.Sprintf("%s.tests[%d]", path, j)
			if test.Name != "" {
				testPath = fmt.Sprintf("%s.tests[%s]", path, test.Name)
			}
			if test.Column != "" {
				if cols, ok := contractColumns[m.ProjectName+"."+m.ModelName]; ok && !cols[test.Column] {
					addErr(errs, testPath, "column %q is not declared in model contract", test.Column)
				}
			}
			if test.Type == "relationships" && test.ToModel != "" && test.ToColumn != "" {
				relationshipChecks = append(relationshipChecks, relationshipTestRef{
					path:           testPath,
					currentProject: m.ProjectName,
					toModel:        test.ToModel,
					toColumn:       test.ToColumn,
				})
			}
		}

		for _, macroName := range referencedMacroNames(m.Spec.SQL) {
			if !macroNames[macroName] {
				addErr(errs, path, "sql references unknown macro %q", macroName)
			}
		}

		key := m.ProjectName + "." + m.ModelName
		if m.ProjectName != "" && m.ModelName != "" {
			if seen[key] {
				addErr(errs, path, "duplicate model %q", key)
			}
			seen[key] = true
		}
	}

	for _, ref := range relationshipChecks {
		targetKey, ok := resolveModelReference(ref.currentProject, ref.toModel)
		if !ok {
			addErr(errs, ref.path, "to_model must be \"model\" or \"project.model\", got %q", ref.toModel)
			continue
		}
		if !seen[targetKey] {
			addErr(errs, ref.path, "relationships test references unknown to_model %q", ref.toModel)
			continue
		}
		if cols, hasContract := contractColumns[targetKey]; hasContract && !cols[ref.toColumn] {
			addErr(errs, ref.path, "relationships test references unknown to_column %q on model %q", ref.toColumn, ref.toModel)
		}
	}
}

func resolveModelReference(currentProject, toModel string) (string, bool) {
	parts := strings.Split(toModel, ".")
	if len(parts) == 1 {
		if currentProject == "" || strings.TrimSpace(parts[0]) == "" {
			return "", false
		}
		return currentProject + "." + parts[0], true
	}
	if len(parts) != 2 {
		return "", false
	}
	if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", false
	}
	return parts[0] + "." + parts[1], true
}

var macroCallPattern = regexp.MustCompile(`\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\(`)

func referencedMacroNames(sql string) []string {
	matches := macroCallPattern.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(matches))
	names := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		name := match[1]
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
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

var validMacroVisibility = map[string]bool{
	"":               true,
	"project":        true,
	"catalog_global": true,
	"system":         true,
}

var validMacroStatus = map[string]bool{
	"":           true,
	"ACTIVE":     true,
	"DEPRECATED": true,
}

func validateSemanticModels(models []SemanticModelResource, errs *[]ValidationError) {
	seenModels := make(map[string]bool, len(models))
	for i, m := range models {
		key := m.ModelName
		path := fmt.Sprintf("semantic_model[%s]", key)
		if m.ModelName == "" {
			addErr(errs, path, "model name is required")
		}
		if seenModels[key] {
			addErr(errs, path, "duplicate semantic model %q", key)
		}
		seenModels[key] = true

		if strings.TrimSpace(m.Spec.BaseModelRef) == "" {
			addErr(errs, path, "spec.base_model_ref is required")
		}

		seenMetrics := make(map[string]bool, len(m.Spec.Metrics))
		for j, metric := range m.Spec.Metrics {
			mpath := fmt.Sprintf("semantic_model[%d].metrics[%d]", i, j)
			if strings.TrimSpace(metric.Name) == "" {
				addErr(errs, mpath, "name is required")
			}
			if strings.TrimSpace(metric.MetricType) == "" {
				addErr(errs, mpath, "metric_type is required")
			}
			if strings.TrimSpace(metric.Expression) == "" {
				addErr(errs, mpath, "expression is required")
			}
			if metric.Name != "" {
				if seenMetrics[metric.Name] {
					addErr(errs, mpath, "duplicate metric name %q", metric.Name)
				}
				seenMetrics[metric.Name] = true
			}
		}

		seenRelationships := make(map[string]bool, len(m.Spec.Relationships))
		for j, rel := range m.Spec.Relationships {
			rpath := fmt.Sprintf("semantic_model[%d].relationships[%d]", i, j)
			if strings.TrimSpace(rel.Name) == "" {
				addErr(errs, rpath, "name is required")
			}
			if strings.TrimSpace(rel.ToModel) == "" {
				addErr(errs, rpath, "to_model is required")
			}
			if strings.TrimSpace(rel.RelationshipType) == "" {
				addErr(errs, rpath, "relationship_type is required")
			}
			if strings.TrimSpace(rel.JoinSQL) == "" {
				addErr(errs, rpath, "join_sql is required")
			}
			if rel.Name != "" {
				if seenRelationships[rel.Name] {
					addErr(errs, rpath, "duplicate relationship name %q", rel.Name)
				}
				seenRelationships[rel.Name] = true
			}
		}

		seenPreAggs := make(map[string]bool, len(m.Spec.PreAggregations))
		for j, p := range m.Spec.PreAggregations {
			ppath := fmt.Sprintf("semantic_model[%d].pre_aggregations[%d]", i, j)
			if strings.TrimSpace(p.Name) == "" {
				addErr(errs, ppath, "name is required")
			}
			if strings.TrimSpace(p.TargetRelation) == "" {
				addErr(errs, ppath, "target_relation is required")
			}
			if p.Name != "" {
				if seenPreAggs[p.Name] {
					addErr(errs, ppath, "duplicate pre_aggregation name %q", p.Name)
				}
				seenPreAggs[p.Name] = true
			}
		}
	}
}

func validateDashboards(
	dashboards []DashboardResource,
	notebookNames map[string]bool,
	notebookCellNames map[string]map[string]bool,
	semanticModelKeys map[string]bool,
	errs *[]ValidationError,
) {
	seen := make(map[string]bool, len(dashboards))
	for i, dashboard := range dashboards {
		path := fmt.Sprintf("dashboard[%d]", i)
		if dashboard.Name != "" {
			path = fmt.Sprintf("dashboard[%s]", dashboard.Name)
		}

		if strings.TrimSpace(dashboard.Name) == "" {
			addErr(errs, path, "name is required")
			continue
		}
		if seen[dashboard.Name] {
			addErr(errs, path, "duplicate dashboard name %q", dashboard.Name)
		}
		seen[dashboard.Name] = true

		if strings.TrimSpace(dashboard.Spec.Owner) == "" {
			addErr(errs, path, "owner is required")
		}
		if err := domain.ValidateDashboardSemanticBinding(dashboard.Spec.SemanticProjectName, dashboard.Spec.SemanticModelName); err != nil {
			addErr(errs, path, "%s", err.Error())
		}
		if dashboard.Spec.Compute != nil {
			if err := dashboard.Spec.Compute.Validate(); err != nil {
				addErr(errs, path+".compute", "%s", err.Error())
			}
		}

		widgetKeys := make(map[string]bool, len(dashboard.Spec.Widgets))
		for j, widget := range dashboard.Spec.Widgets {
			wpath := fmt.Sprintf("%s.widgets[%d]", path, j)
			if widget.Key != "" {
				wpath = fmt.Sprintf("%s.widgets[%s]", path, widget.Key)
			}

			if widgetKeys[widget.Key] {
				addErr(errs, wpath+".key", "duplicate widget key %q", widget.Key)
			} else if strings.TrimSpace(widget.Key) != "" {
				widgetKeys[widget.Key] = true
			}
			if err := domain.ValidateDashboardWidgetFilterOriginKey(widget.Key); err != nil {
				addErr(errs, wpath+".key", "%s", err.Error())
			}
			if strings.TrimSpace(widget.Name) == "" {
				addErr(errs, wpath, "name is required")
			}
			if widget.VisualSpec != nil {
				if err := widget.VisualSpec.Validate(); err != nil {
					addErr(errs, wpath+".visual_spec", "%s", err.Error())
				}
			}
			if err := widget.Layout.Validate(); err != nil {
				addErr(errs, wpath+".layout", "%s", err.Error())
			}
			validateDashboardWidgetSource(wpath+".source", widget.Source, dashboard.Spec, notebookNames, notebookCellNames, semanticModelKeys, errs)
		}
	}
}

func validateDashboardWidgetSource(
	path string,
	source DashboardWidgetSourceSpec,
	dashboard DashboardSpec,
	notebookNames map[string]bool,
	notebookCellNames map[string]map[string]bool,
	semanticModelKeys map[string]bool,
	errs *[]ValidationError,
) {
	switch source.Kind {
	case domain.DashboardWidgetSourceSQLQuery:
		if source.SQLQuery == nil || strings.TrimSpace(source.SQLQuery.SQL) == "" {
			addErr(errs, path, "sql_query source requires sql")
		}
	case domain.DashboardWidgetSourceNotebookCell:
		if source.NotebookCell == nil {
			addErr(errs, path, "notebook_cell source is required")
			return
		}
		notebookName := strings.TrimSpace(source.NotebookCell.NotebookName)
		cellName := strings.TrimSpace(source.NotebookCell.CellName)
		if notebookName == "" || cellName == "" {
			addErr(errs, path, "notebook_cell source requires notebook_name and cell_name")
			return
		}
		if !notebookNames[notebookName] {
			addErr(errs, path+".notebook_cell", "notebook_cell references unknown notebook %q", notebookName)
			return
		}
		if !notebookCellNames[notebookName][cellName] {
			addErr(errs, path+".notebook_cell", "notebook_cell references unknown cell %q in notebook %q", cellName, notebookName)
		}
	case domain.DashboardWidgetSourceSemanticQuery:
		if source.SemanticQuery == nil {
			addErr(errs, path, "semantic_query source is required")
			return
		}
		modelName := strings.TrimSpace(source.SemanticQuery.SemanticModelName)
		if modelName == "" {
			modelName = strings.TrimSpace(dashboard.SemanticModelName)
		}
		if modelName == "" {
			addErr(errs, path+".semantic_query", "semantic_query source requires semantic_model_name or dashboard semantic binding")
			return
		}
		if !semanticModelKeys[modelName] {
			addErr(errs, path+".semantic_query", "semantic_query references unknown semantic model %q", modelName)
		}
		if len(source.SemanticQuery.Metrics) == 0 {
			addErr(errs, path+".semantic_query", "semantic_query source requires at least one metric")
		}
	default:
		addErr(errs, path, "widget source kind must be sql_query, notebook_cell, or semantic_query")
	}
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
		if !validMacroVisibility[m.Spec.Visibility] {
			addErr(errs, path, "visibility must be one of [project, catalog_global, system], got %q", m.Spec.Visibility)
		}
		if m.Spec.Visibility == "project" && m.Spec.ProjectName == "" {
			addErr(errs, path, "project_name is required when visibility is project")
		}
		if m.Spec.Visibility == "catalog_global" && m.Spec.CatalogName == "" {
			addErr(errs, path, "catalog_name is required when visibility is catalog_global")
		}
		if m.Spec.Visibility == "system" {
			if m.Spec.ProjectName != "" {
				addErr(errs, path, "project_name must be empty when visibility is system")
			}
			if m.Spec.CatalogName != "" {
				addErr(errs, path, "catalog_name must be empty when visibility is system")
			}
		}
		if !validMacroStatus[m.Spec.Status] {
			addErr(errs, path, "status must be one of [ACTIVE, DEPRECATED], got %q", m.Spec.Status)
		}
		if m.Name != "" {
			if seen[m.Name] {
				addErr(errs, path, "duplicate macro name %q", m.Name)
			}
			seen[m.Name] = true
		}
	}
}
