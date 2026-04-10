// Package declarative implements a Terraform-style declarative configuration
// system for managing platform resources via version-controlled YAML files.
package declarative

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Diff compares the desired state (from YAML) against the actual state (from server)
// and returns a Plan describing the changes needed.
func Diff(desired, actual *DesiredState) *Plan {
	plan := &Plan{}

	// Diff each resource type. Order doesn't matter here — SortActions handles ordering later.
	diffWorkspaces(plan, desired.Workspaces, actual.Workspaces)
	diffFolders(plan, desired.Folders, actual.Folders)
	diffProjects(plan, desired.Projects, actual.Projects)
	diffEnvironments(plan, desired.Environments, actual.Environments)
	diffDomains(plan, desired.Domains, actual.Domains)
	diffTeams(plan, desired.Teams, actual.Teams)
	diffPrincipals(plan, desired.Principals, actual.Principals)
	diffGroups(plan, desired.Groups, actual.Groups)
	diffGrants(plan, effectiveGrants(desired), effectiveGrants(actual))
	diffCatalogs(plan, desired.Catalogs, actual.Catalogs)
	diffSchemas(plan, desired.Schemas, actual.Schemas)
	diffTables(plan, desired.Tables, actual.Tables)
	diffViews(plan, desired.Views, actual.Views)
	diffVolumes(plan, desired.Volumes, actual.Volumes)
	diffRowFilters(plan, desired.RowFilters, actual.RowFilters)
	diffColumnMasks(plan, desired.ColumnMasks, actual.ColumnMasks)
	diffTags(plan, desired.Tags, actual.Tags)
	diffTagAssignments(plan, desired.TagAssignments, actual.TagAssignments)
	diffStorageCredentials(plan, desired.StorageCredentials, actual.StorageCredentials)
	diffExternalLocations(plan, desired.ExternalLocations, actual.ExternalLocations)
	diffComputeEndpoints(plan, desired.ComputeEndpoints, actual.ComputeEndpoints)
	diffComputeAssignments(plan, desired.ComputeAssignments, actual.ComputeAssignments)
	diffComputeRoutingDefaults(plan, desired.ComputeDefaults, actual.ComputeDefaults)
	diffAPIKeys(plan, desired.APIKeys, actual.APIKeys)
	diffDataProducts(plan, desired.DataProducts, actual.DataProducts)
	diffNotebooks(plan, desired.Notebooks, actual.Notebooks)
	diffAssets(plan, desired.Assets, actual.Assets)
	diffModels(plan, desired.Models, actual.Models)
	diffSemanticModels(plan, desired.SemanticModels, actual.SemanticModels)
	diffMacros(plan, desired.Macros, actual.Macros)

	plan.SortActions()
	return plan
}

func diffWorkspaces(plan *Plan, desired, actual []WorkspaceResource) {
	actualMap := make(map[string]WorkspaceResource, len(actual))
	for _, item := range actual {
		actualMap[item.Name] = item
	}

	seen := make(map[string]bool, len(desired))
	for _, item := range desired {
		seen[item.Name] = true
		current, exists := actualMap[item.Name]
		if !exists {
			createItem := item
			createItem.Spec.DefaultProjectRef = ""
			createItem.Spec.DefaultEnvironmentRef = ""
			addCreate(plan, KindWorkspace, item.Name, "", createItem)
			if workspaceNeedsDeferredDefaults(item.Spec) {
				var changes []FieldDiff
				diffField(&changes, "default_project_ref", "", item.Spec.DefaultProjectRef)
				diffField(&changes, "default_environment_ref", "", item.Spec.DefaultEnvironmentRef)
				addUpdate(plan, KindWorkspace, item.Name, "", item, createItem, changes)
			}
			continue
		}

		var changes []FieldDiff
		diffField(&changes, "kind", current.Spec.Kind, item.Spec.Kind)
		diffField(&changes, "owner_principal", current.Spec.OwnerPrincipal, item.Spec.OwnerPrincipal)
		diffField(&changes, "owner_team_id", current.Spec.OwnerTeamID, item.Spec.OwnerTeamID)
		diffField(&changes, "default_project_ref", current.Spec.DefaultProjectRef, item.Spec.DefaultProjectRef)
		diffField(&changes, "default_environment_ref", current.Spec.DefaultEnvironmentRef, item.Spec.DefaultEnvironmentRef)
		diffField(&changes, "git_repo_id", current.Spec.GitRepoID, item.Spec.GitRepoID)
		diffField(&changes, "git_root_path", current.Spec.GitRootPath, item.Spec.GitRootPath)
		if len(changes) > 0 {
			addUpdate(plan, KindWorkspace, item.Name, "", item, current, changes)
		}
	}

	for _, item := range actual {
		if !seen[item.Name] {
			addDelete(plan, KindWorkspace, item.Name, item)
		}
	}
}

func diffFolders(plan *Plan, desired, actual []FolderResource) {
	actualMap := make(map[string]FolderResource, len(actual))
	for _, item := range actual {
		actualMap[folderRefKey(item.Spec.WorkspaceRef, item.Spec.ParentFolderRef, item.Name)] = item
	}

	seen := make(map[string]bool, len(desired))
	for _, item := range desired {
		key := folderRefKey(item.Spec.WorkspaceRef, item.Spec.ParentFolderRef, item.Name)
		seen[key] = true
		current, exists := actualMap[key]
		if !exists {
			createItem := item
			createItem.Spec.DefaultProjectRef = ""
			createItem.Spec.DefaultEnvironmentRef = ""
			addCreate(plan, KindFolder, key, "", createItem)
			if folderNeedsDeferredDefaults(item.Spec) {
				var changes []FieldDiff
				diffField(&changes, "default_project_ref", "", item.Spec.DefaultProjectRef)
				diffField(&changes, "default_environment_ref", "", item.Spec.DefaultEnvironmentRef)
				addUpdate(plan, KindFolder, key, "", item, createItem, changes)
			}
			continue
		}

		var changes []FieldDiff
		diffField(&changes, "default_project_ref", current.Spec.DefaultProjectRef, item.Spec.DefaultProjectRef)
		diffField(&changes, "default_environment_ref", current.Spec.DefaultEnvironmentRef, item.Spec.DefaultEnvironmentRef)
		diffField(&changes, "git_repo_id", current.Spec.GitRepoID, item.Spec.GitRepoID)
		diffField(&changes, "git_root_path", current.Spec.GitRootPath, item.Spec.GitRootPath)
		if len(changes) > 0 {
			addUpdate(plan, KindFolder, key, "", item, current, changes)
		}
	}

	for key, item := range actualMap {
		if !seen[key] {
			addDelete(plan, KindFolder, key, item)
		}
	}
}

func diffProjects(plan *Plan, desired, actual []ProjectResource) {
	actualMap := make(map[string]ProjectResource, len(actual))
	for _, item := range actual {
		actualMap[projectRefKey(item.Spec.WorkspaceRef, item.Name)] = item
	}

	seen := make(map[string]bool, len(desired))
	for _, item := range desired {
		key := projectRefKey(item.Spec.WorkspaceRef, item.Name)
		seen[key] = true
		current, exists := actualMap[key]
		if !exists {
			addCreate(plan, KindProject, key, "", item)
			continue
		}

		var changes []FieldDiff
		diffField(&changes, "kind", current.Spec.Kind, item.Spec.Kind)
		diffField(&changes, "description", current.Spec.Description, item.Spec.Description)
		diffField(&changes, "product_id", current.Spec.ProductID, item.Spec.ProductID)
		diffField(&changes, "default_branch", current.Spec.DefaultBranch, item.Spec.DefaultBranch)
		if len(changes) > 0 {
			addUpdate(plan, KindProject, key, "", item, current, changes)
		}
	}

	for key, item := range actualMap {
		if !seen[key] {
			addDelete(plan, KindProject, key, item)
		}
	}
}

func diffEnvironments(plan *Plan, desired, actual []EnvironmentResource) {
	actualMap := make(map[string]EnvironmentResource, len(actual))
	for _, item := range actual {
		actualMap[environmentRefKeyFromProjectRef(item.Spec.ProjectRef, item.Name)] = item
	}

	seen := make(map[string]bool, len(desired))
	for _, item := range desired {
		key := environmentRefKeyFromProjectRef(item.Spec.ProjectRef, item.Name)
		seen[key] = true
		current, exists := actualMap[key]
		if !exists {
			addCreate(plan, KindEnvironment, key, "", item)
			continue
		}

		var changes []FieldDiff
		diffField(&changes, "kind", current.Spec.Kind, item.Spec.Kind)
		diffField(&changes, "description", current.Spec.Description, item.Spec.Description)
		diffField(&changes, "target_catalog", current.Spec.TargetCatalog, item.Spec.TargetCatalog)
		diffField(&changes, "target_schema", current.Spec.TargetSchema, item.Spec.TargetSchema)
		diffField(&changes, "compute_endpoint", current.Spec.ComputeEndpoint, item.Spec.ComputeEndpoint)
		diffField(&changes, "defer_to_environment", current.Spec.DeferToEnvironment, item.Spec.DeferToEnvironment)
		diffMapField(&changes, "variables", current.Spec.Variables, item.Spec.Variables)
		diffMapField(&changes, "source_overrides", current.Spec.SourceOverrides, item.Spec.SourceOverrides)
		if len(changes) > 0 {
			addUpdate(plan, KindEnvironment, key, "", item, current, changes)
		}
	}

	for key, item := range actualMap {
		if !seen[key] {
			addDelete(plan, KindEnvironment, key, item)
		}
	}
}

// === Helpers ===

func addCreate(plan *Plan, kind ResourceKind, name, filePath string, desired any) {
	plan.Actions = append(plan.Actions, Action{
		Operation:    OpCreate,
		ResourceKind: kind,
		ResourceName: name,
		FilePath:     filePath,
		Desired:      desired,
	})
}

func addUpdate(plan *Plan, kind ResourceKind, name, filePath string, desired, actual any, changes []FieldDiff) {
	plan.Actions = append(plan.Actions, Action{
		Operation:    OpUpdate,
		ResourceKind: kind,
		ResourceName: name,
		FilePath:     filePath,
		Desired:      desired,
		Actual:       actual,
		Changes:      changes,
	})
}

func addDelete(plan *Plan, kind ResourceKind, name string, actual any) {
	plan.Actions = append(plan.Actions, Action{
		Operation:    OpDelete,
		ResourceKind: kind,
		ResourceName: name,
		Actual:       actual,
	})
}

func addError(plan *Plan, kind ResourceKind, name, msg string) {
	plan.Errors = append(plan.Errors, PlanError{
		ResourceKind: kind,
		ResourceName: name,
		Message:      msg,
	})
}

func workspaceNeedsDeferredDefaults(spec WorkspaceSpec) bool {
	return strings.TrimSpace(spec.DefaultProjectRef) != "" || strings.TrimSpace(spec.DefaultEnvironmentRef) != ""
}

func folderNeedsDeferredDefaults(spec FolderSpec) bool {
	return strings.TrimSpace(spec.DefaultProjectRef) != "" || strings.TrimSpace(spec.DefaultEnvironmentRef) != ""
}

func diffField(changes *[]FieldDiff, field, oldVal, newVal string) {
	if oldVal != newVal {
		*changes = append(*changes, FieldDiff{Field: field, OldValue: oldVal, NewValue: newVal})
	}
}

func diffBoolField(changes *[]FieldDiff, field string, oldVal, newVal bool) {
	diffField(changes, field, fmt.Sprintf("%t", oldVal), fmt.Sprintf("%t", newVal))
}

func diffIntPtrField(changes *[]FieldDiff, field string, oldVal, newVal *int) {
	oldStr := ""
	newStr := ""
	if oldVal != nil {
		oldStr = fmt.Sprintf("%d", *oldVal)
	}
	if newVal != nil {
		newStr = fmt.Sprintf("%d", *newVal)
	}
	diffField(changes, field, oldStr, newStr)
}

func diffStringPtrField(changes *[]FieldDiff, field string, oldVal, newVal *string) {
	oldStr := ""
	newStr := ""
	if oldVal != nil {
		oldStr = *oldVal
	}
	if newVal != nil {
		newStr = *newVal
	}
	diffField(changes, field, oldStr, newStr)
}

func diffInt64PtrField(changes *[]FieldDiff, field string, oldVal, newVal *int64) {
	oldStr := ""
	newStr := ""
	if oldVal != nil {
		oldStr = fmt.Sprintf("%d", *oldVal)
	}
	if newVal != nil {
		newStr = fmt.Sprintf("%d", *newVal)
	}
	diffField(changes, field, oldStr, newStr)
}

func diffMapField(changes *[]FieldDiff, field string, oldVal, newVal map[string]string) {
	diffField(changes, field, formatMap(oldVal), formatMap(newVal))
}

// formatMap returns a stable string representation of a map for comparison.
func formatMap(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(m[k])
	}
	return b.String()
}

// === Principals ===

func diffDomains(plan *Plan, desired, actual []DomainResource) {
	actualMap := make(map[string]DomainResource, len(actual))
	for _, a := range actual {
		actualMap[a.Name] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		seen[d.Name] = true
		a, exists := actualMap[d.Name]
		if !exists {
			addCreate(plan, KindDomain, d.Name, "", d)
			continue
		}

		var changes []FieldDiff
		diffField(&changes, "description", a.Spec.Description, d.Spec.Description)
		if len(changes) > 0 {
			addUpdate(plan, KindDomain, d.Name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[a.Name] {
			addDelete(plan, KindDomain, a.Name, a)
		}
	}
}

func diffTeams(plan *Plan, desired, actual []TeamResource) {
	actualMap := make(map[string]TeamResource, len(actual))
	for _, a := range actual {
		actualMap[a.Name] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		seen[d.Name] = true
		a, exists := actualMap[d.Name]
		if !exists {
			addCreate(plan, KindTeam, d.Name, "", d)
			continue
		}

		var changes []FieldDiff
		diffField(&changes, "domain_ref", a.Spec.DomainRef, d.Spec.DomainRef)
		diffField(&changes, "contact_channel", a.Spec.ContactChannel, d.Spec.ContactChannel)
		if len(changes) > 0 {
			addUpdate(plan, KindTeam, d.Name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[a.Name] {
			addDelete(plan, KindTeam, a.Name, a)
		}
	}
}

func diffDataProducts(plan *Plan, desired, actual []DataProductResource) {
	actualMap := make(map[string]DataProductResource, len(actual))
	for _, a := range actual {
		actualMap[a.Slug] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		seen[d.Slug] = true
		a, exists := actualMap[d.Slug]
		if !exists {
			addCreate(plan, KindDataProduct, d.Slug, "", d)
			continue
		}

		var changes []FieldDiff
		diffField(&changes, "name", a.Spec.Name, d.Spec.Name)
		diffField(&changes, "description", a.Spec.Description, d.Spec.Description)
		diffField(&changes, "domain_ref", a.Spec.DomainRef, d.Spec.DomainRef)
		diffField(&changes, "owner_team_ref", a.Spec.OwnerTeamRef, d.Spec.OwnerTeamRef)
		diffField(&changes, "steward_principal", a.Spec.StewardPrincipal, d.Spec.StewardPrincipal)
		diffField(&changes, "contact_channel", a.Spec.ContactChannel, d.Spec.ContactChannel)
		diffField(&changes, "visibility", a.Spec.Visibility, d.Spec.Visibility)
		diffField(&changes, "consumer_audience", a.Spec.ConsumerAudience, d.Spec.ConsumerAudience)
		diffField(&changes, "docs_url", a.Spec.DocsURL, d.Spec.DocsURL)
		diffField(&changes, "access_request_path", a.Spec.AccessRequestPath, d.Spec.AccessRequestPath)
		diffMapField(&changes, "business_definitions", a.Spec.BusinessDefinitions, d.Spec.BusinessDefinitions)
		diffField(&changes, "contract", stableJSON(a.Spec.Contract), stableJSON(d.Spec.Contract))
		diffField(&changes, "slo", stableJSON(a.Spec.SLO), stableJSON(d.Spec.SLO))
		diffField(&changes, "outputs", formatStringSlice(a.Spec.Outputs), formatStringSlice(d.Spec.Outputs))
		diffField(&changes, "semantic_entrypoints", formatStringSlice(a.Spec.SemanticEntrypoints), formatStringSlice(d.Spec.SemanticEntrypoints))
		diffField(&changes, "dependencies", formatStringSlice(a.Spec.Dependencies), formatStringSlice(d.Spec.Dependencies))
		diffField(&changes, "publication_intent", a.Spec.PublicationIntent, d.Spec.PublicationIntent)
		diffField(&changes, "versions", stableJSON(a.Spec.Versions), stableJSON(d.Spec.Versions))
		if len(changes) > 0 {
			addUpdate(plan, KindDataProduct, d.Slug, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[a.Slug] {
			addDelete(plan, KindDataProduct, a.Slug, a)
		}
	}
}

// === Principals ===

func diffPrincipals(plan *Plan, desired, actual []PrincipalSpec) {
	actualMap := make(map[string]PrincipalSpec, len(actual))
	for _, a := range actual {
		actualMap[a.Name] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		seen[d.Name] = true
		a, exists := actualMap[d.Name]
		if !exists {
			addCreate(plan, KindPrincipal, d.Name, "", d)
			continue
		}
		var changes []FieldDiff
		// Note: principal type is immutable (no API endpoint supports changing it),
		// so we only diff is_admin which can be toggled via PUT /principals/{id}/admin.
		diffBoolField(&changes, "is_admin", a.IsAdmin, d.IsAdmin)
		if len(changes) > 0 {
			addUpdate(plan, KindPrincipal, d.Name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[a.Name] {
			addDelete(plan, KindPrincipal, a.Name, a)
		}
	}
}

// === Groups ===

func diffGroups(plan *Plan, desired, actual []GroupSpec) {
	actualMap := make(map[string]GroupSpec, len(actual))
	for _, a := range actual {
		actualMap[a.Name] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		seen[d.Name] = true
		a, exists := actualMap[d.Name]
		if !exists {
			addCreate(plan, KindGroup, d.Name, "", d)
			// Also create all memberships for the new group.
			for _, m := range d.Members {
				memberName := fmt.Sprintf("%s/%s(%s)", d.Name, m.Name, m.Type)
				addCreate(plan, KindGroupMembership, memberName, "", m)
			}
			continue
		}
		var changes []FieldDiff
		diffField(&changes, "description", a.Description, d.Description)
		if len(changes) > 0 {
			addUpdate(plan, KindGroup, d.Name, "", d, a, changes)
		}
		// Diff memberships inline.
		diffGroupMembers(plan, d.Name, d.Members, a.Members)
	}

	for _, a := range actual {
		if !seen[a.Name] {
			// Delete memberships first (higher layer), then the group.
			for _, m := range a.Members {
				memberName := fmt.Sprintf("%s/%s(%s)", a.Name, m.Name, m.Type)
				addDelete(plan, KindGroupMembership, memberName, m)
			}
			addDelete(plan, KindGroup, a.Name, a)
		}
	}
}

func diffGroupMembers(plan *Plan, groupName string, desired, actual []MemberRef) {
	type memberKey struct {
		Name string
		Type string
	}
	actualMap := make(map[memberKey]MemberRef, len(actual))
	for _, a := range actual {
		actualMap[memberKey{Name: a.Name, Type: a.Type}] = a
	}

	seen := make(map[memberKey]bool, len(desired))
	for _, d := range desired {
		k := memberKey{Name: d.Name, Type: d.Type}
		seen[k] = true
		if _, exists := actualMap[k]; !exists {
			memberName := fmt.Sprintf("%s/%s(%s)", groupName, d.Name, d.Type)
			addCreate(plan, KindGroupMembership, memberName, "", d)
		}
	}

	for _, a := range actual {
		k := memberKey{Name: a.Name, Type: a.Type}
		if !seen[k] {
			memberName := fmt.Sprintf("%s/%s(%s)", groupName, a.Name, a.Type)
			addDelete(plan, KindGroupMembership, memberName, a)
		}
	}
}

// === Grants ===

func diffGrants(plan *Plan, desired, actual []GrantSpec) {
	actualMap := make(map[string]GrantSpec, len(actual))
	for _, a := range actual {
		actualMap[grantIdentityKey(a)] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		k := grantIdentityKey(d)
		seen[k] = true
		if _, exists := actualMap[k]; !exists {
			name := fmt.Sprintf("%s:%s on %s.%s", d.PrincipalType, d.Principal, d.SecurableType, d.Securable)
			addCreate(plan, KindPrivilegeGrant, name, "", d)
		}
	}

	for _, a := range actual {
		if !seen[grantIdentityKey(a)] {
			name := fmt.Sprintf("%s:%s on %s.%s", a.PrincipalType, a.Principal, a.SecurableType, a.Securable)
			addDelete(plan, KindPrivilegeGrant, name, a)
		}
	}
}

// === Catalogs ===

func diffCatalogs(plan *Plan, desired, actual []CatalogResource) {
	actualMap := make(map[string]CatalogResource, len(actual))
	for _, a := range actual {
		actualMap[a.CatalogName] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		seen[d.CatalogName] = true
		a, exists := actualMap[d.CatalogName]
		if !exists {
			addCreate(plan, KindCatalogRegistration, d.CatalogName, "", d)
			continue
		}
		var changes []FieldDiff
		diffField(&changes, "metastore_type", a.Spec.MetastoreType, d.Spec.MetastoreType)
		diffField(&changes, "dsn", a.Spec.DSN, d.Spec.DSN)
		diffField(&changes, "data_path", a.Spec.DataPath, d.Spec.DataPath)
		if d.Spec.IsDefault {
			diffBoolField(&changes, "is_default", a.Spec.IsDefault, d.Spec.IsDefault)
		}
		diffField(&changes, "comment", a.Spec.Comment, d.Spec.Comment)
		if len(changes) > 0 {
			addUpdate(plan, KindCatalogRegistration, d.CatalogName, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[a.CatalogName] {
			if a.DeletionProtection {
				addError(plan, KindCatalogRegistration, a.CatalogName,
					"cannot delete catalog: deletion_protection is enabled")
			} else {
				addDelete(plan, KindCatalogRegistration, a.CatalogName, a)
			}
		}
	}
}

// === Schemas ===

func schemaKey(catalogName, schemaName string) string {
	return catalogName + "." + schemaName
}

func diffSchemas(plan *Plan, desired, actual []SchemaResource) {
	actualMap := make(map[string]SchemaResource, len(actual))
	for _, a := range actual {
		actualMap[schemaKey(a.CatalogName, a.SchemaName)] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		k := schemaKey(d.CatalogName, d.SchemaName)
		name := k
		seen[k] = true
		a, exists := actualMap[k]
		if !exists {
			addCreate(plan, KindSchema, name, "", d)
			continue
		}
		var changes []FieldDiff
		diffField(&changes, "comment", a.Spec.Comment, d.Spec.Comment)
		diffField(&changes, "owner", a.Spec.Owner, d.Spec.Owner)
		diffField(&changes, "location_name", a.Spec.LocationName, d.Spec.LocationName)
		diffMapField(&changes, "properties", a.Spec.Properties, d.Spec.Properties)
		if len(changes) > 0 {
			addUpdate(plan, KindSchema, name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		k := schemaKey(a.CatalogName, a.SchemaName)
		if !seen[k] {
			if a.DeletionProtection {
				addError(plan, KindSchema, k,
					"cannot delete schema: deletion_protection is enabled")
			} else {
				addDelete(plan, KindSchema, k, a)
			}
		}
	}
}

// === Tables ===

func tableKey(catalogName, schemaName, tableName string) string {
	return catalogName + "." + schemaName + "." + tableName
}

func diffTables(plan *Plan, desired, actual []TableResource) {
	actualMap := make(map[string]TableResource, len(actual))
	for _, a := range actual {
		actualMap[tableKey(a.CatalogName, a.SchemaName, a.TableName)] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		k := tableKey(d.CatalogName, d.SchemaName, d.TableName)
		name := k
		seen[k] = true
		a, exists := actualMap[k]
		if !exists {
			addCreate(plan, KindTable, name, "", d)
			continue
		}
		changes, hasColumnTypeError := diffTableSpec(plan, name, a.Spec, d.Spec)
		if hasColumnTypeError {
			// Column type change is a PlanError; skip generating an update action.
			continue
		}
		if len(changes) > 0 {
			addUpdate(plan, KindTable, name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		k := tableKey(a.CatalogName, a.SchemaName, a.TableName)
		if !seen[k] {
			if a.DeletionProtection {
				addError(plan, KindTable, k,
					"cannot delete table: deletion_protection is enabled")
			} else {
				addDelete(plan, KindTable, k, a)
			}
		}
	}
}

// diffTableSpec compares two TableSpecs and returns field diffs plus a flag
// indicating whether a column type change error was emitted.
func diffTableSpec(plan *Plan, tableName string, actual, desired TableSpec) ([]FieldDiff, bool) {
	var changes []FieldDiff
	diffField(&changes, "table_type", actual.TableType, desired.TableType)
	diffField(&changes, "comment", actual.Comment, desired.Comment)
	diffField(&changes, "owner", actual.Owner, desired.Owner)
	diffMapField(&changes, "properties", actual.Properties, desired.Properties)
	diffField(&changes, "source_path", actual.SourcePath, desired.SourcePath)
	diffField(&changes, "file_format", actual.FileFormat, desired.FileFormat)
	diffField(&changes, "location_name", actual.LocationName, desired.LocationName)

	// Column-level diff.
	hasTypeError := diffColumns(plan, tableName, &changes, actual.Columns, desired.Columns)
	return changes, hasTypeError
}

// diffColumns compares column lists and appends field diffs for additions, removals,
// and comment changes. Column type changes produce a PlanError. Returns true if a
// column type error was emitted.
func diffColumns(plan *Plan, tableName string, changes *[]FieldDiff, actual, desired []ColumnDef) bool {
	actualMap := make(map[string]ColumnDef, len(actual))
	for _, c := range actual {
		actualMap[c.Name] = c
	}

	hasTypeError := false
	seen := make(map[string]bool, len(desired))

	for _, dc := range desired {
		seen[dc.Name] = true
		ac, exists := actualMap[dc.Name]
		if !exists {
			*changes = append(*changes, FieldDiff{
				Field:    fmt.Sprintf("columns.%s", dc.Name),
				OldValue: "",
				NewValue: fmt.Sprintf("%s %s", dc.Name, dc.Type),
			})
			continue
		}
		if ac.Type != dc.Type {
			addError(plan, KindTable, tableName,
				fmt.Sprintf("column %q: cannot change type from %q to %q", dc.Name, ac.Type, dc.Type))
			hasTypeError = true
			continue
		}
		if ac.Comment != dc.Comment {
			*changes = append(*changes, FieldDiff{
				Field:    fmt.Sprintf("columns.%s.comment", dc.Name),
				OldValue: ac.Comment,
				NewValue: dc.Comment,
			})
		}
	}

	for _, ac := range actual {
		if !seen[ac.Name] {
			*changes = append(*changes, FieldDiff{
				Field:    fmt.Sprintf("columns.%s", ac.Name),
				OldValue: fmt.Sprintf("%s %s", ac.Name, ac.Type),
				NewValue: "",
			})
		}
	}

	return hasTypeError
}

// === Views ===

func viewKey(catalogName, schemaName, viewName string) string {
	return catalogName + "." + schemaName + "." + viewName
}

func diffViews(plan *Plan, desired, actual []ViewResource) {
	actualMap := make(map[string]ViewResource, len(actual))
	for _, a := range actual {
		actualMap[viewKey(a.CatalogName, a.SchemaName, a.ViewName)] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		k := viewKey(d.CatalogName, d.SchemaName, d.ViewName)
		name := k
		seen[k] = true
		a, exists := actualMap[k]
		if !exists {
			addCreate(plan, KindView, name, "", d)
			continue
		}
		var changes []FieldDiff
		if normalizeViewDefinition(a.Spec.ViewDefinition) != normalizeViewDefinition(d.Spec.ViewDefinition) {
			diffField(&changes, "view_definition", a.Spec.ViewDefinition, d.Spec.ViewDefinition)
		}
		diffField(&changes, "comment", a.Spec.Comment, d.Spec.Comment)
		diffField(&changes, "owner", a.Spec.Owner, d.Spec.Owner)
		diffMapField(&changes, "properties", a.Spec.Properties, d.Spec.Properties)
		if len(changes) > 0 {
			addUpdate(plan, KindView, name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		k := viewKey(a.CatalogName, a.SchemaName, a.ViewName)
		if !seen[k] {
			addDelete(plan, KindView, k, a)
		}
	}
}

func normalizeViewDefinition(value string) string {
	return strings.TrimSpace(value)
}

// === Volumes ===

func volumeKey(catalogName, schemaName, volumeName string) string {
	return catalogName + "." + schemaName + "." + volumeName
}

func diffVolumes(plan *Plan, desired, actual []VolumeResource) {
	actualMap := make(map[string]VolumeResource, len(actual))
	for _, a := range actual {
		actualMap[volumeKey(a.CatalogName, a.SchemaName, a.VolumeName)] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		k := volumeKey(d.CatalogName, d.SchemaName, d.VolumeName)
		name := k
		seen[k] = true
		a, exists := actualMap[k]
		if !exists {
			addCreate(plan, KindVolume, name, "", d)
			continue
		}
		var changes []FieldDiff
		diffField(&changes, "volume_type", a.Spec.VolumeType, d.Spec.VolumeType)
		diffField(&changes, "storage_location", a.Spec.StorageLocation, d.Spec.StorageLocation)
		diffField(&changes, "comment", a.Spec.Comment, d.Spec.Comment)
		diffField(&changes, "owner", a.Spec.Owner, d.Spec.Owner)
		if len(changes) > 0 {
			addUpdate(plan, KindVolume, name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		k := volumeKey(a.CatalogName, a.SchemaName, a.VolumeName)
		if !seen[k] {
			addDelete(plan, KindVolume, k, a)
		}
	}
}

// === Row Filters ===

func rowFilterTableKey(r RowFilterResource) string {
	return r.CatalogName + "." + r.SchemaName + "." + r.TableName
}

func rowFilterKey(catalog, schema, table, filterName string) string {
	return catalog + "." + schema + "." + table + "/" + filterName
}

func diffRowFilters(plan *Plan, desired, actual []RowFilterResource) {
	// Flatten both desired and actual into per-filter maps keyed by table+filterName.
	type filterEntry struct {
		TableKey string
		Spec     RowFilterSpec
	}
	actualMap := make(map[string]filterEntry)
	for _, r := range actual {
		tk := rowFilterTableKey(r)
		for _, f := range r.Filters {
			k := rowFilterKey(r.CatalogName, r.SchemaName, r.TableName, f.Name)
			actualMap[k] = filterEntry{TableKey: tk, Spec: f}
		}
	}

	seen := make(map[string]bool)
	for _, r := range desired {
		for _, f := range r.Filters {
			k := rowFilterKey(r.CatalogName, r.SchemaName, r.TableName, f.Name)
			seen[k] = true
			ae, exists := actualMap[k]
			if !exists {
				addCreate(plan, KindRowFilter, k, "", f)
				// Create bindings for new filter.
				for _, b := range f.Bindings {
					bName := fmt.Sprintf("%s->%s:%s", k, b.PrincipalType, b.Principal)
					addCreate(plan, KindRowFilterBinding, bName, "", b)
				}
				continue
			}
			var changes []FieldDiff
			diffField(&changes, "filter_sql", ae.Spec.FilterSQL, f.FilterSQL)
			diffField(&changes, "description", ae.Spec.Description, f.Description)
			if len(changes) > 0 {
				addUpdate(plan, KindRowFilter, k, "", f, ae.Spec, changes)
			}
			// Diff bindings.
			diffFilterBindings(plan, k, f.Bindings, ae.Spec.Bindings)
		}
	}

	for k, ae := range actualMap {
		if !seen[k] {
			// Delete bindings first, then the filter.
			for _, b := range ae.Spec.Bindings {
				bName := fmt.Sprintf("%s->%s:%s", k, b.PrincipalType, b.Principal)
				addDelete(plan, KindRowFilterBinding, bName, b)
			}
			addDelete(plan, KindRowFilter, k, ae.Spec)
		}
	}
}

func filterBindingKey(b FilterBindingRef) string {
	return b.Principal + "|" + b.PrincipalType
}

func diffFilterBindings(plan *Plan, filterName string, desired, actual []FilterBindingRef) {
	actualMap := make(map[string]FilterBindingRef, len(actual))
	for _, a := range actual {
		actualMap[filterBindingKey(a)] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		k := filterBindingKey(d)
		seen[k] = true
		if _, exists := actualMap[k]; !exists {
			bName := fmt.Sprintf("%s->%s:%s", filterName, d.PrincipalType, d.Principal)
			addCreate(plan, KindRowFilterBinding, bName, "", d)
		}
	}

	for _, a := range actual {
		if !seen[filterBindingKey(a)] {
			bName := fmt.Sprintf("%s->%s:%s", filterName, a.PrincipalType, a.Principal)
			addDelete(plan, KindRowFilterBinding, bName, a)
		}
	}
}

// === Column Masks ===

func columnMaskTableKey(r ColumnMaskResource) string {
	return r.CatalogName + "." + r.SchemaName + "." + r.TableName
}

func columnMaskKey(catalog, schema, table, maskName string) string {
	return catalog + "." + schema + "." + table + "/" + maskName
}

func diffColumnMasks(plan *Plan, desired, actual []ColumnMaskResource) {
	type maskEntry struct {
		TableKey string
		Spec     ColumnMaskSpec
	}
	actualMap := make(map[string]maskEntry)
	for _, r := range actual {
		tk := columnMaskTableKey(r)
		for _, m := range r.Masks {
			k := columnMaskKey(r.CatalogName, r.SchemaName, r.TableName, m.Name)
			actualMap[k] = maskEntry{TableKey: tk, Spec: m}
		}
	}

	seen := make(map[string]bool)
	for _, r := range desired {
		for _, m := range r.Masks {
			k := columnMaskKey(r.CatalogName, r.SchemaName, r.TableName, m.Name)
			seen[k] = true
			ae, exists := actualMap[k]
			if !exists {
				addCreate(plan, KindColumnMask, k, "", m)
				for _, b := range m.Bindings {
					bName := fmt.Sprintf("%s->%s:%s", k, b.PrincipalType, b.Principal)
					addCreate(plan, KindColumnMaskBinding, bName, "", b)
				}
				continue
			}
			var changes []FieldDiff
			diffField(&changes, "column_name", ae.Spec.ColumnName, m.ColumnName)
			diffField(&changes, "mask_expression", ae.Spec.MaskExpression, m.MaskExpression)
			diffField(&changes, "description", ae.Spec.Description, m.Description)
			if len(changes) > 0 {
				addUpdate(plan, KindColumnMask, k, "", m, ae.Spec, changes)
			}
			diffMaskBindings(plan, k, m.Bindings, ae.Spec.Bindings)
		}
	}

	for k, ae := range actualMap {
		if !seen[k] {
			for _, b := range ae.Spec.Bindings {
				bName := fmt.Sprintf("%s->%s:%s", k, b.PrincipalType, b.Principal)
				addDelete(plan, KindColumnMaskBinding, bName, b)
			}
			addDelete(plan, KindColumnMask, k, ae.Spec)
		}
	}
}

func maskBindingKey(b MaskBindingRef) string {
	return b.Principal + "|" + b.PrincipalType
}

func diffMaskBindings(plan *Plan, maskName string, desired, actual []MaskBindingRef) {
	actualMap := make(map[string]MaskBindingRef, len(actual))
	for _, a := range actual {
		actualMap[maskBindingKey(a)] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		k := maskBindingKey(d)
		seen[k] = true
		a, exists := actualMap[k]
		if !exists {
			bName := fmt.Sprintf("%s->%s:%s", maskName, d.PrincipalType, d.Principal)
			addCreate(plan, KindColumnMaskBinding, bName, "", d)
			continue
		}
		// SeeOriginal can be updated.
		if a.SeeOriginal != d.SeeOriginal {
			bName := fmt.Sprintf("%s->%s:%s", maskName, d.PrincipalType, d.Principal)
			var changes []FieldDiff
			diffBoolField(&changes, "see_original", a.SeeOriginal, d.SeeOriginal)
			addUpdate(plan, KindColumnMaskBinding, bName, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[maskBindingKey(a)] {
			bName := fmt.Sprintf("%s->%s:%s", maskName, a.PrincipalType, a.Principal)
			addDelete(plan, KindColumnMaskBinding, bName, a)
		}
	}
}

// === Tags ===

func tagKey(t TagSpec) string {
	if t.Value != nil {
		return t.Key + ":" + *t.Value
	}
	return t.Key
}

func diffTags(plan *Plan, desired, actual []TagSpec) {
	actualMap := make(map[string]TagSpec, len(actual))
	for _, a := range actual {
		actualMap[tagKey(a)] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		k := tagKey(d)
		seen[k] = true
		if _, exists := actualMap[k]; !exists {
			addCreate(plan, KindTag, k, "", d)
		}
	}

	for _, a := range actual {
		if !seen[tagKey(a)] {
			addDelete(plan, KindTag, tagKey(a), a)
		}
	}
}

// === Tag Assignments ===

func tagAssignmentKey(t TagAssignmentSpec) string {
	k := t.Tag + "|" + t.SecurableType + "|" + t.Securable
	if t.ColumnName != "" {
		k += "|" + t.ColumnName
	}
	return k
}

func tagAssignmentName(t TagAssignmentSpec) string {
	name := fmt.Sprintf("%s on %s.%s", t.Tag, t.SecurableType, t.Securable)
	if t.ColumnName != "" {
		name += "." + t.ColumnName
	}
	return name
}

func diffTagAssignments(plan *Plan, desired, actual []TagAssignmentSpec) {
	actualMap := make(map[string]TagAssignmentSpec, len(actual))
	for _, a := range actual {
		actualMap[tagAssignmentKey(a)] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		k := tagAssignmentKey(d)
		seen[k] = true
		if _, exists := actualMap[k]; !exists {
			addCreate(plan, KindTagAssignment, tagAssignmentName(d), "", d)
		}
	}

	for _, a := range actual {
		if !seen[tagAssignmentKey(a)] {
			addDelete(plan, KindTagAssignment, tagAssignmentName(a), a)
		}
	}
}

// === Storage Credentials ===

func diffStorageCredentials(plan *Plan, desired, actual []StorageCredentialSpec) {
	actualMap := make(map[string]StorageCredentialSpec, len(actual))
	for _, a := range actual {
		actualMap[a.Name] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		seen[d.Name] = true
		a, exists := actualMap[d.Name]
		if !exists {
			addCreate(plan, KindStorageCredential, d.Name, "", d)
			continue
		}
		// Compare only non-secret fields. Secrets cannot be read from server.
		var changes []FieldDiff
		diffField(&changes, "credential_type", a.CredentialType, d.CredentialType)
		diffField(&changes, "comment", a.Comment, d.Comment)
		diffStorageCredentialDetails(&changes, a, d)
		if len(changes) > 0 {
			addUpdate(plan, KindStorageCredential, d.Name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[a.Name] {
			addDelete(plan, KindStorageCredential, a.Name, a)
		}
	}
}

// diffStorageCredentialDetails compares non-secret sub-fields of storage credentials.
func diffStorageCredentialDetails(changes *[]FieldDiff, actual, desired StorageCredentialSpec) {
	// S3
	if actual.S3 != nil && desired.S3 != nil {
		diffField(changes, "s3.endpoint", actual.S3.Endpoint, desired.S3.Endpoint)
		diffField(changes, "s3.region", actual.S3.Region, desired.S3.Region)
		diffField(changes, "s3.url_style", actual.S3.URLStyle, desired.S3.URLStyle)
	} else if actual.S3 != desired.S3 {
		// One is nil, the other is not — credential sub-type changed.
		oldVal := ""
		newVal := ""
		if actual.S3 != nil {
			oldVal = "configured"
		}
		if desired.S3 != nil {
			newVal = "configured"
		}
		diffField(changes, "s3", oldVal, newVal)
	}

	// Azure
	if actual.Azure != nil && desired.Azure != nil {
		diffField(changes, "azure.tenant_id", actual.Azure.TenantID, desired.Azure.TenantID)
	} else if actual.Azure != desired.Azure {
		oldVal := ""
		newVal := ""
		if actual.Azure != nil {
			oldVal = "configured"
		}
		if desired.Azure != nil {
			newVal = "configured"
		}
		diffField(changes, "azure", oldVal, newVal)
	}

	// GCS
	if actual.GCS != nil && desired.GCS != nil {
		diffField(changes, "gcs.key_file_path", actual.GCS.KeyFilePath, desired.GCS.KeyFilePath)
	} else if actual.GCS != desired.GCS {
		oldVal := ""
		newVal := ""
		if actual.GCS != nil {
			oldVal = "configured"
		}
		if desired.GCS != nil {
			newVal = "configured"
		}
		diffField(changes, "gcs", oldVal, newVal)
	}
}

// === External Locations ===

func diffExternalLocations(plan *Plan, desired, actual []ExternalLocationSpec) {
	actualMap := make(map[string]ExternalLocationSpec, len(actual))
	for _, a := range actual {
		actualMap[a.Name] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		seen[d.Name] = true
		a, exists := actualMap[d.Name]
		if !exists {
			addCreate(plan, KindExternalLocation, d.Name, "", d)
			continue
		}
		var changes []FieldDiff
		diffField(&changes, "url", a.URL, d.URL)
		diffField(&changes, "credential_name", a.CredentialName, d.CredentialName)
		diffField(&changes, "storage_type", a.StorageType, d.StorageType)
		diffField(&changes, "comment", a.Comment, d.Comment)
		diffBoolField(&changes, "read_only", a.ReadOnly, d.ReadOnly)
		if len(changes) > 0 {
			addUpdate(plan, KindExternalLocation, d.Name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[a.Name] {
			addDelete(plan, KindExternalLocation, a.Name, a)
		}
	}
}

// === Compute Endpoints ===

func diffComputeEndpoints(plan *Plan, desired, actual []ComputeEndpointSpec) {
	actualMap := make(map[string]ComputeEndpointSpec, len(actual))
	for _, a := range actual {
		actualMap[a.Name] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		seen[d.Name] = true
		a, exists := actualMap[d.Name]
		if !exists {
			addCreate(plan, KindComputeEndpoint, d.Name, "", d)
			continue
		}
		var changes []FieldDiff
		diffField(&changes, "url", a.URL, d.URL)
		diffField(&changes, "type", a.Type, d.Type)
		diffField(&changes, "selection_policy", a.SelectionPolicy, d.SelectionPolicy)
		diffField(&changes, "workload_class", a.WorkloadClass, d.WorkloadClass)
		diffField(&changes, "readiness_status", a.ReadinessStatus, d.ReadinessStatus)
		diffField(&changes, "size", a.Size, d.Size)
		diffIntPtrField(&changes, "max_memory_gb", a.MaxMemoryGB, d.MaxMemoryGB)
		diffIntPtrField(&changes, "max_concurrency", a.MaxConcurrency, d.MaxConcurrency)
		diffIntPtrField(&changes, "max_result_size_mb", a.MaxResultSizeMB, d.MaxResultSizeMB)
		diffBoolField(&changes, "recommended_for_large_queries", a.RecommendedForLargeQueries, d.RecommendedForLargeQueries)
		diffBoolField(&changes, "is_draining", a.IsDraining, d.IsDraining)
		if len(changes) > 0 {
			addUpdate(plan, KindComputeEndpoint, d.Name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[a.Name] {
			addDelete(plan, KindComputeEndpoint, a.Name, a)
		}
	}
}

func diffComputeRoutingDefaults(plan *Plan, desired, actual *ComputeRoutingDefaultsSpec) {
	if desired == nil && actual == nil {
		return
	}
	if desired != nil && actual == nil {
		addCreate(plan, KindComputeRoutingDefaults, "global", "", *desired)
		return
	}
	if desired == nil && actual != nil {
		addDelete(plan, KindComputeRoutingDefaults, "global", *actual)
		return
	}

	var changes []FieldDiff
	diffField(&changes, "interactive_mode", actual.InteractiveMode, desired.InteractiveMode)
	diffField(&changes, "scheduled_mode", actual.ScheduledMode, desired.ScheduledMode)
	diffField(&changes, "notebook_mode", actual.NotebookMode, desired.NotebookMode)
	if len(changes) > 0 {
		addUpdate(plan, KindComputeRoutingDefaults, "global", "", *desired, *actual, changes)
	}
}

// === Compute Assignments ===

func computeAssignmentKey(c ComputeAssignmentSpec) string {
	return c.Endpoint + "|" + c.Principal + "|" + c.PrincipalType
}

func computeAssignmentName(c ComputeAssignmentSpec) string {
	return fmt.Sprintf("%s->%s:%s", c.Endpoint, c.PrincipalType, c.Principal)
}

func diffComputeAssignments(plan *Plan, desired, actual []ComputeAssignmentSpec) {
	actualMap := make(map[string]ComputeAssignmentSpec, len(actual))
	for _, a := range actual {
		actualMap[computeAssignmentKey(a)] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		k := computeAssignmentKey(d)
		name := computeAssignmentName(d)
		seen[k] = true
		a, exists := actualMap[k]
		if !exists {
			addCreate(plan, KindComputeAssignment, name, "", d)
			continue
		}
		var changes []FieldDiff
		diffBoolField(&changes, "is_default", a.IsDefault, d.IsDefault)
		diffBoolField(&changes, "fallback_local", a.FallbackLocal, d.FallbackLocal)
		if len(changes) > 0 {
			addUpdate(plan, KindComputeAssignment, name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[computeAssignmentKey(a)] {
			addDelete(plan, KindComputeAssignment, computeAssignmentName(a), a)
		}
	}
}

// === API Keys ===

func diffAPIKeys(plan *Plan, desired, actual []APIKeySpec) {
	actualMap := make(map[string]APIKeySpec, len(actual))
	for _, a := range actual {
		actualMap[a.Name] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		seen[d.Name] = true
		a, exists := actualMap[d.Name]
		if !exists {
			addCreate(plan, KindAPIKey, d.Name, "", d)
			continue
		}
		var changes []FieldDiff
		diffField(&changes, "principal", a.Principal, d.Principal)
		diffStringPtrField(&changes, "expires_at", a.ExpiresAt, d.ExpiresAt)
		if len(changes) > 0 {
			addUpdate(plan, KindAPIKey, d.Name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[a.Name] {
			addDelete(plan, KindAPIKey, a.Name, a)
		}
	}
}

// === Notebooks ===

func diffNotebooks(plan *Plan, desired, actual []NotebookResource) {
	actualMap := make(map[string]NotebookResource, len(actual))
	for _, a := range actual {
		actualMap[a.Name] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		seen[d.Name] = true
		a, exists := actualMap[d.Name]
		if !exists {
			addCreate(plan, KindNotebook, d.Name, "", d)
			continue
		}
		var changes []FieldDiff
		diffField(&changes, "description", a.Spec.Description, d.Spec.Description)
		if strings.TrimSpace(d.Spec.Owner) != "" || strings.TrimSpace(a.Spec.Owner) == "" {
			diffField(&changes, "owner", a.Spec.Owner, d.Spec.Owner)
		}
		diffField(&changes, "workspace_ref", a.Spec.WorkspaceRef, d.Spec.WorkspaceRef)
		diffField(&changes, "folder_ref", a.Spec.FolderRef, d.Spec.FolderRef)
		diffField(&changes, "project_ref", a.Spec.ProjectRef, d.Spec.ProjectRef)
		diffField(&changes, "environment_ref", a.Spec.EnvironmentRef, d.Spec.EnvironmentRef)
		diffField(&changes, "publish", mustJSON(a.Spec.Publish), mustJSON(d.Spec.Publish))
		diffCells(&changes, a.Spec.Cells, d.Spec.Cells)
		if len(changes) > 0 {
			addUpdate(plan, KindNotebook, d.Name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[a.Name] {
			addDelete(plan, KindNotebook, a.Name, a)
		}
	}
}

// === Assets ===

func diffAssets(plan *Plan, desired, actual []AssetResource) {
	actualMap := make(map[string]AssetResource, len(actual))
	for _, a := range actual {
		actualMap[a.Name] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		seen[d.Name] = true
		a, exists := actualMap[d.Name]
		if !exists {
			addCreate(plan, KindAsset, d.Name, "", d)
			continue
		}

		var changes []FieldDiff
		diffField(&changes, "asset_type", a.Spec.AssetType, d.Spec.AssetType)
		if a.Spec.ProductRef != "" || d.Spec.ProductRef == "" {
			diffField(&changes, "product_ref", a.Spec.ProductRef, d.Spec.ProductRef)
		}
		diffField(&changes, "owner", a.Spec.Owner, d.Spec.Owner)
		diffField(&changes, "description", a.Spec.Description, d.Spec.Description)
		diffField(&changes, "tags", formatStringSlice(a.Spec.Tags), formatStringSlice(d.Spec.Tags))
		diffField(&changes, "depends_on", formatStringSlice(a.Spec.DependsOn), formatStringSlice(d.Spec.DependsOn))
		diffField(&changes, "io_profile", a.Spec.IOProfile, d.Spec.IOProfile)
		diffField(&changes, "partition_definition", stableJSON(a.Spec.PartitionDefinition), stableJSON(d.Spec.PartitionDefinition))
		diffField(&changes, "auto_materialize_policy", stableJSON(a.Spec.AutoMaterializePolicy), stableJSON(d.Spec.AutoMaterializePolicy))
		diffField(&changes, "freshness_policy", stableJSON(a.Spec.FreshnessPolicy), stableJSON(d.Spec.FreshnessPolicy))
		diffField(&changes, "materialization_policy", stableJSON(a.Spec.MaterializationPolicy), stableJSON(d.Spec.MaterializationPolicy))
		diffField(&changes, "partition_type", a.Spec.PartitionType, d.Spec.PartitionType)
		diffBoolField(&changes, "auto_materialize", a.Spec.AutoMaterialize, d.Spec.AutoMaterialize)
		diffInt64PtrField(&changes, "max_lag_seconds", a.Spec.MaxLagSeconds, d.Spec.MaxLagSeconds)
		diffField(&changes, "cron_schedule", a.Spec.CronSchedule, d.Spec.CronSchedule)
		diffField(&changes, "checks", stableJSON(a.Spec.CheckDefinitions), stableJSON(d.Spec.CheckDefinitions))
		diffMapField(&changes, "properties", a.Spec.Properties, d.Spec.Properties)

		if len(changes) > 0 {
			addUpdate(plan, KindAsset, d.Name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[a.Name] {
			addDelete(plan, KindAsset, a.Name, a)
		}
	}
}

// diffCells compares two cell lists and appends a single FieldDiff if they differ.
func diffCells(changes *[]FieldDiff, actual, desired []CellSpec) {
	if len(actual) == len(desired) {
		equal := true
		for i := range actual {
			if mustJSON(actual[i]) != mustJSON(desired[i]) {
				equal = false
				break
			}
		}
		if equal {
			return
		}
	}
	*changes = append(*changes, FieldDiff{
		Field:    "cells",
		OldValue: fmt.Sprintf("%d cells", len(actual)),
		NewValue: fmt.Sprintf("%d cells", len(desired)),
	})
}

func mustJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// === Helpers ===

// formatStringSlice returns a stable comma-separated string for comparison.
func formatStringSlice(s []string) string {
	if len(s) == 0 {
		return ""
	}
	sorted := make([]string, len(s))
	copy(sorted, s)
	sort.Strings(sorted)
	return strings.Join(sorted, ",")
}

func normalizeIncrementalStrategy(strategy string) string {
	value := strings.ToLower(strings.TrimSpace(strategy))
	if value == "delete+insert" {
		return "delete_insert"
	}
	return value
}

func normalizeOnSchemaChange(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeSemanticMetricSpecs(metrics []SemanticMetricSpec) []SemanticMetricSpec {
	if len(metrics) == 0 {
		return nil
	}
	normalized := append([]SemanticMetricSpec(nil), metrics...)
	for i := range normalized {
		if normalized[i].CertificationState == "" {
			normalized[i].CertificationState = "DRAFT"
		}
	}
	return normalized
}

func normalizeMacroType(value string) string {
	v := strings.ToUpper(strings.TrimSpace(value))
	if v == "" {
		return "SCALAR"
	}
	return v
}

func normalizeMacroVisibility(value string) string {
	v := strings.ToLower(strings.TrimSpace(value))
	if v == "" {
		return "project"
	}
	return v
}

func normalizeMacroStatus(value string) string {
	v := strings.ToUpper(strings.TrimSpace(value))
	if v == "" {
		return "ACTIVE"
	}
	return v
}

func normalizeModelConfig(spec ModelSpec) *ModelConfigSpec {
	if spec.Config == nil {
		if strings.EqualFold(spec.Materialization, "INCREMENTAL") {
			return &ModelConfigSpec{
				IncrementalStrategy: "merge",
				OnSchemaChange:      "ignore",
			}
		}
		return nil
	}

	normalized := &ModelConfigSpec{
		UniqueKey:           append([]string(nil), spec.Config.UniqueKey...),
		IncrementalStrategy: normalizeIncrementalStrategy(spec.Config.IncrementalStrategy),
		OnSchemaChange:      normalizeOnSchemaChange(spec.Config.OnSchemaChange),
	}

	if strings.EqualFold(spec.Materialization, "INCREMENTAL") {
		if normalized.IncrementalStrategy == "" {
			normalized.IncrementalStrategy = "merge"
		}
		if normalized.OnSchemaChange == "" {
			normalized.OnSchemaChange = "ignore"
		}
	}

	if len(normalized.UniqueKey) > 0 {
		sort.Strings(normalized.UniqueKey)
	}

	if len(normalized.UniqueKey) == 0 && normalized.IncrementalStrategy == "" && normalized.OnSchemaChange == "" {
		return nil
	}

	return normalized
}

func stableJSON(value interface{}) string {
	if value == nil {
		return ""
	}
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(data)
}

func formatContract(contract *ContractSpec) string {
	if contract == nil {
		return ""
	}
	type normalizedColumn struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Nullable bool   `json:"nullable"`
	}

	cols := make([]normalizedColumn, len(contract.Columns))
	for i, col := range contract.Columns {
		cols[i] = normalizedColumn(col)
	}
	sort.Slice(cols, func(i, j int) bool {
		if cols[i].Name != cols[j].Name {
			return cols[i].Name < cols[j].Name
		}
		if cols[i].Type != cols[j].Type {
			return cols[i].Type < cols[j].Type
		}
		return !cols[i].Nullable && cols[j].Nullable
	})

	return stableJSON(struct {
		Enforce bool               `json:"enforce"`
		Columns []normalizedColumn `json:"columns,omitempty"`
	}{
		Enforce: contract.Enforce,
		Columns: cols,
	})
}

func formatFreshness(freshness *FreshnessSpecYAML) string {
	if freshness == nil {
		return ""
	}
	if freshness.MaxLagSeconds == 0 && freshness.CronSchedule == "" {
		return ""
	}
	return stableJSON(struct {
		MaxLagSeconds int64  `json:"max_lag_seconds,omitempty"`
		CronSchedule  string `json:"cron_schedule,omitempty"`
	}{
		MaxLagSeconds: freshness.MaxLagSeconds,
		CronSchedule:  freshness.CronSchedule,
	})
}

func formatModelTests(tests []TestSpec) string {
	if len(tests) == 0 {
		return ""
	}
	type normalizedTest struct {
		Name     string   `json:"name"`
		Type     string   `json:"type"`
		Column   string   `json:"column,omitempty"`
		Values   []string `json:"values,omitempty"`
		ToModel  string   `json:"to_model,omitempty"`
		ToColumn string   `json:"to_column,omitempty"`
		SQL      string   `json:"sql,omitempty"`
	}

	normalized := make([]normalizedTest, len(tests))
	for i, test := range tests {
		values := append([]string(nil), test.Values...)
		sort.Strings(values)
		normalized[i] = normalizedTest{
			Name:     test.Name,
			Type:     test.Type,
			Column:   test.Column,
			Values:   values,
			ToModel:  test.ToModel,
			ToColumn: test.ToColumn,
			SQL:      test.SQL,
		}
	}

	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].Name != normalized[j].Name {
			return normalized[i].Name < normalized[j].Name
		}
		if normalized[i].Type != normalized[j].Type {
			return normalized[i].Type < normalized[j].Type
		}
		if normalized[i].Column != normalized[j].Column {
			return normalized[i].Column < normalized[j].Column
		}
		if normalized[i].ToModel != normalized[j].ToModel {
			return normalized[i].ToModel < normalized[j].ToModel
		}
		if normalized[i].ToColumn != normalized[j].ToColumn {
			return normalized[i].ToColumn < normalized[j].ToColumn
		}
		return normalized[i].SQL < normalized[j].SQL
	})

	return stableJSON(normalized)
}

// === Models ===

func modelKey(projectName, modelName string) string {
	return projectName + "." + modelName
}

func diffModels(plan *Plan, desired, actual []ModelResource) {
	actualMap := make(map[string]ModelResource, len(actual))
	for _, a := range actual {
		actualMap[modelKey(a.ProjectName, a.ModelName)] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		k := modelKey(d.ProjectName, d.ModelName)
		seen[k] = true
		a, exists := actualMap[k]
		if !exists {
			addCreate(plan, KindModel, k, "", d)
			continue
		}
		var changes []FieldDiff
		diffField(&changes, "materialization", a.Spec.Materialization, d.Spec.Materialization)
		diffField(&changes, "description", a.Spec.Description, d.Spec.Description)
		diffField(&changes, "sql", a.Spec.SQL, d.Spec.SQL)
		diffField(&changes, "tags", formatStringSlice(a.Spec.Tags), formatStringSlice(d.Spec.Tags))
		diffField(&changes, "config", stableJSON(normalizeModelConfig(a.Spec)), stableJSON(normalizeModelConfig(d.Spec)))
		diffField(&changes, "contract", formatContract(a.Spec.Contract), formatContract(d.Spec.Contract))
		diffField(&changes, "tests", formatModelTests(a.Spec.Tests), formatModelTests(d.Spec.Tests))
		diffField(&changes, "freshness", formatFreshness(a.Spec.Freshness), formatFreshness(d.Spec.Freshness))
		if len(changes) > 0 {
			addUpdate(plan, KindModel, k, "", d, a, changes)
		}
	}

	for _, a := range actual {
		k := modelKey(a.ProjectName, a.ModelName)
		if !seen[k] {
			addDelete(plan, KindModel, k, a)
		}
	}
}

// === Semantic Models ===

func semanticModelKey(modelName string) string {
	return modelName
}

func diffSemanticModels(plan *Plan, desired, actual []SemanticModelResource) {
	actualMap := make(map[string]SemanticModelResource, len(actual))
	for _, a := range actual {
		actualMap[semanticModelKey(a.ModelName)] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		k := semanticModelKey(d.ModelName)
		seen[k] = true
		a, exists := actualMap[k]
		if !exists {
			addCreate(plan, KindSemanticModel, k, "", d)
			continue
		}

		var changes []FieldDiff
		diffField(&changes, "description", a.Spec.Description, d.Spec.Description)
		diffField(&changes, "base_model_ref", a.Spec.BaseModelRef, d.Spec.BaseModelRef)
		diffField(&changes, "default_time_dimension", a.Spec.DefaultTimeDimension, d.Spec.DefaultTimeDimension)
		diffField(&changes, "tags", formatStringSlice(a.Spec.Tags), formatStringSlice(d.Spec.Tags))
		diffField(&changes, "metrics", stableJSON(normalizeSemanticMetricSpecs(a.Spec.Metrics)), stableJSON(normalizeSemanticMetricSpecs(d.Spec.Metrics)))
		diffField(&changes, "relationships", stableJSON(a.Spec.Relationships), stableJSON(d.Spec.Relationships))
		diffField(&changes, "pre_aggregations", stableJSON(a.Spec.PreAggregations), stableJSON(d.Spec.PreAggregations))
		if len(changes) > 0 {
			addUpdate(plan, KindSemanticModel, k, "", d, a, changes)
		}
	}

	for _, a := range actual {
		k := semanticModelKey(a.ModelName)
		if !seen[k] {
			addDelete(plan, KindSemanticModel, k, a)
		}
	}
}

// === Macros ===

func diffMacros(plan *Plan, desired, actual []MacroResource) {
	actualMap := make(map[string]MacroResource, len(actual))
	for _, a := range actual {
		actualMap[a.Name] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		seen[d.Name] = true
		a, exists := actualMap[d.Name]
		if !exists {
			addCreate(plan, KindMacro, d.Name, "", d)
			continue
		}
		var changes []FieldDiff
		diffField(&changes, "macro_type", normalizeMacroType(a.Spec.MacroType), normalizeMacroType(d.Spec.MacroType))
		diffField(&changes, "body", a.Spec.Body, d.Spec.Body)
		diffField(&changes, "description", a.Spec.Description, d.Spec.Description)
		diffField(&changes, "parameters", formatStringSlice(a.Spec.Parameters), formatStringSlice(d.Spec.Parameters))
		diffField(&changes, "catalog_name", a.Spec.CatalogName, d.Spec.CatalogName)
		diffField(&changes, "project_name", a.Spec.ProjectName, d.Spec.ProjectName)
		diffField(&changes, "visibility", normalizeMacroVisibility(a.Spec.Visibility), normalizeMacroVisibility(d.Spec.Visibility))
		diffField(&changes, "owner", a.Spec.Owner, d.Spec.Owner)
		diffMapField(&changes, "properties", a.Spec.Properties, d.Spec.Properties)
		diffField(&changes, "tags", formatStringSlice(a.Spec.Tags), formatStringSlice(d.Spec.Tags))
		diffField(&changes, "status", normalizeMacroStatus(a.Spec.Status), normalizeMacroStatus(d.Spec.Status))
		if len(changes) > 0 {
			addUpdate(plan, KindMacro, d.Name, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[a.Name] {
			addDelete(plan, KindMacro, a.Name, a)
		}
	}
}
