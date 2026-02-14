package declarative

import (
	"fmt"
	"sort"
	"strings"
)

// Diff compares the desired state (from YAML) against the actual state (from server)
// and returns a Plan describing the changes needed.
func Diff(desired, actual *DesiredState) *Plan {
	plan := &Plan{}

	// Diff each resource type. Order doesn't matter here — SortActions handles ordering later.
	diffPrincipals(plan, desired.Principals, actual.Principals)
	diffGroups(plan, desired.Groups, actual.Groups)
	diffGrants(plan, desired.Grants, actual.Grants)
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
	diffAPIKeys(plan, desired.APIKeys, actual.APIKeys)
	diffNotebooks(plan, desired.Notebooks, actual.Notebooks)
	diffPipelines(plan, desired.Pipelines, actual.Pipelines)

	plan.SortActions()
	return plan
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
		diffField(&changes, "type", a.Type, d.Type)
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
		actualMap[memberKey{a.Name, a.Type}] = a
	}

	seen := make(map[memberKey]bool, len(desired))
	for _, d := range desired {
		k := memberKey{d.Name, d.Type}
		seen[k] = true
		if _, exists := actualMap[k]; !exists {
			memberName := fmt.Sprintf("%s/%s(%s)", groupName, d.Name, d.Type)
			addCreate(plan, KindGroupMembership, memberName, "", d)
		}
	}

	for _, a := range actual {
		k := memberKey{a.Name, a.Type}
		if !seen[k] {
			memberName := fmt.Sprintf("%s/%s(%s)", groupName, a.Name, a.Type)
			addDelete(plan, KindGroupMembership, memberName, a)
		}
	}
}

// === Grants ===

func grantKey(g GrantSpec) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s", g.Principal, g.PrincipalType, g.SecurableType, g.Securable, g.Privilege)
}

func diffGrants(plan *Plan, desired, actual []GrantSpec) {
	actualMap := make(map[string]GrantSpec, len(actual))
	for _, a := range actual {
		actualMap[grantKey(a)] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		k := grantKey(d)
		seen[k] = true
		if _, exists := actualMap[k]; !exists {
			name := fmt.Sprintf("%s:%s on %s.%s", d.PrincipalType, d.Principal, d.SecurableType, d.Securable)
			addCreate(plan, KindPrivilegeGrant, name, "", d)
		}
	}

	for _, a := range actual {
		if !seen[grantKey(a)] {
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
		diffBoolField(&changes, "is_default", a.Spec.IsDefault, d.Spec.IsDefault)
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
		diffField(&changes, "view_definition", a.Spec.ViewDefinition, d.Spec.ViewDefinition)
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
		diffField(&changes, "size", a.Size, d.Size)
		diffIntPtrField(&changes, "max_memory_gb", a.MaxMemoryGB, d.MaxMemoryGB)
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
		diffField(&changes, "owner", a.Spec.Owner, d.Spec.Owner)
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

// diffCells compares two cell lists and appends a single FieldDiff if they differ.
func diffCells(changes *[]FieldDiff, actual, desired []CellSpec) {
	if len(actual) == len(desired) {
		equal := true
		for i := range actual {
			if actual[i].Type != desired[i].Type || actual[i].Content != desired[i].Content {
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

// === Pipelines ===

func diffPipelines(plan *Plan, desired, actual []PipelineResource) {
	actualMap := make(map[string]PipelineResource, len(actual))
	for _, a := range actual {
		actualMap[a.Name] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		seen[d.Name] = true
		a, exists := actualMap[d.Name]
		if !exists {
			addCreate(plan, KindPipeline, d.Name, "", d)
			for _, j := range d.Spec.Jobs {
				jobName := d.Name + "/" + j.Name
				addCreate(plan, KindPipelineJob, jobName, "", j)
			}
			continue
		}
		var changes []FieldDiff
		diffField(&changes, "description", a.Spec.Description, d.Spec.Description)
		diffField(&changes, "schedule_cron", a.Spec.ScheduleCron, d.Spec.ScheduleCron)
		diffBoolField(&changes, "is_paused", a.Spec.IsPaused, d.Spec.IsPaused)
		diffIntPtrField(&changes, "concurrency_limit", a.Spec.ConcurrencyLimit, d.Spec.ConcurrencyLimit)
		if len(changes) > 0 {
			addUpdate(plan, KindPipeline, d.Name, "", d, a, changes)
		}
		// Diff jobs inline.
		diffPipelineJobs(plan, d.Name, d.Spec.Jobs, a.Spec.Jobs)
	}

	for _, a := range actual {
		if !seen[a.Name] {
			// Delete jobs first (same layer, but SortActions handles ordering).
			for _, j := range a.Spec.Jobs {
				jobName := a.Name + "/" + j.Name
				addDelete(plan, KindPipelineJob, jobName, j)
			}
			addDelete(plan, KindPipeline, a.Name, a)
		}
	}
}

func diffPipelineJobs(plan *Plan, pipelineName string, desired, actual []PipelineJobSpec) {
	actualMap := make(map[string]PipelineJobSpec, len(actual))
	for _, a := range actual {
		actualMap[a.Name] = a
	}

	seen := make(map[string]bool, len(desired))
	for _, d := range desired {
		jobName := pipelineName + "/" + d.Name
		seen[d.Name] = true
		a, exists := actualMap[d.Name]
		if !exists {
			addCreate(plan, KindPipelineJob, jobName, "", d)
			continue
		}
		var changes []FieldDiff
		diffField(&changes, "notebook", a.Notebook, d.Notebook)
		diffField(&changes, "compute_endpoint", a.ComputeEndpoint, d.ComputeEndpoint)
		diffField(&changes, "depends_on", formatStringSlice(a.DependsOn), formatStringSlice(d.DependsOn))
		diffIntPtrField(&changes, "timeout_seconds", a.TimeoutSeconds, d.TimeoutSeconds)
		diffIntPtrField(&changes, "retry_count", a.RetryCount, d.RetryCount)
		diffIntPtrField(&changes, "order", a.Order, d.Order)
		if len(changes) > 0 {
			addUpdate(plan, KindPipelineJob, jobName, "", d, a, changes)
		}
	}

	for _, a := range actual {
		if !seen[a.Name] {
			jobName := pipelineName + "/" + a.Name
			addDelete(plan, KindPipelineJob, jobName, a)
		}
	}
}

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
