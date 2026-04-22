package declarative

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	cueformat "cuelang.org/go/cue/format"
	cueyaml "cuelang.org/go/encoding/yaml"
	"gopkg.in/yaml.v3"
)

const cueModuleFile = `module: "quackstack.local/quackstack-config"
language: {
	version: "v0.14.0"
}
`

type mutableFolder struct {
	spec     cueFolder
	children map[string]*mutableFolder
}

// ExportDirectory writes the given state as CUE files in the hierarchical
// directory structure under dir. If overwrite is false and dir is non-empty,
// it returns an error.
func ExportDirectory(dir string, state *DesiredState, overwrite bool) error {
	if !overwrite {
		if err := ensureEmptyOrMissing(dir); err != nil {
			return err
		}
	}

	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("reset export directory %s: %w", dir, err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "cue.mod"), 0o750); err != nil {
		return fmt.Errorf("create export module dir %s: %w", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cue.mod", "module.cue"), []byte(cueModuleFile), 0o600); err != nil {
		return fmt.Errorf("write CUE module file: %w", err)
	}

	if err := exportSecurity(dir, state); err != nil {
		return err
	}
	if err := exportGovernance(dir, state); err != nil {
		return err
	}
	if err := exportStorage(dir, state); err != nil {
		return err
	}
	if err := exportCompute(dir, state); err != nil {
		return err
	}
	if err := exportCatalogs(dir, state); err != nil {
		return err
	}
	if err := exportWorkspaces(dir, state); err != nil {
		return err
	}
	if err := exportProjects(dir, state); err != nil {
		return err
	}
	if err := exportAssets(dir, state); err != nil {
		return err
	}

	return nil
}

func ensureEmptyOrMissing(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("check directory %s: %w", dir, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("directory %s is not empty; use overwrite to replace existing files", dir)
	}
	return nil
}

func writeCueFile(path string, fragment any) error {
	wrapper := map[string]any{"platform": fragment}
	payload, err := yaml.Marshal(wrapper)
	if err != nil {
		return fmt.Errorf("marshal fragment for %s: %w", path, err)
	}
	file, err := cueyaml.Extract(path, payload)
	if err != nil {
		return fmt.Errorf("extract CUE for %s: %w", path, err)
	}
	body, err := cueformat.Node(file, cueformat.Simplify())
	if err != nil {
		return fmt.Errorf("format CUE for %s: %w", path, err)
	}
	content := append([]byte("package "+cuePackageName+"\n\n"), body...)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create directory for %s: %w", path, err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func exportSecurity(dir string, state *DesiredState) error {
	if len(state.Principals) > 0 {
		principals := map[string]any{}
		for _, item := range state.Principals {
			principals[item.Name] = map[string]any{
				"type":     item.Type,
				"is_admin": item.IsAdmin,
			}
		}
		if err := writeCueFile(filepath.Join(dir, "security", "principals.cue"), map[string]any{
			"security": map[string]any{"principals": principals},
		}); err != nil {
			return err
		}
	}
	if len(state.Groups) > 0 {
		groups := map[string]any{}
		for _, item := range state.Groups {
			groups[item.Name] = map[string]any{
				"description": item.Description,
				"members":     item.Members,
			}
		}
		if err := writeCueFile(filepath.Join(dir, "security", "groups.cue"), map[string]any{
			"security": map[string]any{"groups": groups},
		}); err != nil {
			return err
		}
	}
	if len(state.Grants) > 0 {
		if err := writeCueFile(filepath.Join(dir, "security", "grants.cue"), map[string]any{
			"security": cueSecuritySection{Grants: state.Grants},
		}); err != nil {
			return err
		}
	}
	if len(state.PrivilegePresets) > 0 {
		presets := map[string]any{}
		for _, item := range state.PrivilegePresets {
			presets[item.Name] = map[string]any{
				"description": item.Description,
				"privileges":  item.Privileges,
			}
		}
		if err := writeCueFile(filepath.Join(dir, "security", "privilege_presets.cue"), map[string]any{
			"security": map[string]any{"privilege_presets": presets},
		}); err != nil {
			return err
		}
	}
	if len(state.Bindings) > 0 {
		if err := writeCueFile(filepath.Join(dir, "security", "bindings.cue"), map[string]any{
			"security": cueSecuritySection{Bindings: state.Bindings},
		}); err != nil {
			return err
		}
	}
	if len(state.APIKeys) > 0 {
		apiKeys := map[string]any{}
		for _, item := range state.APIKeys {
			itemMap := map[string]any{
				"principal": item.Principal,
			}
			if item.ExpiresAt != nil {
				itemMap["expires_at"] = *item.ExpiresAt
			}
			apiKeys[item.Name] = itemMap
		}
		if err := writeCueFile(filepath.Join(dir, "security", "api_keys.cue"), map[string]any{
			"security": map[string]any{"api_keys": apiKeys},
		}); err != nil {
			return err
		}
	}
	return nil
}

func exportGovernance(dir string, state *DesiredState) error {
	if len(state.Tags) == 0 && len(state.TagAssignments) == 0 {
		return nil
	}
	tags := map[string][]string{}
	for _, item := range state.Tags {
		if item.Value == nil {
			if _, ok := tags[item.Key]; !ok {
				tags[item.Key] = []string{}
			}
			continue
		}
		tags[item.Key] = append(tags[item.Key], *item.Value)
	}
	return writeCueFile(filepath.Join(dir, "governance", "tags.cue"), map[string]any{
		"governance": cueGovernanceSection{
			Tags:        tags,
			Assignments: state.TagAssignments,
		},
	})
}

func exportStorage(dir string, state *DesiredState) error {
	if len(state.StorageCredentials) > 0 {
		credentials := map[string]StorageCredentialSpec{}
		for _, item := range state.StorageCredentials {
			specCopy := item
			specCopy.Name = ""
			credentials[item.Name] = specCopy
		}
		if err := writeCueFile(filepath.Join(dir, "storage", "credentials.cue"), map[string]any{
			"storage": cueStorageSection{Credentials: credentials},
		}); err != nil {
			return err
		}
	}
	if len(state.ExternalLocations) > 0 {
		locations := map[string]ExternalLocationSpec{}
		for _, item := range state.ExternalLocations {
			specCopy := item
			specCopy.Name = ""
			locations[item.Name] = specCopy
		}
		if err := writeCueFile(filepath.Join(dir, "storage", "external_locations.cue"), map[string]any{
			"storage": cueStorageSection{ExternalLocations: locations},
		}); err != nil {
			return err
		}
	}
	return nil
}

func exportCompute(dir string, state *DesiredState) error {
	if len(state.ComputeEndpoints) > 0 {
		endpoints := map[string]ComputeEndpointSpec{}
		for _, item := range state.ComputeEndpoints {
			specCopy := item
			specCopy.Name = ""
			endpoints[item.Name] = specCopy
		}
		if err := writeCueFile(filepath.Join(dir, "compute", "endpoints.cue"), map[string]any{
			"compute": cueComputeSection{Endpoints: endpoints},
		}); err != nil {
			return err
		}
	}
	if len(state.ComputeAssignments) > 0 {
		if err := writeCueFile(filepath.Join(dir, "compute", "assignments.cue"), map[string]any{
			"compute": cueComputeSection{Assignments: state.ComputeAssignments},
		}); err != nil {
			return err
		}
	}
	if state.ComputeDefaults != nil {
		if err := writeCueFile(filepath.Join(dir, "compute", "defaults.cue"), map[string]any{
			"compute": cueComputeSection{Defaults: state.ComputeDefaults},
		}); err != nil {
			return err
		}
	}
	return nil
}

func exportCatalogs(dir string, state *DesiredState) error {
	catalogs := map[string]*cueCatalog{}
	schemas := map[string]*cueSchema{}
	tables := map[string]*cueTable{}

	for _, item := range state.Catalogs {
		specCopy := item.Spec
		catalogs[item.CatalogName] = &cueCatalog{
			DeletionProtection: item.DeletionProtection,
			CatalogSpec:        specCopy,
			Schemas:            map[string]cueSchema{},
		}
	}
	for _, item := range state.Schemas {
		specCopy := item.Spec
		key := item.CatalogName + "." + item.SchemaName
		schema := &cueSchema{
			DeletionProtection: item.DeletionProtection,
			SchemaSpec:         specCopy,
			Tables:             map[string]cueTable{},
			Views:              map[string]ViewSpec{},
			Volumes:            map[string]VolumeSpec{},
		}
		schemas[key] = schema
		if catalogs[item.CatalogName] != nil {
			catalogs[item.CatalogName].Schemas[item.SchemaName] = *schema
		}
	}
	for _, item := range state.Tables {
		specCopy := item.Spec
		key := item.CatalogName + "." + item.SchemaName + "." + item.TableName
		table := &cueTable{
			DeletionProtection: item.DeletionProtection,
			TableSpec:          specCopy,
		}
		tables[key] = table
		schemaKey := item.CatalogName + "." + item.SchemaName
		if schemas[schemaKey] != nil {
			schemas[schemaKey].Tables[item.TableName] = *table
		}
	}
	for _, item := range state.RowFilters {
		key := item.CatalogName + "." + item.SchemaName + "." + item.TableName
		if table := tables[key]; table != nil {
			table.RowFilters = append(table.RowFilters, item.Filters...)
		}
	}
	for _, item := range state.ColumnMasks {
		key := item.CatalogName + "." + item.SchemaName + "." + item.TableName
		if table := tables[key]; table != nil {
			table.ColumnMasks = append(table.ColumnMasks, item.Masks...)
		}
	}
	for _, item := range state.Views {
		schemaKey := item.CatalogName + "." + item.SchemaName
		if schema := schemas[schemaKey]; schema != nil {
			schema.Views[item.ViewName] = item.Spec
		}
	}
	for _, item := range state.Volumes {
		schemaKey := item.CatalogName + "." + item.SchemaName
		if schema := schemas[schemaKey]; schema != nil {
			schema.Volumes[item.VolumeName] = item.Spec
		}
	}
	for _, catalogName := range sortedKeys(catalogs) {
		catalog := catalogs[catalogName]
		for schemaName, schema := range catalog.Schemas {
			schemaPtr := schemas[catalogName+"."+schemaName]
			if schemaPtr != nil {
				catalog.Schemas[schemaName] = *schemaPtr
			} else {
				catalog.Schemas[schemaName] = schema
			}
		}
		if err := writeCueFile(filepath.Join(dir, "catalogs", catalogName, "catalog.cue"), map[string]any{
			"catalogs": map[string]cueCatalog{catalogName: *catalog},
		}); err != nil {
			return err
		}
	}
	return nil
}

func exportWorkspaces(dir string, state *DesiredState) error {
	workspaceMap := map[string]*cueWorkspace{}
	for _, item := range state.Workspaces {
		specCopy := item.Spec
		workspaceMap[item.Name] = &cueWorkspace{
			WorkspaceSpec:  specCopy,
			Folders:        map[string]cueFolder{},
			Dashboards:     map[string]DashboardSpec{},
			SemanticModels: map[string]SemanticModelSpec{},
		}
	}

	folderNodes := map[string]*mutableFolder{}
	for _, item := range state.Folders {
		folderNodes[folderRefKey(item.Spec.WorkspaceRef, item.Spec.ParentFolderRef, item.Name)] = &mutableFolder{
			spec: cueFolder{
				DefaultProjectRef:     item.Spec.DefaultProjectRef,
				DefaultEnvironmentRef: item.Spec.DefaultEnvironmentRef,
				GitRepoID:             item.Spec.GitRepoID,
				GitRootPath:           item.Spec.GitRootPath,
				Notebooks:             map[string]NotebookSpec{},
				Folders:               map[string]cueFolder{},
			},
			children: map[string]*mutableFolder{},
		}
	}
	for _, item := range state.Notebooks {
		if item.Spec.FolderRef == "" {
			continue
		}
		node := folderNodes[item.Spec.FolderRef]
		if node == nil {
			continue
		}
		specCopy := item.Spec
		specCopy.WorkspaceRef = ""
		specCopy.FolderRef = ""
		node.spec.Notebooks[item.Name] = specCopy
	}
	for _, item := range state.Folders {
		key := folderRefKey(item.Spec.WorkspaceRef, item.Spec.ParentFolderRef, item.Name)
		node := folderNodes[key]
		if node == nil {
			continue
		}
		if item.Spec.ParentFolderRef == "" {
			if workspaceMap[item.Spec.WorkspaceRef] != nil {
				workspaceMap[item.Spec.WorkspaceRef].Folders[item.Name] = freezeFolder(node)
			}
			continue
		}
		parent := folderNodes[item.Spec.ParentFolderRef]
		if parent != nil {
			parent.children[item.Name] = node
		}
	}
	for _, workspaceName := range sortedKeys(workspaceMap) {
		workspace := workspaceMap[workspaceName]
		for name, folder := range workspace.Folders {
			_ = folder
			if rootNode := folderNodes[workspaceName+"/"+name]; rootNode != nil {
				workspace.Folders[name] = freezeFolder(rootNode)
			}
		}
	}

	dashboardWorkspace, err := inferDashboardWorkspaces(state)
	if err != nil {
		return err
	}
	for _, item := range state.Dashboards {
		workspaceName := dashboardWorkspace[item.Name]
		if workspaceMap[workspaceName] == nil {
			continue
		}
		workspaceMap[workspaceName].Dashboards[item.Name] = item.Spec
	}
	for _, item := range state.SemanticModels {
		workspaceName := strings.TrimSpace(item.Spec.WorkspaceRef)
		if workspaceMap[workspaceName] == nil {
			continue
		}
		specCopy := item.Spec
		specCopy.WorkspaceRef = ""
		workspaceMap[workspaceName].SemanticModels[item.ModelName] = specCopy
	}

	for _, workspaceName := range sortedKeys(workspaceMap) {
		if err := writeCueFile(filepath.Join(dir, "workspaces", slugPath(workspaceName), "workspace.cue"), map[string]any{
			"workspaces": map[string]cueWorkspace{workspaceName: *workspaceMap[workspaceName]},
		}); err != nil {
			return err
		}
	}
	return nil
}

func freezeFolder(node *mutableFolder) cueFolder {
	frozen := node.spec
	if frozen.Notebooks == nil {
		frozen.Notebooks = map[string]NotebookSpec{}
	}
	frozen.Folders = map[string]cueFolder{}
	for _, name := range sortedKeys(node.children) {
		frozen.Folders[name] = freezeFolder(node.children[name])
	}
	return frozen
}

func inferDashboardWorkspaces(state *DesiredState) (map[string]string, error) {
	workspaceByProject := map[string]string{}
	for _, item := range state.Projects {
		workspaceByProject[item.Name] = item.Spec.WorkspaceRef
	}
	workspaceByNotebook := map[string]string{}
	for _, item := range state.Notebooks {
		workspace := item.Spec.WorkspaceRef
		if workspace == "" && item.Spec.FolderRef != "" {
			workspace = workspaceNameFromFolderRef(item.Spec.FolderRef)
		}
		workspaceByNotebook[item.Name] = workspace
	}
	workspaces := map[string]struct{}{}
	for _, item := range state.Workspaces {
		workspaces[item.Name] = struct{}{}
	}
	result := map[string]string{}
	for _, item := range state.Dashboards {
		candidates := map[string]struct{}{}
		if workspace := strings.TrimSpace(workspaceByProject[item.Spec.SemanticProjectName]); workspace != "" {
			candidates[workspace] = struct{}{}
		}
		for _, widget := range item.Spec.Widgets {
			if widget.Source.NotebookCell != nil {
				if workspace := strings.TrimSpace(workspaceByNotebook[widget.Source.NotebookCell.NotebookName]); workspace != "" {
					candidates[workspace] = struct{}{}
				}
			}
			if widget.Source.SemanticQuery != nil {
				if workspace := strings.TrimSpace(workspaceByProject[widget.Source.SemanticQuery.ProjectName]); workspace != "" {
					candidates[workspace] = struct{}{}
				}
			}
		}
		switch len(candidates) {
		case 0:
			if len(workspaces) == 1 {
				for name := range workspaces {
					result[item.Name] = name
				}
				continue
			}
			return nil, fmt.Errorf("cannot infer workspace for dashboard %q", item.Name)
		case 1:
			for name := range candidates {
				result[item.Name] = name
			}
		default:
			return nil, fmt.Errorf("dashboard %q maps to multiple workspaces during export", item.Name)
		}
	}
	return result, nil
}

func exportProjects(dir string, state *DesiredState) error {
	projectMap := map[string]*cueProject{}
	for _, item := range state.Projects {
		specCopy := item.Spec
		projectMap[item.Name] = &cueProject{
			ProjectSpec:  specCopy,
			Environments: map[string]EnvironmentSpec{},
			Macros:       map[string]MacroSpec{},
			Models:       map[string]ModelSpec{},
		}
	}
	for _, item := range state.Environments {
		_, projectName, ok := parseProjectRef(item.Spec.ProjectRef)
		if !ok || projectMap[projectName] == nil {
			continue
		}
		specCopy := item.Spec
		specCopy.ProjectRef = ""
		projectMap[projectName].Environments[item.Name] = specCopy
	}
	for _, item := range state.Macros {
		if projectMap[item.Spec.ProjectName] == nil {
			continue
		}
		specCopy := item.Spec
		specCopy.ProjectName = ""
		projectMap[item.Spec.ProjectName].Macros[item.Name] = specCopy
	}
	for _, item := range state.Models {
		if projectMap[item.ProjectName] == nil {
			continue
		}
		projectMap[item.ProjectName].Models[item.ModelName] = item.Spec
	}
	for _, projectName := range sortedKeys(projectMap) {
		if err := writeCueFile(filepath.Join(dir, "projects", slugPath(projectName), "project.cue"), map[string]any{
			"projects": map[string]cueProject{projectName: *projectMap[projectName]},
		}); err != nil {
			return err
		}
	}
	return nil
}

func exportAssets(dir string, state *DesiredState) error {
	for _, item := range state.Assets {
		if err := writeCueFile(filepath.Join(dir, "assets", slugPath(item.Name)+".cue"), map[string]any{
			"assets": map[string]AssetSpec{item.Name: item.Spec},
		}); err != nil {
			return err
		}
	}
	return nil
}

func slugPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		sum := sha256.Sum256([]byte(value))
		return hex.EncodeToString(sum[:6])
	}
	value = strings.ReplaceAll(value, "/", "-")
	return value
}
