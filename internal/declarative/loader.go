package declarative

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/cue/load"
	"gopkg.in/yaml.v3"
)

const cuePackageName = "duckconfig"

const cuePlatformSchemaSource = `
#WorkspaceKind: "personal" | "shared" | "library"
#ProjectKind: "personal" | "shared" | "library"
#EnvironmentKind: "development" | "staging" | "production"

#Platform: close({
	security?: {
		principals?: [string]: {
			type: string
			is_admin?: bool
		}
		groups?: [string]: {
			description?: string
			members?: [...{
				name: string
				type: "user" | "group"
			}]
		}
		grants?: [..._]
		privilege_presets?: [string]: {
			description?: string
			privileges?: [...string]
		}
		bindings?: [..._]
		api_keys?: [string]: _
	}
	governance?: {
		tags?: [string]: [...string]
		assignments?: [..._]
	}
	storage?: {
		credentials?: [string]: _
		external_locations?: [string]: _
	}
	compute?: {
		endpoints?: [string]: _
		assignments?: [..._]
		defaults?: _
	}
	catalogs?: [string]: _
	domains?: [string]: _
	teams?: [string]: _
	data_products?: [string]: _
	workspaces?: [string]: {
		kind: #WorkspaceKind
		owner_principal?: string
		owner_team_id?: string
		default_project_ref?: string
		default_environment_ref?: string
		git_repo_id?: string
		git_root_path?: string
		folders?: [string]: _
		dashboards?: [string]: _
	}
	projects?: [string]: {
		workspace_ref: string
		kind: #ProjectKind
		description?: string
		product_id?: string
		default_branch?: string
		environments?: [string]: {
			kind: #EnvironmentKind
			project_ref?: string
			target_catalog: string
			target_schema: string
		}
		macros?: [string]: _
		models?: [string]: _
		semantic_models?: [string]: _
	}
	assets?: [string]: _
})
`

var errLegacyPipelines = fmt.Errorf("legacy pipelines/ configs are no longer supported; migrate pipeline definitions to assets/ and remove pipelines/")

// LoadOptions configures CUE loading behavior.
type LoadOptions struct {
	AllowUnknownFields bool
}

// LoadDirectory compiles all CUE fragments under the given root into desired state.
func LoadDirectory(dir string) (*DesiredState, error) {
	return LoadDirectoryWithOptions(dir, LoadOptions{})
}

// LoadDirectoryWithOptions compiles all CUE fragments under the given root into desired state.
func LoadDirectoryWithOptions(dir string, _ LoadOptions) (*DesiredState, error) {
	if err := ensureCUEConfigRoot(dir); err != nil {
		return nil, err
	}

	ctx := cuecontext.New()
	schema := ctx.CompileString(cuePlatformSchemaSource)
	if err := schema.Err(); err != nil {
		return nil, fmt.Errorf("compile built-in declarative schema: %w", err)
	}
	platformValue := schema.LookupPath(cue.ParsePath("#Platform"))
	if err := platformValue.Err(); err != nil {
		return nil, fmt.Errorf("resolve built-in declarative schema: %w", err)
	}

	packageDirs, err := discoverCuePackageDirs(dir)
	if err != nil {
		return nil, err
	}
	for _, packageDir := range packageDirs {
		insts := load.Instances([]string{"."}, &load.Config{
			Dir:                 packageDir,
			ModuleRoot:          dir,
			Package:             "*",
			AcceptLegacyModules: true,
		})
		for _, inst := range insts {
			if inst.Err != nil {
				return nil, fmt.Errorf("load CUE package %q: %w", inst.ImportPath, inst.Err)
			}
			value := ctx.BuildInstance(inst)
			if err := value.Err(); err != nil {
				return nil, fmt.Errorf("build CUE package %q: %w", inst.ImportPath, err)
			}

			fragment := value.LookupPath(cue.ParsePath("platform"))
			if !fragment.Exists() {
				continue
			}
			platformValue = platformValue.Unify(fragment)
		}
	}

	if err := platformValue.Validate(cue.Concrete(true)); err != nil {
		return nil, fmt.Errorf("validate CUE platform value: %w", err)
	}

	payload, err := platformValue.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("marshal CUE platform value: %w", err)
	}

	var compiled cuePlatform
	decoder := yaml.NewDecoder(bytes.NewReader(payload))
	decoder.KnownFields(true)
	if err := decoder.Decode(&compiled); err != nil {
		return nil, fmt.Errorf("decode compiled CUE platform: %w", err)
	}

	state, err := normalizeCuePlatform(compiled)
	if err != nil {
		return nil, err
	}

	return state, nil
}

func ensureCUEConfigRoot(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("config directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("config directory: %s is not a directory", dir)
	}
	moduleFile := filepath.Join(dir, "cue.mod", "module.cue")
	if _, err := os.Stat(moduleFile); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("config directory %q is not a valid CUE module root: missing %s", dir, moduleFile)
		}
		return fmt.Errorf("stat CUE module file: %w", err)
	}
	if err := failIfLegacyPipelinesPresent(dir); err != nil {
		return err
	}
	return filepath.Walk(dir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			base := info.Name()
			if base == ".git" || base == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		name := strings.ToLower(info.Name())
		if strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml") {
			return fmt.Errorf("YAML declarative config is no longer supported: %s", path)
		}
		return nil
	})
}

func discoverCuePackageDirs(root string) ([]string, error) {
	dirs := []string{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !info.IsDir() {
			return nil
		}
		if path != root && strings.HasPrefix(path, filepath.Join(root, "cue.mod")) {
			return filepath.SkipDir
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			if strings.HasSuffix(strings.ToLower(entry.Name()), ".cue") {
				dirs = append(dirs, path)
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("discover CUE packages: %w", err)
	}
	sort.Strings(dirs)
	return dirs, nil
}

func failIfLegacyPipelinesPresent(root string) error {
	pipelinesDir := filepath.Join(root, "pipelines")
	if _, err := os.Stat(pipelinesDir); os.IsNotExist(err) {
		return nil
	}
	return errLegacyPipelines
}

func normalizeCuePlatform(compiled cuePlatform) (*DesiredState, error) {
	state := &DesiredState{}

	appendNamedMap(compiled.Security.Principals, func(name string, item PrincipalSpec) {
		if strings.TrimSpace(item.Name) == "" {
			item.Name = name
		}
		state.Principals = append(state.Principals, item)
	})
	appendNamedMap(compiled.Security.Groups, func(name string, item GroupSpec) {
		if strings.TrimSpace(item.Name) == "" {
			item.Name = name
		}
		state.Groups = append(state.Groups, item)
	})
	state.Grants = append(state.Grants, compiled.Security.Grants...)
	appendNamedMap(compiled.Security.PrivilegePresets, func(name string, item PrivilegePresetSpec) {
		if strings.TrimSpace(item.Name) == "" {
			item.Name = name
		}
		state.PrivilegePresets = append(state.PrivilegePresets, item)
	})
	state.Bindings = append(state.Bindings, compiled.Security.Bindings...)
	appendNamedMap(compiled.Security.APIKeys, func(name string, item APIKeySpec) {
		if strings.TrimSpace(item.Name) == "" {
			item.Name = name
		}
		state.APIKeys = append(state.APIKeys, item)
	})

	tagKeys := sortedKeys(compiled.Governance.Tags)
	for _, key := range tagKeys {
		values := compiled.Governance.Tags[key]
		if len(values) == 0 {
			state.Tags = append(state.Tags, TagSpec{Key: key})
			continue
		}
		for _, value := range values {
			v := value
			state.Tags = append(state.Tags, TagSpec{Key: key, Value: &v})
		}
	}
	state.TagAssignments = append(state.TagAssignments, compiled.Governance.Assignments...)

	appendNamedMap(compiled.Storage.Credentials, func(name string, item StorageCredentialSpec) {
		if strings.TrimSpace(item.Name) == "" {
			item.Name = name
		}
		state.StorageCredentials = append(state.StorageCredentials, item)
	})
	appendNamedMap(compiled.Storage.ExternalLocations, func(name string, item ExternalLocationSpec) {
		if strings.TrimSpace(item.Name) == "" {
			item.Name = name
		}
		state.ExternalLocations = append(state.ExternalLocations, item)
	})

	appendNamedMap(compiled.Compute.Endpoints, func(name string, item ComputeEndpointSpec) {
		if strings.TrimSpace(item.Name) == "" {
			item.Name = name
		}
		state.ComputeEndpoints = append(state.ComputeEndpoints, item)
	})
	state.ComputeAssignments = append(state.ComputeAssignments, compiled.Compute.Assignments...)
	state.ComputeDefaults = compiled.Compute.Defaults

	appendNamedMap(compiled.Domains, func(name string, item DomainSpec) {
		state.Domains = append(state.Domains, DomainResource{Name: name, Spec: item})
	})
	appendNamedMap(compiled.Teams, func(name string, item TeamSpec) {
		state.Teams = append(state.Teams, TeamResource{Name: name, Spec: item})
	})
	appendNamedMap(compiled.DataProducts, func(name string, item DataProductSpec) {
		state.DataProducts = append(state.DataProducts, DataProductResource{Slug: name, Spec: item})
	})

	for _, catalogName := range sortedKeys(compiled.Catalogs) {
		catalog := compiled.Catalogs[catalogName]
		state.Catalogs = append(state.Catalogs, CatalogResource{
			CatalogName:        catalogName,
			DeletionProtection: catalog.DeletionProtection,
			Spec:               catalog.CatalogSpec,
		})
		for _, schemaName := range sortedKeys(catalog.Schemas) {
			schema := catalog.Schemas[schemaName]
			state.Schemas = append(state.Schemas, SchemaResource{
				CatalogName:        catalogName,
				SchemaName:         schemaName,
				DeletionProtection: schema.DeletionProtection,
				Spec:               schema.SchemaSpec,
			})
			for _, tableName := range sortedKeys(schema.Tables) {
				table := schema.Tables[tableName]
				state.Tables = append(state.Tables, TableResource{
					CatalogName:        catalogName,
					SchemaName:         schemaName,
					TableName:          tableName,
					DeletionProtection: table.DeletionProtection,
					Spec:               table.TableSpec,
				})
				if len(table.RowFilters) > 0 {
					state.RowFilters = append(state.RowFilters, RowFilterResource{
						CatalogName: catalogName,
						SchemaName:  schemaName,
						TableName:   tableName,
						Filters:     table.RowFilters,
					})
				}
				if len(table.ColumnMasks) > 0 {
					state.ColumnMasks = append(state.ColumnMasks, ColumnMaskResource{
						CatalogName: catalogName,
						SchemaName:  schemaName,
						TableName:   tableName,
						Masks:       table.ColumnMasks,
					})
				}
			}
			appendNamedMap(schema.Views, func(viewName string, item ViewSpec) {
				state.Views = append(state.Views, ViewResource{
					CatalogName: catalogName,
					SchemaName:  schemaName,
					ViewName:    viewName,
					Spec:        item,
				})
			})
			appendNamedMap(schema.Volumes, func(volumeName string, item VolumeSpec) {
				state.Volumes = append(state.Volumes, VolumeResource{
					CatalogName: catalogName,
					SchemaName:  schemaName,
					VolumeName:  volumeName,
					Spec:        item,
				})
			})
		}
	}

	for _, workspaceName := range sortedKeys(compiled.Workspaces) {
		workspace := compiled.Workspaces[workspaceName]
		state.Workspaces = append(state.Workspaces, WorkspaceResource{
			Name: workspaceName,
			Spec: workspace.WorkspaceSpec,
		})
		appendNamedMap(workspace.Dashboards, func(name string, item DashboardSpec) {
			state.Dashboards = append(state.Dashboards, DashboardResource{Name: name, Spec: item})
		})
		if err := normalizeWorkspaceFolders(state, workspaceName, "", workspace.Folders); err != nil {
			return nil, err
		}
	}

	for _, projectName := range sortedKeys(compiled.Projects) {
		project := compiled.Projects[projectName]
		state.Projects = append(state.Projects, ProjectResource{
			Name: projectName,
			Spec: project.ProjectSpec,
		})
		projectRef := projectRefKey(project.WorkspaceRef, projectName)
		appendNamedMap(project.Environments, func(name string, item EnvironmentSpec) {
			if strings.TrimSpace(item.ProjectRef) == "" {
				item.ProjectRef = projectRef
			}
			state.Environments = append(state.Environments, EnvironmentResource{Name: name, Spec: item})
		})
		appendNamedMap(project.Macros, func(name string, item MacroSpec) {
			if strings.TrimSpace(item.ProjectName) == "" {
				item.ProjectName = projectName
			}
			state.Macros = append(state.Macros, MacroResource{Name: name, Spec: item})
		})
		appendNamedMap(project.Models, func(name string, item ModelSpec) {
			state.Models = append(state.Models, ModelResource{ProjectName: projectName, ModelName: name, Spec: item})
		})
		appendNamedMap(project.SemanticModels, func(name string, item SemanticModelSpec) {
			state.SemanticModels = append(state.SemanticModels, SemanticModelResource{ModelName: name, Spec: item})
		})
	}

	appendNamedMap(compiled.Assets, func(name string, item AssetSpec) {
		state.Assets = append(state.Assets, AssetResource{Name: name, Spec: item})
	})

	return state, nil
}

func normalizeWorkspaceFolders(state *DesiredState, workspaceName, parentFolderRef string, folders map[string]cueFolder) error {
	for _, folderName := range sortedKeys(folders) {
		folder := folders[folderName]
		folderSpec := FolderSpec{
			WorkspaceRef:          workspaceName,
			ParentFolderRef:       parentFolderRef,
			DefaultProjectRef:     folder.DefaultProjectRef,
			DefaultEnvironmentRef: folder.DefaultEnvironmentRef,
			GitRepoID:             folder.GitRepoID,
			GitRootPath:           folder.GitRootPath,
		}
		state.Folders = append(state.Folders, FolderResource{Name: folderName, Spec: folderSpec})
		folderRef := folderRefKey(workspaceName, parentFolderRef, folderName)
		appendNamedMap(folder.Notebooks, func(name string, item NotebookSpec) {
			if strings.TrimSpace(item.WorkspaceRef) == "" {
				item.WorkspaceRef = workspaceName
			}
			if strings.TrimSpace(item.FolderRef) == "" {
				item.FolderRef = folderRef
			}
			state.Notebooks = append(state.Notebooks, NotebookResource{Name: name, Spec: item})
		})
		if err := normalizeWorkspaceFolders(state, workspaceName, folderRef, folder.Folders); err != nil {
			return err
		}
	}
	return nil
}

func appendNamedMap[T any](items map[string]T, appendFn func(string, T)) {
	for _, name := range sortedKeys(items) {
		appendFn(name, items[name])
	}
}

func sortedKeys[T any](items map[string]T) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
