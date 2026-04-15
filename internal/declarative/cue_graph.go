package declarative

// cuePlatform is the CUE-native declarative graph rooted at `platform`.
type cuePlatform struct {
	Security     cueSecuritySection         `yaml:"security,omitempty"`
	Governance   cueGovernanceSection       `yaml:"governance,omitempty"`
	Storage      cueStorageSection          `yaml:"storage,omitempty"`
	Compute      cueComputeSection          `yaml:"compute,omitempty"`
	Catalogs     map[string]cueCatalog      `yaml:"catalogs,omitempty"`
	Domains      map[string]DomainSpec      `yaml:"domains,omitempty"`
	Teams        map[string]TeamSpec        `yaml:"teams,omitempty"`
	DataProducts map[string]DataProductSpec `yaml:"data_products,omitempty"`
	Workspaces   map[string]cueWorkspace    `yaml:"workspaces,omitempty"`
	Projects     map[string]cueProject      `yaml:"projects,omitempty"`
	Assets       map[string]AssetSpec       `yaml:"assets,omitempty"`
}

type cueSecuritySection struct {
	Principals       map[string]PrincipalSpec       `yaml:"principals,omitempty"`
	Groups           map[string]GroupSpec           `yaml:"groups,omitempty"`
	Grants           []GrantSpec                    `yaml:"grants,omitempty"`
	PrivilegePresets map[string]PrivilegePresetSpec `yaml:"privilege_presets,omitempty"`
	Bindings         []BindingSpec                  `yaml:"bindings,omitempty"`
	APIKeys          map[string]APIKeySpec          `yaml:"api_keys,omitempty"`
}

type cueGovernanceSection struct {
	Tags        map[string][]string `yaml:"tags,omitempty"`
	Assignments []TagAssignmentSpec `yaml:"assignments,omitempty"`
}

type cueStorageSection struct {
	Credentials       map[string]StorageCredentialSpec `yaml:"credentials,omitempty"`
	ExternalLocations map[string]ExternalLocationSpec  `yaml:"external_locations,omitempty"`
}

type cueComputeSection struct {
	Endpoints   map[string]ComputeEndpointSpec `yaml:"endpoints,omitempty"`
	Assignments []ComputeAssignmentSpec        `yaml:"assignments,omitempty"`
	Defaults    *ComputeRoutingDefaultsSpec    `yaml:"defaults,omitempty"`
}

type cueCatalog struct {
	DeletionProtection bool `yaml:"deletion_protection,omitempty"`
	CatalogSpec        `yaml:",inline"`
	Schemas            map[string]cueSchema `yaml:"schemas,omitempty"`
}

type cueSchema struct {
	DeletionProtection bool `yaml:"deletion_protection,omitempty"`
	SchemaSpec         `yaml:",inline"`
	Tables             map[string]cueTable   `yaml:"tables,omitempty"`
	Views              map[string]ViewSpec   `yaml:"views,omitempty"`
	Volumes            map[string]VolumeSpec `yaml:"volumes,omitempty"`
}

type cueTable struct {
	DeletionProtection bool `yaml:"deletion_protection,omitempty"`
	TableSpec          `yaml:",inline"`
	RowFilters         []RowFilterSpec  `yaml:"row_filters,omitempty"`
	ColumnMasks        []ColumnMaskSpec `yaml:"column_masks,omitempty"`
}

type cueWorkspace struct {
	WorkspaceSpec `yaml:",inline"`
	Folders        map[string]cueFolder        `yaml:"folders,omitempty"`
	Dashboards     map[string]DashboardSpec    `yaml:"dashboards,omitempty"`
	SemanticModels map[string]SemanticModelSpec `yaml:"semantic_models,omitempty"`
}

type cueFolder struct {
	DefaultProjectRef     string                  `yaml:"default_project_ref,omitempty"`
	DefaultEnvironmentRef string                  `yaml:"default_environment_ref,omitempty"`
	GitRepoID             string                  `yaml:"git_repo_id,omitempty"`
	GitRootPath           string                  `yaml:"git_root_path,omitempty"`
	Notebooks             map[string]NotebookSpec `yaml:"notebooks,omitempty"`
	Folders               map[string]cueFolder    `yaml:"folders,omitempty"`
}

type cueProject struct {
	ProjectSpec  `yaml:",inline"`
	Environments map[string]EnvironmentSpec `yaml:"environments,omitempty"`
	Macros       map[string]MacroSpec       `yaml:"macros,omitempty"`
	Models       map[string]ModelSpec       `yaml:"models,omitempty"`
}
