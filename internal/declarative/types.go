package declarative

// Document is the generic envelope parsed first to determine Kind.
type Document struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
}

// ObjectMeta holds common metadata for named resources.
type ObjectMeta struct {
	Name               string `yaml:"name"`
	DeletionProtection bool   `yaml:"deletion_protection,omitempty"`
}

// === Security Resources ===

// PrincipalListDoc declares a set of principals (users or service principals).
type PrincipalListDoc struct {
	APIVersion string          `yaml:"apiVersion"`
	Kind       string          `yaml:"kind"`
	Principals []PrincipalSpec `yaml:"principals"`
}

// PrincipalSpec describes a single principal.
type PrincipalSpec struct {
	Name    string `yaml:"name"`
	Type    string `yaml:"type"` // "user" or "service_principal"
	IsAdmin bool   `yaml:"is_admin"`
}

// GroupListDoc declares a set of groups with optional membership.
type GroupListDoc struct {
	APIVersion string      `yaml:"apiVersion"`
	Kind       string      `yaml:"kind"`
	Groups     []GroupSpec `yaml:"groups"`
}

// GroupSpec describes a single group and its members.
type GroupSpec struct {
	Name        string      `yaml:"name"`
	Description string      `yaml:"description,omitempty"`
	Members     []MemberRef `yaml:"members,omitempty"`
}

// MemberRef is a reference to a user or nested group within a group.
type MemberRef struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`       // "user" or "group"
	MemberID string `yaml:"-" json:"-"` // populated from API during ReadState, not from YAML
}

// GrantListDoc declares a set of privilege grants.
type GrantListDoc struct {
	APIVersion string      `yaml:"apiVersion"`
	Kind       string      `yaml:"kind"`
	Grants     []GrantSpec `yaml:"grants"`
}

// GrantSpec describes a single privilege grant on a securable object.
type GrantSpec struct {
	Principal     string `yaml:"principal"`
	PrincipalType string `yaml:"principal_type"` // "user" or "group"
	SecurableType string `yaml:"securable_type"` // catalog, schema, table, external_location, storage_credential, volume
	Securable     string `yaml:"securable"`      // dot-path: "main.analytics.orders"
	Privilege     string `yaml:"privilege"`      // SELECT, INSERT, UPDATE, DELETE, USAGE, CREATE_TABLE, CREATE_SCHEMA, ALL_PRIVILEGES, etc.
}

// APIKeyListDoc declares a set of API keys.
type APIKeyListDoc struct {
	APIVersion string       `yaml:"apiVersion"`
	Kind       string       `yaml:"kind"`
	APIKeys    []APIKeySpec `yaml:"api_keys"`
}

// APIKeySpec describes a single API key bound to a principal.
type APIKeySpec struct {
	Name      string  `yaml:"name"`
	Principal string  `yaml:"principal"`
	ExpiresAt *string `yaml:"expires_at,omitempty"` // RFC3339
}

// === Catalog Resources ===

// CatalogDoc declares a catalog registration.
type CatalogDoc struct {
	APIVersion string      `yaml:"apiVersion"`
	Kind       string      `yaml:"kind"`
	Metadata   ObjectMeta  `yaml:"metadata"`
	Spec       CatalogSpec `yaml:"spec"`
}

// CatalogSpec holds the configuration for a catalog.
type CatalogSpec struct {
	MetastoreType string `yaml:"metastore_type"` // sqlite or postgres
	DSN           string `yaml:"dsn"`
	DataPath      string `yaml:"data_path"`
	IsDefault     bool   `yaml:"is_default,omitempty"`
	Comment       string `yaml:"comment,omitempty"`
}

// SchemaDoc declares a schema within a catalog.
type SchemaDoc struct {
	APIVersion string     `yaml:"apiVersion"`
	Kind       string     `yaml:"kind"`
	Metadata   ObjectMeta `yaml:"metadata"`
	Spec       SchemaSpec `yaml:"spec"`
}

// SchemaSpec holds the configuration for a schema.
type SchemaSpec struct {
	Comment      string            `yaml:"comment,omitempty"`
	Owner        string            `yaml:"owner,omitempty"`
	LocationName string            `yaml:"location_name,omitempty"`
	Properties   map[string]string `yaml:"properties,omitempty"`
}

// TableDoc declares a table within a schema.
type TableDoc struct {
	APIVersion string     `yaml:"apiVersion"`
	Kind       string     `yaml:"kind"`
	Metadata   ObjectMeta `yaml:"metadata"`
	Spec       TableSpec  `yaml:"spec"`
}

// TableSpec holds the configuration for a table.
type TableSpec struct {
	TableType    string            `yaml:"table_type,omitempty"` // MANAGED (default) or EXTERNAL
	Comment      string            `yaml:"comment,omitempty"`
	Owner        string            `yaml:"owner,omitempty"`
	Columns      []ColumnDef       `yaml:"columns,omitempty"`
	Properties   map[string]string `yaml:"properties,omitempty"`
	SourcePath   string            `yaml:"source_path,omitempty"`   // for EXTERNAL tables
	FileFormat   string            `yaml:"file_format,omitempty"`   // for EXTERNAL tables
	LocationName string            `yaml:"location_name,omitempty"` // for EXTERNAL tables
}

// ColumnDef describes a single column in a table definition.
type ColumnDef struct {
	Name    string `yaml:"name"`
	Type    string `yaml:"type"`
	Comment string `yaml:"comment,omitempty"`
}

// ViewDoc declares a view within a schema.
type ViewDoc struct {
	APIVersion string     `yaml:"apiVersion"`
	Kind       string     `yaml:"kind"`
	Metadata   ObjectMeta `yaml:"metadata"`
	Spec       ViewSpec   `yaml:"spec"`
}

// ViewSpec holds the configuration for a view.
type ViewSpec struct {
	ViewDefinition string            `yaml:"view_definition"`
	Comment        string            `yaml:"comment,omitempty"`
	Owner          string            `yaml:"owner,omitempty"`
	Properties     map[string]string `yaml:"properties,omitempty"`
}

// VolumeDoc declares a volume within a schema.
type VolumeDoc struct {
	APIVersion string     `yaml:"apiVersion"`
	Kind       string     `yaml:"kind"`
	Metadata   ObjectMeta `yaml:"metadata"`
	Spec       VolumeSpec `yaml:"spec"`
}

// VolumeSpec holds the configuration for a volume.
type VolumeSpec struct {
	VolumeType      string `yaml:"volume_type,omitempty"` // MANAGED or EXTERNAL
	StorageLocation string `yaml:"storage_location,omitempty"`
	Comment         string `yaml:"comment,omitempty"`
	Owner           string `yaml:"owner,omitempty"`
}

// === Security Policies ===

// RowFilterListDoc declares row-level security filters for a table.
type RowFilterListDoc struct {
	APIVersion string          `yaml:"apiVersion"`
	Kind       string          `yaml:"kind"`
	Filters    []RowFilterSpec `yaml:"filters"`
}

// RowFilterSpec describes a single row filter and its bindings.
type RowFilterSpec struct {
	Name        string             `yaml:"name"`
	FilterSQL   string             `yaml:"filter_sql"`
	Description string             `yaml:"description,omitempty"`
	Bindings    []FilterBindingRef `yaml:"bindings,omitempty"`
}

// FilterBindingRef binds a row filter to a principal.
type FilterBindingRef struct {
	Principal     string `yaml:"principal"`
	PrincipalType string `yaml:"principal_type"` // user or group
}

// ColumnMaskListDoc declares column masking rules for a table.
type ColumnMaskListDoc struct {
	APIVersion string           `yaml:"apiVersion"`
	Kind       string           `yaml:"kind"`
	Masks      []ColumnMaskSpec `yaml:"masks"`
}

// ColumnMaskSpec describes a single column mask and its bindings.
type ColumnMaskSpec struct {
	Name           string           `yaml:"name"`
	ColumnName     string           `yaml:"column_name"`
	MaskExpression string           `yaml:"mask_expression"`
	Description    string           `yaml:"description,omitempty"`
	Bindings       []MaskBindingRef `yaml:"bindings,omitempty"`
}

// MaskBindingRef binds a column mask to a principal with visibility control.
type MaskBindingRef struct {
	Principal     string `yaml:"principal"`
	PrincipalType string `yaml:"principal_type"` // user or group
	SeeOriginal   bool   `yaml:"see_original,omitempty"`
}

// === Governance ===

// TagConfigDoc declares tags and their assignments to securables.
type TagConfigDoc struct {
	APIVersion  string              `yaml:"apiVersion"`
	Kind        string              `yaml:"kind"`
	Tags        []TagSpec           `yaml:"tags"`
	Assignments []TagAssignmentSpec `yaml:"assignments,omitempty"`
}

// TagSpec describes a tag key with an optional value.
type TagSpec struct {
	Key   string  `yaml:"key"`
	Value *string `yaml:"value,omitempty"` // nil for key-only tags
}

// TagAssignmentSpec assigns a tag to a securable object.
type TagAssignmentSpec struct {
	Tag           string `yaml:"tag"`            // "key:value" format
	SecurableType string `yaml:"securable_type"` // schema, table, column
	Securable     string `yaml:"securable"`      // dot-path
	ColumnName    string `yaml:"column_name,omitempty"`
}

// === Storage ===

// StorageCredentialListDoc declares storage credentials for external data access.
type StorageCredentialListDoc struct {
	APIVersion  string                  `yaml:"apiVersion"`
	Kind        string                  `yaml:"kind"`
	Credentials []StorageCredentialSpec `yaml:"credentials"`
}

// StorageCredentialSpec describes a single storage credential.
type StorageCredentialSpec struct {
	Name           string               `yaml:"name"`
	CredentialType string               `yaml:"credential_type"` // S3, AZURE, GCS
	Comment        string               `yaml:"comment,omitempty"`
	S3             *S3CredentialSpec    `yaml:"s3,omitempty"`
	Azure          *AzureCredentialSpec `yaml:"azure,omitempty"`
	GCS            *GCSCredentialSpec   `yaml:"gcs,omitempty"`
}

// S3CredentialSpec holds S3-compatible credential references.
type S3CredentialSpec struct {
	KeyIDFromEnv  string `yaml:"key_id_from_env"`
	SecretFromEnv string `yaml:"secret_from_env"`
	Endpoint      string `yaml:"endpoint,omitempty"`
	Region        string `yaml:"region,omitempty"`
	URLStyle      string `yaml:"url_style,omitempty"`
}

// AzureCredentialSpec holds Azure credential references.
type AzureCredentialSpec struct {
	AccountNameFromEnv  string `yaml:"account_name_from_env"`
	AccountKeyFromEnv   string `yaml:"account_key_from_env,omitempty"`
	ClientIDFromEnv     string `yaml:"client_id_from_env,omitempty"`
	ClientSecretFromEnv string `yaml:"client_secret_from_env,omitempty"`
	TenantID            string `yaml:"tenant_id,omitempty"`
}

// GCSCredentialSpec holds GCS credential references.
type GCSCredentialSpec struct {
	KeyFilePath string `yaml:"key_file_path,omitempty"`
}

// ExternalLocationListDoc declares external storage locations.
type ExternalLocationListDoc struct {
	APIVersion string                 `yaml:"apiVersion"`
	Kind       string                 `yaml:"kind"`
	Locations  []ExternalLocationSpec `yaml:"locations"`
}

// ExternalLocationSpec describes a single external storage location.
type ExternalLocationSpec struct {
	Name           string `yaml:"name"`
	URL            string `yaml:"url"`
	CredentialName string `yaml:"credential_name"`
	StorageType    string `yaml:"storage_type,omitempty"` // S3, AZURE, GCS
	Comment        string `yaml:"comment,omitempty"`
	ReadOnly       bool   `yaml:"read_only,omitempty"`
}

// === Compute ===

// ComputeEndpointListDoc declares compute endpoints.
type ComputeEndpointListDoc struct {
	APIVersion string                `yaml:"apiVersion"`
	Kind       string                `yaml:"kind"`
	Endpoints  []ComputeEndpointSpec `yaml:"endpoints"`
}

// ComputeEndpointSpec describes a single compute endpoint.
type ComputeEndpointSpec struct {
	Name             string `yaml:"name"`
	URL              string `yaml:"url,omitempty"`
	Type             string `yaml:"type"` // LOCAL or REMOTE
	Size             string `yaml:"size,omitempty"`
	MaxMemoryGB      *int   `yaml:"max_memory_gb,omitempty"`
	AuthTokenFromEnv string `yaml:"auth_token_from_env,omitempty"`
}

// ComputeAssignmentListDoc declares compute endpoint assignments to principals.
type ComputeAssignmentListDoc struct {
	APIVersion  string                  `yaml:"apiVersion"`
	Kind        string                  `yaml:"kind"`
	Assignments []ComputeAssignmentSpec `yaml:"assignments"`
}

// ComputeAssignmentSpec assigns a compute endpoint to a principal.
type ComputeAssignmentSpec struct {
	Endpoint      string `yaml:"endpoint"`
	Principal     string `yaml:"principal"`
	PrincipalType string `yaml:"principal_type"` // user or group
	IsDefault     bool   `yaml:"is_default,omitempty"`
	FallbackLocal bool   `yaml:"fallback_local,omitempty"`
}

// === Workflows ===

// NotebookDoc declares a notebook with SQL or markdown cells.
type NotebookDoc struct {
	APIVersion string       `yaml:"apiVersion"`
	Kind       string       `yaml:"kind"`
	Metadata   ObjectMeta   `yaml:"metadata"`
	Spec       NotebookSpec `yaml:"spec"`
}

// NotebookSpec holds the configuration for a notebook.
type NotebookSpec struct {
	Description string     `yaml:"description,omitempty"`
	Owner       string     `yaml:"owner,omitempty"`
	Cells       []CellSpec `yaml:"cells,omitempty"`
}

// CellSpec describes a single cell in a notebook.
type CellSpec struct {
	Type    string `yaml:"type"` // sql or markdown
	Content string `yaml:"content"`
}

// PipelineDoc declares a pipeline of notebook jobs.
type PipelineDoc struct {
	APIVersion string       `yaml:"apiVersion"`
	Kind       string       `yaml:"kind"`
	Metadata   ObjectMeta   `yaml:"metadata"`
	Spec       PipelineSpec `yaml:"spec"`
}

// PipelineSpec holds the configuration for a pipeline.
type PipelineSpec struct {
	Description      string            `yaml:"description,omitempty"`
	ScheduleCron     string            `yaml:"schedule_cron,omitempty"`
	IsPaused         bool              `yaml:"is_paused,omitempty"`
	ConcurrencyLimit *int              `yaml:"concurrency_limit,omitempty"`
	Jobs             []PipelineJobSpec `yaml:"jobs,omitempty"`
}

// PipelineJobSpec describes a single job within a pipeline.
type PipelineJobSpec struct {
	Name            string   `yaml:"name"`
	Notebook        string   `yaml:"notebook"`
	ComputeEndpoint string   `yaml:"compute_endpoint,omitempty"`
	DependsOn       []string `yaml:"depends_on,omitempty"`
	TimeoutSeconds  *int     `yaml:"timeout_seconds,omitempty"`
	RetryCount      *int     `yaml:"retry_count,omitempty"`
	Order           *int     `yaml:"order,omitempty"`
}

// === State Containers ===

// DesiredState is the fully-parsed representation of all YAML files.
type DesiredState struct {
	Catalogs           []CatalogResource
	Schemas            []SchemaResource
	Tables             []TableResource
	Views              []ViewResource
	Volumes            []VolumeResource
	Principals         []PrincipalSpec
	Groups             []GroupSpec
	Grants             []GrantSpec
	RowFilters         []RowFilterResource
	ColumnMasks        []ColumnMaskResource
	Tags               []TagSpec
	TagAssignments     []TagAssignmentSpec
	StorageCredentials []StorageCredentialSpec
	ExternalLocations  []ExternalLocationSpec
	ComputeEndpoints   []ComputeEndpointSpec
	ComputeAssignments []ComputeAssignmentSpec
	APIKeys            []APIKeySpec
	Notebooks          []NotebookResource
	Pipelines          []PipelineResource
	Models             []ModelResource
	Macros             []MacroResource
}

// CatalogResource is a catalog with positional context from the directory tree.
type CatalogResource struct {
	CatalogName        string
	DeletionProtection bool
	Spec               CatalogSpec
}

// SchemaResource is a schema with catalog context from the directory tree.
type SchemaResource struct {
	CatalogName        string
	SchemaName         string
	DeletionProtection bool
	Spec               SchemaSpec
}

// TableResource is a table with catalog and schema context from the directory tree.
type TableResource struct {
	CatalogName        string
	SchemaName         string
	TableName          string
	DeletionProtection bool
	Spec               TableSpec
}

// ViewResource is a view with catalog and schema context from the directory tree.
type ViewResource struct {
	CatalogName string
	SchemaName  string
	ViewName    string
	Spec        ViewSpec
}

// VolumeResource is a volume with catalog and schema context from the directory tree.
type VolumeResource struct {
	CatalogName string
	SchemaName  string
	VolumeName  string
	Spec        VolumeSpec
}

// RowFilterResource is a set of row filters scoped to a specific table.
type RowFilterResource struct {
	CatalogName string
	SchemaName  string
	TableName   string
	Filters     []RowFilterSpec
}

// ColumnMaskResource is a set of column masks scoped to a specific table.
type ColumnMaskResource struct {
	CatalogName string
	SchemaName  string
	TableName   string
	Masks       []ColumnMaskSpec
}

// NotebookResource is a notebook with its resolved name.
type NotebookResource struct {
	Name string
	Spec NotebookSpec
}

// PipelineResource is a pipeline with its resolved name.
type PipelineResource struct {
	Name string
	Spec PipelineSpec
}

// === SQL Macros ===

// MacroDoc declares a SQL macro.
type MacroDoc struct {
	APIVersion string     `yaml:"apiVersion"`
	Kind       string     `yaml:"kind"`
	Metadata   ObjectMeta `yaml:"metadata"`
	Spec       MacroSpec  `yaml:"spec"`
}

// MacroSpec holds the configuration for a SQL macro.
type MacroSpec struct {
	MacroType   string            `yaml:"macro_type,omitempty"` // SCALAR or TABLE
	Parameters  []string          `yaml:"parameters,omitempty"`
	Body        string            `yaml:"body"`
	Description string            `yaml:"description,omitempty"`
	CatalogName string            `yaml:"catalog_name,omitempty"`
	ProjectName string            `yaml:"project_name,omitempty"`
	Visibility  string            `yaml:"visibility,omitempty"`
	Owner       string            `yaml:"owner,omitempty"`
	Properties  map[string]string `yaml:"properties,omitempty"`
	Tags        []string          `yaml:"tags,omitempty"`
	Status      string            `yaml:"status,omitempty"`
}

// MacroResource is a macro with its resolved name.
type MacroResource struct {
	Name string
	Spec MacroSpec
}

// === Transformation Models ===

// ModelDoc declares a transformation model.
type ModelDoc struct {
	APIVersion string     `yaml:"apiVersion"`
	Kind       string     `yaml:"kind"`
	Metadata   ObjectMeta `yaml:"metadata"`
	Spec       ModelSpec  `yaml:"spec"`
}

// ModelSpec holds the configuration for a transformation model.
type ModelSpec struct {
	Materialization string             `yaml:"materialization"`
	Description     string             `yaml:"description,omitempty"`
	Tags            []string           `yaml:"tags,omitempty"`
	SQL             string             `yaml:"sql"`
	Config          *ModelConfigSpec   `yaml:"config,omitempty"`
	Contract        *ContractSpec      `yaml:"contract,omitempty"`
	Tests           []TestSpec         `yaml:"tests,omitempty"`
	Freshness       *FreshnessSpecYAML `yaml:"freshness,omitempty"`
}

// FreshnessSpecYAML defines freshness policy in YAML.
type FreshnessSpecYAML struct {
	MaxLagSeconds int64  `yaml:"max_lag_seconds,omitempty"`
	CronSchedule  string `yaml:"cron_schedule,omitempty"`
}

// ContractSpec defines enforced output column types for a model.
type ContractSpec struct {
	Enforce bool                 `yaml:"enforce"`
	Columns []ContractColumnSpec `yaml:"columns,omitempty"`
}

// ContractColumnSpec defines an expected column in a model contract.
type ContractColumnSpec struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`
	Nullable bool   `yaml:"nullable"`
}

// TestSpec describes a test assertion for a model.
type TestSpec struct {
	Name     string   `yaml:"name"`
	Type     string   `yaml:"type"`
	Column   string   `yaml:"column,omitempty"`
	Values   []string `yaml:"values,omitempty"`
	ToModel  string   `yaml:"to_model,omitempty"`
	ToColumn string   `yaml:"to_column,omitempty"`
	SQL      string   `yaml:"sql,omitempty"`
}

// ModelConfigSpec holds optional model configuration options.
type ModelConfigSpec struct {
	UniqueKey           []string `yaml:"unique_key,omitempty"`
	IncrementalStrategy string   `yaml:"incremental_strategy,omitempty"`
	OnSchemaChange      string   `yaml:"on_schema_change,omitempty"`
}

// ModelResource is a model with project context from the directory tree.
type ModelResource struct {
	ProjectName string
	ModelName   string
	Spec        ModelSpec
}

// ActualState mirrors DesiredState but populated from the server.
type ActualState = DesiredState
