package domain

import (
	"context"
	"time"
)

// PrincipalRepository provides CRUD operations for principals.
type PrincipalRepository interface {
	Create(ctx context.Context, p *Principal) (*Principal, error)
	GetByID(ctx context.Context, id string) (*Principal, error)
	GetByName(ctx context.Context, name string) (*Principal, error)
	GetByExternalID(ctx context.Context, issuer, externalID string) (*Principal, error)
	List(ctx context.Context, page PageRequest) ([]Principal, int64, error)
	Delete(ctx context.Context, id string) error
	SetAdmin(ctx context.Context, id string, isAdmin bool) error
	BindExternalID(ctx context.Context, id string, externalID string, externalIssuer string) error
}

// GroupRepository provides CRUD operations for groups and membership.
type GroupRepository interface {
	Create(ctx context.Context, g *Group) (*Group, error)
	GetByID(ctx context.Context, id string) (*Group, error)
	GetByName(ctx context.Context, name string) (*Group, error)
	List(ctx context.Context, page PageRequest) ([]Group, int64, error)
	Delete(ctx context.Context, id string) error
	AddMember(ctx context.Context, m *GroupMember) error
	RemoveMember(ctx context.Context, m *GroupMember) error
	ListMembers(ctx context.Context, groupID string, page PageRequest) ([]GroupMember, int64, error)
	GetGroupsForMember(ctx context.Context, memberType string, memberID string) ([]Group, error)
}

// GrantRepository provides operations for privilege grants.
type GrantRepository interface {
	Grant(ctx context.Context, g *PrivilegeGrant) (*PrivilegeGrant, error)
	Revoke(ctx context.Context, g *PrivilegeGrant) error
	RevokeByID(ctx context.Context, id string) error
	ListAll(ctx context.Context, page PageRequest) ([]PrivilegeGrant, int64, error)
	ListForPrincipal(ctx context.Context, principalID string, principalType string, page PageRequest) ([]PrivilegeGrant, int64, error)
	ListForSecurable(ctx context.Context, securableType string, securableID string, page PageRequest) ([]PrivilegeGrant, int64, error)
	HasPrivilege(ctx context.Context, principalID string, principalType, securableType string, securableID string, privilege string) (bool, error)
}

// RowFilterRepository provides CRUD operations for row filters and bindings.
type RowFilterRepository interface {
	Create(ctx context.Context, f *RowFilter) (*RowFilter, error)
	GetForTable(ctx context.Context, tableID string, page PageRequest) ([]RowFilter, int64, error)
	Delete(ctx context.Context, id string) error
	Bind(ctx context.Context, b *RowFilterBinding) error
	Unbind(ctx context.Context, b *RowFilterBinding) error
	ListBindings(ctx context.Context, filterID string) ([]RowFilterBinding, error)
	GetForTableAndPrincipal(ctx context.Context, tableID, principalID string, principalType string) ([]RowFilter, error)
}

// ColumnMaskRepository provides CRUD operations for column masks and bindings.
type ColumnMaskRepository interface {
	Create(ctx context.Context, m *ColumnMask) (*ColumnMask, error)
	GetForTable(ctx context.Context, tableID string, page PageRequest) ([]ColumnMask, int64, error)
	Delete(ctx context.Context, id string) error
	Bind(ctx context.Context, b *ColumnMaskBinding) error
	Unbind(ctx context.Context, b *ColumnMaskBinding) error
	ListBindings(ctx context.Context, maskID string) ([]ColumnMaskBinding, error)
	GetForTableAndPrincipal(ctx context.Context, tableID, principalID string, principalType string) ([]ColumnMaskWithBinding, error)
}

// APIKeyRepository provides CRUD operations for API keys.
type APIKeyRepository interface {
	Create(ctx context.Context, key *APIKey) error
	GetByID(ctx context.Context, id string) (*APIKey, error)
	GetByHash(ctx context.Context, hash string) (*APIKey, *Principal, error)
	ListByPrincipal(ctx context.Context, principalID string, page PageRequest) ([]APIKey, int64, error)
	ListAll(ctx context.Context, page PageRequest) ([]APIKey, int64, error)
	Delete(ctx context.Context, id string) error
	DeleteExpired(ctx context.Context) (int64, error)
}

// AuditFilter holds filter parameters for querying audit logs.
type AuditFilter struct {
	PrincipalName *string
	Action        *string
	Status        *string
	Since         *time.Time
	Page          PageRequest
}

// AuditRepository provides operations for audit log entries.
type AuditRepository interface {
	Insert(ctx context.Context, e *AuditEntry) error
	List(ctx context.Context, filter AuditFilter) ([]AuditEntry, int64, error)
}

// IntrospectionRepository provides read-only access to DuckLake metadata.
type IntrospectionRepository interface {
	ListSchemas(ctx context.Context, page PageRequest) ([]Schema, int64, error)
	ListTables(ctx context.Context, schemaID string, page PageRequest) ([]Table, int64, error)
	GetTable(ctx context.Context, tableID string) (*Table, error)
	ListColumns(ctx context.Context, tableID string, page PageRequest) ([]Column, int64, error)
	GetTableByName(ctx context.Context, tableName string) (*Table, error)
	GetSchemaByName(ctx context.Context, schemaName string) (*Schema, error)
}

// CatalogRepository provides catalog management operations via DuckLake.
type CatalogRepository interface {
	GetCatalogInfo(ctx context.Context) (*CatalogInfo, error)
	GetMetastoreSummary(ctx context.Context) (*MetastoreSummary, error)

	CreateSchema(ctx context.Context, name, comment, owner string) (*SchemaDetail, error)
	GetSchema(ctx context.Context, name string) (*SchemaDetail, error)
	ListSchemas(ctx context.Context, page PageRequest) ([]SchemaDetail, int64, error)
	UpdateSchema(ctx context.Context, name string, comment *string, props map[string]string) (*SchemaDetail, error)
	DeleteSchema(ctx context.Context, name string, force bool) error

	CreateTable(ctx context.Context, schemaName string, req CreateTableRequest, owner string) (*TableDetail, error)
	CreateExternalTable(ctx context.Context, schemaName string, req CreateTableRequest, owner string) (*TableDetail, error)
	GetTable(ctx context.Context, schemaName, tableName string) (*TableDetail, error)
	ListTables(ctx context.Context, schemaName string, page PageRequest) ([]TableDetail, int64, error)
	DeleteTable(ctx context.Context, schemaName, tableName string) error
	UpdateTable(ctx context.Context, schemaName, tableName string, comment *string, props map[string]string, owner *string) (*TableDetail, error)
	UpdateCatalog(ctx context.Context, comment *string) (*CatalogInfo, error)
	UpdateColumn(ctx context.Context, schemaName, tableName, columnName string, comment *string, props map[string]string) (*ColumnDetail, error)
	ListColumns(ctx context.Context, schemaName, tableName string, page PageRequest) ([]ColumnDetail, int64, error)
	SetSchemaStoragePath(ctx context.Context, schemaID string, path string) error
}

// QueryHistoryRepository provides query history operations.
type QueryHistoryRepository interface {
	List(ctx context.Context, filter QueryHistoryFilter) ([]QueryHistoryEntry, int64, error)
}

// LineageRepository provides operations for lineage edges.
type LineageRepository interface {
	InsertEdge(ctx context.Context, edge *LineageEdge) error
	GetUpstream(ctx context.Context, tableName string, page PageRequest) ([]LineageEdge, int64, error)
	GetDownstream(ctx context.Context, tableName string, page PageRequest) ([]LineageEdge, int64, error)
	DeleteEdge(ctx context.Context, id string) error
	PurgeOlderThan(ctx context.Context, before time.Time) (int64, error)
}

// ColumnLineageRepository provides operations for column-level lineage edges.
type ColumnLineageRepository interface {
	InsertBatch(ctx context.Context, edgeID string, edges []ColumnLineageEdge) error
	GetByEdgeID(ctx context.Context, edgeID string) ([]ColumnLineageEdge, error)
	GetForTable(ctx context.Context, schema, table string) ([]ColumnLineageEdge, error)
	GetForSourceColumn(ctx context.Context, schema, table, column string) ([]ColumnLineageEdge, error)
	DeleteByEdgeID(ctx context.Context, edgeID string) error
}

// TableStatisticsRepository provides operations for table statistics.
type TableStatisticsRepository interface {
	Upsert(ctx context.Context, securableName string, stats *TableStatistics) error
	Get(ctx context.Context, securableName string) (*TableStatistics, error)
	Delete(ctx context.Context, securableName string) error
}

// SearchRepository provides catalog search operations.
type SearchRepository interface {
	Search(ctx context.Context, query string, objectType *string, maxResults int, offset int) ([]SearchResult, int64, error)
}

// TagRepository provides CRUD operations for tags and assignments.
type TagRepository interface {
	CreateTag(ctx context.Context, tag *Tag) (*Tag, error)
	GetTag(ctx context.Context, id string) (*Tag, error)
	ListTags(ctx context.Context, page PageRequest) ([]Tag, int64, error)
	DeleteTag(ctx context.Context, id string) error
	AssignTag(ctx context.Context, assignment *TagAssignment) (*TagAssignment, error)
	UnassignTag(ctx context.Context, id string) error
	ListTagsForSecurable(ctx context.Context, securableType string, securableID string, columnName *string) ([]Tag, error)
	ListAssignmentsForTag(ctx context.Context, tagID string) ([]TagAssignment, error)
}

// ViewRepository provides CRUD operations for views.
type ViewRepository interface {
	Create(ctx context.Context, view *ViewDetail) (*ViewDetail, error)
	GetByName(ctx context.Context, schemaID string, viewName string) (*ViewDetail, error)
	List(ctx context.Context, schemaID string, page PageRequest) ([]ViewDetail, int64, error)
	Delete(ctx context.Context, schemaID string, viewName string) error
	Update(ctx context.Context, schemaID string, viewName string, comment *string, props map[string]string, viewDef *string) (*ViewDetail, error)
}

// StorageCredentialRepository provides CRUD operations for storage credentials.
type StorageCredentialRepository interface {
	Create(ctx context.Context, cred *StorageCredential) (*StorageCredential, error)
	GetByID(ctx context.Context, id string) (*StorageCredential, error)
	GetByName(ctx context.Context, name string) (*StorageCredential, error)
	List(ctx context.Context, page PageRequest) ([]StorageCredential, int64, error)
	Update(ctx context.Context, id string, req UpdateStorageCredentialRequest) (*StorageCredential, error)
	Delete(ctx context.Context, id string) error
}

// ExternalLocationRepository provides CRUD operations for external locations.
type ExternalLocationRepository interface {
	Create(ctx context.Context, loc *ExternalLocation) (*ExternalLocation, error)
	GetByID(ctx context.Context, id string) (*ExternalLocation, error)
	GetByName(ctx context.Context, name string) (*ExternalLocation, error)
	List(ctx context.Context, page PageRequest) ([]ExternalLocation, int64, error)
	Update(ctx context.Context, id string, req UpdateExternalLocationRequest) (*ExternalLocation, error)
	Delete(ctx context.Context, id string) error
}

// VolumeRepository provides CRUD operations for volumes.
type VolumeRepository interface {
	Create(ctx context.Context, vol *Volume) (*Volume, error)
	GetByName(ctx context.Context, schemaName, name string) (*Volume, error)
	List(ctx context.Context, schemaName string, page PageRequest) ([]Volume, int64, error)
	Update(ctx context.Context, id string, req UpdateVolumeRequest) (*Volume, error)
	Delete(ctx context.Context, id string) error
}

// ExternalTableRepository provides CRUD operations for external tables.
type ExternalTableRepository interface {
	Create(ctx context.Context, et *ExternalTableRecord) (*ExternalTableRecord, error)
	GetByName(ctx context.Context, schemaName, tableName string) (*ExternalTableRecord, error)
	GetByID(ctx context.Context, id string) (*ExternalTableRecord, error)
	GetByTableName(ctx context.Context, tableName string) (*ExternalTableRecord, error)
	List(ctx context.Context, schemaName string, page PageRequest) ([]ExternalTableRecord, int64, error)
	ListAll(ctx context.Context) ([]ExternalTableRecord, error)
	Delete(ctx context.Context, schemaName, tableName string) error
	DeleteBySchema(ctx context.Context, schemaName string) error
}

// CatalogRegistrationRepository provides CRUD operations for catalog registrations.
type CatalogRegistrationRepository interface {
	Create(ctx context.Context, reg *CatalogRegistration) (*CatalogRegistration, error)
	GetByID(ctx context.Context, id string) (*CatalogRegistration, error)
	GetByName(ctx context.Context, name string) (*CatalogRegistration, error)
	List(ctx context.Context, page PageRequest) ([]CatalogRegistration, int64, error)
	Update(ctx context.Context, id string, req UpdateCatalogRegistrationRequest) (*CatalogRegistration, error)
	Delete(ctx context.Context, id string) error
	UpdateStatus(ctx context.Context, id string, status CatalogStatus, message string) error
	GetDefault(ctx context.Context) (*CatalogRegistration, error)
	SetDefault(ctx context.Context, id string) error
}

// ComputeEndpointRepository provides CRUD operations for compute endpoints and assignments.
type ComputeEndpointRepository interface {
	Create(ctx context.Context, ep *ComputeEndpoint) (*ComputeEndpoint, error)
	GetByID(ctx context.Context, id string) (*ComputeEndpoint, error)
	GetByName(ctx context.Context, name string) (*ComputeEndpoint, error)
	List(ctx context.Context, page PageRequest) ([]ComputeEndpoint, int64, error)
	Update(ctx context.Context, id string, req UpdateComputeEndpointRequest) (*ComputeEndpoint, error)
	Delete(ctx context.Context, id string) error
	UpdateStatus(ctx context.Context, id string, status string) error
	Assign(ctx context.Context, a *ComputeAssignment) (*ComputeAssignment, error)
	Unassign(ctx context.Context, id string) error
	ListAssignments(ctx context.Context, endpointID string, page PageRequest) ([]ComputeAssignment, int64, error)
	GetDefaultForPrincipal(ctx context.Context, principalID string, principalType string) (*ComputeEndpoint, error)
	GetAssignmentsForPrincipal(ctx context.Context, principalID string, principalType string) ([]ComputeEndpoint, error)
}

// NotebookRepository provides CRUD operations for notebooks and cells.
type NotebookRepository interface {
	CreateNotebook(ctx context.Context, nb *Notebook) (*Notebook, error)
	GetNotebook(ctx context.Context, id string) (*Notebook, error)
	ListNotebooks(ctx context.Context, owner *string, page PageRequest) ([]Notebook, int64, error)
	UpdateNotebook(ctx context.Context, id string, req UpdateNotebookRequest) (*Notebook, error)
	DeleteNotebook(ctx context.Context, id string) error

	CreateCell(ctx context.Context, cell *Cell) (*Cell, error)
	GetCell(ctx context.Context, id string) (*Cell, error)
	ListCells(ctx context.Context, notebookID string) ([]Cell, error)
	UpdateCell(ctx context.Context, id string, req UpdateCellRequest) (*Cell, error)
	DeleteCell(ctx context.Context, id string) error
	UpdateCellResult(ctx context.Context, cellID string, result *string) error
	ReorderCells(ctx context.Context, notebookID string, cellIDs []string) error
	GetMaxPosition(ctx context.Context, notebookID string) (int, error)
}

// NotebookJobRepository provides CRUD operations for async notebook jobs.
type NotebookJobRepository interface {
	CreateJob(ctx context.Context, job *NotebookJob) (*NotebookJob, error)
	GetJob(ctx context.Context, id string) (*NotebookJob, error)
	ListJobs(ctx context.Context, notebookID string, page PageRequest) ([]NotebookJob, int64, error)
	UpdateJobState(ctx context.Context, id string, state JobState, result *string, errMsg *string) error
}

// GitRepoRepository provides CRUD operations for registered Git repositories.
type GitRepoRepository interface {
	Create(ctx context.Context, repo *GitRepo) (*GitRepo, error)
	GetByID(ctx context.Context, id string) (*GitRepo, error)
	List(ctx context.Context, page PageRequest) ([]GitRepo, int64, error)
	Delete(ctx context.Context, id string) error
	UpdateSyncStatus(ctx context.Context, id string, commitSHA string, syncedAt time.Time) error
}

// PipelineRepository provides CRUD operations for pipelines and jobs.
type PipelineRepository interface {
	CreatePipeline(ctx context.Context, p *Pipeline) (*Pipeline, error)
	GetPipelineByID(ctx context.Context, id string) (*Pipeline, error)
	GetPipelineByName(ctx context.Context, name string) (*Pipeline, error)
	ListPipelines(ctx context.Context, page PageRequest) ([]Pipeline, int64, error)
	UpdatePipeline(ctx context.Context, id string, req UpdatePipelineRequest) (*Pipeline, error)
	DeletePipeline(ctx context.Context, id string) error
	ListScheduledPipelines(ctx context.Context) ([]Pipeline, error)
	CreateJob(ctx context.Context, job *PipelineJob) (*PipelineJob, error)
	GetJobByID(ctx context.Context, id string) (*PipelineJob, error)
	ListJobsByPipeline(ctx context.Context, pipelineID string) ([]PipelineJob, error)
	DeleteJob(ctx context.Context, id string) error
	DeleteJobsByPipeline(ctx context.Context, pipelineID string) error
}

// PipelineRunRepository provides CRUD operations for pipeline runs and job runs.
type PipelineRunRepository interface {
	CreateRun(ctx context.Context, run *PipelineRun) (*PipelineRun, error)
	GetRunByID(ctx context.Context, id string) (*PipelineRun, error)
	ListRuns(ctx context.Context, filter PipelineRunFilter) ([]PipelineRun, int64, error)
	UpdateRunStatus(ctx context.Context, id string, status string, errorMsg *string) error
	UpdateRunStarted(ctx context.Context, id string) error
	UpdateRunFinished(ctx context.Context, id string, status string, errorMsg *string) error
	CountActiveRuns(ctx context.Context, pipelineID string) (int64, error)
	CancelPendingRuns(ctx context.Context, pipelineID string) (int64, error)
	CreateJobRun(ctx context.Context, jr *PipelineJobRun) (*PipelineJobRun, error)
	GetJobRunByID(ctx context.Context, id string) (*PipelineJobRun, error)
	ListJobRunsByRun(ctx context.Context, runID string) ([]PipelineJobRun, error)
	UpdateJobRunStatus(ctx context.Context, id string, status string, errorMsg *string) error
	UpdateJobRunStarted(ctx context.Context, id string) error
	UpdateJobRunFinished(ctx context.Context, id string, status string, errorMsg *string) error
}

// ModelRepository provides CRUD operations for transformation models.
type ModelRepository interface {
	Create(ctx context.Context, m *Model) (*Model, error)
	GetByID(ctx context.Context, id string) (*Model, error)
	GetByName(ctx context.Context, projectName, name string) (*Model, error)
	List(ctx context.Context, projectName *string, page PageRequest) ([]Model, int64, error)
	Update(ctx context.Context, id string, req UpdateModelRequest) (*Model, error)
	Delete(ctx context.Context, id string) error
	ListAll(ctx context.Context) ([]Model, error)
	UpdateDependencies(ctx context.Context, id string, deps []string) error
}

// ModelRunRepository provides CRUD operations for model runs and steps.
type ModelRunRepository interface {
	CreateRun(ctx context.Context, run *ModelRun) (*ModelRun, error)
	GetRunByID(ctx context.Context, id string) (*ModelRun, error)
	ListRuns(ctx context.Context, filter ModelRunFilter) ([]ModelRun, int64, error)
	UpdateRunStarted(ctx context.Context, id string) error
	UpdateRunFinished(ctx context.Context, id string, status string, errMsg *string) error
	CreateStep(ctx context.Context, step *ModelRunStep) (*ModelRunStep, error)
	ListStepsByRun(ctx context.Context, runID string) ([]ModelRunStep, error)
	UpdateStepStarted(ctx context.Context, id string) error
	UpdateStepFinished(ctx context.Context, id string, status string, rowsAffected *int64, errMsg *string) error
}

// ModelTestRepository provides CRUD operations for model tests.
type ModelTestRepository interface {
	Create(ctx context.Context, test *ModelTest) (*ModelTest, error)
	GetByID(ctx context.Context, id string) (*ModelTest, error)
	ListByModel(ctx context.Context, modelID string) ([]ModelTest, error)
	Delete(ctx context.Context, id string) error
}

// ModelTestResultRepository provides operations for model test results.
type ModelTestResultRepository interface {
	Create(ctx context.Context, result *ModelTestResult) (*ModelTestResult, error)
	ListByStep(ctx context.Context, runStepID string) ([]ModelTestResult, error)
}

// MacroRepository provides CRUD operations for SQL macros.
type MacroRepository interface {
	Create(ctx context.Context, m *Macro) (*Macro, error)
	GetByName(ctx context.Context, name string) (*Macro, error)
	List(ctx context.Context, page PageRequest) ([]Macro, int64, error)
	Update(ctx context.Context, name string, req UpdateMacroRequest) (*Macro, error)
	Delete(ctx context.Context, name string) error
	ListAll(ctx context.Context) ([]Macro, error)
	ListRevisions(ctx context.Context, macroName string) ([]MacroRevision, error)
	GetRevisionByVersion(ctx context.Context, macroName string, version int) (*MacroRevision, error)
}
