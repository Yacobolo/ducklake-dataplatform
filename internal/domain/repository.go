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
	Update(ctx context.Context, id string, g *Group) (*Group, error)
	Delete(ctx context.Context, id string) error
	AddMember(ctx context.Context, m *GroupMember) error
	RemoveMember(ctx context.Context, m *GroupMember) error
	ListMembers(ctx context.Context, groupID string, page PageRequest) ([]GroupMember, int64, error)
	GetGroupsForMember(ctx context.Context, memberType string, memberID string) ([]Group, error)
}

// DomainRepository provides CRUD operations for product domains.
//
//nolint:revive // Repository interfaces follow the package-wide naming convention.
type DomainRepository interface {
	Create(ctx context.Context, d *Domain) (*Domain, error)
	GetByID(ctx context.Context, id string) (*Domain, error)
	GetByName(ctx context.Context, name string) (*Domain, error)
	List(ctx context.Context, page PageRequest) ([]Domain, int64, error)
	Update(ctx context.Context, name string, d *Domain) (*Domain, error)
	Delete(ctx context.Context, name string) error
}

// TeamRepository provides CRUD operations for owning teams.
type TeamRepository interface {
	Create(ctx context.Context, t *Team) (*Team, error)
	GetByID(ctx context.Context, id string) (*Team, error)
	GetByDomainAndName(ctx context.Context, domainID, name string) (*Team, error)
	List(ctx context.Context, page PageRequest) ([]Team, int64, error)
	Update(ctx context.Context, domainID, name string, t *Team) (*Team, error)
	Delete(ctx context.Context, domainID, name string) error
}

// DataProductRepository provides CRUD operations for products and their linked resources.
type DataProductRepository interface {
	Create(ctx context.Context, p *DataProduct) (*DataProduct, error)
	GetByID(ctx context.Context, productID string) (*DataProduct, error)
	GetBySlug(ctx context.Context, slug string) (*DataProductDetail, error)
	List(ctx context.Context, filter DataProductFilter) ([]DataProductListItem, int64, error)
	Update(ctx context.Context, p *DataProduct) (*DataProduct, error)
	Delete(ctx context.Context, productID string) error
	CreateVersion(ctx context.Context, version *DataProductVersion) (*DataProductVersion, error)
	GetVersionByNumber(ctx context.Context, productID string, version int) (*DataProductVersion, error)
	ListVersions(ctx context.Context, productID string) ([]DataProductVersion, error)
	DeleteVersion(ctx context.Context, versionID string) error
	UpdateVersionReleaseState(ctx context.Context, versionID string, releaseState string) error
	UpdatePublicationIntent(ctx context.Context, productID string, publicationIntent string) error
	UpsertStatus(ctx context.Context, status *DataProductStatus) error
	GetStatus(ctx context.Context, productID string) (*DataProductStatus, error)
	AddOutput(ctx context.Context, output *ProductOutput) error
	ListOutputs(ctx context.Context, productVersionID string) ([]ProductOutput, error)
	ReplaceOutputs(ctx context.Context, productVersionID string, outputs []ProductOutput) error
	AddSemanticEntrypoint(ctx context.Context, entrypoint *ProductSemanticEntrypoint) error
	ListSemanticEntrypoints(ctx context.Context, productVersionID string) ([]ProductSemanticEntrypoint, error)
	ReplaceSemanticEntrypoints(ctx context.Context, productVersionID string, entrypoints []ProductSemanticEntrypoint) error
	AddDependency(ctx context.Context, dependency *ProductDependency) error
	ListDependencies(ctx context.Context, productID string) ([]DataProductListItem, error)
	AddSubscription(ctx context.Context, subscription *ProductSubscription) (*ProductSubscription, error)
	ListSubscriptions(ctx context.Context, productID string) ([]ProductSubscription, error)
	AddEvent(ctx context.Context, event *ProductEvent) (*ProductEvent, error)
	ListEvents(ctx context.Context, productID string, page PageRequest) ([]ProductEvent, int64, error)
	CountDependents(ctx context.Context, productID string) (int64, error)
	ListOrphanAssets(ctx context.Context) ([]OrphanResource, error)
	ListOrphanSemanticModels(ctx context.Context) ([]OrphanResource, error)
	GetByAssetID(ctx context.Context, assetID string) (*DataProductListItem, error)
}

// ProjectRepository provides CRUD operations for internal authoring projects.
type ProjectRepository interface {
	Create(ctx context.Context, p *Project) (*Project, error)
	GetByID(ctx context.Context, id string) (*Project, error)
	GetByName(ctx context.Context, name string) (*Project, error)
	List(ctx context.Context, page PageRequest) ([]Project, int64, error)
	ListByProduct(ctx context.Context, productID string, page PageRequest) ([]Project, int64, error)
	Update(ctx context.Context, id string, req UpdateProjectRequest) (*Project, error)
	Delete(ctx context.Context, id string) error
}

// EnvironmentRepository provides CRUD operations for internal project environments.
type EnvironmentRepository interface {
	Create(ctx context.Context, e *Environment) (*Environment, error)
	GetByID(ctx context.Context, id string) (*Environment, error)
	GetByName(ctx context.Context, projectID, name string) (*Environment, error)
	ListByProject(ctx context.Context, projectID string, page PageRequest) ([]Environment, int64, error)
	Update(ctx context.Context, id string, req UpdateEnvironmentRequest) (*Environment, error)
	Delete(ctx context.Context, id string) error
}

// BuildRepository provides CRUD operations for internal immutable build snapshots.
type BuildRepository interface {
	Create(ctx context.Context, b *Build) (*Build, error)
	GetByID(ctx context.Context, id string) (*Build, error)
	ListByProject(ctx context.Context, projectID string, page PageRequest) ([]Build, int64, error)
	UpdateState(ctx context.Context, id string, state string) error
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

// AuthIdentityRepository manages principal identity links.
type AuthIdentityRepository interface {
	Create(ctx context.Context, identity *AuthIdentity) (*AuthIdentity, error)
	GetByProviderSubject(ctx context.Context, provider string, issuer *string, subject string) (*AuthIdentity, error)
	ListByPrincipal(ctx context.Context, principalID string) ([]AuthIdentity, error)
	Delete(ctx context.Context, id string) error
}

// LocalCredentialRepository manages local username/password credentials.
type LocalCredentialRepository interface {
	Upsert(ctx context.Context, credential *LocalCredential) error
	GetByUsername(ctx context.Context, username string) (*LocalCredential, error)
	GetByPrincipalID(ctx context.Context, principalID string) (*LocalCredential, error)
	Delete(ctx context.Context, principalID string) error
}

// AuthSessionRepository manages interactive sessions.
type AuthSessionRepository interface {
	Create(ctx context.Context, session *AuthSession) (*AuthSession, error)
	GetActiveByHash(ctx context.Context, sessionHash string) (*AuthSession, error)
	Touch(ctx context.Context, sessionID string, idleExpiresAt time.Time) error
	Revoke(ctx context.Context, sessionID string) error
	RevokeByHash(ctx context.Context, sessionHash string) error
	RevokeAllForPrincipal(ctx context.Context, principalID string) error
	CountActive(ctx context.Context) (int64, error)
	DeleteExpiredOrRevoked(ctx context.Context) (int64, error)
}

// AuthRecoveryRepository manages recovery codes.
type AuthRecoveryRepository interface {
	Create(ctx context.Context, code *AuthRecoveryCode) (*AuthRecoveryCode, error)
	ListByPrincipal(ctx context.Context, principalID string) ([]AuthRecoveryCode, error)
	GetUnusedByHash(ctx context.Context, codeHash string) (*AuthRecoveryCode, error)
	MarkUsed(ctx context.Context, id string) error
	DeleteExpired(ctx context.Context) (int64, error)
}

// AuthLoginAttemptRepository tracks local login attempts.
type AuthLoginAttemptRepository interface {
	Insert(ctx context.Context, attempt *AuthLoginAttempt) error
	CountRecentFailedByUsername(ctx context.Context, username string, since time.Time) (int64, error)
	CountRecentFailedByIP(ctx context.Context, ipAddress string, since time.Time) (int64, error)
}

// SetupStateRepository manages first-run bootstrap state.
type SetupStateRepository interface {
	Get(ctx context.Context) (*SetupState, error)
	Complete(ctx context.Context, principalID string) error
	SetBootstrapToken(ctx context.Context, tokenHash string, expiresAt time.Time) error
	ClearBootstrapToken(ctx context.Context) error
}

// AuthProviderRepository manages runtime OIDC provider settings.
type AuthProviderRepository interface {
	Get(ctx context.Context) (*AuthProviderConfig, error)
	Upsert(ctx context.Context, cfg *AuthProviderConfig) error
}

// WebAuthnCredentialRepository manages passkey credentials.
type WebAuthnCredentialRepository interface {
	Create(ctx context.Context, credential *WebAuthnCredential) (*WebAuthnCredential, error)
	ListByPrincipal(ctx context.Context, principalID string) ([]WebAuthnCredential, error)
	GetByCredentialID(ctx context.Context, credentialID string) (*WebAuthnCredential, error)
	UpdateCounter(ctx context.Context, credentialID string, signCount int64) error
	Delete(ctx context.Context, id string) error
}

// ResourceAccessRepository manages principal-scoped recent resource access history.
type ResourceAccessRepository interface {
	TrackVisit(ctx context.Context, principalID string, resource ResourceRef) error
	ListRecent(ctx context.Context, principalID string, limit int) ([]ResourceAccessEvent, error)
}

// SavedResourceRepository manages principal-scoped saved resources.
type SavedResourceRepository interface {
	Save(ctx context.Context, principalID string, resource ResourceRef) error
	Unsave(ctx context.Context, principalID string, resourceType string, resourceKey string) error
	ListSaved(ctx context.Context, principalID string, limit int) ([]SavedResource, error)
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
	GetCatalogVersionSummary(ctx context.Context) (*CatalogVersionSummary, error)
	ListCatalogHistory(ctx context.Context, filter CatalogHistoryFilter) ([]CatalogHistoryEntry, error)

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

// QueryJobRepository provides CRUD operations for durable async query jobs.
type QueryJobRepository interface {
	Create(ctx context.Context, job *QueryJob) (*QueryJob, error)
	GetByID(ctx context.Context, id string) (*QueryJob, error)
	ListByPrincipal(ctx context.Context, principalName string, page PageRequest) ([]QueryJob, int64, error)
	GetByRequestID(ctx context.Context, principalName, requestID string) (*QueryJob, error)
	SetResolvedCompute(ctx context.Context, id string, mode string, endpointName *string) error
	MarkRunning(ctx context.Context, id string, attempt int) error
	MarkRetrying(ctx context.Context, id string, attempt int, nextRetryAt time.Time, message string) error
	Heartbeat(ctx context.Context, id string, at time.Time) error
	MarkSucceeded(ctx context.Context, id string, columns []string, rows [][]interface{}, rowCount int) error
	MarkFailed(ctx context.Context, id string, message string) error
	MarkCanceled(ctx context.Context, id string) error
	Delete(ctx context.Context, id string) error
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
	GetByID(ctx context.Context, id string) (*ViewDetail, error)
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
	UpdateHealth(ctx context.Context, id string, health ComputeEndpointHealthResult) error
	Assign(ctx context.Context, a *ComputeAssignment) (*ComputeAssignment, error)
	Unassign(ctx context.Context, id string) error
	ListAssignments(ctx context.Context, endpointID string, page PageRequest) ([]ComputeAssignment, int64, error)
	GetDefaultForPrincipal(ctx context.Context, principalID string, principalType string) (*ComputeEndpoint, error)
	GetAssignmentsForPrincipal(ctx context.Context, principalID string, principalType string) ([]ComputeEndpoint, error)
}

// ComputeRoutingRepository stores global compute routing defaults.
type ComputeRoutingRepository interface {
	GetDefaults(ctx context.Context) (*ComputeRoutingDefaults, error)
	UpdateDefaults(ctx context.Context, defaults ComputeRoutingDefaults) (*ComputeRoutingDefaults, error)
}

// NotebookRepository provides CRUD operations for notebooks and cells.
type NotebookRepository interface {
	CreateNotebook(ctx context.Context, nb *Notebook) (*Notebook, error)
	GetNotebook(ctx context.Context, id string) (*Notebook, error)
	ListNotebooks(ctx context.Context, owner *string, page PageRequest) ([]Notebook, int64, error)
	ListByFolders(ctx context.Context, folderIDs []string) ([]Notebook, error)
	UpdateNotebook(ctx context.Context, id string, req UpdateNotebookRequest) (*Notebook, error)
	UpdateNotebookSync(ctx context.Context, nb *Notebook) (*Notebook, error)
	DeleteNotebook(ctx context.Context, id string) error

	CreateCell(ctx context.Context, cell *Cell) (*Cell, error)
	GetCell(ctx context.Context, id string) (*Cell, error)
	ListCells(ctx context.Context, notebookID string) ([]Cell, error)
	UpdateCell(ctx context.Context, id string, req UpdateCellRequest) (*Cell, error)
	UpdateCellSync(ctx context.Context, cell *Cell) (*Cell, error)
	DeleteCell(ctx context.Context, id string) error
	UpdateCellResult(ctx context.Context, cellID string, result *string) error
	ReorderCells(ctx context.Context, notebookID string, cellIDs []string) error
	GetMaxPosition(ctx context.Context, notebookID string) (int, error)
}

// FolderRepository provides CRUD and inheritance helpers for folders.
type FolderRepository interface {
	Create(ctx context.Context, folder *Folder) (*Folder, error)
	GetByID(ctx context.Context, id string) (*Folder, error)
	ListAll(ctx context.Context) ([]Folder, error)
	ListByOwner(ctx context.Context, owner string) ([]Folder, error)
	Update(ctx context.Context, id string, req UpdateFolderRequest) (*Folder, error)
	Move(ctx context.Context, id string, parentFolderID *string) (*Folder, error)
	Delete(ctx context.Context, id string) error
	EnsurePersonalRoot(ctx context.Context, owner string) (*Folder, error)
	EnsureGitSyncRoot(ctx context.Context, owner string, repo *GitRepo) (*Folder, error)
	ListAncestors(ctx context.Context, folderID string) ([]Folder, error)
}

// FolderShareRepository manages inherited folder ACL entries.
type FolderShareRepository interface {
	Upsert(ctx context.Context, share *FolderShare) (*FolderShare, error)
	Delete(ctx context.Context, folderID string, principalName string) error
	ListByFolder(ctx context.Context, folderID string) ([]FolderShare, error)
	ListByPrincipal(ctx context.Context, principalName string) ([]FolderShare, error)
}

// NotebookShareRepository manages notebook-level ACL entries.
type NotebookShareRepository interface {
	Upsert(ctx context.Context, share *NotebookShare) (*NotebookShare, error)
	Delete(ctx context.Context, notebookID string, principalName string) error
	ListByNotebook(ctx context.Context, notebookID string) ([]NotebookShare, error)
	ListByPrincipal(ctx context.Context, principalName string) ([]NotebookShare, error)
}

// DashboardRepository provides CRUD operations for dashboards.
type DashboardRepository interface {
	Create(ctx context.Context, d *Dashboard) (*Dashboard, error)
	GetByID(ctx context.Context, id string) (*Dashboard, error)
	List(ctx context.Context, owner *string, page PageRequest) ([]Dashboard, int64, error)
	ListByFolders(ctx context.Context, folderIDs []string) ([]Dashboard, error)
	Update(ctx context.Context, id string, req UpdateDashboardRequest) (*Dashboard, error)
	Delete(ctx context.Context, id string) error
}

// DashboardWidgetRepository provides CRUD operations for dashboard widgets.
type DashboardWidgetRepository interface {
	Create(ctx context.Context, w *DashboardWidget) (*DashboardWidget, error)
	GetByID(ctx context.Context, id string) (*DashboardWidget, error)
	ListByDashboard(ctx context.Context, dashboardID string) ([]DashboardWidget, error)
	Update(ctx context.Context, id string, req UpdateDashboardWidgetRequest) (*DashboardWidget, error)
	Delete(ctx context.Context, id string) error
}

// NotebookJobRepository provides CRUD operations for async notebook jobs.
type NotebookJobRepository interface {
	CreateJob(ctx context.Context, job *NotebookJob) (*NotebookJob, error)
	GetJob(ctx context.Context, id string) (*NotebookJob, error)
	ListJobs(ctx context.Context, notebookID string, page PageRequest) ([]NotebookJob, int64, error)
	UpdateJobState(ctx context.Context, id string, state JobState, result *string, errMsg *string) error
}

// NotebookModelLinkRepository provides CRUD operations for notebook-model links.
type NotebookModelLinkRepository interface {
	Upsert(ctx context.Context, link *NotebookModelLink) error
	GetByNotebookID(ctx context.Context, notebookID string) (*NotebookModelLink, error)
	GetByModelID(ctx context.Context, modelID string) (*NotebookModelLink, error)
	DeleteByNotebookID(ctx context.Context, notebookID string) error
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
	ListPipelinesByFolders(ctx context.Context, folderIDs []string) ([]Pipeline, error)
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

// DataAssetRepository provides CRUD operations for data assets.
type DataAssetRepository interface {
	Create(ctx context.Context, a *DataAsset) (*DataAsset, error)
	GetByID(ctx context.Context, id string) (*DataAsset, error)
	GetByKey(ctx context.Context, assetKey string) (*DataAsset, error)
	List(ctx context.Context, filter AssetFilter) ([]DataAsset, int64, error)
	Update(ctx context.Context, id string, a *DataAsset) (*DataAsset, error)
	Delete(ctx context.Context, id string) error
}

// AssetDependencyRepository provides CRUD operations for asset dependencies.
type AssetDependencyRepository interface {
	Create(ctx context.Context, d *AssetDependency) (*AssetDependency, error)
	ListUpstream(ctx context.Context, assetID string) ([]AssetDependency, error)
	ListDownstream(ctx context.Context, upstreamAssetID string) ([]AssetDependency, error)
	Delete(ctx context.Context, id string) error
	DeleteByAsset(ctx context.Context, assetID string) error
}

// AssetPartitionRepository provides CRUD operations for asset partitions.
type AssetPartitionRepository interface {
	Upsert(ctx context.Context, p *AssetPartition) (*AssetPartition, error)
	GetByKey(ctx context.Context, assetID, partitionKey string) (*AssetPartition, error)
	ListByAsset(ctx context.Context, assetID string, page PageRequest) ([]AssetPartition, int64, error)
	UpdateStatus(ctx context.Context, assetID, partitionKey, status string, metadata map[string]any, materializedAt *time.Time) error
}

// AssetRunRepository provides operations for asset runs, events, and materializations.
type AssetRunRepository interface {
	CreateRun(ctx context.Context, run *AssetRun) (*AssetRun, error)
	GetRunByID(ctx context.Context, id string) (*AssetRun, error)
	ListRuns(ctx context.Context, filter AssetRunFilter) ([]AssetRun, int64, error)
	UpdateRunStarted(ctx context.Context, id string) error
	UpdateRunFinished(ctx context.Context, id string, status string, errMsg *string) error
	UpdateRunRetrying(ctx context.Context, id string, attempt int, errMsg *string) error
	CreateRunEvent(ctx context.Context, event *AssetRunEvent) (*AssetRunEvent, error)
	ListRunEvents(ctx context.Context, runID string, page PageRequest) ([]AssetRunEvent, int64, error)
	CreateMaterialization(ctx context.Context, m *AssetMaterialization) (*AssetMaterialization, error)
	ListMaterializationsByAsset(ctx context.Context, assetID string, page PageRequest) ([]AssetMaterialization, int64, error)
}

// AssetCheckRepository provides CRUD operations for asset checks and results.
type AssetCheckRepository interface {
	CreateCheck(ctx context.Context, c *AssetCheck) (*AssetCheck, error)
	GetCheckByID(ctx context.Context, id string) (*AssetCheck, error)
	ListChecksByAsset(ctx context.Context, assetID string) ([]AssetCheck, error)
	UpdateCheck(ctx context.Context, id string, c *AssetCheck) (*AssetCheck, error)
	DeleteCheck(ctx context.Context, id string) error
	CreateCheckResult(ctx context.Context, r *AssetCheckResult) (*AssetCheckResult, error)
	ListCheckResults(ctx context.Context, checkID string, page PageRequest) ([]AssetCheckResult, int64, error)
}

// OrchestrationEventRepository provides durable event-inbox operations.
type OrchestrationEventRepository interface {
	Enqueue(ctx context.Context, event *OrchestrationEvent) (*OrchestrationEvent, error)
	ClaimNextPending(ctx context.Context, now time.Time) (*OrchestrationEvent, error)
	MarkProcessed(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id string, errMsg string, retryAt *time.Time) error
	List(ctx context.Context, filter OrchestrationEventFilter) ([]OrchestrationEvent, int64, error)
}

// BackfillRepository provides request/slice persistence for partition backfills.
type BackfillRepository interface {
	CreateRequest(ctx context.Context, req *BackfillRequest) (*BackfillRequest, error)
	GetRequestByID(ctx context.Context, id string) (*BackfillRequest, error)
	ListRequests(ctx context.Context, filter BackfillFilter) ([]BackfillRequest, int64, error)
	UpdateRequestStatus(ctx context.Context, id string, status string, errMsg *string) error
	CreateSlice(ctx context.Context, slice *BackfillSlice) (*BackfillSlice, error)
	ListSlicesByRequest(ctx context.Context, requestID string) ([]BackfillSlice, error)
	UpdateSliceStatus(ctx context.Context, id string, status string, runID *string, errMsg *string) error
}

// ModelRepository provides CRUD operations for transformation models.
type ModelRepository interface {
	Create(ctx context.Context, m *Model) (*Model, error)
	CreateWithNotebookLink(ctx context.Context, m *Model, notebookID, outputCellID string) (*Model, error)
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
	UpdateRunBuild(ctx context.Context, id string, buildID string) error
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

// SemanticModelRepository provides CRUD operations for semantic models.
type SemanticModelRepository interface {
	Create(ctx context.Context, m *SemanticModel) (*SemanticModel, error)
	GetByID(ctx context.Context, id string) (*SemanticModel, error)
	GetByName(ctx context.Context, name string) (*SemanticModel, error)
	List(ctx context.Context, page PageRequest) ([]SemanticModel, int64, error)
	Update(ctx context.Context, id string, req UpdateSemanticModelRequest) (*SemanticModel, error)
	Delete(ctx context.Context, id string) error
	ListAll(ctx context.Context) ([]SemanticModel, error)
}

// SemanticMetricRepository provides CRUD operations for semantic metrics.
type SemanticMetricRepository interface {
	Create(ctx context.Context, m *SemanticMetric) (*SemanticMetric, error)
	GetByID(ctx context.Context, id string) (*SemanticMetric, error)
	GetByName(ctx context.Context, semanticModelID, name string) (*SemanticMetric, error)
	ListByModel(ctx context.Context, semanticModelID string) ([]SemanticMetric, error)
	Update(ctx context.Context, id string, req UpdateSemanticMetricRequest) (*SemanticMetric, error)
	Delete(ctx context.Context, id string) error
}

// SemanticRelationshipRepository provides CRUD operations for semantic relationships.
type SemanticRelationshipRepository interface {
	Create(ctx context.Context, r *SemanticRelationship) (*SemanticRelationship, error)
	GetByID(ctx context.Context, id string) (*SemanticRelationship, error)
	GetByName(ctx context.Context, fromSemanticID, name string) (*SemanticRelationship, error)
	List(ctx context.Context, page PageRequest) ([]SemanticRelationship, int64, error)
	ListByModel(ctx context.Context, semanticModelID string) ([]SemanticRelationship, error)
	Update(ctx context.Context, id string, req UpdateSemanticRelationshipRequest) (*SemanticRelationship, error)
	Delete(ctx context.Context, id string) error
}

// SemanticPreAggregationRepository provides CRUD operations for semantic pre-aggregations.
type SemanticPreAggregationRepository interface {
	Create(ctx context.Context, p *SemanticPreAggregation) (*SemanticPreAggregation, error)
	GetByID(ctx context.Context, id string) (*SemanticPreAggregation, error)
	GetByName(ctx context.Context, semanticModelID, name string) (*SemanticPreAggregation, error)
	ListByModel(ctx context.Context, semanticModelID string) ([]SemanticPreAggregation, error)
	Update(ctx context.Context, id string, req UpdateSemanticPreAggregationRequest) (*SemanticPreAggregation, error)
	Delete(ctx context.Context, id string) error
}
