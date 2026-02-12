package domain

import (
	"context"
	"time"
)

// PrincipalRepository provides CRUD operations for principals.
type PrincipalRepository interface {
	Create(ctx context.Context, p *Principal) (*Principal, error)
	GetByID(ctx context.Context, id int64) (*Principal, error)
	GetByName(ctx context.Context, name string) (*Principal, error)
	List(ctx context.Context, page PageRequest) ([]Principal, int64, error)
	Delete(ctx context.Context, id int64) error
	SetAdmin(ctx context.Context, id int64, isAdmin bool) error
}

// GroupRepository provides CRUD operations for groups and membership.
type GroupRepository interface {
	Create(ctx context.Context, g *Group) (*Group, error)
	GetByID(ctx context.Context, id int64) (*Group, error)
	GetByName(ctx context.Context, name string) (*Group, error)
	List(ctx context.Context, page PageRequest) ([]Group, int64, error)
	Delete(ctx context.Context, id int64) error
	AddMember(ctx context.Context, m *GroupMember) error
	RemoveMember(ctx context.Context, m *GroupMember) error
	ListMembers(ctx context.Context, groupID int64, page PageRequest) ([]GroupMember, int64, error)
	GetGroupsForMember(ctx context.Context, memberType string, memberID int64) ([]Group, error)
}

// GrantRepository provides operations for privilege grants.
type GrantRepository interface {
	Grant(ctx context.Context, g *PrivilegeGrant) (*PrivilegeGrant, error)
	Revoke(ctx context.Context, g *PrivilegeGrant) error
	ListForPrincipal(ctx context.Context, principalID int64, principalType string, page PageRequest) ([]PrivilegeGrant, int64, error)
	ListForSecurable(ctx context.Context, securableType string, securableID int64, page PageRequest) ([]PrivilegeGrant, int64, error)
	HasPrivilege(ctx context.Context, principalID int64, principalType, securableType string, securableID int64, privilege string) (bool, error)
}

// RowFilterRepository provides CRUD operations for row filters and bindings.
type RowFilterRepository interface {
	Create(ctx context.Context, f *RowFilter) (*RowFilter, error)
	GetForTable(ctx context.Context, tableID int64, page PageRequest) ([]RowFilter, int64, error)
	Delete(ctx context.Context, id int64) error
	Bind(ctx context.Context, b *RowFilterBinding) error
	Unbind(ctx context.Context, b *RowFilterBinding) error
	ListBindings(ctx context.Context, filterID int64) ([]RowFilterBinding, error)
	GetForTableAndPrincipal(ctx context.Context, tableID, principalID int64, principalType string) ([]RowFilter, error)
}

// ColumnMaskRepository provides CRUD operations for column masks and bindings.
type ColumnMaskRepository interface {
	Create(ctx context.Context, m *ColumnMask) (*ColumnMask, error)
	GetForTable(ctx context.Context, tableID int64, page PageRequest) ([]ColumnMask, int64, error)
	Delete(ctx context.Context, id int64) error
	Bind(ctx context.Context, b *ColumnMaskBinding) error
	Unbind(ctx context.Context, b *ColumnMaskBinding) error
	ListBindings(ctx context.Context, maskID int64) ([]ColumnMaskBinding, error)
	GetForTableAndPrincipal(ctx context.Context, tableID, principalID int64, principalType string) ([]ColumnMaskWithBinding, error)
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
	ListTables(ctx context.Context, schemaID int64, page PageRequest) ([]Table, int64, error)
	GetTable(ctx context.Context, tableID int64) (*Table, error)
	ListColumns(ctx context.Context, tableID int64, page PageRequest) ([]Column, int64, error)
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
	SetSchemaStoragePath(ctx context.Context, schemaID int64, path string) error
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
	DeleteEdge(ctx context.Context, id int64) error
	PurgeOlderThan(ctx context.Context, before time.Time) (int64, error)
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
	GetTag(ctx context.Context, id int64) (*Tag, error)
	ListTags(ctx context.Context, page PageRequest) ([]Tag, int64, error)
	DeleteTag(ctx context.Context, id int64) error
	AssignTag(ctx context.Context, assignment *TagAssignment) (*TagAssignment, error)
	UnassignTag(ctx context.Context, id int64) error
	ListTagsForSecurable(ctx context.Context, securableType string, securableID int64, columnName *string) ([]Tag, error)
	ListAssignmentsForTag(ctx context.Context, tagID int64) ([]TagAssignment, error)
}

// ViewRepository provides CRUD operations for views.
type ViewRepository interface {
	Create(ctx context.Context, view *ViewDetail) (*ViewDetail, error)
	GetByName(ctx context.Context, schemaID int64, viewName string) (*ViewDetail, error)
	List(ctx context.Context, schemaID int64, page PageRequest) ([]ViewDetail, int64, error)
	Delete(ctx context.Context, schemaID int64, viewName string) error
	Update(ctx context.Context, schemaID int64, viewName string, comment *string, props map[string]string, viewDef *string) (*ViewDetail, error)
}

// StorageCredentialRepository provides CRUD operations for storage credentials.
type StorageCredentialRepository interface {
	Create(ctx context.Context, cred *StorageCredential) (*StorageCredential, error)
	GetByID(ctx context.Context, id int64) (*StorageCredential, error)
	GetByName(ctx context.Context, name string) (*StorageCredential, error)
	List(ctx context.Context, page PageRequest) ([]StorageCredential, int64, error)
	Update(ctx context.Context, id int64, req UpdateStorageCredentialRequest) (*StorageCredential, error)
	Delete(ctx context.Context, id int64) error
}

// ExternalLocationRepository provides CRUD operations for external locations.
type ExternalLocationRepository interface {
	Create(ctx context.Context, loc *ExternalLocation) (*ExternalLocation, error)
	GetByID(ctx context.Context, id int64) (*ExternalLocation, error)
	GetByName(ctx context.Context, name string) (*ExternalLocation, error)
	List(ctx context.Context, page PageRequest) ([]ExternalLocation, int64, error)
	Update(ctx context.Context, id int64, req UpdateExternalLocationRequest) (*ExternalLocation, error)
	Delete(ctx context.Context, id int64) error
}

// VolumeRepository provides CRUD operations for volumes.
type VolumeRepository interface {
	Create(ctx context.Context, vol *Volume) (*Volume, error)
	GetByName(ctx context.Context, schemaName, name string) (*Volume, error)
	List(ctx context.Context, schemaName string, page PageRequest) ([]Volume, int64, error)
	Update(ctx context.Context, id int64, req UpdateVolumeRequest) (*Volume, error)
	Delete(ctx context.Context, id int64) error
}

// ExternalTableRepository provides CRUD operations for external tables.
type ExternalTableRepository interface {
	Create(ctx context.Context, et *ExternalTableRecord) (*ExternalTableRecord, error)
	GetByName(ctx context.Context, schemaName, tableName string) (*ExternalTableRecord, error)
	GetByID(ctx context.Context, id int64) (*ExternalTableRecord, error)
	GetByTableName(ctx context.Context, tableName string) (*ExternalTableRecord, error)
	List(ctx context.Context, schemaName string, page PageRequest) ([]ExternalTableRecord, int64, error)
	ListAll(ctx context.Context) ([]ExternalTableRecord, error)
	Delete(ctx context.Context, schemaName, tableName string) error
	DeleteBySchema(ctx context.Context, schemaName string) error
}

// ComputeEndpointRepository provides CRUD operations for compute endpoints and assignments.
type ComputeEndpointRepository interface {
	Create(ctx context.Context, ep *ComputeEndpoint) (*ComputeEndpoint, error)
	GetByID(ctx context.Context, id int64) (*ComputeEndpoint, error)
	GetByName(ctx context.Context, name string) (*ComputeEndpoint, error)
	List(ctx context.Context, page PageRequest) ([]ComputeEndpoint, int64, error)
	Update(ctx context.Context, id int64, req UpdateComputeEndpointRequest) (*ComputeEndpoint, error)
	Delete(ctx context.Context, id int64) error
	UpdateStatus(ctx context.Context, id int64, status string) error
	Assign(ctx context.Context, a *ComputeAssignment) (*ComputeAssignment, error)
	Unassign(ctx context.Context, id int64) error
	ListAssignments(ctx context.Context, endpointID int64, page PageRequest) ([]ComputeAssignment, int64, error)
	GetDefaultForPrincipal(ctx context.Context, principalID int64, principalType string) (*ComputeEndpoint, error)
	GetAssignmentsForPrincipal(ctx context.Context, principalID int64, principalType string) ([]ComputeEndpoint, error)
}
