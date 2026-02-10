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
	GetTable(ctx context.Context, schemaName, tableName string) (*TableDetail, error)
	ListTables(ctx context.Context, schemaName string, page PageRequest) ([]TableDetail, int64, error)
	DeleteTable(ctx context.Context, schemaName, tableName string) error
	ListColumns(ctx context.Context, schemaName, tableName string, page PageRequest) ([]ColumnDetail, int64, error)
}

// AuthorizationService defines the interface for permission checking.
// The engine depends on this interface rather than a concrete service type.
type AuthorizationService interface {
	LookupTableID(ctx context.Context, tableName string) (tableID, schemaID int64, err error)
	CheckPrivilege(ctx context.Context, principalName, securableType string, securableID int64, privilege string) (bool, error)
	GetEffectiveRowFilters(ctx context.Context, principalName string, tableID int64) ([]string, error)
	GetEffectiveColumnMasks(ctx context.Context, principalName string, tableID int64) (map[string]string, error)
}
