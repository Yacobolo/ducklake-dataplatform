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
	List(ctx context.Context) ([]Principal, error)
	Delete(ctx context.Context, id int64) error
	SetAdmin(ctx context.Context, id int64, isAdmin bool) error
}

// GroupRepository provides CRUD operations for groups and membership.
type GroupRepository interface {
	Create(ctx context.Context, g *Group) (*Group, error)
	GetByID(ctx context.Context, id int64) (*Group, error)
	GetByName(ctx context.Context, name string) (*Group, error)
	List(ctx context.Context) ([]Group, error)
	Delete(ctx context.Context, id int64) error
	AddMember(ctx context.Context, m *GroupMember) error
	RemoveMember(ctx context.Context, m *GroupMember) error
	ListMembers(ctx context.Context, groupID int64) ([]GroupMember, error)
	GetGroupsForMember(ctx context.Context, memberType string, memberID int64) ([]Group, error)
}

// GrantRepository provides operations for privilege grants.
type GrantRepository interface {
	Grant(ctx context.Context, g *PrivilegeGrant) (*PrivilegeGrant, error)
	Revoke(ctx context.Context, g *PrivilegeGrant) error
	ListForPrincipal(ctx context.Context, principalID int64, principalType string) ([]PrivilegeGrant, error)
	ListForSecurable(ctx context.Context, securableType string, securableID int64) ([]PrivilegeGrant, error)
}

// RowFilterRepository provides CRUD operations for row filters and bindings.
type RowFilterRepository interface {
	Create(ctx context.Context, f *RowFilter) (*RowFilter, error)
	GetForTable(ctx context.Context, tableID int64) ([]RowFilter, error)
	Delete(ctx context.Context, id int64) error
	Bind(ctx context.Context, b *RowFilterBinding) error
	Unbind(ctx context.Context, b *RowFilterBinding) error
	ListBindings(ctx context.Context, filterID int64) ([]RowFilterBinding, error)
}

// ColumnMaskRepository provides CRUD operations for column masks and bindings.
type ColumnMaskRepository interface {
	Create(ctx context.Context, m *ColumnMask) (*ColumnMask, error)
	GetForTable(ctx context.Context, tableID int64) ([]ColumnMask, error)
	Delete(ctx context.Context, id int64) error
	Bind(ctx context.Context, b *ColumnMaskBinding) error
	Unbind(ctx context.Context, b *ColumnMaskBinding) error
	ListBindings(ctx context.Context, maskID int64) ([]ColumnMaskBinding, error)
}

// AuditFilter holds filter parameters for querying audit logs.
type AuditFilter struct {
	PrincipalName *string
	Action        *string
	Status        *string
	Since         *time.Time
	Limit         int
	Offset        int
}

// AuditRepository provides operations for audit log entries.
type AuditRepository interface {
	Insert(ctx context.Context, e *AuditEntry) error
	List(ctx context.Context, filter AuditFilter) ([]AuditEntry, int64, error)
}

// IntrospectionRepository provides read-only access to DuckLake metadata.
type IntrospectionRepository interface {
	ListSchemas(ctx context.Context) ([]Schema, error)
	ListTables(ctx context.Context, schemaID int64) ([]Table, error)
	GetTable(ctx context.Context, tableID int64) (*Table, error)
	ListColumns(ctx context.Context, tableID int64) ([]Column, error)
}
