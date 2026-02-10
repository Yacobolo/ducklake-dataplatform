package domain

import "time"

// PrivilegeGrant represents a privilege granted to a principal on a securable.
type PrivilegeGrant struct {
	ID            int64
	PrincipalID   int64
	PrincipalType string // "user" or "group"
	SecurableType string // "catalog", "schema", "table"
	SecurableID   int64
	Privilege     string
	GrantedBy     *int64
	GrantedAt     time.Time
}
