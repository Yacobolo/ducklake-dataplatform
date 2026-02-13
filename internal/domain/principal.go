package domain

import "time"

// Principal represents a user or service principal in the system.
type Principal struct {
	ID             string
	Name           string
	Type           string // "user" or "service_principal"
	IsAdmin        bool
	ExternalID     *string // IdP subject identifier (JWT `sub` claim)
	ExternalIssuer *string // Issuer URL that owns this external ID
	CreatedAt      time.Time
}

// Group represents a named collection of principals.
type Group struct {
	ID          string
	Name        string
	Description string
	CreatedAt   time.Time
}

// CreatePrincipalRequest holds parameters for creating a new principal.
type CreatePrincipalRequest struct {
	Name    string
	Type    string // "user" or "service_principal"; defaults to "user"
	IsAdmin bool
}

// Validate checks that the request is well-formed.
func (r *CreatePrincipalRequest) Validate() error {
	if r.Name == "" {
		return ErrValidation("principal name is required")
	}
	if r.Type == "" {
		r.Type = "user"
	}
	if r.Type != "user" && r.Type != "service_principal" {
		return ErrValidation("type must be 'user' or 'service_principal'")
	}
	return nil
}

// ResolveOrProvisionRequest holds parameters for resolving or JIT-provisioning a principal.
type ResolveOrProvisionRequest struct {
	Issuer      string
	ExternalID  string
	DisplayName string
	IsBootstrap bool
}

// Validate checks that the request is well-formed.
func (r *ResolveOrProvisionRequest) Validate() error {
	if r.ExternalID == "" {
		return ErrValidation("external_id is required")
	}
	if r.Issuer == "" {
		return ErrValidation("issuer is required")
	}
	return nil
}

// CreateGroupRequest holds parameters for creating a new group.
type CreateGroupRequest struct {
	Name        string
	Description string
}

// Validate checks that the request is well-formed.
func (r *CreateGroupRequest) Validate() error {
	if r.Name == "" {
		return ErrValidation("group name is required")
	}
	return nil
}

// AddGroupMemberRequest holds parameters for adding a member to a group.
type AddGroupMemberRequest struct {
	GroupID    string
	MemberType string // "user" or "group"
	MemberID   string
}

// Validate checks that the request is well-formed.
func (r *AddGroupMemberRequest) Validate() error {
	if r.GroupID == "" {
		return ErrValidation("group_id is required")
	}
	if r.MemberID == "" {
		return ErrValidation("member_id is required")
	}
	if r.MemberType != "user" && r.MemberType != "group" {
		return ErrValidation("member_type must be 'user' or 'group'")
	}
	return nil
}

// RemoveGroupMemberRequest holds parameters for removing a member from a group.
type RemoveGroupMemberRequest struct {
	GroupID    string
	MemberType string // "user" or "group"
	MemberID   string
}

// Validate checks that the request is well-formed.
func (r *RemoveGroupMemberRequest) Validate() error {
	if r.GroupID == "" {
		return ErrValidation("group_id is required")
	}
	if r.MemberID == "" {
		return ErrValidation("member_id is required")
	}
	if r.MemberType != "user" && r.MemberType != "group" {
		return ErrValidation("member_type must be 'user' or 'group'")
	}
	return nil
}

// GroupMember represents the membership of a principal in a group.
type GroupMember struct {
	GroupID    string
	MemberType string // "user" or "group"
	MemberID   string
}
