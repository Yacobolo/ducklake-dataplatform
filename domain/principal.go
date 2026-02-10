package domain

import "time"

// Principal represents a user or service principal in the system.
type Principal struct {
	ID        int64
	Name      string
	Type      string // "user" or "service_principal"
	IsAdmin   bool
	CreatedAt time.Time
}

// Group represents a named collection of principals.
type Group struct {
	ID          int64
	Name        string
	Description string
	CreatedAt   time.Time
}

// GroupMember represents the membership of a principal in a group.
type GroupMember struct {
	GroupID    int64
	MemberType string // "user" or "group"
	MemberID   int64
}
