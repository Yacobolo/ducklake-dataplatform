package domain

import "time"

// RowFilter represents a row-level security filter on a table.
type RowFilter struct {
	ID          string
	TableID     string
	FilterSQL   string
	Description string
	CreatedAt   time.Time
}

// RowFilterBinding binds a row filter to a principal or group.
type RowFilterBinding struct {
	ID            string
	RowFilterID   string
	PrincipalID   string
	PrincipalType string // "user" or "group"
}
