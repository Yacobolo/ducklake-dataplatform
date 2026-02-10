package domain

import "time"

// RowFilter represents a row-level security filter on a table.
type RowFilter struct {
	ID          int64
	TableID     int64
	FilterSQL   string
	Description string
	CreatedAt   time.Time
}

// RowFilterBinding binds a row filter to a principal or group.
type RowFilterBinding struct {
	ID            int64
	RowFilterID   int64
	PrincipalID   int64
	PrincipalType string // "user" or "group"
}
