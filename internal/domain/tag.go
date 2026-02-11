package domain

import "time"

// Tag represents a tag definition.
type Tag struct {
	ID        int64
	Key       string
	Value     *string
	CreatedBy string
	CreatedAt time.Time
}

// TagAssignment represents a tag assigned to a securable object.
type TagAssignment struct {
	ID            int64
	TagID         int64
	SecurableType string // "schema", "table", "column"
	SecurableID   int64
	ColumnName    *string
	AssignedBy    string
	AssignedAt    time.Time
}
