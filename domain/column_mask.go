package domain

import "time"

// ColumnMask represents a masking expression for a column on a table.
type ColumnMask struct {
	ID             int64
	TableID        int64
	ColumnName     string
	MaskExpression string
	Description    string
	CreatedAt      time.Time
}

// ColumnMaskBinding binds a column mask to a principal or group.
type ColumnMaskBinding struct {
	ID            int64
	ColumnMaskID  int64
	PrincipalID   int64
	PrincipalType string // "user" or "group"
	SeeOriginal   bool
}
