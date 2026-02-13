package domain

import "time"

// ColumnMask represents a masking expression for a column on a table.
type ColumnMask struct {
	ID             string
	TableID        string
	ColumnName     string
	MaskExpression string
	Description    string
	CreatedAt      time.Time
}

// ColumnMaskBinding binds a column mask to a principal or group.
type ColumnMaskBinding struct {
	ID            string
	ColumnMaskID  string
	PrincipalID   string
	PrincipalType string // "user" or "group"
	SeeOriginal   bool
}

// ColumnMaskWithBinding is a denormalised view combining mask + binding info,
// returned by repository queries that join column_masks with their bindings.
type ColumnMaskWithBinding struct {
	ColumnName     string
	MaskExpression string
	SeeOriginal    bool
}
