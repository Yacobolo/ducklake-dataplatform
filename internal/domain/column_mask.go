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

// CreateColumnMaskRequest holds parameters for creating a column mask.
type CreateColumnMaskRequest struct {
	TableID        int64
	ColumnName     string
	MaskExpression string
	Description    string
}

// Validate checks that the request is well-formed.
func (r *CreateColumnMaskRequest) Validate() error {
	if r.TableID <= 0 {
		return ErrValidation("table_id is required")
	}
	if r.ColumnName == "" {
		return ErrValidation("column_name is required")
	}
	if r.MaskExpression == "" {
		return ErrValidation("mask_expression is required")
	}
	return nil
}

// BindColumnMaskRequest holds parameters for binding a column mask to a principal.
type BindColumnMaskRequest struct {
	ColumnMaskID  int64
	PrincipalID   int64
	PrincipalType string // "user" or "group"
	SeeOriginal   bool
}

// Validate checks that the request is well-formed.
func (r *BindColumnMaskRequest) Validate() error {
	if r.ColumnMaskID <= 0 {
		return ErrValidation("column_mask_id is required")
	}
	if r.PrincipalID <= 0 {
		return ErrValidation("principal_id is required")
	}
	if r.PrincipalType != "user" && r.PrincipalType != "group" {
		return ErrValidation("principal_type must be 'user' or 'group'")
	}
	return nil
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
