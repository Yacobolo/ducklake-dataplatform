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

// CreateRowFilterRequest holds parameters for creating a row filter.
type CreateRowFilterRequest struct {
	TableID     string
	FilterSQL   string
	Description string
}

// Validate checks that the request is well-formed.
func (r *CreateRowFilterRequest) Validate() error {
	if r.TableID == "" {
		return ErrValidation("table_id is required")
	}
	if r.FilterSQL == "" {
		return ErrValidation("filter_sql is required")
	}
	return nil
}

// BindRowFilterRequest holds parameters for binding a row filter to a principal.
type BindRowFilterRequest struct {
	RowFilterID   string
	PrincipalID   string
	PrincipalType string // "user" or "group"
}

// Validate checks that the request is well-formed.
func (r *BindRowFilterRequest) Validate() error {
	if r.RowFilterID == "" {
		return ErrValidation("row_filter_id is required")
	}
	if r.PrincipalID == "" {
		return ErrValidation("principal_id is required")
	}
	if r.PrincipalType != "user" && r.PrincipalType != "group" {
		return ErrValidation("principal_type must be 'user' or 'group'")
	}
	return nil
}

// RowFilterBinding binds a row filter to a principal or group.
type RowFilterBinding struct {
	ID            string
	RowFilterID   string
	PrincipalID   string
	PrincipalType string // "user" or "group"
}
