package domain

import "time"

// Tag represents a tag definition.
type Tag struct {
	ID        string
	Key       string
	Value     *string
	CreatedBy string
	CreatedAt time.Time
}

// CreateTagRequest holds parameters for creating a new tag.
type CreateTagRequest struct {
	Key   string
	Value *string
}

// Validate checks that the request is well-formed.
func (r *CreateTagRequest) Validate() error {
	if r.Key == "" {
		return ErrValidation("tag key is required")
	}
	return nil
}

// AssignTagRequest holds parameters for assigning a tag to a securable.
type AssignTagRequest struct {
	TagID         string
	SecurableType string // "schema", "table", "column", "macro"
	SecurableID   string
	ColumnName    *string
}

// Validate checks that the request is well-formed.
func (r *AssignTagRequest) Validate() error {
	if r.TagID == "" {
		return ErrValidation("tag_id is required")
	}
	if r.SecurableType == "" {
		return ErrValidation("securable_type is required")
	}
	if !IsValidTagSecurableType(r.SecurableType) {
		return ErrValidation("securable_type must be one of: schema, table, column, macro")
	}
	return nil
}

// TagAssignment represents a tag assigned to a securable object.
type TagAssignment struct {
	ID            string
	TagID         string
	SecurableType string // "schema", "table", "column", "macro"
	SecurableID   string
	ColumnName    *string
	AssignedBy    string
	AssignedAt    time.Time
}

// Valid securable types for tag assignments.
const (
	TagSecurableTypeSchema = "schema"
	TagSecurableTypeTable  = "table"
	TagSecurableTypeColumn = "column"
	TagSecurableTypeMacro  = "macro"
)

// IsValidTagSecurableType reports whether the securable type supports tag assignment.
func IsValidTagSecurableType(securableType string) bool {
	switch securableType {
	case TagSecurableTypeSchema, TagSecurableTypeTable, TagSecurableTypeColumn, TagSecurableTypeMacro:
		return true
	default:
		return false
	}
}

// Classification tag key prefixes for governed taxonomy.
const (
	ClassificationPrefix = "classification"
	SensitivityPrefix    = "sensitivity"
)

// ValidClassifications maps classification tag keys to their allowed values.
var ValidClassifications = map[string][]string{
	ClassificationPrefix: {"pii", "sensitive", "confidential", "public", "personal_data"},
	SensitivityPrefix:    {"high", "medium", "low"},
}
