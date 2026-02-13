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
	SecurableType string // "schema", "table", "column"
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
	return nil
}

// TagAssignment represents a tag assigned to a securable object.
type TagAssignment struct {
	ID            string
	TagID         string
	SecurableType string // "schema", "table", "column"
	SecurableID   string
	ColumnName    *string
	AssignedBy    string
	AssignedAt    time.Time
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
