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

// Classification tag key prefixes for governed taxonomy.
const (
	ClassificationPrefix = "classification"
	SensitivityPrefix    = "sensitivity"
)

// Well-known classification values.
var ValidClassifications = map[string][]string{
	ClassificationPrefix: {"pii", "sensitive", "confidential", "public", "personal_data"},
	SensitivityPrefix:    {"high", "medium", "low"},
}
