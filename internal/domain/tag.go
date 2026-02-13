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
