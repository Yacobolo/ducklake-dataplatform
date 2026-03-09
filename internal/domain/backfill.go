//nolint:revive // asset-centric domain model uses self-describing exported symbols.
package domain

import "time"

const (
	BackfillStatusPending   = "PENDING"
	BackfillStatusRunning   = "RUNNING"
	BackfillStatusSuccess   = "SUCCESS"
	BackfillStatusFailed    = "FAILED"
	BackfillStatusCancelled = "CANCELLED"
)

type BackfillRequest struct {
	ID             string
	AssetID        string
	PartitionFrom  string
	PartitionTo    string
	Status         string
	RequestedBy    string
	CreatedAt      time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
	ErrorMessage   *string
	MaxParallelism int
}

type BackfillSlice struct {
	ID           string
	RequestID    string
	AssetID      string
	PartitionKey string
	Status       string
	RunID        *string
	CreatedAt    time.Time
	StartedAt    *time.Time
	FinishedAt   *time.Time
	ErrorMessage *string
	AttemptCount int
	MaxAttempts  int
}

type BackfillFilter struct {
	AssetID *string
	Status  *string
	Page    PageRequest
}
