//nolint:revive // asset-centric domain model uses self-describing exported symbols.
package domain

import "time"

const (
	OrchestrationEventStatusPending    = "PENDING"
	OrchestrationEventStatusProcessing = "PROCESSING"
	OrchestrationEventStatusProcessed  = "PROCESSED"
	OrchestrationEventStatusFailed     = "FAILED"
)

type OrchestrationEvent struct {
	ID             string
	EventType      string
	AssetID        *string
	PartitionKey   *string
	PayloadJSON    map[string]any
	Status         string
	AttemptCount   int
	AvailableAt    time.Time
	LastError      *string
	IdempotencyKey *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type OrchestrationEventFilter struct {
	Status *string
	Page   PageRequest
}
