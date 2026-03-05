//nolint:revive // orchestration components are exported for app wiring and tests.
package orchestration

import (
	"context"

	"duck-demo/internal/domain"
)

type TriggerRouter struct {
	events domain.OrchestrationEventRepository
}

func NewTriggerRouter(events domain.OrchestrationEventRepository) *TriggerRouter {
	return &TriggerRouter{events: events}
}

func (r *TriggerRouter) Ingest(ctx context.Context, eventType string, assetID *string, partitionKey *string, payload map[string]any, idemKey *string) (*domain.OrchestrationEvent, error) {
	return r.events.Enqueue(ctx, &domain.OrchestrationEvent{
		ID:             domain.NewID(),
		EventType:      eventType,
		AssetID:        assetID,
		PartitionKey:   partitionKey,
		PayloadJSON:    payload,
		Status:         domain.OrchestrationEventStatusPending,
		IdempotencyKey: idemKey,
	})
}
