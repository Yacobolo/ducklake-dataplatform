package notebook

import (
	"context"
	"fmt"
	"strings"
	"time"

	"duck-demo/internal/domain"
)

// OrchestrationEventHandler processes notebook-related orchestration events.
type OrchestrationEventHandler struct {
	invalidator notebookContextInvalidator
}

// OrchestrationEventEnqueuer converts notebook invalidation requests into orchestration events.
type OrchestrationEventEnqueuer struct {
	events domain.OrchestrationEventRepository
}

// NewOrchestrationEventEnqueuer creates a queue-backed notebook invalidator.
func NewOrchestrationEventEnqueuer(events domain.OrchestrationEventRepository) *OrchestrationEventEnqueuer {
	return &OrchestrationEventEnqueuer{events: events}
}

// InvalidateNotebook enqueues a context invalidation event for a notebook.
func (e *OrchestrationEventEnqueuer) InvalidateNotebook(ctx context.Context, notebookID string) error {
	if e == nil || e.events == nil {
		return nil
	}
	notebookID = strings.TrimSpace(notebookID)
	if notebookID == "" {
		return nil
	}
	key := fmt.Sprintf("%s:%s", domain.NotebookEventTypeInvalidateContext, notebookID)
	_, err := e.events.Enqueue(ctx, &domain.OrchestrationEvent{
		EventType:      domain.NotebookEventTypeInvalidateContext,
		PayloadJSON:    map[string]any{domain.NotebookEventPayloadNotebookID: notebookID},
		Status:         domain.OrchestrationEventStatusPending,
		IdempotencyKey: &key,
	})
	if err != nil {
		return fmt.Errorf("enqueue notebook invalidation: %w", err)
	}
	return nil
}

// NewOrchestrationEventHandler creates a notebook event handler for queue-driven invalidation.
func NewOrchestrationEventHandler(invalidator notebookContextInvalidator) *OrchestrationEventHandler {
	return &OrchestrationEventHandler{invalidator: invalidator}
}

// HandleOrchestrationEvent handles notebook orchestration events.
func (h *OrchestrationEventHandler) HandleOrchestrationEvent(ctx context.Context, event *domain.OrchestrationEvent) (bool, *time.Time, error) {
	if event == nil || event.EventType != domain.NotebookEventTypeInvalidateContext {
		return false, nil, nil
	}
	if h.invalidator == nil {
		return true, nil, nil
	}
	rawNotebookID, _ := event.PayloadJSON[domain.NotebookEventPayloadNotebookID].(string)
	notebookID := strings.TrimSpace(rawNotebookID)
	if notebookID == "" {
		return true, nil, fmt.Errorf("missing %q in notebook invalidation event payload", domain.NotebookEventPayloadNotebookID)
	}
	if err := h.invalidator.InvalidateNotebook(ctx, notebookID); err != nil {
		return true, nil, fmt.Errorf("invalidate notebook %s: %w", notebookID, err)
	}
	return true, nil, nil
}
