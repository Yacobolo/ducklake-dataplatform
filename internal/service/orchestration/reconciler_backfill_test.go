package orchestration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestReconciler_ShadowModeProcessesEvent(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-1"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:        "ev-1",
		EventType: domain.AssetTriggerTypeUpstreamUpdate,
		AssetID:   &assetID,
		Status:    domain.OrchestrationEventStatusPending,
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{assetID: {ID: assetID}}}
	runRepo := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	scheduler := NewAssetScheduler(assetRepo, &fakeDeps{downstream: map[string][]domain.AssetDependency{}}, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})

	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, true)
	require.NoError(t, r.Tick(context.Background()))

	status := domain.OrchestrationEventStatusProcessed
	list, total, err := eventRepo.List(context.Background(), domain.OrchestrationEventFilter{Status: &status, Page: domain.PageRequest{MaxResults: 10}})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, list, 1)
}

func TestBackfillService_CreateEmitsEvents(t *testing.T) {
	eventRepo := &memEventRepo{}
	router := NewTriggerRouter(eventRepo)
	backfills := &memBackfillRepo{}
	svc := NewBackfillService(backfills, router, nil)

	req, slices, err := svc.Create(context.Background(), "asset-1", "admin", "2026-03-01", "2026-03-03", 2)
	require.NoError(t, err)
	require.NotNil(t, req)
	assert.Len(t, slices, 3)
	assert.Len(t, backfills.slices, 3)
	assert.Len(t, eventRepo.events, 3)
}

type memEventRepo struct {
	events []domain.OrchestrationEvent
}

func (m *memEventRepo) Enqueue(_ context.Context, event *domain.OrchestrationEvent) (*domain.OrchestrationEvent, error) {
	eventCopy := *event
	if eventCopy.ID == "" {
		eventCopy.ID = domain.NewID()
	}
	if eventCopy.AvailableAt.IsZero() {
		eventCopy.AvailableAt = time.Now().UTC()
	}
	m.events = append(m.events, eventCopy)
	return &eventCopy, nil
}

func (m *memEventRepo) ClaimNextPending(_ context.Context, now time.Time) (*domain.OrchestrationEvent, error) {
	for i := range m.events {
		e := &m.events[i]
		if e.Status == domain.OrchestrationEventStatusPending && !e.AvailableAt.After(now) {
			e.Status = domain.OrchestrationEventStatusProcessing
			e.AttemptCount++
			return e, nil
		}
	}
	return nil, domain.ErrNotFound("no pending orchestration event")
}

func (m *memEventRepo) MarkProcessed(_ context.Context, id string) error {
	for i := range m.events {
		if m.events[i].ID == id {
			m.events[i].Status = domain.OrchestrationEventStatusProcessed
			return nil
		}
	}
	return domain.ErrNotFound("event not found")
}

func (m *memEventRepo) MarkFailed(_ context.Context, id string, errMsg string, retryAt *time.Time) error {
	for i := range m.events {
		if m.events[i].ID == id {
			if retryAt != nil {
				m.events[i].Status = domain.OrchestrationEventStatusPending
				m.events[i].AvailableAt = *retryAt
			} else {
				m.events[i].Status = domain.OrchestrationEventStatusFailed
			}
			m.events[i].LastError = &errMsg
			return nil
		}
	}
	return domain.ErrNotFound("event not found")
}

func (m *memEventRepo) List(_ context.Context, filter domain.OrchestrationEventFilter) ([]domain.OrchestrationEvent, int64, error) {
	out := make([]domain.OrchestrationEvent, 0)
	for _, e := range m.events {
		if filter.Status == nil || e.Status == *filter.Status {
			out = append(out, e)
		}
	}
	return out, int64(len(out)), nil
}

type memBackfillRepo struct {
	requests []domain.BackfillRequest
	slices   []domain.BackfillSlice
}

func (m *memBackfillRepo) CreateRequest(_ context.Context, req *domain.BackfillRequest) (*domain.BackfillRequest, error) {
	copyReq := *req
	if copyReq.ID == "" {
		copyReq.ID = domain.NewID()
	}
	m.requests = append(m.requests, copyReq)
	return &copyReq, nil
}

func (m *memBackfillRepo) GetRequestByID(_ context.Context, id string) (*domain.BackfillRequest, error) {
	for i := range m.requests {
		if m.requests[i].ID == id {
			return &m.requests[i], nil
		}
	}
	return nil, domain.ErrNotFound("backfill request not found")
}

func (m *memBackfillRepo) ListRequests(_ context.Context, _ domain.BackfillFilter) ([]domain.BackfillRequest, int64, error) {
	return m.requests, int64(len(m.requests)), nil
}

func (m *memBackfillRepo) UpdateRequestStatus(_ context.Context, _ string, _ string, _ *string) error {
	return nil
}

func (m *memBackfillRepo) CreateSlice(_ context.Context, slice *domain.BackfillSlice) (*domain.BackfillSlice, error) {
	copySlice := *slice
	if copySlice.ID == "" {
		copySlice.ID = domain.NewID()
	}
	m.slices = append(m.slices, copySlice)
	return &copySlice, nil
}

func (m *memBackfillRepo) ListSlicesByRequest(_ context.Context, requestID string) ([]domain.BackfillSlice, error) {
	out := make([]domain.BackfillSlice, 0)
	for _, s := range m.slices {
		if s.RequestID == requestID {
			out = append(out, s)
		}
	}
	return out, nil
}

func (m *memBackfillRepo) UpdateSliceStatus(_ context.Context, _ string, _ string, _ *string, _ *string) error {
	return nil
}
