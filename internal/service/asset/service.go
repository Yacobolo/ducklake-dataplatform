//nolint:revive // service methods are exported for API wiring.
package asset

import (
	"context"
	"strings"

	"duck-demo/internal/domain"
)

type Service struct {
	assets     domain.DataAssetRepository
	deps       domain.AssetDependencyRepository
	partitions domain.AssetPartitionRepository
	runs       domain.AssetRunRepository
	checks     domain.AssetCheckRepository
	backfills  domain.BackfillRepository
	events     domain.OrchestrationEventRepository
	audit      domain.AuditRepository
}

func NewService(
	assets domain.DataAssetRepository,
	deps domain.AssetDependencyRepository,
	partitions domain.AssetPartitionRepository,
	runs domain.AssetRunRepository,
	checks domain.AssetCheckRepository,
	backfills domain.BackfillRepository,
	events domain.OrchestrationEventRepository,
	audit domain.AuditRepository,
) *Service {
	return &Service{
		assets:     assets,
		deps:       deps,
		partitions: partitions,
		runs:       runs,
		checks:     checks,
		backfills:  backfills,
		events:     events,
		audit:      audit,
	}
}

func (s *Service) ListAssets(ctx context.Context, filter domain.AssetFilter) ([]domain.DataAsset, int64, error) {
	return s.assets.List(ctx, filter)
}

func (s *Service) GetAsset(ctx context.Context, key string) (*domain.DataAsset, error) {
	return s.assets.GetByKey(ctx, key)
}

func (s *Service) ListRuns(ctx context.Context, filter domain.AssetRunFilter) ([]domain.AssetRun, int64, error) {
	return s.runs.ListRuns(ctx, filter)
}

func (s *Service) ListMaterializations(ctx context.Context, assetID string, page domain.PageRequest) ([]domain.AssetMaterialization, int64, error) {
	return s.runs.ListMaterializationsByAsset(ctx, assetID, page)
}

func (s *Service) ListChecks(ctx context.Context, assetID string) ([]domain.AssetCheck, error) {
	return s.checks.ListChecksByAsset(ctx, assetID)
}

func (s *Service) ListBackfills(ctx context.Context, filter domain.BackfillFilter) ([]domain.BackfillRequest, int64, error) {
	return s.backfills.ListRequests(ctx, filter)
}

func (s *Service) ListPartitions(ctx context.Context, assetID string, page domain.PageRequest) ([]domain.AssetPartition, int64, error) {
	return s.partitions.ListByAsset(ctx, assetID, page)
}

func (s *Service) GetGraph(ctx context.Context, assetID string) ([]domain.AssetDependency, []domain.AssetDependency, error) {
	upstream, err := s.deps.ListUpstream(ctx, assetID)
	if err != nil {
		return nil, nil, err
	}
	downstream, err := s.deps.ListDownstream(ctx, assetID)
	if err != nil {
		return nil, nil, err
	}
	return upstream, downstream, nil
}

func (s *Service) TriggerMaterialization(ctx context.Context, assetID string, partitionKey *string, payload map[string]any, idempotencyKey *string) (*domain.OrchestrationEvent, error) {
	if strings.TrimSpace(assetID) == "" {
		return nil, domain.ErrValidation("asset_id is required")
	}
	if payload == nil {
		payload = map[string]any{}
	}
	principal, _ := domain.PrincipalFromContext(ctx)

	event, err := s.events.Enqueue(ctx, &domain.OrchestrationEvent{
		ID:             domain.NewID(),
		EventType:      domain.AssetTriggerTypeManual,
		AssetID:        &assetID,
		PartitionKey:   partitionKey,
		PayloadJSON:    payload,
		Status:         domain.OrchestrationEventStatusPending,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return nil, err
	}

	if s.audit != nil {
		_ = s.audit.Insert(ctx, &domain.AuditEntry{
			ID:            domain.NewID(),
			PrincipalName: principal.Name,
			Action:        "asset.materialize.trigger",
			Status:        "ALLOWED",
			CreatedAt:     event.CreatedAt,
		})
	}

	return event, nil
}
