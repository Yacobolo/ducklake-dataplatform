//nolint:revive // service methods are exported for API wiring.
package asset

import (
	"context"
	"fmt"
	"sort"
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
	auth       domain.AuthorizationService
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
	auth domain.AuthorizationService,
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
		auth:       auth,
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

func (s *Service) ListCheckResults(ctx context.Context, assetID string, page domain.PageRequest) ([]domain.AssetCheckResult, int64, error) {
	checks, err := s.checks.ListChecksByAsset(ctx, assetID)
	if err != nil {
		return nil, 0, err
	}

	allResults := make([]domain.AssetCheckResult, 0)
	for i := range checks {
		offset := 0
		for {
			results, total, listErr := s.checks.ListCheckResults(ctx, checks[i].ID, domain.PageRequest{MaxResults: domain.MaxMaxResults, PageToken: domain.EncodePageToken(offset)})
			if listErr != nil {
				return nil, 0, listErr
			}
			allResults = append(allResults, results...)

			offset += len(results)
			if len(results) == 0 || int64(offset) >= total {
				break
			}
		}
	}

	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].CreatedAt.After(allResults[j].CreatedAt)
	})

	total := int64(len(allResults))
	offset := page.Offset()
	if offset >= len(allResults) {
		return []domain.AssetCheckResult{}, total, nil
	}

	end := offset + page.Limit()
	if end > len(allResults) {
		end = len(allResults)
	}

	return allResults[offset:end], total, nil
}

func (s *Service) ListBackfills(ctx context.Context, filter domain.BackfillFilter) ([]domain.BackfillRequest, int64, error) {
	return s.backfills.ListRequests(ctx, filter)
}

func (s *Service) GetBackfill(ctx context.Context, assetID, backfillID string) (*domain.BackfillRequest, []domain.BackfillSlice, error) {
	if strings.TrimSpace(assetID) == "" {
		return nil, nil, domain.ErrValidation("asset_id is required")
	}
	if strings.TrimSpace(backfillID) == "" {
		return nil, nil, domain.ErrValidation("backfill_id is required")
	}

	request, err := s.backfills.GetRequestByID(ctx, backfillID)
	if err != nil {
		return nil, nil, err
	}
	if request.AssetID != assetID {
		return nil, nil, domain.ErrNotFound("backfill request not found")
	}

	slices, err := s.backfills.ListSlicesByRequest(ctx, request.ID)
	if err != nil {
		return nil, nil, err
	}

	return request, slices, nil
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
	if err := s.requirePrivilege(ctx, domain.PrivExecuteAssetMaterialization); err != nil {
		return nil, err
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

func (s *Service) requirePrivilege(ctx context.Context, privilege string) error {
	principal, _ := domain.PrincipalFromContext(ctx)
	if strings.TrimSpace(principal.Name) == "" {
		return domain.ErrAccessDenied("principal context is required")
	}
	if s.auth == nil {
		if principal.IsAdmin {
			return nil
		}
		return domain.ErrAccessDenied("asset orchestration requires %s on catalog", privilege)
	}

	allowed, err := s.auth.CheckPrivilege(ctx, principal.Name, domain.SecurableCatalog, domain.CatalogID, privilege)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return domain.ErrAccessDenied("%q lacks %s on catalog", principal.Name, privilege)
	}
	return nil
}
