//nolint:revive // orchestration components are exported for app wiring and tests.
package orchestration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"duck-demo/internal/domain"
)

type BackfillService struct {
	backfills domain.BackfillRepository
	router    *TriggerRouter
	audit     domain.AuditRepository
	auth      domain.AuthorizationService
}

func NewBackfillService(backfills domain.BackfillRepository, router *TriggerRouter, audit domain.AuditRepository, auth domain.AuthorizationService) *BackfillService {
	return &BackfillService{backfills: backfills, router: router, audit: audit, auth: auth}
}

func (s *BackfillService) Create(ctx context.Context, assetID, requestedBy, from, to string, maxParallelism int) (*domain.BackfillRequest, []domain.BackfillSlice, error) {
	if strings.TrimSpace(assetID) == "" {
		return nil, nil, domain.ErrValidation("asset_id is required")
	}
	if s.router == nil {
		return nil, nil, domain.ErrValidation("trigger router is required")
	}
	if err := s.requirePrivilege(ctx, requestedBy, domain.PrivExecuteAssetMaterialization); err != nil {
		return nil, nil, err
	}
	if from == "" || to == "" {
		return nil, nil, domain.ErrValidation("partition_from and partition_to are required")
	}

	keys, err := partitionRange(from, to)
	if err != nil {
		return nil, nil, err
	}

	req, err := s.backfills.CreateRequest(ctx, &domain.BackfillRequest{
		ID:             domain.NewID(),
		AssetID:        assetID,
		PartitionFrom:  from,
		PartitionTo:    to,
		Status:         domain.BackfillStatusPending,
		RequestedBy:    requestedBy,
		MaxParallelism: maxParallelism,
	})
	if err != nil {
		return nil, nil, err
	}
	if s.audit != nil {
		_ = s.audit.Insert(ctx, &domain.AuditEntry{
			ID:            domain.NewID(),
			PrincipalName: requestedBy,
			Action:        "asset.backfill.create",
			Status:        "ALLOWED",
			CreatedAt:     req.CreatedAt,
		})
	}

	slices := make([]domain.BackfillSlice, 0, len(keys))
	createdSliceIDs := make([]string, 0, len(keys))
	for _, key := range keys {
		slice, createErr := s.backfills.CreateSlice(ctx, &domain.BackfillSlice{
			ID:           domain.NewID(),
			RequestID:    req.ID,
			AssetID:      assetID,
			PartitionKey: key,
			Status:       domain.BackfillStatusPending,
			MaxAttempts:  1,
		})
		if createErr != nil {
			s.failCreate(ctx, req.ID, createdSliceIDs, createErr)
			return nil, nil, createErr
		}
		slices = append(slices, *slice)
		createdSliceIDs = append(createdSliceIDs, slice.ID)

		if _, ingestErr := s.router.Ingest(ctx, domain.AssetTriggerTypeBackfill, &assetID, &key, map[string]any{
			"backfill_request_id": req.ID,
		}, nil); ingestErr != nil {
			s.failCreate(ctx, req.ID, createdSliceIDs, ingestErr)
			return nil, nil, fmt.Errorf("enqueue backfill slice %s: %w", key, ingestErr)
		}
	}

	return req, slices, nil
}

func (s *BackfillService) failCreate(ctx context.Context, requestID string, sliceIDs []string, cause error) {
	errMsg := cause.Error()
	if requestID != "" {
		_ = s.backfills.UpdateRequestStatus(ctx, requestID, domain.BackfillStatusFailed, &errMsg)
	}
	for _, sliceID := range sliceIDs {
		_ = s.backfills.UpdateSliceStatus(ctx, sliceID, domain.BackfillStatusFailed, nil, &errMsg)
	}
}

func (s *BackfillService) requirePrivilege(ctx context.Context, principalName string, privilege string) error {
	if strings.TrimSpace(principalName) == "" {
		return domain.ErrAccessDenied("principal context is required")
	}
	principal, _ := domain.PrincipalFromContext(ctx)
	if s.auth == nil {
		if principal.IsAdmin {
			return nil
		}
		return domain.ErrAccessDenied("asset orchestration requires %s on catalog", privilege)
	}

	allowed, err := s.auth.CheckPrivilege(ctx, principalName, domain.SecurableCatalog, domain.CatalogID, privilege)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return domain.ErrAccessDenied("%q lacks %s on catalog", principalName, privilege)
	}
	return nil
}

func partitionRange(from, to string) ([]string, error) {
	if from > to {
		return nil, domain.ErrValidation("partition_from must be <= partition_to")
	}
	if from == to {
		return []string{from}, nil
	}
	// Current implementation expects lexicographically ordered partition keys,
	// suitable for YYYY-MM-DD style keys.
	keys := []string{from}
	if len(from) != len(to) {
		return nil, domain.ErrValidation("partition range format mismatch")
	}
	if len(from) == 10 && strings.Count(from, "-") == 2 {
		current := from
		for current < to {
			next, err := nextDay(current)
			if err != nil {
				return nil, err
			}
			current = next
			keys = append(keys, current)
		}
		if keys[len(keys)-1] != to {
			return nil, domain.ErrValidation("invalid daily partition range")
		}
		return keys, nil
	}
	return nil, fmt.Errorf("unsupported partition range format: %s..%s", from, to)
}

func nextDay(day string) (string, error) {
	t, err := time.Parse("2006-01-02", day)
	if err != nil {
		return "", domain.ErrValidation("invalid date partition %q", day)
	}
	return t.AddDate(0, 0, 1).Format("2006-01-02"), nil
}
