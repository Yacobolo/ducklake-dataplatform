//nolint:revive // orchestration components are exported for app wiring and tests.
package orchestration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
	servicepolicy "github.com/Yacobolo/quackstack/internal/service/policy"
)

type BackfillService struct {
	backfills domain.BackfillRepository
	router    *TriggerRouter
	audit     domain.AuditRepository
	auth      domain.AuthorizationService
}

const maxActiveBackfillsPerPrincipal = 4

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
	principalName, err := s.principalNameFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	if strings.TrimSpace(requestedBy) != "" && requestedBy != principalName {
		return nil, nil, domain.ErrAccessDenied("requested_by does not match authenticated principal")
	}
	if err := s.requirePrivilege(ctx, principalName, domain.PrivExecuteAssetMaterialization); err != nil {
		return nil, nil, err
	}
	if err := s.ensurePrincipalBackfillQuota(ctx, principalName); err != nil {
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
		RequestedBy:    principalName,
		MaxParallelism: maxParallelism,
	})
	if err != nil {
		return nil, nil, err
	}
	if s.audit != nil {
		_ = s.audit.Insert(ctx, &domain.AuditEntry{
			ID:            domain.NewID(),
			PrincipalName: principalName,
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
	}

	idemKey := backfillRequestEventIdempotencyKey(req.ID)
	if _, ingestErr := s.router.Ingest(ctx, domain.AssetTriggerTypeBackfill, &assetID, nil, map[string]any{
		"backfill_request_id": req.ID,
	}, &idemKey); ingestErr != nil {
		s.failCreate(ctx, req.ID, createdSliceIDs, ingestErr)
		return nil, nil, fmt.Errorf("enqueue backfill request %s: %w", req.ID, ingestErr)
	}

	return req, slices, nil
}

func backfillRequestEventIdempotencyKey(requestID string) string {
	return fmt.Sprintf("backfill-request:%s", requestID)
}

func (s *BackfillService) CanCreate(ctx context.Context, requestedBy string) (bool, error) {
	if s.router == nil || s.backfills == nil {
		return false, nil
	}
	principalName, err := s.principalNameFromContext(ctx)
	if err != nil {
		var accessDenied *domain.AccessDeniedError
		if errors.As(err, &accessDenied) {
			return false, nil
		}
		return false, err
	}
	if strings.TrimSpace(requestedBy) != "" && requestedBy != principalName {
		return false, nil
	}

	err = s.requirePrivilege(ctx, principalName, domain.PrivExecuteAssetMaterialization)
	if err == nil {
		quotaErr := s.ensurePrincipalBackfillQuota(ctx, principalName)
		if quotaErr == nil {
			return true, nil
		}
		var denied *domain.AccessDeniedError
		if errors.As(quotaErr, &denied) {
			return false, nil
		}
		return false, quotaErr
	}

	var accessDenied *domain.AccessDeniedError
	if errors.As(err, &accessDenied) {
		return false, nil
	}

	return false, err
}

func (s *BackfillService) principalNameFromContext(ctx context.Context) (string, error) {
	return servicepolicy.RequirePrincipalName(ctx)
}

func (s *BackfillService) ensurePrincipalBackfillQuota(ctx context.Context, principalName string) error {
	activeCount, err := s.activeBackfillCountByPrincipal(ctx, principalName)
	if err != nil {
		return err
	}
	if activeCount >= maxActiveBackfillsPerPrincipal {
		return domain.ErrAccessDenied("%q exceeded active backfill quota (%d)", principalName, maxActiveBackfillsPerPrincipal)
	}
	return nil
}

func (s *BackfillService) activeBackfillCountByPrincipal(ctx context.Context, principalName string) (int, error) {
	count := 0
	for _, status := range []string{domain.BackfillStatusPending, domain.BackfillStatusRunning} {
		status := status
		offset := 0
		for {
			requests, total, err := s.backfills.ListRequests(ctx, domain.BackfillFilter{
				Status: &status,
				Page: domain.PageRequest{
					MaxResults: domain.MaxMaxResults,
					PageToken:  domain.EncodePageToken(offset),
				},
			})
			if err != nil {
				return 0, fmt.Errorf("list backfill requests by status %s: %w", status, err)
			}
			for _, req := range requests {
				if req.RequestedBy == principalName {
					count++
				}
			}
			offset += len(requests)
			if len(requests) == 0 || int64(offset) >= total {
				break
			}
		}
	}
	return count, nil
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
	return servicepolicy.RequireCatalogPrivilege(ctx, s.auth, principalName, privilege, "asset orchestration requires %s on catalog")
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
