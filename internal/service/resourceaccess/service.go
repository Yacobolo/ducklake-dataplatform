// Package resourceaccess provides principal-scoped recent resource activity services.
package resourceaccess

import (
	"context"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/resourceref"
)

// DefaultRecentLimit is the default number of recent resources returned to callers.
const DefaultRecentLimit = 6

// Service manages recent resource access history for a principal.
type Service struct {
	repo domain.ResourceAccessRepository
}

// NewService constructs a recent resource access service.
func NewService(repo domain.ResourceAccessRepository) *Service {
	return &Service{repo: repo}
}

// TrackVisit records a recent visit when the resource is eligible for recent history.
func (s *Service) TrackVisit(ctx context.Context, principal domain.ContextPrincipal, resource domain.ResourceRef) error {
	normalized, err := resourceref.Normalize(resource)
	if err != nil {
		return err
	}
	if !resourceref.IsRecentResource(normalized) {
		return nil
	}
	if err := requirePrincipal(principal); err != nil {
		return err
	}
	return s.repo.TrackVisit(ctx, principal.ID, normalized)
}

// ListRecent returns hydrated recent resources for the principal.
func (s *Service) ListRecent(ctx context.Context, principal domain.ContextPrincipal, limit int) ([]domain.ResourceAccessEvent, error) {
	if err := requirePrincipal(principal); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = DefaultRecentLimit
	}
	fetchLimit := limit
	if fetchLimit < 50 {
		fetchLimit = 50
	}
	items, err := s.repo.ListRecent(ctx, principal.ID, fetchLimit)
	if err != nil {
		return nil, err
	}
	hydrated, err := resourceref.HydrateRecent(items)
	if err != nil {
		return nil, err
	}
	filtered := make([]domain.ResourceAccessEvent, 0, min(limit, len(hydrated)))
	for i := range hydrated {
		if !resourceref.IsRecentResource(hydrated[i].ResourceRef) {
			continue
		}
		filtered = append(filtered, hydrated[i])
		if len(filtered) == limit {
			break
		}
	}
	return filtered, nil
}

func requirePrincipal(principal domain.ContextPrincipal) error {
	if strings.TrimSpace(principal.ID) == "" {
		return domain.ErrValidation("principal_id is required")
	}
	return nil
}
