package resourceaccess

import (
	"context"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/resourceref"
)

const DefaultRecentLimit = 6

type Service struct {
	repo domain.ResourceAccessRepository
}

func NewService(repo domain.ResourceAccessRepository) *Service {
	return &Service{repo: repo}
}

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
