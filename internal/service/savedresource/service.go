// Package savedresource provides principal-scoped saved resource services.
package savedresource

import (
	"context"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/service/resourceref"
)

// DefaultSavedLimit is the default number of saved resources returned to callers.
const DefaultSavedLimit = 6

// Service manages saved resources for a principal.
type Service struct {
	repo domain.SavedResourceRepository
}

// NewService constructs a saved resource service.
func NewService(repo domain.SavedResourceRepository) *Service {
	return &Service{repo: repo}
}

// Save persists a saved resource for the principal.
func (s *Service) Save(ctx context.Context, principal domain.ContextPrincipal, resource domain.ResourceRef) error {
	normalized, err := resourceref.Normalize(resource)
	if err != nil {
		return err
	}
	if !resourceref.IsRecentResource(normalized) {
		return domain.ErrValidation("saved resources must be UUID-backed resources")
	}
	if err := requirePrincipal(principal); err != nil {
		return err
	}
	return s.repo.Save(ctx, principal.ID, normalized)
}

// Unsave removes a saved resource for the principal.
func (s *Service) Unsave(ctx context.Context, principal domain.ContextPrincipal, resourceType string, resourceKey string) error {
	if err := requirePrincipal(principal); err != nil {
		return err
	}
	resourceType = strings.TrimSpace(resourceType)
	ref, err := resourceref.Normalize(domain.ResourceRef{
		ResourceType: resourceType,
		ResourceKey:  resourceKey,
	})
	if err != nil {
		return err
	}
	return s.repo.Unsave(ctx, principal.ID, resourceType, ref.ResourceKey)
}

// ListSaved returns hydrated saved resources for the principal.
func (s *Service) ListSaved(ctx context.Context, principal domain.ContextPrincipal, limit int) ([]domain.SavedResource, error) {
	if err := requirePrincipal(principal); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = DefaultSavedLimit
	}
	fetchLimit := limit
	if fetchLimit < 50 {
		fetchLimit = 50
	}
	items, err := s.repo.ListSaved(ctx, principal.ID, fetchLimit)
	if err != nil {
		return nil, err
	}
	hydrated, err := resourceref.HydrateSaved(items)
	if err != nil {
		return nil, err
	}
	filtered := make([]domain.SavedResource, 0, min(limit, len(hydrated)))
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
