package compute

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"duck-demo/internal/domain"
)

var _ domain.ComputeResolver = (*DefaultResolver)(nil)

// DefaultResolver implements ComputeResolver. It resolves a principal to a
// ComputeExecutor by looking up compute assignments in the repository.
// Resolution order: direct user assignment → group assignments → local fallback.
type DefaultResolver struct {
	localExec     *LocalExecutor
	computeRepo   domain.ComputeEndpointRepository
	principalRepo domain.PrincipalRepository
	groupRepo     domain.GroupRepository
	cache         *RemoteCache
	logger        *slog.Logger
}

// NewResolver creates a fully-wired resolver that can resolve principals to
// remote executors based on compute assignments.
func NewResolver(
	localExec *LocalExecutor,
	computeRepo domain.ComputeEndpointRepository,
	principalRepo domain.PrincipalRepository,
	groupRepo domain.GroupRepository,
	cache *RemoteCache,
	logger *slog.Logger,
) *DefaultResolver {
	return &DefaultResolver{
		localExec:     localExec,
		computeRepo:   computeRepo,
		principalRepo: principalRepo,
		groupRepo:     groupRepo,
		cache:         cache,
		logger:        logger,
	}
}

// Resolve maps a principal name to a ComputeExecutor. Returns nil when no
// compute endpoint is assigned (engine falls back to local *sql.DB).
//
// Resolution order:
//  1. Direct user assignment (is_default=true, status=ACTIVE)
//  2. Group assignments (check each group the user belongs to)
//  3. nil (local fallback)
func (r *DefaultResolver) Resolve(ctx context.Context, principalName string) (domain.ComputeExecutor, error) {
	if r.computeRepo == nil || r.principalRepo == nil {
		return nil, fmt.Errorf("compute resolver is not fully configured")
	}

	// 1. Look up principal
	principal, err := r.principalRepo.GetByName(ctx, principalName)
	if err != nil {
		// If principal not found, fall back to local
		var notFound *domain.NotFoundError
		if errors.As(err, &notFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("resolve principal %q: %w", principalName, err)
	}

	// 2. Check direct user assignment
	ep, err := r.computeRepo.GetDefaultForPrincipal(ctx, principal.ID, "user")
	if err == nil && ep != nil {
		return r.resolveEndpoint(ctx, ep)
	}
	// Ignore not-found errors — continue to group lookup
	var notFound *domain.NotFoundError
	if err != nil && !errors.As(err, &notFound) {
		return nil, fmt.Errorf("resolve user assignment: %w", err)
	}

	// 3. Check group assignments
	if r.groupRepo != nil {
		groups, err := r.groupRepo.GetGroupsForMember(ctx, "user", principal.ID)
		if err != nil {
			return nil, fmt.Errorf("resolve group membership: %w", err)
		}
		for _, g := range groups {
			ep, err := r.computeRepo.GetDefaultForPrincipal(ctx, g.ID, "group")
			if err == nil && ep != nil {
				return r.resolveEndpoint(ctx, ep)
			}
			if err != nil && !errors.As(err, &notFound) {
				return nil, fmt.Errorf("resolve group assignment: %w", err)
			}
		}
	}

	// 4. Default: local fallback
	return nil, nil
}

// resolveEndpoint returns a ComputeExecutor for the given endpoint.
// For LOCAL endpoints, returns the local executor.
// For REMOTE endpoints, returns a cached RemoteExecutor after a health check.
func (r *DefaultResolver) resolveEndpoint(ctx context.Context, ep *domain.ComputeEndpoint) (domain.ComputeExecutor, error) {
	if ep.Type == "LOCAL" {
		return r.localExec, nil
	}

	if r.cache == nil {
		return nil, fmt.Errorf("remote cache not configured for endpoint %q", ep.Name)
	}

	remote := r.cache.GetOrCreate(ep)

	// Health check
	if err := remote.Ping(ctx); err != nil {
		if r.logger != nil {
			r.logger.Warn("remote agent unhealthy", "endpoint", ep.Name, "error", err)
		}
		// Check if fallback_local is enabled for this assignment
		// For now, fall back to local on failure
		return nil, fmt.Errorf("remote agent %q unhealthy: %w", ep.Name, err)
	}

	return remote, nil
}
