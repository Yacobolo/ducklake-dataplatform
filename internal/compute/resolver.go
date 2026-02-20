package compute

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	computerouter "duck-demo/internal/compute/router"
	"duck-demo/internal/domain"
)

const assignmentLookupPageSize = 200

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
	selector      computerouter.EndpointSelector
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
		selector:      computerouter.NewActiveFirstSelector(),
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

		selected, err := r.selectFromAssignments(ctx, principal.ID, groups)
		if err != nil {
			return nil, err
		}
		if selected != nil {
			return r.resolveEndpoint(ctx, selected)
		}
	} else {
		selected, err := r.selectFromAssignments(ctx, principal.ID, nil)
		if err != nil {
			return nil, err
		}
		if selected != nil {
			return r.resolveEndpoint(ctx, selected)
		}
	}

	// 4. Default: local fallback
	return nil, nil
}

func (r *DefaultResolver) selectFromAssignments(ctx context.Context, principalID string, groups []domain.Group) (*domain.ComputeEndpoint, error) {
	if r.computeRepo == nil {
		return nil, fmt.Errorf("compute repository is not configured")
	}

	candidates := make([]domain.ComputeEndpoint, 0)
	seen := map[string]struct{}{}

	appendUnique := func(endpoints []domain.ComputeEndpoint) {
		for _, ep := range endpoints {
			if _, ok := seen[ep.ID]; ok {
				continue
			}
			seen[ep.ID] = struct{}{}
			candidates = append(candidates, ep)
		}
	}

	userEndpoints, err := r.computeRepo.GetAssignmentsForPrincipal(ctx, principalID, "user")
	if err != nil {
		return nil, fmt.Errorf("resolve user assignments: %w", err)
	}
	appendUnique(userEndpoints)

	for _, g := range groups {
		groupEndpoints, err := r.computeRepo.GetAssignmentsForPrincipal(ctx, g.ID, "group")
		if err != nil {
			return nil, fmt.Errorf("resolve group assignments: %w", err)
		}
		appendUnique(groupEndpoints)
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	if r.selector == nil {
		r.selector = computerouter.NewActiveFirstSelector()
	}

	return r.selector.Select(ctx, candidates)
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
		fallbackLocal, lookupErr := r.fallbackLocalEnabled(ctx, ep)
		if lookupErr != nil {
			return nil, fmt.Errorf("resolve assignment fallback policy for endpoint %q: %w", ep.Name, lookupErr)
		}

		if r.logger != nil {
			r.logger.Warn("remote agent unhealthy", "endpoint", ep.Name, "error", err, "fallback_local", fallbackLocal)
		}

		if fallbackLocal {
			return nil, nil
		}

		return nil, fmt.Errorf("remote agent %q unhealthy: %w", ep.Name, err)
	}

	return remote, nil
}

func (r *DefaultResolver) fallbackLocalEnabled(ctx context.Context, ep *domain.ComputeEndpoint) (bool, error) {
	if r.computeRepo == nil {
		return false, fmt.Errorf("compute repository is not configured")
	}

	offset := 0
	for {
		assignments, total, err := r.computeRepo.ListAssignments(ctx, ep.ID, domain.PageRequest{
			MaxResults: assignmentLookupPageSize,
			PageToken:  domain.EncodePageToken(offset),
		})
		if err != nil {
			return false, fmt.Errorf("list assignments for endpoint %q: %w", ep.Name, err)
		}

		for _, assignment := range assignments {
			if assignment.IsDefault && assignment.FallbackLocal {
				return true, nil
			}
		}

		offset += len(assignments)
		if int64(offset) >= total || len(assignments) == 0 {
			return false, nil
		}
	}
}
