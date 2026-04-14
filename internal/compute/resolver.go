package compute

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	computerouter "github.com/Yacobolo/quackstack/internal/compute/router"
	"github.com/Yacobolo/quackstack/internal/domain"
)

const assignmentLookupPageSize = 200

var _ domain.ComputeResolver = (*DefaultResolver)(nil)

// DefaultResolver implements ComputeResolver. It resolves a principal to a
// ComputeExecutor by looking up compute assignments in the repository.
// Resolution order: direct user assignment → group assignments → local fallback.
type DefaultResolver struct {
	localExec      *LocalExecutor
	computeRepo    domain.ComputeEndpointRepository
	principalRepo  domain.PrincipalRepository
	groupRepo      domain.GroupRepository
	cache          *RemoteCache
	selector       computerouter.EndpointSelector
	logger         *slog.Logger
	routingEnabled bool
	canaryUsers    map[string]struct{}
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
		localExec:      localExec,
		computeRepo:    computeRepo,
		principalRepo:  principalRepo,
		groupRepo:      groupRepo,
		cache:          cache,
		selector:       computerouter.NewActiveFirstSelector(),
		logger:         logger,
		routingEnabled: true,
		canaryUsers:    map[string]struct{}{},
	}
}

// SetRoutingEnabled toggles remote routing globally.
func (r *DefaultResolver) SetRoutingEnabled(enabled bool) {
	r.routingEnabled = enabled
}

// SetCanaryUsers restricts remote routing to a user allowlist.
// Empty list means all users are eligible.
func (r *DefaultResolver) SetCanaryUsers(users []string) {
	allow := make(map[string]struct{}, len(users))
	for _, user := range users {
		trimmed := strings.TrimSpace(user)
		if trimmed == "" {
			continue
		}
		allow[trimmed] = struct{}{}
	}
	r.canaryUsers = allow
}

// Resolve maps a principal name to a ComputeExecutor. Returns nil when no
// compute endpoint is assigned (engine falls back to local *sql.DB).
//
// Resolution order:
//  1. Direct user assignment (is_default=true, status=ACTIVE)
//  2. Group assignments (check each group the user belongs to)
//  3. nil (local fallback)
func (r *DefaultResolver) Resolve(ctx context.Context, principalName string) (domain.ComputeExecutor, error) {
	execReq, _ := domain.ComputeExecutionRequestFromContext(ctx)
	execReq = execReq.Normalize()

	if !r.routingEnabled {
		domain.RecordComputeResolution(ctx, domain.ComputeResolution{ResolvedMode: domain.ComputeModeByocLocal})
		return nil, nil
	}
	if len(r.canaryUsers) > 0 {
		if _, ok := r.canaryUsers[principalName]; !ok {
			domain.RecordComputeResolution(ctx, domain.ComputeResolution{ResolvedMode: domain.ComputeModeByocLocal})
			return nil, nil
		}
	}

	if r.computeRepo == nil || r.principalRepo == nil {
		return nil, fmt.Errorf("compute resolver is not fully configured")
	}

	if execReq.Mode == domain.ComputeModeByocLocal {
		domain.RecordComputeResolution(ctx, domain.ComputeResolution{
			RequestedMode:     execReq.Mode,
			RequestedEndpoint: execReq.EndpointName,
			ResolvedMode:      domain.ComputeModeByocLocal,
		})
		return nil, nil
	}

	// 1. Look up principal
	principal, err := r.principalRepo.GetByName(ctx, principalName)
	if err != nil {
		// If principal not found, fall back to local
		var notFound *domain.NotFoundError
		if errors.As(err, &notFound) {
			domain.RecordComputeResolution(ctx, domain.ComputeResolution{ResolvedMode: domain.ComputeModeByocLocal})
			return nil, nil
		}
		return nil, fmt.Errorf("resolve principal %q: %w", principalName, err)
	}

	if execReq.EndpointName != "" {
		if execReq.AuthoritativeEndpoint {
			selected, err := r.computeRepo.GetByName(ctx, execReq.EndpointName)
			if err != nil {
				return nil, fmt.Errorf("resolve authoritative compute endpoint %q: %w", execReq.EndpointName, err)
			}
			if !endpointEligibleForRequest(*selected, execReq) {
				return nil, domain.ErrAccessDenied("compute endpoint %q is not eligible for the requested workload", execReq.EndpointName)
			}
			return r.resolveEndpoint(ctx, selected, execReq)
		}
		selected, err := r.resolveRequestedEndpoint(ctx, principal.ID, execReq.EndpointName)
		if err != nil {
			return nil, err
		}
		return r.resolveEndpoint(ctx, selected, execReq)
	}

	// 2. Check direct user assignment
	ep, err := r.computeRepo.GetDefaultForPrincipal(ctx, principal.ID, "user")
	if err == nil && ep != nil {
		if endpointEligibleForRequest(*ep, execReq) {
			return r.resolveEndpoint(ctx, ep, execReq)
		}
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
				if endpointEligibleForRequest(*ep, execReq) {
					return r.resolveEndpoint(ctx, ep, execReq)
				}
			}
			if err != nil && !errors.As(err, &notFound) {
				return nil, fmt.Errorf("resolve group assignment: %w", err)
			}
		}

		selected, err := r.selectFromAssignments(ctx, principal.ID, groups, execReq)
		if err != nil {
			return nil, err
		}
		if selected != nil {
			return r.resolveEndpoint(ctx, selected, execReq)
		}
	} else {
		selected, err := r.selectFromAssignments(ctx, principal.ID, nil, execReq)
		if err != nil {
			return nil, err
		}
		if selected != nil {
			return r.resolveEndpoint(ctx, selected, execReq)
		}
	}

	// 4. Default: local fallback
	domain.RecordComputeResolution(ctx, domain.ComputeResolution{ResolvedMode: domain.ComputeModeByocLocal})
	return nil, nil
}

func (r *DefaultResolver) selectFromAssignments(ctx context.Context, principalID string, groups []domain.Group, req domain.ComputeExecutionRequest) (*domain.ComputeEndpoint, error) {
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
			if !endpointEligibleForRequest(ep, req) {
				continue
			}
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

func endpointEligibleForRequest(ep domain.ComputeEndpoint, req domain.ComputeExecutionRequest) bool {
	if ep.Status != "ACTIVE" {
		return false
	}
	readinessStatus := strings.ToUpper(strings.TrimSpace(ep.ReadinessStatus))
	if readinessStatus == "" {
		readinessStatus = domain.ComputeReadinessReady
	}
	if readinessStatus == domain.ComputeReadinessUnavailable {
		return false
	}
	if ep.IsDraining {
		return false
	}
	workloadClass := strings.ToUpper(strings.TrimSpace(ep.WorkloadClass))
	if workloadClass == "" {
		workloadClass = domain.ComputeEndpointWorkloadMixed
	}

	switch req.WorkloadType {
	case domain.ComputeWorkloadScheduled:
		return workloadClass == domain.ComputeEndpointWorkloadScheduled || workloadClass == domain.ComputeEndpointWorkloadMixed
	case domain.ComputeWorkloadHeavy:
		return workloadClass == domain.ComputeEndpointWorkloadHeavy || workloadClass == domain.ComputeEndpointWorkloadMixed
	case domain.ComputeWorkloadNotebook:
		return workloadClass != domain.ComputeEndpointWorkloadInteractive || workloadClass == domain.ComputeEndpointWorkloadMixed
	default:
		return workloadClass == domain.ComputeEndpointWorkloadInteractive || workloadClass == domain.ComputeEndpointWorkloadMixed
	}
}

// resolveEndpoint returns a ComputeExecutor for the given endpoint.
// For LOCAL endpoints, returns the local executor.
// For REMOTE endpoints, returns a cached RemoteExecutor after a health check.
func (r *DefaultResolver) resolveEndpoint(ctx context.Context, ep *domain.ComputeEndpoint, req domain.ComputeExecutionRequest) (domain.ComputeExecutor, error) {
	if ep.Type == "LOCAL" {
		domain.RecordComputeResolution(ctx, domain.ComputeResolution{
			RequestedMode:     req.Mode,
			RequestedEndpoint: req.EndpointName,
			ResolvedMode:      domain.ComputeModeByocLocal,
			ResolvedEndpoint:  ep.Name,
		})
		return r.localExec, nil
	}

	if r.cache == nil {
		return nil, fmt.Errorf("remote cache not configured for endpoint %q", ep.Name)
	}

	remote := r.cache.GetOrCreate(ep)

	// Health check
	if err := remote.Ping(ctx); err != nil {
		fallbackLocal := req.FallbackLocal
		if !req.AuthoritativeEndpoint {
			lookupFallbackLocal, lookupErr := r.fallbackLocalEnabled(ctx, ep)
			if lookupErr != nil {
				return nil, fmt.Errorf("resolve assignment fallback policy for endpoint %q: %w", ep.Name, lookupErr)
			}
			fallbackLocal = lookupFallbackLocal
		}

		if r.logger != nil {
			r.logger.Warn("remote agent unhealthy", "endpoint", ep.Name, "error", err, "fallback_local", fallbackLocal)
		}

		if fallbackLocal {
			domain.RecordComputeResolution(ctx, domain.ComputeResolution{
				RequestedMode:     req.Mode,
				RequestedEndpoint: req.EndpointName,
				ResolvedMode:      domain.ComputeModeByocLocal,
				ResolvedEndpoint:  ep.Name,
			})
			return nil, nil
		}

		return nil, fmt.Errorf("remote agent %q unhealthy: %w", ep.Name, err)
	}

	domain.RecordComputeResolution(ctx, domain.ComputeResolution{
		RequestedMode:     req.Mode,
		RequestedEndpoint: req.EndpointName,
		ResolvedMode:      domain.ComputeModeSharedEndpoint,
		ResolvedEndpoint:  ep.Name,
	})
	return remote, nil
}

func (r *DefaultResolver) resolveRequestedEndpoint(ctx context.Context, principalID, endpointName string) (*domain.ComputeEndpoint, error) {
	endpoints, err := r.computeRepo.GetAssignmentsForPrincipal(ctx, principalID, "user")
	if err != nil {
		return nil, fmt.Errorf("resolve requested user assignments: %w", err)
	}
	for _, ep := range endpoints {
		if ep.Name == endpointName {
			return &ep, nil
		}
	}

	if r.groupRepo != nil {
		groups, err := r.groupRepo.GetGroupsForMember(ctx, "user", principalID)
		if err != nil {
			return nil, fmt.Errorf("resolve requested group membership: %w", err)
		}
		for _, group := range groups {
			groupEndpoints, err := r.computeRepo.GetAssignmentsForPrincipal(ctx, group.ID, "group")
			if err != nil {
				return nil, fmt.Errorf("resolve requested group assignments: %w", err)
			}
			for _, ep := range groupEndpoints {
				if ep.Name == endpointName {
					return &ep, nil
				}
			}
		}
	}

	return nil, domain.ErrAccessDenied("compute endpoint %q is not assigned to the principal", endpointName)
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
