package compute

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

// ListAvailableTargets returns compute targets visible to the principal for selection.
func (s *ComputeEndpointService) ListAvailableTargets(ctx context.Context, principal string, workloadType string) ([]domain.ComputeTarget, error) {
	if s.principals == nil {
		return nil, fmt.Errorf("principal repository is not configured")
	}

	defaults, err := s.getRoutingDefaults(ctx)
	if err != nil {
		return nil, err
	}

	workloadType = strings.ToUpper(strings.TrimSpace(workloadType))
	if workloadType == "" {
		workloadType = domain.ComputeWorkloadInteractive
	}

	defaultMode := defaults.InteractiveMode
	switch workloadType {
	case domain.ComputeWorkloadScheduled, domain.ComputeWorkloadHeavy:
		defaultMode = defaults.ScheduledMode
	case domain.ComputeWorkloadNotebook:
		defaultMode = defaults.NotebookMode
	}

	localDefault := defaultMode == domain.ComputeModeByocLocal
	targets := []domain.ComputeTarget{{
		Mode:                     domain.ComputeModeByocLocal,
		DisplayName:              "Local",
		Status:                   "ACTIVE",
		SuitabilityLabels:        []string{"interactive", "byoc"},
		IsDefault:                localDefault,
		SelectableForInteractive: true,
		SelectableForScheduled:   false,
	}}

	endpoints, defaultEndpointName, err := s.collectPrincipalEndpoints(ctx, principal)
	if err != nil {
		return nil, err
	}

	for _, ep := range endpoints {
		name := ep.Name
		typ := ep.Type
		labels := []string{strings.ToLower(ep.WorkloadClass)}
		if ep.RecommendedForLargeQueries {
			labels = append(labels, "large-queries")
		}
		if ep.IsDraining {
			labels = append(labels, "draining")
		}
		selectableForInteractive := ep.SelectionPolicy != domain.ComputeSelectionPolicyAdminOnly
		selectableForScheduled := ep.WorkloadClass != domain.ComputeEndpointWorkloadInteractive
		availabilityReason := computeAvailabilityReason(ep)
		targets = append(targets, domain.ComputeTarget{
			Mode:                     domain.ComputeModeSharedEndpoint,
			EndpointName:             &name,
			EndpointType:             &typ,
			DisplayName:              ep.Name,
			Status:                   ep.Status,
			SuitabilityLabels:        labels,
			AvailabilityReason:       availabilityReason,
			IsDefault:                defaultMode == domain.ComputeModeSharedEndpoint && defaultEndpointName != nil && *defaultEndpointName == ep.Name,
			SelectableForInteractive: selectableForInteractive && ep.ReadinessStatus != domain.ComputeReadinessUnavailable && !ep.IsDraining,
			SelectableForScheduled:   selectableForScheduled && ep.ReadinessStatus == domain.ComputeReadinessReady && !ep.IsDraining,
		})
	}

	return targets, nil
}

func computeAvailabilityReason(ep domain.ComputeEndpoint) *string {
	switch {
	case ep.IsDraining:
		reason := "endpoint is draining"
		return &reason
	case ep.ReadinessStatus == domain.ComputeReadinessUnavailable:
		reason := "endpoint is unavailable"
		return &reason
	case ep.SelectionPolicy == domain.ComputeSelectionPolicyAdminOnly:
		reason := "admin only"
		return &reason
	default:
		return nil
	}
}

// GetRoutingDefaults returns the current routing defaults.
func (s *ComputeEndpointService) GetRoutingDefaults(ctx context.Context, principal string) (*domain.ComputeRoutingDefaults, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "GET_COMPUTE_ROUTING_DEFAULTS", "Denied get compute routing defaults"); err != nil {
		return nil, err
	}
	return s.getRoutingDefaults(ctx)
}

// UpdateRoutingDefaults updates the global routing defaults.
func (s *ComputeEndpointService) UpdateRoutingDefaults(ctx context.Context, principal string, defaults domain.ComputeRoutingDefaults) (*domain.ComputeRoutingDefaults, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivManageCompute, "UPDATE_COMPUTE_ROUTING_DEFAULTS", "Denied update compute routing defaults"); err != nil {
		return nil, err
	}
	if s.routingRepo == nil {
		return nil, fmt.Errorf("compute routing repository is not configured")
	}
	if err := defaults.Validate(); err != nil {
		return nil, err
	}
	updated, err := s.routingRepo.UpdateDefaults(ctx, defaults)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "UPDATE_COMPUTE_ROUTING_DEFAULTS", "Updated compute routing defaults")
	return updated, nil
}

func (s *ComputeEndpointService) getRoutingDefaults(ctx context.Context) (*domain.ComputeRoutingDefaults, error) {
	if s.routingRepo == nil {
		defaults := domain.ComputeRoutingDefaults{}.Normalize()
		return &defaults, nil
	}
	defaults, err := s.routingRepo.GetDefaults(ctx)
	if err != nil {
		return nil, err
	}
	return defaults, nil
}

func (s *ComputeEndpointService) collectPrincipalEndpoints(ctx context.Context, principalName string) ([]domain.ComputeEndpoint, *string, error) {
	principal, err := s.principals.GetByName(ctx, principalName)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve principal %q: %w", principalName, err)
	}

	candidates := make([]domain.ComputeEndpoint, 0)
	seen := map[string]struct{}{}
	appendUnique := func(items []domain.ComputeEndpoint) {
		for _, ep := range items {
			if _, ok := seen[ep.ID]; ok {
				continue
			}
			seen[ep.ID] = struct{}{}
			candidates = append(candidates, ep)
		}
	}

	userEndpoints, err := s.repo.GetAssignmentsForPrincipal(ctx, principal.ID, "user")
	if err != nil {
		return nil, nil, fmt.Errorf("list user compute assignments: %w", err)
	}
	appendUnique(userEndpoints)

	var defaultEndpointName *string
	defaultEndpoint, err := s.repo.GetDefaultForPrincipal(ctx, principal.ID, "user")
	if err == nil && defaultEndpoint != nil {
		defaultEndpointName = &defaultEndpoint.Name
	} else {
		var notFound *domain.NotFoundError
		if err != nil && !errors.As(err, &notFound) {
			return nil, nil, fmt.Errorf("resolve default compute assignment: %w", err)
		}
	}

	if s.groups != nil {
		groups, err := s.groups.GetGroupsForMember(ctx, "user", principal.ID)
		if err != nil {
			return nil, nil, fmt.Errorf("list group membership: %w", err)
		}
		for _, group := range groups {
			groupEndpoints, err := s.repo.GetAssignmentsForPrincipal(ctx, group.ID, "group")
			if err != nil {
				return nil, nil, fmt.Errorf("list group compute assignments: %w", err)
			}
			appendUnique(groupEndpoints)
			if defaultEndpointName == nil {
				groupDefault, err := s.repo.GetDefaultForPrincipal(ctx, group.ID, "group")
				if err == nil && groupDefault != nil {
					defaultEndpointName = &groupDefault.Name
					continue
				}
				var notFound *domain.NotFoundError
				if err != nil && !errors.As(err, &notFound) {
					return nil, nil, fmt.Errorf("resolve group default compute assignment: %w", err)
				}
			}
		}
	}

	return candidates, defaultEndpointName, nil
}
