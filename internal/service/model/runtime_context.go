package model

import (
	"context"
	"fmt"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
)

type resolvedRunContext struct {
	project            *domain.Project
	environment        *domain.Environment
	stateEnvironment   *domain.Environment
	targetCatalog      string
	targetSchema       string
	variables          map[string]string
	sourceOverrides    map[string]string
	dependencyProjects []string
	allowedRefProjects map[string]struct{}
}

func (s *Service) resolveExecutionContext(
	ctx context.Context,
	projectName string,
	environmentName string,
	req domain.TriggerModelRunRequest,
) (*resolvedRunContext, error) {
	if s.projects == nil || s.environments == nil {
		return nil, domain.ErrNotImplemented("project-backed model execution is not configured")
	}

	project, err := s.projects.GetByName(ctx, strings.TrimSpace(projectName))
	if err != nil {
		return nil, err
	}

	environment, err := s.resolveRequestedEnvironment(ctx, project, environmentName)
	if err != nil {
		return nil, err
	}

	stack, err := s.resolveEnvironmentStack(ctx, project.ID, environment, map[string]struct{}{})
	if err != nil {
		return nil, err
	}
	if len(stack) == 0 {
		return nil, domain.ErrValidation("project %s has no execution environments", project.Name)
	}

	resolved := &resolvedRunContext{
		project:          project,
		environment:      environment,
		stateEnvironment: &stack[0],
		targetCatalog:    environment.TargetCatalog,
		targetSchema:     environment.TargetSchema,
		variables:        map[string]string{},
		sourceOverrides:  map[string]string{},
	}

	for _, item := range stack {
		if strings.TrimSpace(item.TargetCatalog) != "" {
			resolved.targetCatalog = strings.TrimSpace(item.TargetCatalog)
		}
		if strings.TrimSpace(item.TargetSchema) != "" {
			resolved.targetSchema = strings.TrimSpace(item.TargetSchema)
		}
		for key, value := range item.Variables {
			resolved.variables[key] = value
		}
		for key, value := range item.SourceOverrides {
			resolved.sourceOverrides[key] = value
		}
	}
	for key, value := range builtInEnvironmentVars(project, environment, resolved.targetCatalog, resolved.targetSchema) {
		resolved.variables[key] = value
	}
	for key, value := range req.Variables {
		resolved.variables[key] = value
	}
	if strings.TrimSpace(req.TargetCatalog) != "" {
		resolved.targetCatalog = strings.TrimSpace(req.TargetCatalog)
		resolved.variables["target_catalog"] = resolved.targetCatalog
	}
	if strings.TrimSpace(req.TargetSchema) != "" {
		resolved.targetSchema = strings.TrimSpace(req.TargetSchema)
		resolved.variables["target_schema"] = resolved.targetSchema
	}

	resolved.allowedRefProjects = map[string]struct{}{
		project.Name: {},
	}
	if s.projectDeps != nil {
		deps, err := s.projectDeps.ListByProject(ctx, project.ID)
		if err != nil {
			return nil, fmt.Errorf("list project dependencies for %s: %w", project.Name, err)
		}
		for _, dep := range deps {
			name := strings.TrimSpace(dep.DependencyProject)
			if name == "" {
				continue
			}
			if _, ok := resolved.allowedRefProjects[name]; ok {
				continue
			}
			resolved.allowedRefProjects[name] = struct{}{}
			resolved.dependencyProjects = append(resolved.dependencyProjects, name)
		}
	}

	return resolved, nil
}

func (s *Service) resolveRequestedEnvironment(
	ctx context.Context,
	project *domain.Project,
	environmentName string,
) (*domain.Environment, error) {
	if strings.TrimSpace(environmentName) != "" {
		return s.environments.GetByName(ctx, project.ID, strings.TrimSpace(environmentName))
	}
	environments, _, err := s.environments.ListByProject(ctx, project.ID, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return nil, err
	}
	return selectDefaultDevelopmentEnvironment(environments)
}

func (s *Service) resolveEnvironmentStack(
	ctx context.Context,
	projectID string,
	environment *domain.Environment,
	seen map[string]struct{},
) ([]domain.Environment, error) {
	if environment == nil {
		return nil, domain.ErrValidation("environment is required")
	}
	if _, ok := seen[environment.Name]; ok {
		return nil, domain.ErrValidation("environment defer_to_environment cycle detected at %q", environment.Name)
	}
	seen[environment.Name] = struct{}{}

	if environment.DeferToEnvironment == nil || strings.TrimSpace(*environment.DeferToEnvironment) == "" {
		return []domain.Environment{*environment}, nil
	}

	parent, err := s.environments.GetByName(ctx, projectID, strings.TrimSpace(*environment.DeferToEnvironment))
	if err != nil {
		return nil, fmt.Errorf("resolve deferred environment %q: %w", strings.TrimSpace(*environment.DeferToEnvironment), err)
	}
	stack, err := s.resolveEnvironmentStack(ctx, projectID, parent, seen)
	if err != nil {
		return nil, err
	}
	return append(stack, *environment), nil
}

func builtInEnvironmentVars(
	project *domain.Project,
	environment *domain.Environment,
	targetCatalog string,
	targetSchema string,
) map[string]string {
	var computeEndpoint string
	if environment.ComputeEndpoint != nil {
		computeEndpoint = strings.TrimSpace(*environment.ComputeEndpoint)
	}
	return map[string]string{
		"target":                  environment.Name,
		"target_name":             environment.Name,
		"target_project":          project.Name,
		"target_environment":      environment.Name,
		"target_environment_kind": environment.Kind,
		"target_catalog":          strings.TrimSpace(targetCatalog),
		"target_schema":           strings.TrimSpace(targetSchema),
		"target_compute_endpoint": computeEndpoint,
	}
}

func cloneStringMap(value map[string]string) map[string]string {
	if len(value) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(value))
	for key, item := range value {
		out[key] = item
	}
	return out
}
