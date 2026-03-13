// Package project implements the internal authoring control plane used by products.
package project

import (
	"context"
	"strings"

	"duck-demo/internal/domain"
	servicepolicy "duck-demo/internal/service/policy"
)

// Service manages internal authoring projects, environments, and builds.
type Service struct {
	projects     domain.ProjectRepository
	environments domain.EnvironmentRepository
	builds       domain.BuildRepository
	teams        domain.TeamRepository
	products     domain.DataProductRepository
}

// NewService constructs the internal project service.
func NewService(
	projects domain.ProjectRepository,
	environments domain.EnvironmentRepository,
	builds domain.BuildRepository,
	teams domain.TeamRepository,
	products domain.DataProductRepository,
) *Service {
	return &Service{
		projects:     projects,
		environments: environments,
		builds:       builds,
		teams:        teams,
		products:     products,
	}
}

// CreateProject creates an internal project after validating team and product linkage.
func (s *Service) CreateProject(ctx context.Context, principal string, req domain.CreateProjectRequest) (*domain.Project, error) {
	if err := servicepolicy.RequireAdminForAction(ctx, "create project"); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}

	normalizedKind := normalizedProjectKind(req.Kind)
	ownerTeamID := normalizedStringPtr(req.OwnerTeamID)
	ownerPrincipal := normalizedStringPtr(req.OwnerPrincipal)
	productID := normalizedStringPtr(req.ProductID)

	if normalizedKind == domain.ProjectKindPersonal {
		if ownerPrincipal == nil || *ownerPrincipal != principal {
			return nil, domain.ErrValidation("personal projects must be owned by the creating principal")
		}
	}
	if ownerTeamID != nil {
		if _, err := s.teams.GetByID(ctx, *ownerTeamID); err != nil {
			return nil, err
		}
	}
	if productID != nil {
		product, err := s.products.GetByID(ctx, *productID)
		if err != nil {
			return nil, err
		}
		if ownerTeamID != nil && product.OwnerTeamID != *ownerTeamID {
			return nil, domain.ErrValidation("project owner_team_id must match the attached product owner team")
		}
	}

	project := &domain.Project{
		Name:           strings.TrimSpace(req.Name),
		Kind:           normalizedKind,
		Description:    strings.TrimSpace(req.Description),
		OwnerTeamID:    ownerTeamID,
		OwnerPrincipal: ownerPrincipal,
		ProductID:      productID,
		DefaultBranch:  defaultBranch(req.DefaultBranch),
		CreatedBy:      principal,
	}
	return s.projects.Create(ctx, project)
}

// GetProject loads an internal project by name.
func (s *Service) GetProject(ctx context.Context, name string) (*domain.Project, error) {
	if err := servicepolicy.RequireAdminForAction(ctx, "get project"); err != nil {
		return nil, err
	}
	return s.projects.GetByName(ctx, name)
}

// ListProjects returns internal projects.
func (s *Service) ListProjects(ctx context.Context, page domain.PageRequest) ([]domain.Project, int64, error) {
	if err := servicepolicy.RequireAdminForAction(ctx, "list projects"); err != nil {
		return nil, 0, err
	}
	return s.projects.List(ctx, page)
}

// CreateEnvironment creates an execution environment for a project.
func (s *Service) CreateEnvironment(ctx context.Context, principal, projectName string, req domain.CreateEnvironmentRequest) (*domain.Environment, error) {
	if err := servicepolicy.RequireAdminForAction(ctx, "create environment"); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	project, err := s.projects.GetByName(ctx, projectName)
	if err != nil {
		return nil, err
	}
	if project.Kind == domain.ProjectKindPersonal && normalizedEnvironmentKind(req.Kind) != domain.EnvironmentKindDevelopment {
		return nil, domain.ErrValidation("personal projects only support development environments")
	}
	return s.environments.Create(ctx, &domain.Environment{
		ProjectID:          project.ID,
		Name:               strings.TrimSpace(req.Name),
		Kind:               normalizedEnvironmentKind(req.Kind),
		Description:        strings.TrimSpace(req.Description),
		TargetCatalog:      strings.TrimSpace(req.TargetCatalog),
		TargetSchema:       strings.TrimSpace(req.TargetSchema),
		ComputeEndpoint:    normalizedStringPtr(req.ComputeEndpoint),
		DeferToEnvironment: normalizedStringPtr(req.DeferToEnvironment),
		Variables:          cloneStringMap(req.Variables),
		SourceOverrides:    cloneStringMap(req.SourceOverrides),
		CreatedBy:          principal,
	})
}

// ListEnvironments returns environments for a project.
func (s *Service) ListEnvironments(ctx context.Context, projectName string, page domain.PageRequest) ([]domain.Environment, int64, error) {
	if err := servicepolicy.RequireAdminForAction(ctx, "list environments"); err != nil {
		return nil, 0, err
	}
	project, err := s.projects.GetByName(ctx, projectName)
	if err != nil {
		return nil, 0, err
	}
	return s.environments.ListByProject(ctx, project.ID, page)
}

// CreateBuild creates an immutable build for a project/environment pair.
func (s *Service) CreateBuild(ctx context.Context, principal, projectName string, req domain.CreateBuildRequest) (*domain.Build, error) {
	if err := servicepolicy.RequireAdminForAction(ctx, "create build"); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s.builds == nil {
		return nil, domain.ErrNotImplemented("build creation is not configured")
	}
	project, err := s.projects.GetByName(ctx, projectName)
	if err != nil {
		return nil, err
	}
	environment, err := s.environments.GetByName(ctx, project.ID, strings.TrimSpace(req.EnvironmentName))
	if err != nil {
		return nil, err
	}
	return s.builds.Create(ctx, &domain.Build{
		ProjectID:          project.ID,
		ProductID:          project.ProductID,
		EnvironmentID:      environment.ID,
		State:              domain.BuildStateReady,
		GitRef:             strings.TrimSpace(req.GitRef),
		CommitSHA:          normalizedStringPtr(req.CommitSHA),
		Selector:           strings.TrimSpace(req.Selector),
		TargetCatalog:      strings.TrimSpace(req.TargetCatalog),
		TargetSchema:       strings.TrimSpace(req.TargetSchema),
		SourceModelRunID:   normalizedStringPtr(req.SourceModelRunID),
		CompileManifest:    strings.TrimSpace(req.CompileManifest),
		CompileDiagnostics: normalizedStringPtr(req.CompileDiagnostics),
		CreatedBy:          principal,
	})
}

// ListBuilds returns builds for a project.
func (s *Service) ListBuilds(ctx context.Context, projectName string, page domain.PageRequest) ([]domain.Build, int64, error) {
	if err := servicepolicy.RequireAdminForAction(ctx, "list builds"); err != nil {
		return nil, 0, err
	}
	if s.builds == nil {
		return nil, 0, domain.ErrNotImplemented("build listing is not configured")
	}
	project, err := s.projects.GetByName(ctx, projectName)
	if err != nil {
		return nil, 0, err
	}
	return s.builds.ListByProject(ctx, project.ID, page)
}

func normalizedProjectKind(kind string) string {
	if strings.TrimSpace(kind) == "" {
		return domain.ProjectKindShared
	}
	return strings.ToLower(strings.TrimSpace(kind))
}

func normalizedEnvironmentKind(kind string) string {
	if strings.TrimSpace(kind) == "" {
		return domain.EnvironmentKindDevelopment
	}
	return strings.ToLower(strings.TrimSpace(kind))
}

func normalizedStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func defaultBranch(branch string) string {
	if strings.TrimSpace(branch) == "" {
		return "main"
	}
	return strings.TrimSpace(branch)
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
