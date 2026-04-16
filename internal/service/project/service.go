// Package project implements the internal authoring control plane used by products.
package project

import (
	"context"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	servicepolicy "github.com/Yacobolo/quackstack/internal/service/policy"
)

// Service manages internal authoring projects, environments, and builds.
type Service struct {
	workspaces   domain.WorkspaceRepository
	projects     domain.ProjectRepository
	environments domain.EnvironmentRepository
	projectDeps  domain.ProjectDependencyRepository
	sources      domain.SourceDefinitionRepository
	seeds        domain.SeedRepository
	builds       domain.BuildRepository
	teams        domain.TeamRepository
	products     domain.DataProductRepository
	audit        domain.AuditRepository
}

// NewService constructs the internal project service.
func NewService(
	workspaces domain.WorkspaceRepository,
	projects domain.ProjectRepository,
	environments domain.EnvironmentRepository,
	projectDeps domain.ProjectDependencyRepository,
	sources domain.SourceDefinitionRepository,
	seeds domain.SeedRepository,
	builds domain.BuildRepository,
	teams domain.TeamRepository,
	products domain.DataProductRepository,
	audit ...domain.AuditRepository,
) *Service {
	var auditRepo domain.AuditRepository
	if len(audit) > 0 {
		auditRepo = audit[0]
	}
	return &Service{
		workspaces:   workspaces,
		projects:     projects,
		environments: environments,
		projectDeps:  projectDeps,
		sources:      sources,
		seeds:        seeds,
		builds:       builds,
		teams:        teams,
		products:     products,
		audit:        auditRepo,
	}
}

// CreateProject creates an internal project after validating team and product linkage.
func (s *Service) CreateProject(ctx context.Context, principal string, req domain.CreateProjectRequest) (*domain.Project, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	workspace, err := s.workspaces.GetByID(ctx, strings.TrimSpace(req.WorkspaceID))
	if err != nil {
		return nil, err
	}
	if err := s.requireWorkspaceWrite(ctx, principal, workspace.ID); err != nil {
		return nil, err
	}
	normalizedKind := normalizedProjectKind(req.Kind)
	if normalizedKind == "" {
		normalizedKind = workspace.Kind
	}
	if normalizedKind != workspace.Kind {
		return nil, domain.ErrValidation("project kind must match workspace kind")
	}
	ownerTeamID := normalizedStringPtr(workspace.OwnerTeamID)
	ownerPrincipal := normalizedStringPtr(workspace.OwnerPrincipal)
	productID := normalizedStringPtr(req.ProductID)

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
		WorkspaceID:    workspace.ID,
		Name:           strings.TrimSpace(req.Name),
		Kind:           normalizedKind,
		Description:    strings.TrimSpace(req.Description),
		OwnerTeamID:    ownerTeamID,
		OwnerPrincipal: ownerPrincipal,
		ProductID:      productID,
		DefaultBranch:  defaultBranch(req.DefaultBranch),
		CreatedBy:      principal,
	}
	created, err := s.projects.Create(ctx, project)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "CREATE_INTERNAL_PROJECT")
	return created, nil
}

// ListProjectsForPrincipal returns projects visible within a workspace.
func (s *Service) ListProjectsForPrincipal(ctx context.Context, principal string, isAdmin bool, workspaceID string, page domain.PageRequest) ([]domain.Project, int64, error) {
	if !isAdmin {
		if err := s.requireWorkspaceRead(ctx, principal, workspaceID); err != nil {
			return nil, 0, err
		}
	}
	return s.projects.ListByWorkspace(ctx, strings.TrimSpace(workspaceID), page)
}

// GetProjectForPrincipal loads a project by id when the caller can access its workspace.
func (s *Service) GetProjectForPrincipal(ctx context.Context, principal string, isAdmin bool, id string) (*domain.Project, error) {
	project, err := s.projects.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		if err := s.requireWorkspaceRead(ctx, principal, project.WorkspaceID); err != nil {
			return nil, err
		}
	}
	return project, nil
}

// UpdateProjectForPrincipal updates mutable project fields after workspace access checks.
func (s *Service) UpdateProjectForPrincipal(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateProjectRequest) (*domain.Project, error) {
	if err := domain.ValidateUpdateProjectRequest(req); err != nil {
		return nil, err
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, id)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		if err := s.requireWorkspaceWrite(ctx, principal, project.WorkspaceID); err != nil {
			return nil, err
		}
	}
	if productID := normalizedStringPtr(req.ProductID); productID != nil {
		product, err := s.products.GetByID(ctx, *productID)
		if err != nil {
			return nil, err
		}
		if project.OwnerTeamID != nil && product.OwnerTeamID != *project.OwnerTeamID {
			return nil, domain.ErrValidation("project owner_team_id must match the attached product owner team")
		}
	}
	updated, err := s.projects.Update(ctx, project.ID, req)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "UPDATE_INTERNAL_PROJECT")
	return updated, nil
}

// DeleteProjectForPrincipal deletes a project after workspace access checks.
func (s *Service) DeleteProjectForPrincipal(ctx context.Context, principal string, isAdmin bool, id string) error {
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, id)
	if err != nil {
		return err
	}
	if !isAdmin {
		if err := s.requireWorkspaceWrite(ctx, principal, project.WorkspaceID); err != nil {
			return err
		}
	}
	if err := s.projects.Delete(ctx, project.ID); err != nil {
		return err
	}
	s.logAudit(ctx, principal, "DELETE_INTERNAL_PROJECT")
	return nil
}

// CreateDependencyForProject creates a declared project dependency.
func (s *Service) CreateDependencyForProject(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateProjectDependencyRequest) (*domain.ProjectDependency, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s.projectDeps == nil {
		return nil, domain.ErrNotImplemented("project dependencies are not configured")
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		if err := s.requireWorkspaceWrite(ctx, principal, project.WorkspaceID); err != nil {
			return nil, err
		}
	}
	if _, err := s.projects.GetByName(ctx, strings.TrimSpace(req.DependencyProject)); err != nil {
		return nil, err
	}
	created, err := s.projectDeps.Create(ctx, &domain.ProjectDependency{
		ProjectID:         project.ID,
		ProjectName:       project.Name,
		DependencyProject: strings.TrimSpace(req.DependencyProject),
		DependencyKind:    strings.TrimSpace(req.DependencyKind),
		Position:          req.Position,
		CreatedBy:         principal,
	})
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "CREATE_PROJECT_DEPENDENCY")
	return created, nil
}

// ListDependenciesForProject lists project dependency declarations.
func (s *Service) ListDependenciesForProject(ctx context.Context, principal string, isAdmin bool, projectID string, page domain.PageRequest) ([]domain.ProjectDependency, int64, error) {
	if s.projectDeps == nil {
		return nil, 0, domain.ErrNotImplemented("project dependencies are not configured")
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, 0, err
	}
	items, err := s.projectDeps.ListByProject(ctx, project.ID)
	if err != nil {
		return nil, 0, err
	}
	return paginateProjectItems(items, page)
}

// DeleteDependencyForProject deletes a dependency declaration by dependency project name.
func (s *Service) DeleteDependencyForProject(ctx context.Context, principal string, isAdmin bool, projectID string, dependencyProject string) error {
	if s.projectDeps == nil {
		return domain.ErrNotImplemented("project dependencies are not configured")
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return err
	}
	if !isAdmin {
		if err := s.requireWorkspaceWrite(ctx, principal, project.WorkspaceID); err != nil {
			return err
		}
	}
	if err := s.projectDeps.Delete(ctx, project.ID, strings.TrimSpace(dependencyProject)); err != nil {
		return err
	}
	s.logAudit(ctx, principal, "DELETE_PROJECT_DEPENDENCY")
	return nil
}

// CreateSourceForProject creates a project-owned source definition.
func (s *Service) CreateSourceForProject(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateSourceDefinitionRequest) (*domain.SourceDefinition, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s.sources == nil {
		return nil, domain.ErrNotImplemented("project sources are not configured")
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		if err := s.requireWorkspaceWrite(ctx, principal, project.WorkspaceID); err != nil {
			return nil, err
		}
	}
	created, err := s.sources.Create(ctx, &domain.SourceDefinition{
		ProjectName: project.Name,
		SourceName:  strings.TrimSpace(req.SourceName),
		TableName:   strings.TrimSpace(req.TableName),
		RelationRef: strings.TrimSpace(req.RelationRef),
		Description: strings.TrimSpace(req.Description),
		Freshness:   req.Freshness,
		CreatedBy:   principal,
	})
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "CREATE_PROJECT_SOURCE")
	return created, nil
}

// ListSourcesForProject lists source definitions owned by a project.
func (s *Service) ListSourcesForProject(ctx context.Context, principal string, isAdmin bool, projectID string, page domain.PageRequest) ([]domain.SourceDefinition, int64, error) {
	if s.sources == nil {
		return nil, 0, domain.ErrNotImplemented("project sources are not configured")
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, 0, err
	}
	items, err := s.sources.ListByProject(ctx, project.Name)
	if err != nil {
		return nil, 0, err
	}
	return paginateProjectItems(items, page)
}

// GetSourceForProject loads a source definition by logical key.
func (s *Service) GetSourceForProject(ctx context.Context, principal string, isAdmin bool, projectID string, sourceName string, tableName string) (*domain.SourceDefinition, error) {
	if s.sources == nil {
		return nil, domain.ErrNotImplemented("project sources are not configured")
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, err
	}
	return s.sources.GetByName(ctx, project.Name, strings.TrimSpace(sourceName), strings.TrimSpace(tableName))
}

// UpdateSourceForProject updates a source definition by logical key.
func (s *Service) UpdateSourceForProject(ctx context.Context, principal string, isAdmin bool, projectID string, sourceName string, tableName string, req domain.UpdateSourceDefinitionRequest) (*domain.SourceDefinition, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s.sources == nil {
		return nil, domain.ErrNotImplemented("project sources are not configured")
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		if err := s.requireWorkspaceWrite(ctx, principal, project.WorkspaceID); err != nil {
			return nil, err
		}
	}
	current, err := s.sources.GetByName(ctx, project.Name, strings.TrimSpace(sourceName), strings.TrimSpace(tableName))
	if err != nil {
		return nil, err
	}
	next := *current
	if req.RelationRef != nil {
		next.RelationRef = strings.TrimSpace(*req.RelationRef)
	}
	if req.Description != nil {
		next.Description = strings.TrimSpace(*req.Description)
	}
	if req.Freshness != nil {
		next.Freshness = req.Freshness
	}
	updated, err := s.sources.Update(ctx, current.ID, &next)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "UPDATE_PROJECT_SOURCE")
	return updated, nil
}

// DeleteSourceForProject deletes a source definition by logical key.
func (s *Service) DeleteSourceForProject(ctx context.Context, principal string, isAdmin bool, projectID string, sourceName string, tableName string) error {
	if s.sources == nil {
		return domain.ErrNotImplemented("project sources are not configured")
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return err
	}
	if !isAdmin {
		if err := s.requireWorkspaceWrite(ctx, principal, project.WorkspaceID); err != nil {
			return err
		}
	}
	current, err := s.sources.GetByName(ctx, project.Name, strings.TrimSpace(sourceName), strings.TrimSpace(tableName))
	if err != nil {
		return err
	}
	if err := s.sources.Delete(ctx, current.ID); err != nil {
		return err
	}
	s.logAudit(ctx, principal, "DELETE_PROJECT_SOURCE")
	return nil
}

// CreateSeedForProject creates a project-owned seed resource.
func (s *Service) CreateSeedForProject(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateSeedRequest) (*domain.Seed, error) {
	if s.seeds == nil {
		return nil, domain.ErrNotImplemented("project seeds are not configured")
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, err
	}
	req.ProjectName = project.Name
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if !isAdmin {
		if err := s.requireWorkspaceWrite(ctx, principal, project.WorkspaceID); err != nil {
			return nil, err
		}
	}
	created, err := s.seeds.Create(ctx, &domain.Seed{
		ProjectName: project.Name,
		Name:        strings.TrimSpace(req.Name),
		Description: strings.TrimSpace(req.Description),
		InputRef:    strings.TrimSpace(req.InputRef),
		Format:      domain.NormalizeSeedFormat(req.Format),
		Delimiter:   seedDelimiter(req.Delimiter),
		HasHeader:   seedHasHeader(req.HasHeader),
		ColumnTypes: cloneStringMap(req.ColumnTypes),
		Tags:        append([]string(nil), req.Tags...),
		CreatedBy:   principal,
	})
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "CREATE_PROJECT_SEED")
	return created, nil
}

// ListSeedsForProject lists seed resources owned by a project.
func (s *Service) ListSeedsForProject(ctx context.Context, principal string, isAdmin bool, projectID string, page domain.PageRequest) ([]domain.Seed, int64, error) {
	if s.seeds == nil {
		return nil, 0, domain.ErrNotImplemented("project seeds are not configured")
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, 0, err
	}
	items, err := s.seeds.ListByProject(ctx, project.Name)
	if err != nil {
		return nil, 0, err
	}
	return paginateProjectItems(items, page)
}

// GetSeedForProject loads a seed by name.
func (s *Service) GetSeedForProject(ctx context.Context, principal string, isAdmin bool, projectID string, seedName string) (*domain.Seed, error) {
	if s.seeds == nil {
		return nil, domain.ErrNotImplemented("project seeds are not configured")
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, err
	}
	return s.seeds.GetByName(ctx, project.Name, strings.TrimSpace(seedName))
}

// UpdateSeedForProject updates a seed by name.
func (s *Service) UpdateSeedForProject(ctx context.Context, principal string, isAdmin bool, projectID string, seedName string, req domain.UpdateSeedRequest) (*domain.Seed, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s.seeds == nil {
		return nil, domain.ErrNotImplemented("project seeds are not configured")
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		if err := s.requireWorkspaceWrite(ctx, principal, project.WorkspaceID); err != nil {
			return nil, err
		}
	}
	current, err := s.seeds.GetByName(ctx, project.Name, strings.TrimSpace(seedName))
	if err != nil {
		return nil, err
	}
	next := *current
	if req.Description != nil {
		next.Description = strings.TrimSpace(*req.Description)
	}
	if req.InputRef != nil {
		next.InputRef = strings.TrimSpace(*req.InputRef)
	}
	if req.Format != nil {
		next.Format = domain.NormalizeSeedFormat(*req.Format)
	}
	if req.Delimiter != nil {
		next.Delimiter = strings.TrimSpace(*req.Delimiter)
	}
	if req.HasHeader != nil {
		next.HasHeader = *req.HasHeader
	}
	if req.ColumnTypes != nil {
		next.ColumnTypes = cloneStringMap(*req.ColumnTypes)
	}
	if req.Tags != nil {
		next.Tags = append([]string(nil), req.Tags...)
	}
	updated, err := s.seeds.Update(ctx, current.ID, &next)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "UPDATE_PROJECT_SEED")
	return updated, nil
}

// DeleteSeedForProject deletes a seed by name.
func (s *Service) DeleteSeedForProject(ctx context.Context, principal string, isAdmin bool, projectID string, seedName string) error {
	if s.seeds == nil {
		return domain.ErrNotImplemented("project seeds are not configured")
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return err
	}
	if !isAdmin {
		if err := s.requireWorkspaceWrite(ctx, principal, project.WorkspaceID); err != nil {
			return err
		}
	}
	current, err := s.seeds.GetByName(ctx, project.Name, strings.TrimSpace(seedName))
	if err != nil {
		return err
	}
	if err := s.seeds.Delete(ctx, current.ID); err != nil {
		return err
	}
	s.logAudit(ctx, principal, "DELETE_PROJECT_SEED")
	return nil
}

// CreateEnvironmentForProject creates an environment beneath a project id.
func (s *Service) CreateEnvironmentForProject(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateEnvironmentRequest) (*domain.Environment, error) {
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		if err := s.requireWorkspaceWrite(ctx, principal, project.WorkspaceID); err != nil {
			return nil, err
		}
	}
	created, err := s.createEnvironmentForProject(ctx, principal, project, req)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "CREATE_INTERNAL_ENVIRONMENT")
	return created, nil
}

// ListEnvironmentsForProject returns environments for a project id.
func (s *Service) ListEnvironmentsForProject(ctx context.Context, principal string, isAdmin bool, projectID string, page domain.PageRequest) ([]domain.Environment, int64, error) {
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, 0, err
	}
	return s.environments.ListByProject(ctx, project.ID, page)
}

// GetEnvironmentForProject loads a single environment after verifying project access.
func (s *Service) GetEnvironmentForProject(ctx context.Context, principal string, isAdmin bool, projectID string, environmentID string) (*domain.Environment, error) {
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, err
	}
	environment, err := s.environments.GetByID(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	if environment.ProjectID != project.ID {
		return nil, domain.ErrValidation("environment does not belong to project")
	}
	return environment, nil
}

// UpdateEnvironmentForProject updates mutable environment fields after project ownership checks.
func (s *Service) UpdateEnvironmentForProject(ctx context.Context, principal string, isAdmin bool, projectID string, environmentID string, req domain.UpdateEnvironmentRequest) (*domain.Environment, error) {
	if err := domain.ValidateUpdateEnvironmentRequest(req); err != nil {
		return nil, err
	}
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		if err := s.requireWorkspaceWrite(ctx, principal, project.WorkspaceID); err != nil {
			return nil, err
		}
	}
	environment, err := s.environments.GetByID(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	if environment.ProjectID != project.ID {
		return nil, domain.ErrValidation("environment does not belong to project")
	}
	updated, err := s.environments.Update(ctx, environment.ID, req)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "UPDATE_INTERNAL_ENVIRONMENT")
	return updated, nil
}

// DeleteEnvironmentForProject deletes an environment after project ownership checks.
func (s *Service) DeleteEnvironmentForProject(ctx context.Context, principal string, isAdmin bool, projectID string, environmentID string) error {
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return err
	}
	if !isAdmin {
		if err := s.requireWorkspaceWrite(ctx, principal, project.WorkspaceID); err != nil {
			return err
		}
	}
	environment, err := s.environments.GetByID(ctx, environmentID)
	if err != nil {
		return err
	}
	if environment.ProjectID != project.ID {
		return domain.ErrValidation("environment does not belong to project")
	}
	if err := s.environments.Delete(ctx, environment.ID); err != nil {
		return err
	}
	s.logAudit(ctx, principal, "DELETE_INTERNAL_ENVIRONMENT")
	return nil
}

// CreateBuildForProject creates a build beneath a project id.
func (s *Service) CreateBuildForProject(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateBuildRequest) (*domain.Build, error) {
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		if err := s.requireWorkspaceWrite(ctx, principal, project.WorkspaceID); err != nil {
			return nil, err
		}
	}
	created, err := s.createBuildForProject(ctx, principal, project, req)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "CREATE_INTERNAL_BUILD")
	return created, nil
}

// ListBuildsForProject returns builds for a project id.
func (s *Service) ListBuildsForProject(ctx context.Context, principal string, isAdmin bool, projectID string, page domain.PageRequest) ([]domain.Build, int64, error) {
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, 0, err
	}
	if s.builds == nil {
		return nil, 0, domain.ErrNotImplemented("build listing is not configured")
	}
	return s.builds.ListByProject(ctx, project.ID, page)
}

// GetBuildForProject loads a single build after verifying project access.
func (s *Service) GetBuildForProject(ctx context.Context, principal string, isAdmin bool, projectID string, buildID string) (*domain.Build, error) {
	project, err := s.GetProjectForPrincipal(ctx, principal, isAdmin, projectID)
	if err != nil {
		return nil, err
	}
	if s.builds == nil {
		return nil, domain.ErrNotImplemented("build reads are not configured")
	}
	build, err := s.builds.GetByID(ctx, buildID)
	if err != nil {
		return nil, err
	}
	if build.ProjectID != project.ID {
		return nil, domain.ErrValidation("build does not belong to project")
	}
	return build, nil
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
	project, err := s.projects.GetByName(ctx, projectName)
	if err != nil {
		return nil, err
	}
	created, err := s.createEnvironmentForProject(ctx, principal, project, req)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "CREATE_INTERNAL_ENVIRONMENT")
	return created, nil
}

func (s *Service) createEnvironmentForProject(ctx context.Context, principal string, project *domain.Project, req domain.CreateEnvironmentRequest) (*domain.Environment, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if project.Kind == domain.ProjectKindPersonal && normalizedEnvironmentKind(req.Kind) != domain.EnvironmentKindDevelopment {
		return nil, domain.ErrValidation("personal projects only support development environments")
	}
	created, err := s.environments.Create(ctx, &domain.Environment{
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
	if err != nil {
		return nil, err
	}
	return created, nil
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
	project, err := s.projects.GetByName(ctx, projectName)
	if err != nil {
		return nil, err
	}
	created, err := s.createBuildForProject(ctx, principal, project, req)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "CREATE_INTERNAL_BUILD")
	return created, nil
}

func (s *Service) createBuildForProject(ctx context.Context, principal string, project *domain.Project, req domain.CreateBuildRequest) (*domain.Build, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s.builds == nil {
		return nil, domain.ErrNotImplemented("build creation is not configured")
	}
	environment, err := s.environments.GetByName(ctx, project.ID, strings.TrimSpace(req.EnvironmentName))
	if err != nil {
		return nil, err
	}
	created, err := s.builds.Create(ctx, &domain.Build{
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
	if err != nil {
		return nil, err
	}
	return created, nil
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

func (s *Service) requireWorkspaceRead(ctx context.Context, principal string, workspaceID string) error {
	cp, _ := domain.PrincipalFromContext(ctx)
	if cp.IsAdmin {
		return nil
	}
	role, err := s.workspaces.GetMemberRole(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(principal))
	if err != nil {
		return err
	}
	if role == "" {
		return domain.ErrAccessDenied("principal %q cannot read workspace %q", principal, workspaceID)
	}
	return nil
}

func (s *Service) requireWorkspaceWrite(ctx context.Context, principal string, workspaceID string) error {
	cp, _ := domain.PrincipalFromContext(ctx)
	if cp.IsAdmin {
		return nil
	}
	role, err := s.workspaces.GetMemberRole(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(principal))
	if err != nil {
		return err
	}
	if role != domain.FolderShareRoleEditor && role != domain.FolderShareRoleManager {
		return domain.ErrAccessDenied("principal %q cannot write workspace %q", principal, workspaceID)
	}
	return nil
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

func paginateProjectItems[T any](items []T, page domain.PageRequest) ([]T, int64, error) {
	total := int64(len(items))
	start := page.Offset()
	if start >= len(items) {
		return []T{}, total, nil
	}
	end := start + page.Limit()
	if end > len(items) {
		end = len(items)
	}
	return items[start:end], total, nil
}

func seedDelimiter(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return ","
	}
	return strings.TrimSpace(*value)
}

func seedHasHeader(value *bool) bool {
	if value == nil {
		return true
	}
	return *value
}

func (s *Service) logAudit(ctx context.Context, principal, action string) {
	if s == nil || s.audit == nil {
		return
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "ALLOWED",
	})
}
