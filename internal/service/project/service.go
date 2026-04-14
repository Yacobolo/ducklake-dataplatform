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
