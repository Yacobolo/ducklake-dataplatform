// Package workspace manages authoring workspaces, memberships, and defaults.
package workspace

import (
	"context"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

// Service manages authoring workspaces and memberships.
type Service struct {
	workspaces   domain.WorkspaceRepository
	folders      domain.FolderRepository
	projects     domain.ProjectRepository
	environments domain.EnvironmentRepository
	teams        domain.TeamRepository
	audit        domain.AuditRepository
}

// NewService constructs a workspace service.
func NewService(
	workspaces domain.WorkspaceRepository,
	folders domain.FolderRepository,
	projects domain.ProjectRepository,
	environments domain.EnvironmentRepository,
	teams domain.TeamRepository,
	audit domain.AuditRepository,
) *Service {
	return &Service{
		workspaces:   workspaces,
		folders:      folders,
		projects:     projects,
		environments: environments,
		teams:        teams,
		audit:        audit,
	}
}

// CreateWorkspace validates and creates a new workspace with a root folder and creator membership.
func (s *Service) CreateWorkspace(ctx context.Context, principal string, isAdmin bool, req domain.CreateWorkspaceRequest) (*domain.Workspace, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	kind := strings.ToLower(strings.TrimSpace(req.Kind))
	if kind == "" {
		kind = domain.WorkspaceKindShared
	}
	if kind == domain.WorkspaceKindPersonal {
		if req.OwnerPrincipal == nil || strings.TrimSpace(*req.OwnerPrincipal) != strings.TrimSpace(principal) {
			return nil, domain.ErrValidation("personal workspaces must be owned by the creating principal")
		}
	} else if !isAdmin {
		return nil, domain.ErrAccessDenied("only admins can create shared or library workspaces")
	}
	if req.OwnerTeamID != nil && s.teams != nil {
		if _, err := s.teams.GetByID(ctx, strings.TrimSpace(*req.OwnerTeamID)); err != nil {
			return nil, err
		}
	}
	if err := s.validateDefaults(ctx, trimmedPtrValue(req.DefaultProjectID), trimmedPtrValue(req.DefaultEnvironmentID), ""); err != nil {
		return nil, err
	}

	workspace := &domain.Workspace{
		Name:                 strings.TrimSpace(req.Name),
		Kind:                 kind,
		OwnerTeamID:          req.OwnerTeamID,
		OwnerPrincipal:       req.OwnerPrincipal,
		DefaultProjectID:     req.DefaultProjectID,
		DefaultEnvironmentID: req.DefaultEnvironmentID,
		GitRepoID:            req.GitRepoID,
		GitRootPath:          req.GitRootPath,
		CreatedBy:            principal,
	}
	created, err := s.workspaces.Create(ctx, workspace)
	if err != nil {
		return nil, err
	}
	if _, err := s.workspaces.UpsertMember(ctx, &domain.WorkspaceMember{
		WorkspaceID:   created.ID,
		PrincipalName: principal,
		Role:          domain.FolderShareRoleManager,
	}); err != nil {
		return nil, err
	}
	if s.folders != nil {
		if _, err := s.folders.EnsureWorkspaceRoot(ctx, created.ID, principal); err != nil {
			return nil, err
		}
	}
	s.logAudit(ctx, principal, "CREATE_WORKSPACE")
	return created, nil
}

// EnsurePersonalWorkspace returns the caller's personal workspace and its root folder, creating them if needed.
func (s *Service) EnsurePersonalWorkspace(ctx context.Context, principal string) (*domain.Workspace, *domain.Folder, error) {
	workspace, err := s.workspaces.GetPersonalByPrincipal(ctx, strings.TrimSpace(principal))
	if err != nil {
		req := domain.CreateWorkspaceRequest{
			Name:           strings.TrimSpace(principal) + " workspace",
			Kind:           domain.WorkspaceKindPersonal,
			OwnerPrincipal: strPtr(strings.TrimSpace(principal)),
		}
		workspace, err = s.CreateWorkspace(ctx, principal, true, req)
		if err != nil {
			return nil, nil, err
		}
	}
	root, err := s.folders.EnsureWorkspaceRoot(ctx, workspace.ID, principal)
	if err != nil {
		return nil, nil, err
	}
	return workspace, root, nil
}

// GetWorkspaceForPrincipal loads a workspace after membership checks.
func (s *Service) GetWorkspaceForPrincipal(ctx context.Context, principal string, isAdmin bool, id string) (*domain.Workspace, error) {
	workspace, err := s.workspaces.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if isAdmin {
		return workspace, nil
	}
	role, err := s.workspaces.GetMemberRole(ctx, id, strings.TrimSpace(principal))
	if err != nil {
		return nil, err
	}
	if role == "" {
		return nil, domain.ErrAccessDenied("principal %q cannot access workspace %q", principal, id)
	}
	return workspace, nil
}

// ListWorkspacesForPrincipal lists workspaces visible to a principal.
func (s *Service) ListWorkspacesForPrincipal(ctx context.Context, principal string, isAdmin bool, page domain.PageRequest) ([]domain.Workspace, int64, error) {
	if isAdmin {
		return s.workspaces.List(ctx, page)
	}
	return s.workspaces.ListForPrincipal(ctx, strings.TrimSpace(principal), page)
}

// UpdateWorkspace validates workspace defaults and applies mutable workspace fields.
func (s *Service) UpdateWorkspace(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateWorkspaceRequest) (*domain.Workspace, error) {
	if err := domain.ValidateUpdateWorkspaceRequest(req); err != nil {
		return nil, err
	}
	current, err := s.GetWorkspaceForPrincipal(ctx, principal, isAdmin, id)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		role, err := s.workspaces.GetMemberRole(ctx, current.ID, strings.TrimSpace(principal))
		if err != nil {
			return nil, err
		}
		if role != domain.FolderShareRoleManager {
			return nil, domain.ErrAccessDenied("principal %q cannot manage workspace %q", principal, id)
		}
	}
	nextProjectID := ptrOrValue(current.DefaultProjectID, req.DefaultProjectID)
	nextEnvironmentID := ptrOrValue(current.DefaultEnvironmentID, req.DefaultEnvironmentID)
	if err := s.validateDefaults(ctx, strings.TrimSpace(nextProjectID), strings.TrimSpace(nextEnvironmentID), current.ID); err != nil {
		return nil, err
	}
	updated, err := s.workspaces.Update(ctx, id, req)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "UPDATE_WORKSPACE")
	return updated, nil
}

// DeleteWorkspace removes a workspace after manager or admin authorization.
func (s *Service) DeleteWorkspace(ctx context.Context, principal string, isAdmin bool, id string) error {
	workspace, err := s.GetWorkspaceForPrincipal(ctx, principal, isAdmin, id)
	if err != nil {
		return err
	}
	if !isAdmin {
		role, err := s.workspaces.GetMemberRole(ctx, workspace.ID, strings.TrimSpace(principal))
		if err != nil {
			return err
		}
		if role != domain.FolderShareRoleManager {
			return domain.ErrAccessDenied("principal %q cannot delete workspace %q", principal, id)
		}
	}
	if err := s.workspaces.Delete(ctx, id); err != nil {
		return err
	}
	s.logAudit(ctx, principal, "DELETE_WORKSPACE")
	return nil
}

// ListMembers returns the membership list for a workspace visible to the caller.
func (s *Service) ListMembers(ctx context.Context, principal string, isAdmin bool, workspaceID string) ([]domain.WorkspaceMember, error) {
	if _, err := s.GetWorkspaceForPrincipal(ctx, principal, isAdmin, workspaceID); err != nil {
		return nil, err
	}
	return s.workspaces.ListMembers(ctx, workspaceID)
}

// AddMember adds or updates a workspace member after manager checks.
func (s *Service) AddMember(ctx context.Context, principal string, isAdmin bool, workspaceID string, req domain.AddWorkspaceMemberRequest) (*domain.WorkspaceMember, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if err := s.requireManager(ctx, principal, isAdmin, workspaceID); err != nil {
		return nil, err
	}
	member, err := s.workspaces.UpsertMember(ctx, &domain.WorkspaceMember{
		WorkspaceID:   workspaceID,
		PrincipalName: strings.TrimSpace(req.PrincipalName),
		Role:          domain.NormalizeShareRole(req.Role),
	})
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "ADD_WORKSPACE_MEMBER")
	return member, nil
}

// RemoveMember deletes a workspace membership after manager checks.
func (s *Service) RemoveMember(ctx context.Context, principal string, isAdmin bool, workspaceID string, principalName string) error {
	if err := s.requireManager(ctx, principal, isAdmin, workspaceID); err != nil {
		return err
	}
	if err := s.workspaces.DeleteMember(ctx, workspaceID, strings.TrimSpace(principalName)); err != nil {
		return err
	}
	s.logAudit(ctx, principal, "REMOVE_WORKSPACE_MEMBER")
	return nil
}

// RoleForPrincipal resolves the caller's effective workspace role.
func (s *Service) RoleForPrincipal(ctx context.Context, workspaceID string, principal string, isAdmin bool) (string, error) {
	if isAdmin {
		return domain.FolderShareRoleManager, nil
	}
	return s.workspaces.GetMemberRole(ctx, workspaceID, strings.TrimSpace(principal))
}

func (s *Service) requireManager(ctx context.Context, principal string, isAdmin bool, workspaceID string) error {
	workspace, err := s.GetWorkspaceForPrincipal(ctx, principal, isAdmin, workspaceID)
	if err != nil {
		return err
	}
	if isAdmin {
		return nil
	}
	role, err := s.workspaces.GetMemberRole(ctx, workspace.ID, strings.TrimSpace(principal))
	if err != nil {
		return err
	}
	if role != domain.FolderShareRoleManager {
		return domain.ErrAccessDenied("principal %q cannot manage workspace %q", principal, workspaceID)
	}
	return nil
}

func (s *Service) validateDefaults(ctx context.Context, projectID string, environmentID string, workspaceID string) error {
	if environmentID == "" {
		return nil
	}
	if projectID == "" {
		return domain.ErrValidation("default_environment_id requires default_project_id")
	}
	if s.projects == nil || s.environments == nil {
		return nil
	}
	project, err := s.projects.GetByID(ctx, projectID)
	if err != nil {
		return fmt.Errorf("get workspace default project %q: %w", projectID, err)
	}
	if workspaceID != "" && strings.TrimSpace(project.WorkspaceID) != strings.TrimSpace(workspaceID) {
		return domain.ErrValidation("default_project_id must belong to the workspace")
	}
	environment, err := s.environments.GetByID(ctx, environmentID)
	if err != nil {
		return fmt.Errorf("get workspace default environment %q: %w", environmentID, err)
	}
	if strings.TrimSpace(environment.ProjectID) != strings.TrimSpace(project.ID) {
		return domain.ErrValidation("default_environment_id must belong to default_project_id")
	}
	return nil
}

func (s *Service) logAudit(ctx context.Context, principal string, action string) {
	if s == nil || s.audit == nil {
		return
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "ALLOWED",
	})
}

func strPtr(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	v := strings.TrimSpace(value)
	return &v
}

func ptrOrValue(current *string, next *string) string {
	if next == nil {
		if current == nil {
			return ""
		}
		return *current
	}
	return *next
}

func trimmedPtrValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}
