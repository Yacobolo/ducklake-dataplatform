package notebook

import (
	"context"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

// FolderService manages notebook folders.
type FolderService struct {
	repo         domain.FolderRepository
	folderShares domain.FolderShareRepository
	auth         domain.AuthorizationService
	grants       domain.GrantRepository
	projects     domain.ProjectRepository
	environments domain.EnvironmentRepository
	notebooks    domain.NotebookRepository
	events       domain.OrchestrationEventRepository
	audit        domain.AuditRepository
}

// NewFolderService creates a new folder service.
func NewFolderService(repo domain.FolderRepository, audit domain.AuditRepository) *FolderService {
	return &FolderService{repo: repo, audit: audit}
}

// SetAuthorization configures folder privilege checks.
func (s *FolderService) SetAuthorization(auth domain.AuthorizationService) {
	s.auth = auth
}

// SetGrantRepository configures grant-backed folder sharing.
func (s *FolderService) SetGrantRepository(grants domain.GrantRepository) {
	s.grants = grants
}

// SetProjectRepositories configures folder project/environment validation.
func (s *FolderService) SetProjectRepositories(projects domain.ProjectRepository, environments domain.EnvironmentRepository) {
	s.projects = projects
	s.environments = environments
}

// SetShareRepository is kept as a compatibility shim while folder ACLs move to grants.
func (s *FolderService) SetShareRepository(folderShares domain.FolderShareRepository) {
	s.folderShares = folderShares
}

// SetContextInvalidation configures queue-backed invalidation for folder context changes.
func (s *FolderService) SetContextInvalidation(notebooks domain.NotebookRepository, events domain.OrchestrationEventRepository) {
	s.notebooks = notebooks
	s.events = events
}

func (s *FolderService) accessResolver(ctx context.Context, principal string, isAdmin bool) (*principalAccessResolver, error) {
	return newPrincipalAccessResolver(ctx, s.repo, s.folderShares, s.auth, nil, principal, isAdmin)
}

func (s *FolderService) requireFolderRole(ctx context.Context, principal string, isAdmin bool, id string, allowed func(string) bool, action string) (*domain.Folder, error) {
	folder, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	resolver, err := s.accessResolver(ctx, principal, isAdmin)
	if err != nil {
		return nil, fmt.Errorf("resolve folder access: %w", err)
	}
	role, err := resolver.folderRole(ctx, folder)
	if err != nil {
		return nil, fmt.Errorf("resolve folder access: %w", err)
	}
	if !allowed(role) {
		return nil, domain.ErrAccessDenied("principal %q cannot %s folder %q", principal, action, id)
	}
	return folder, nil
}

// CreateFolder creates a folder for the caller.
func (s *FolderService) CreateFolder(ctx context.Context, principal string, req domain.CreateFolderRequest) (*domain.Folder, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if err := s.validateFolderBindings(ctx, req.DefaultProjectID, req.DefaultEnvironmentID); err != nil {
		return nil, err
	}
	if req.ParentFolderID != nil && strings.TrimSpace(*req.ParentFolderID) != "" {
		parent, err := s.requireFolderRole(ctx, principal, false, strings.TrimSpace(*req.ParentFolderID), roleAllowsManage, "manage")
		if err != nil {
			return nil, fmt.Errorf("get parent folder: %w", err)
		}
		_ = parent
	}

	folder, err := s.repo.Create(ctx, &domain.Folder{
		ID:                   domain.NewID(),
		Name:                 strings.TrimSpace(req.Name),
		Owner:                principal,
		ParentFolderID:       req.ParentFolderID,
		GitRepoID:            req.GitRepoID,
		GitRootPath:          req.GitRootPath,
		DefaultProjectID:     req.DefaultProjectID,
		DefaultEnvironmentID: req.DefaultEnvironmentID,
	})
	if err != nil {
		return nil, fmt.Errorf("create folder: %w", err)
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "CREATE_FOLDER",
		Status:        "ALLOWED",
	})
	return folder, nil
}

// GetFolderForPrincipal retrieves a folder if the caller can access it.
func (s *FolderService) GetFolderForPrincipal(ctx context.Context, principal string, isAdmin bool, id string) (*domain.Folder, error) {
	return s.requireFolderRole(ctx, principal, isAdmin, id, roleAllowsRead, "read")
}

// ListFoldersForPrincipal lists folders visible to the caller.
func (s *FolderService) ListFoldersForPrincipal(ctx context.Context, principal string, isAdmin bool, owner *string) ([]domain.Folder, error) {
	targetOwner := strings.TrimSpace(principal)
	if owner != nil && strings.TrimSpace(*owner) != "" {
		targetOwner = strings.TrimSpace(*owner)
	}
	if targetOwner != "" {
		if _, err := s.repo.EnsurePersonalRoot(ctx, targetOwner); err != nil {
			return nil, fmt.Errorf("ensure personal root: %w", err)
		}
	}

	if isAdmin {
		if owner != nil && strings.TrimSpace(*owner) != "" {
			return s.repo.ListByOwner(ctx, strings.TrimSpace(*owner))
		}
		return s.repo.ListAll(ctx)
	}

	resolver, err := s.accessResolver(ctx, principal, isAdmin)
	if err != nil {
		return nil, fmt.Errorf("resolve folder access: %w", err)
	}
	items, err := s.repo.ListAll(ctx)
	if err != nil {
		return nil, err
	}
	filtered := make([]domain.Folder, 0, len(items))
	for _, item := range items {
		if owner != nil && strings.TrimSpace(*owner) != "" && item.Owner != strings.TrimSpace(*owner) {
			continue
		}
		role, roleErr := resolver.folderRole(ctx, &item)
		if roleErr != nil {
			return nil, fmt.Errorf("resolve folder access: %w", roleErr)
		}
		if roleAllowsRead(role) {
			filtered = append(filtered, item)
		}
	}
	return filtered, nil
}

// UpdateFolder updates folder metadata.
func (s *FolderService) UpdateFolder(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateFolderRequest) (*domain.Folder, error) {
	folder, err := s.requireFolderRole(ctx, principal, isAdmin, id, roleAllowsManage, "manage")
	if err != nil {
		return nil, err
	}
	if folder.SystemRole != nil && *folder.SystemRole == domain.FolderSystemRolePersonalRoot {
		return nil, domain.ErrValidation("personal root folders cannot be modified")
	}
	nextProjectID := folder.DefaultProjectID
	if req.DefaultProjectID != nil {
		nextProjectID = req.DefaultProjectID
	}
	nextEnvironmentID := folder.DefaultEnvironmentID
	if req.DefaultEnvironmentID != nil {
		nextEnvironmentID = req.DefaultEnvironmentID
	}
	if err := s.validateFolderBindings(ctx, nextProjectID, nextEnvironmentID); err != nil {
		return nil, err
	}
	updated, err := s.repo.Update(ctx, id, req)
	if err != nil {
		return nil, fmt.Errorf("update folder: %w", err)
	}
	if folderContextChanged(folder, updated) {
		if err := s.enqueueSubtreeInvalidations(ctx, updated); err != nil {
			return nil, err
		}
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "UPDATE_FOLDER",
		Status:        "ALLOWED",
	})
	return updated, nil
}

// MoveFolder reparents a folder subtree while enforcing git and context boundaries.
func (s *FolderService) MoveFolder(ctx context.Context, principal string, isAdmin bool, id string, req domain.MoveFolderRequest) (*domain.Folder, error) {
	folder, err := s.requireFolderRole(ctx, principal, isAdmin, id, roleAllowsManage, "manage")
	if err != nil {
		return nil, err
	}
	if folder.SystemRole != nil && *folder.SystemRole == domain.FolderSystemRolePersonalRoot {
		return nil, domain.ErrValidation("personal root folders cannot be moved")
	}

	var parent *domain.Folder
	if req.ParentFolderID != nil && strings.TrimSpace(*req.ParentFolderID) != "" {
		parent, err = s.requireFolderRole(ctx, principal, isAdmin, strings.TrimSpace(*req.ParentFolderID), roleAllowsManage, "manage")
		if err != nil {
			return nil, fmt.Errorf("get destination parent folder: %w", err)
		}
	}

	currentCtx, err := s.resolveFolderContext(ctx, folder, folder.ParentFolderID)
	if err != nil {
		return nil, fmt.Errorf("resolve current folder context: %w", err)
	}
	targetCtx, err := s.resolveFolderContext(ctx, folder, req.ParentFolderID)
	if err != nil {
		return nil, fmt.Errorf("resolve destination folder context: %w", err)
	}
	if currentCtx.gitRepoID != nil && targetCtx.gitRepoID == nil && !req.ConfirmLeaveGit {
		return nil, domain.ErrValidation("moving a git-backed folder into a non-git parent requires confirmation")
	}
	if ptrValue(currentCtx.gitRepoID) != "" &&
		ptrValue(targetCtx.gitRepoID) != "" &&
		ptrValue(currentCtx.gitRepoID) != ptrValue(targetCtx.gitRepoID) {
		return nil, domain.ErrConflict("moving folders across git repositories is not supported")
	}
	if (ptrValue(currentCtx.projectID) != ptrValue(targetCtx.projectID) ||
		ptrValue(currentCtx.environmentID) != ptrValue(targetCtx.environmentID)) && !req.ConfirmContextChange {
		return nil, domain.ErrValidation("moving this folder changes its execution context and requires confirmation")
	}
	if err := s.validateDescendantNotebookOwners(ctx, folder, req.ParentFolderID); err != nil {
		return nil, err
	}

	moved, err := s.repo.Move(ctx, folder.ID, req.ParentFolderID)
	if err != nil {
		return nil, fmt.Errorf("move folder: %w", err)
	}
	if folderContextChanged(folder, moved) || ptrValue(currentCtx.projectID) != ptrValue(targetCtx.projectID) || ptrValue(currentCtx.environmentID) != ptrValue(targetCtx.environmentID) || ptrValue(currentCtx.gitRepoID) != ptrValue(targetCtx.gitRepoID) {
		if err := s.enqueueSubtreeInvalidations(ctx, moved); err != nil {
			return nil, err
		}
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "MOVE_FOLDER",
		Status:        "ALLOWED",
	})
	_ = parent
	return moved, nil
}

// DeleteFolder deletes a folder owned by the caller.
func (s *FolderService) DeleteFolder(ctx context.Context, principal string, isAdmin bool, id string) error {
	folder, err := s.requireFolderRole(ctx, principal, isAdmin, id, roleAllowsManage, "manage")
	if err != nil {
		return err
	}
	if folder.SystemRole != nil && *folder.SystemRole == domain.FolderSystemRolePersonalRoot {
		return domain.ErrValidation("personal root folders cannot be deleted")
	}
	if err := s.repo.Delete(ctx, id); err != nil {
		return fmt.Errorf("delete folder: %w", err)
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "DELETE_FOLDER",
		Status:        "ALLOWED",
	})
	return nil
}

// ShareFolder grants or updates folder access for another principal.
func (s *FolderService) ShareFolder(ctx context.Context, principal string, isAdmin bool, folderID string, share domain.FolderShare) (*domain.FolderShare, error) {
	if _, err := s.requireFolderRole(ctx, principal, isAdmin, folderID, roleAllowsManage, "manage"); err != nil {
		return nil, err
	}
	share.FolderID = folderID
	share.PrincipalName = strings.TrimSpace(share.PrincipalName)
	share.Role = strings.TrimSpace(share.Role)
	if share.PrincipalName == "" {
		return nil, domain.ErrValidation("principal_name is required")
	}
	if share.Role == "" {
		share.Role = domain.FolderShareRoleViewer
	}
	if s.grants == nil {
		if s.folderShares == nil {
			return nil, domain.ErrValidation("folder sharing is not configured")
		}
		created, err := s.folderShares.Upsert(ctx, &share)
		if err != nil {
			return nil, fmt.Errorf("share folder: %w", err)
		}
		_ = s.audit.Insert(ctx, &domain.AuditEntry{
			PrincipalName: principal,
			Action:        "SHARE_FOLDER",
			Status:        "ALLOWED",
		})
		return created, nil
	}
	share.Role = domain.NormalizeShareRole(share.Role)
	if share.Role == "" {
		return nil, domain.ErrValidation("role must be viewer, editor, or manager")
	}
	if err := s.replaceFolderGrant(ctx, share.FolderID, share.PrincipalName, share.Role); err != nil {
		return nil, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "SHARE_FOLDER",
		Status:        "ALLOWED",
	})
	return &share, nil
}

// UnshareFolder removes an inherited folder share.
func (s *FolderService) UnshareFolder(ctx context.Context, principal string, isAdmin bool, folderID string, principalName string) error {
	if s.grants == nil {
		if s.folderShares == nil {
			return domain.ErrValidation("folder sharing is not configured")
		}
		if err := s.folderShares.Delete(ctx, folderID, strings.TrimSpace(principalName)); err != nil {
			return err
		}
		_ = s.audit.Insert(ctx, &domain.AuditEntry{
			PrincipalName: principal,
			Action:        "UNSHARE_FOLDER",
			Status:        "ALLOWED",
		})
		return nil
	}
	if _, err := s.requireFolderRole(ctx, principal, isAdmin, folderID, roleAllowsManage, "manage"); err != nil {
		return err
	}
	if err := s.clearFolderGrants(ctx, folderID, strings.TrimSpace(principalName)); err != nil {
		return fmt.Errorf("unshare folder: %w", err)
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "UNSHARE_FOLDER",
		Status:        "ALLOWED",
	})
	return nil
}

// ListFolderShares returns explicit shares on a folder.
func (s *FolderService) ListFolderShares(ctx context.Context, principal string, isAdmin bool, folderID string) ([]domain.FolderShare, error) {
	if s.grants == nil {
		if s.folderShares == nil {
			return nil, nil
		}
		return s.folderShares.ListByFolder(ctx, folderID)
	}
	if _, err := s.requireFolderRole(ctx, principal, isAdmin, folderID, roleAllowsRead, "read"); err != nil {
		return nil, err
	}
	items, _, err := s.grants.ListForSecurable(ctx, domain.SecurableFolder, folderID, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return nil, fmt.Errorf("list folder shares: %w", err)
	}
	byPrincipal := map[string]domain.FolderShare{}
	for _, item := range items {
		if item.PrincipalType != "user" {
			continue
		}
		role := folderRoleFromPrivilege(item.Privilege)
		if role == "" {
			continue
		}
		existing := byPrincipal[item.PrincipalID]
		if shareRoleLevel(role) <= shareRoleLevel(existing.Role) {
			continue
		}
		byPrincipal[item.PrincipalID] = domain.FolderShare{
			FolderID:      folderID,
			PrincipalName: item.PrincipalID,
			Role:          role,
		}
	}
	out := make([]domain.FolderShare, 0, len(byPrincipal))
	for _, share := range byPrincipal {
		out = append(out, share)
	}
	return out, nil
}

type folderEffectiveContext struct {
	projectID     *string
	environmentID *string
	gitRepoID     *string
}

func (s *FolderService) resolveFolderContext(ctx context.Context, folder *domain.Folder, parentFolderID *string) (*folderEffectiveContext, error) {
	resolved := &folderEffectiveContext{}
	if folder == nil {
		return resolved, nil
	}
	if parentFolderID != nil && strings.TrimSpace(*parentFolderID) != "" {
		ancestors, err := s.repo.ListAncestors(ctx, strings.TrimSpace(*parentFolderID))
		if err != nil {
			return nil, err
		}
		for _, ancestor := range ancestors {
			if resolved.projectID == nil && ancestor.DefaultProjectID != nil {
				resolved.projectID = ancestor.DefaultProjectID
			}
			if resolved.environmentID == nil && ancestor.DefaultEnvironmentID != nil {
				resolved.environmentID = ancestor.DefaultEnvironmentID
			}
			if resolved.gitRepoID == nil && ancestor.GitRepoID != nil {
				resolved.gitRepoID = ancestor.GitRepoID
			}
		}
	}
	if folder.DefaultProjectID != nil {
		resolved.projectID = folder.DefaultProjectID
	}
	if folder.DefaultEnvironmentID != nil {
		resolved.environmentID = folder.DefaultEnvironmentID
	}
	if folder.GitRepoID != nil {
		resolved.gitRepoID = folder.GitRepoID
	}
	return resolved, nil
}

func (s *FolderService) validateFolderBindings(ctx context.Context, projectID, environmentID *string) error {
	if environmentID == nil || strings.TrimSpace(*environmentID) == "" {
		return nil
	}
	if projectID == nil || strings.TrimSpace(*projectID) == "" {
		return domain.ErrValidation("default_environment_id requires default_project_id")
	}
	if s.environments == nil {
		return nil
	}
	environment, err := s.environments.GetByID(ctx, strings.TrimSpace(*environmentID))
	if err != nil {
		return fmt.Errorf("get environment %q: %w", strings.TrimSpace(*environmentID), err)
	}
	if strings.TrimSpace(environment.ProjectID) != strings.TrimSpace(*projectID) {
		return domain.ErrValidation("default_environment_id must belong to default_project_id")
	}
	return nil
}

func (s *FolderService) replaceFolderGrant(ctx context.Context, folderID, principalName, role string) error {
	if err := s.clearFolderGrants(ctx, folderID, principalName); err != nil {
		return err
	}
	_, err := s.grants.Grant(ctx, &domain.PrivilegeGrant{
		PrincipalID:   principalName,
		PrincipalType: "user",
		SecurableType: domain.SecurableFolder,
		SecurableID:   folderID,
		Privilege:     folderPrivilegeForRole(role),
	})
	if err != nil {
		return fmt.Errorf("grant folder privilege: %w", err)
	}
	return nil
}

func (s *FolderService) clearFolderGrants(ctx context.Context, folderID, principalName string) error {
	items, _, err := s.grants.ListForSecurable(ctx, domain.SecurableFolder, folderID, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return err
	}
	for _, item := range items {
		if item.PrincipalType != "user" || item.PrincipalID != principalName {
			continue
		}
		if err := s.grants.Revoke(ctx, &item); err != nil {
			return err
		}
	}
	return nil
}

func folderPrivilegeForRole(role string) string {
	switch domain.NormalizeShareRole(role) {
	case domain.FolderShareRoleManager:
		return domain.PrivManage
	case domain.FolderShareRoleEditor:
		return domain.PrivModify
	case domain.FolderShareRoleViewer:
		return domain.PrivSelect
	default:
		return ""
	}
}

func folderRoleFromPrivilege(privilege string) string {
	switch strings.TrimSpace(privilege) {
	case domain.PrivManage:
		return domain.FolderShareRoleManager
	case domain.PrivModify:
		return domain.FolderShareRoleEditor
	case domain.PrivSelect:
		return domain.FolderShareRoleViewer
	default:
		return ""
	}
}

func ptrValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func (s *FolderService) validateDescendantNotebookOwners(ctx context.Context, folder *domain.Folder, parentFolderID *string) error {
	if s.notebooks == nil || folder == nil {
		return nil
	}
	notebooks, _, err := s.notebooks.ListNotebooks(ctx, nil, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return fmt.Errorf("list notebooks for owner validation: %w", err)
	}
	folders, err := s.repo.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list folders for owner validation: %w", err)
	}
	folderPaths := make(map[string]string, len(folders))
	for _, item := range folders {
		folderPaths[item.ID] = item.Path
	}
	checkedOwners := map[string]bool{}
	for _, nb := range notebooks {
		itemPath := strings.TrimSpace(folderPaths[nb.FolderID])
		if itemPath != folder.Path && !strings.HasPrefix(itemPath, folder.Path+"/") {
			continue
		}
		if checkedOwners[nb.Owner] {
			continue
		}
		checkedOwners[nb.Owner] = true
		if strings.TrimSpace(nb.Owner) == strings.TrimSpace(folder.Owner) {
			continue
		}
		allowedDirect := false
		if s.auth != nil {
			for _, privilege := range []string{domain.PrivManage, domain.PrivModify, domain.PrivSelect} {
				allowed, checkErr := s.auth.CheckPrivilege(ctx, nb.Owner, domain.SecurableFolder, folder.ID, privilege)
				if checkErr != nil {
					return fmt.Errorf("check direct folder grant for %q: %w", nb.Owner, checkErr)
				}
				if allowed {
					allowedDirect = true
					break
				}
			}
		}
		if allowedDirect {
			continue
		}
		if parentFolderID == nil || strings.TrimSpace(*parentFolderID) == "" {
			return domain.ErrValidation("moving folder would strand notebook owner %q outside the destination ACLs", nb.Owner)
		}
		parent, err := s.repo.GetByID(ctx, strings.TrimSpace(*parentFolderID))
		if err != nil {
			return fmt.Errorf("get destination parent folder: %w", err)
		}
		resolver, err := s.accessResolver(ctx, nb.Owner, false)
		if err != nil {
			return fmt.Errorf("resolve destination owner access: %w", err)
		}
		role, err := resolver.folderRole(ctx, parent)
		if err != nil {
			return fmt.Errorf("resolve destination owner access: %w", err)
		}
		if !roleAllowsRead(role) {
			return domain.ErrValidation("moving folder would strand notebook owner %q outside the destination ACLs", nb.Owner)
		}
	}
	return nil
}

func folderContextChanged(before *domain.Folder, after *domain.Folder) bool {
	if before == nil || after == nil {
		return false
	}
	return notebookStringValue(before.GitRepoID) != notebookStringValue(after.GitRepoID) ||
		notebookStringValue(before.GitRootPath) != notebookStringValue(after.GitRootPath) ||
		notebookStringValue(before.DefaultProjectID) != notebookStringValue(after.DefaultProjectID) ||
		notebookStringValue(before.DefaultEnvironmentID) != notebookStringValue(after.DefaultEnvironmentID)
}

func (s *FolderService) enqueueSubtreeInvalidations(ctx context.Context, folder *domain.Folder) error {
	if s.notebooks == nil || s.events == nil || folder == nil {
		return nil
	}
	folders, err := s.repo.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list folders for invalidation fan-out: %w", err)
	}
	folderPaths := make(map[string]string, len(folders))
	for _, item := range folders {
		folderPaths[item.ID] = item.Path
	}
	targetPath := strings.TrimSpace(folder.Path)
	if targetPath == "" {
		return nil
	}
	notebooks, _, err := s.notebooks.ListNotebooks(ctx, nil, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return fmt.Errorf("list notebooks for invalidation fan-out: %w", err)
	}
	for _, nb := range notebooks {
		itemPath := strings.TrimSpace(folderPaths[nb.FolderID])
		if itemPath != targetPath && !strings.HasPrefix(itemPath, targetPath+"/") {
			continue
		}
		key := fmt.Sprintf("%s:%s:%d", domain.NotebookEventTypeInvalidateContext, nb.ID, folder.UpdatedAt.UnixNano())
		if _, err := s.events.Enqueue(ctx, &domain.OrchestrationEvent{
			EventType:      domain.NotebookEventTypeInvalidateContext,
			PayloadJSON:    map[string]any{domain.NotebookEventPayloadNotebookID: nb.ID},
			Status:         domain.OrchestrationEventStatusPending,
			IdempotencyKey: &key,
		}); err != nil {
			return fmt.Errorf("enqueue notebook invalidation for %s: %w", nb.ID, err)
		}
	}
	return nil
}
