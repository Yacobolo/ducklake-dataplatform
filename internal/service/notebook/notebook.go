package notebook

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"duck-demo/internal/domain"
)

type notebookContextInvalidator interface {
	InvalidateNotebook(ctx context.Context, notebookID string) error
}

// Service provides business logic for notebook and cell operations.
type Service struct {
	repo           domain.NotebookRepository
	workspaces     domain.WorkspaceRepository
	folders        domain.FolderRepository
	folderShares   domain.FolderShareRepository
	auth           domain.AuthorizationService
	grants         domain.GrantRepository
	notebookShares domain.NotebookShareRepository
	projects       domain.ProjectRepository
	environments   domain.EnvironmentRepository
	audit          domain.AuditRepository
	models         domain.ModelRepository
	links          domain.NotebookModelLinkRepository
	invalidator    notebookContextInvalidator
}

// New creates a new Service.
func New(repo domain.NotebookRepository, audit domain.AuditRepository) *Service {
	return &Service{repo: repo, audit: audit}
}

// SetWorkspaceRepository configures workspace lookups for notebook access and defaults.
func (s *Service) SetWorkspaceRepository(workspaces domain.WorkspaceRepository) {
	s.workspaces = workspaces
}

// SetFolderRepository configures folder defaults for notebook creation and context resolution.
func (s *Service) SetFolderRepository(folders domain.FolderRepository) {
	s.folders = folders
}

// SetAuthorization configures folder privilege checks.
func (s *Service) SetAuthorization(auth domain.AuthorizationService) {
	s.auth = auth
}

// SetGrantRepository configures folder share wrappers backed by privilege grants.
func (s *Service) SetGrantRepository(grants domain.GrantRepository) {
	s.grants = grants
}

// SetProjectRepositories configures project/environment validation helpers.
func (s *Service) SetProjectRepositories(projects domain.ProjectRepository, environments domain.EnvironmentRepository) {
	s.projects = projects
	s.environments = environments
}

// SetShareRepositories configures direct notebook sharing repositories.
func (s *Service) SetShareRepositories(folderShares domain.FolderShareRepository, notebookShares domain.NotebookShareRepository) {
	s.folderShares = folderShares
	s.notebookShares = notebookShares
}

// SetPublishRepositories configures optional repositories for notebook publish metadata lookups.
func (s *Service) SetPublishRepositories(models domain.ModelRepository, links domain.NotebookModelLinkRepository) {
	s.models = models
	s.links = links
}

// SetContextInvalidator configures runtime session invalidation for context-changing notebook mutations.
func (s *Service) SetContextInvalidator(invalidator notebookContextInvalidator) {
	s.invalidator = invalidator
}

func (s *Service) accessResolver(ctx context.Context, principal string, isAdmin bool) (*principalAccessResolver, error) {
	return newPrincipalAccessResolver(ctx, s.workspaces, s.folders, s.folderShares, s.auth, s.notebookShares, principal, isAdmin)
}

// CreateNotebook creates a new notebook owned by the given principal.
func (s *Service) CreateNotebook(ctx context.Context, principal string, req domain.CreateNotebookRequest) (*domain.Notebook, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	nb := &domain.Notebook{
		ID:          domain.NewID(),
		Name:        req.Name,
		Description: req.Description,
		Owner:       principal,
	}
	if req.FolderID != nil && strings.TrimSpace(*req.FolderID) != "" {
		nb.FolderID = strings.TrimSpace(*req.FolderID)
	} else if s.folders != nil {
		root, err := s.folders.EnsurePersonalWorkspaceRoot(ctx, principal)
		if err != nil {
			return nil, fmt.Errorf("ensure personal workspace root: %w", err)
		}
		nb.FolderID = root.ID
	}
	if err := s.requireFolderWriteAccess(ctx, principal, false, nb.FolderID); err != nil {
		return nil, err
	}
	if err := s.validateNotebookEffectiveContext(ctx, nb); err != nil {
		return nil, err
	}
	result, err := s.repo.CreateNotebook(ctx, nb)
	if err != nil {
		return nil, fmt.Errorf("create notebook: %w", err)
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "CREATE_NOTEBOOK",
		Status:        "ALLOWED",
	})
	return result, nil
}

func (s *Service) requireNotebookRole(ctx context.Context, principal string, isAdmin bool, id string, allowed func(string) bool, action string) (*domain.Notebook, error) {
	nb, err := s.repo.GetNotebook(ctx, id)
	if err != nil {
		return nil, err
	}
	resolver, err := s.accessResolver(ctx, principal, isAdmin)
	if err != nil {
		return nil, fmt.Errorf("resolve notebook access: %w", err)
	}
	role, err := resolver.notebookRole(ctx, nb)
	if err != nil {
		return nil, fmt.Errorf("resolve notebook access: %w", err)
	}
	if !allowed(role) {
		return nil, domain.ErrAccessDenied("principal %q cannot %s notebook %q", principal, action, id)
	}
	return nb, nil
}

func (s *Service) requireNotebookReadAccess(ctx context.Context, principal string, isAdmin bool, id string) (*domain.Notebook, error) {
	return s.requireNotebookRole(ctx, principal, isAdmin, id, roleAllowsRead, "read")
}

func (s *Service) requireNotebookAccess(ctx context.Context, principal string, isAdmin bool, id string) (*domain.Notebook, error) {
	return s.requireNotebookReadAccess(ctx, principal, isAdmin, id)
}

func (s *Service) requireNotebookWriteAccess(ctx context.Context, principal string, isAdmin bool, id string) (*domain.Notebook, error) {
	return s.requireNotebookRole(ctx, principal, isAdmin, id, roleAllowsWrite, "modify")
}

func (s *Service) requireNotebookManageAccess(ctx context.Context, principal string, isAdmin bool, id string) (*domain.Notebook, error) {
	return s.requireNotebookRole(ctx, principal, isAdmin, id, roleAllowsManage, "manage")
}

func (s *Service) requireFolderWriteAccess(ctx context.Context, principal string, isAdmin bool, folderID string) error {
	if s.folders == nil || strings.TrimSpace(folderID) == "" {
		return nil
	}
	resolver, err := s.accessResolver(ctx, principal, isAdmin)
	if err != nil {
		return fmt.Errorf("resolve folder access: %w", err)
	}
	folder, err := s.folders.GetByID(ctx, folderID)
	if err != nil {
		return err
	}
	role, err := resolver.folderRole(ctx, folder)
	if err != nil {
		return fmt.Errorf("resolve folder access: %w", err)
	}
	if !roleAllowsWrite(role) {
		return domain.ErrAccessDenied("principal %q cannot create notebooks in folder %q", principal, folderID)
	}
	return nil
}

// GetNotebook retrieves a notebook and its cells.
func (s *Service) GetNotebook(ctx context.Context, id string) (*domain.Notebook, []domain.Cell, error) {
	nb, err := s.repo.GetNotebook(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	cells, err := s.repo.ListCells(ctx, id)
	if err != nil {
		return nil, nil, fmt.Errorf("list cells: %w", err)
	}
	return nb, cells, nil
}

// GetNotebookForPrincipal retrieves a notebook and its cells for the owner or an admin.
func (s *Service) GetNotebookForPrincipal(ctx context.Context, principal string, isAdmin bool, id string) (*domain.Notebook, []domain.Cell, error) {
	nb, err := s.requireNotebookAccess(ctx, principal, isAdmin, id)
	if err != nil {
		return nil, nil, err
	}
	cells, err := s.repo.ListCells(ctx, id)
	if err != nil {
		return nil, nil, fmt.Errorf("list cells: %w", err)
	}
	return nb, cells, nil
}

// GetNotebookContext resolves the effective project/environment/git context for a notebook.
func (s *Service) GetNotebookContext(ctx context.Context, principal string, isAdmin bool, id string) (*domain.NotebookContext, error) {
	nb, err := s.requireNotebookReadAccess(ctx, principal, isAdmin, id)
	if err != nil {
		return nil, err
	}
	return s.resolveContextForNotebook(ctx, nb)
}

// GetPublishModel resolves model publish metadata for a notebook.
func (s *Service) GetPublishModel(ctx context.Context, notebookID string) (*domain.NotebookPublishModel, error) {
	if s.models == nil || s.links == nil {
		return nil, nil
	}

	link, err := s.links.GetByNotebookID(ctx, notebookID)
	if err != nil {
		if errors.As(err, new(*domain.NotFoundError)) {
			return nil, nil
		}
		return nil, err
	}

	model, err := s.models.GetByID(ctx, link.ModelID)
	if err != nil {
		if errors.As(err, new(*domain.NotFoundError)) {
			return nil, nil
		}
		return nil, err
	}

	return &domain.NotebookPublishModel{
		ProjectName:     model.ProjectName,
		Name:            model.Name,
		Materialization: model.Materialization,
		OutputCellID:    link.OutputCellID,
	}, nil
}

// ListNotebooks lists notebooks, optionally filtered by owner.
func (s *Service) ListNotebooks(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Notebook, int64, error) {
	return s.repo.ListNotebooks(ctx, owner, page)
}

// ListNotebooksForPrincipal lists notebooks visible to the caller.
func (s *Service) ListNotebooksForPrincipal(ctx context.Context, principal string, isAdmin bool, owner *string, page domain.PageRequest) ([]domain.Notebook, int64, error) {
	if isAdmin {
		return s.repo.ListNotebooks(ctx, owner, page)
	}
	requestedOwner := strings.TrimSpace(derefString(owner))
	if requestedOwner == principal {
		ownerName := requestedOwner
		return s.repo.ListNotebooks(ctx, &ownerName, page)
	}
	resolver, err := s.accessResolver(ctx, principal, isAdmin)
	if err != nil {
		return nil, 0, fmt.Errorf("resolve notebook access: %w", err)
	}
	items, _, err := s.repo.ListNotebooks(ctx, nil, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return nil, 0, err
	}
	filtered := make([]domain.Notebook, 0, len(items))
	for _, item := range items {
		if owner != nil && strings.TrimSpace(*owner) != "" && item.Owner != strings.TrimSpace(*owner) {
			continue
		}
		role, roleErr := resolver.notebookRole(ctx, &item)
		if roleErr != nil {
			return nil, 0, fmt.Errorf("resolve notebook access: %w", roleErr)
		}
		if roleAllowsRead(role) {
			filtered = append(filtered, item)
		}
	}
	total := int64(len(filtered))
	if requestedOwner != "" && requestedOwner != principal && total == 0 {
		return nil, 0, domain.ErrAccessDenied("principal %q cannot list notebooks owned by %q", principal, requestedOwner)
	}
	start := page.Offset()
	if start >= len(filtered) {
		return []domain.Notebook{}, total, nil
	}
	end := start + page.Limit()
	if end > len(filtered) {
		end = len(filtered)
	}
	return filtered[start:end], total, nil
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// UpdateNotebook updates notebook metadata. Only the owner or admin can update.
func (s *Service) UpdateNotebook(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateNotebookRequest) (*domain.Notebook, error) {
	nb, err := s.requireNotebookWriteAccess(ctx, principal, isAdmin, id)
	if err != nil {
		return nil, err
	}
	if req.FolderID != nil && strings.TrimSpace(*req.FolderID) != "" && strings.TrimSpace(*req.FolderID) != strings.TrimSpace(nb.FolderID) {
		return nil, domain.ErrValidation("use move notebook to change folder placement")
	}
	beforeCtx, err := s.resolveContextForNotebook(ctx, nb)
	if err != nil {
		return nil, fmt.Errorf("resolve notebook context before update: %w", err)
	}
	candidate := *nb
	if req.ProjectOverrideID != nil {
		candidate.ProjectOverrideID = req.ProjectOverrideID
	}
	if req.EnvironmentOverrideID != nil {
		candidate.EnvironmentOverrideID = req.EnvironmentOverrideID
	}
	if err := s.validateNotebookEffectiveContext(ctx, &candidate); err != nil {
		return nil, err
	}
	result, err := s.repo.UpdateNotebook(ctx, id, req)
	if err != nil {
		return nil, fmt.Errorf("update notebook: %w", err)
	}
	afterCtx, err := s.resolveContextForNotebook(ctx, result)
	if err != nil {
		return nil, fmt.Errorf("resolve notebook context after update: %w", err)
	}
	if contextsDiffer(beforeCtx, afterCtx) {
		if err := s.invalidateNotebookContext(ctx, result.ID); err != nil {
			return nil, err
		}
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "UPDATE_NOTEBOOK",
		Status:        "ALLOWED",
	})
	return result, nil
}

// MoveNotebook moves a notebook between folders while enforcing git and project-context boundaries.
func (s *Service) MoveNotebook(ctx context.Context, principal string, isAdmin bool, id string, req domain.MoveNotebookRequest) (*domain.Notebook, error) {
	nb, err := s.requireNotebookManageAccess(ctx, principal, isAdmin, id)
	if err != nil {
		return nil, err
	}
	if s.folders == nil {
		return nil, domain.ErrValidation("folder repository is not configured")
	}
	targetFolderID := strings.TrimSpace(req.FolderID)
	if targetFolderID == "" {
		return nil, domain.ErrValidation("folder_id is required")
	}
	targetFolder, err := s.folders.GetByID(ctx, targetFolderID)
	if err != nil {
		return nil, fmt.Errorf("get destination folder: %w", err)
	}
	if err := s.requireFolderWriteAccess(ctx, principal, isAdmin, targetFolder.ID); err != nil {
		return nil, err
	}
	ownerResolver, err := s.accessResolver(ctx, nb.Owner, false)
	if err != nil {
		return nil, fmt.Errorf("resolve owner folder access: %w", err)
	}
	ownerRole, err := ownerResolver.folderRole(ctx, targetFolder)
	if err != nil {
		return nil, fmt.Errorf("resolve owner folder access: %w", err)
	}
	if !roleAllowsRead(ownerRole) {
		return nil, domain.ErrValidation("destination folder must remain readable to the notebook owner")
	}

	currentCtx, err := s.resolveContextForNotebook(ctx, nb)
	if err != nil {
		return nil, fmt.Errorf("resolve current notebook context: %w", err)
	}
	targetCtx, err := s.resolveContextForNotebook(ctx, &domain.Notebook{
		ID:                    nb.ID,
		FolderID:              targetFolder.ID,
		Name:                  nb.Name,
		Owner:                 nb.Owner,
		ProjectOverrideID:     nb.ProjectOverrideID,
		EnvironmentOverrideID: nb.EnvironmentOverrideID,
	})
	if err != nil {
		return nil, fmt.Errorf("resolve destination notebook context: %w", err)
	}
	if currentCtx.EffectiveGitRepoID != nil && targetCtx.EffectiveGitRepoID == nil && !req.ConfirmLeaveGit {
		return nil, domain.ErrValidation("moving a git-backed notebook into a non-git folder requires confirmation")
	}
	if notebookStringValue(currentCtx.EffectiveGitRepoID) != "" &&
		notebookStringValue(targetCtx.EffectiveGitRepoID) != "" &&
		notebookStringValue(currentCtx.EffectiveGitRepoID) != notebookStringValue(targetCtx.EffectiveGitRepoID) {
		return nil, domain.ErrConflict("moving notebooks across git repositories is not supported; duplicate the notebook into the destination repo folder instead")
	}
	if projectOrEnvironmentChanged(currentCtx, targetCtx) && !req.ConfirmContextChange {
		return nil, domain.ErrValidation("moving this notebook changes its execution context and requires confirmation")
	}

	targetGitPath, err := s.resolveTargetGitPath(ctx, targetCtx, targetFolder.ID, nb.Name, req.GitPath)
	if err != nil {
		return nil, err
	}
	if targetCtx.EffectiveGitRepoID != nil {
		if err := s.ensureUniqueGitPath(ctx, nb.ID, *targetCtx.EffectiveGitRepoID, *targetGitPath); err != nil {
			return nil, err
		}
	}
	if err := s.validateResolvedContext(ctx, targetCtx); err != nil {
		return nil, err
	}

	result, err := s.repo.UpdateNotebookSync(ctx, &domain.Notebook{
		ID:                    nb.ID,
		FolderID:              targetFolder.ID,
		Name:                  nb.Name,
		Description:           nb.Description,
		Owner:                 nb.Owner,
		GitRepoID:             targetCtx.EffectiveGitRepoID,
		GitPath:               targetGitPath,
		ProjectOverrideID:     nb.ProjectOverrideID,
		EnvironmentOverrideID: nb.EnvironmentOverrideID,
	})
	if err != nil {
		return nil, fmt.Errorf("move notebook: %w", err)
	}
	if contextsDiffer(currentCtx, targetCtx) {
		if err := s.invalidateNotebookContext(ctx, result.ID); err != nil {
			return nil, err
		}
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "MOVE_NOTEBOOK",
		Status:        "ALLOWED",
	})
	return result, nil
}

// DuplicateNotebook copies a notebook and its cells into a destination folder.
func (s *Service) DuplicateNotebook(ctx context.Context, principal string, isAdmin bool, id string, req domain.DuplicateNotebookRequest) (*domain.Notebook, error) {
	source, err := s.requireNotebookReadAccess(ctx, principal, isAdmin, id)
	if err != nil {
		return nil, err
	}
	if s.folders == nil {
		return nil, domain.ErrValidation("folder repository is not configured")
	}
	targetFolderID := strings.TrimSpace(req.FolderID)
	if targetFolderID == "" {
		return nil, domain.ErrValidation("folder_id is required")
	}
	targetFolder, err := s.folders.GetByID(ctx, targetFolderID)
	if err != nil {
		return nil, fmt.Errorf("get destination folder: %w", err)
	}
	if err := s.requireFolderWriteAccess(ctx, principal, isAdmin, targetFolder.ID); err != nil {
		return nil, err
	}

	targetName := source.Name
	if req.Name != nil && strings.TrimSpace(*req.Name) != "" {
		targetName = strings.TrimSpace(*req.Name)
	}
	targetCtx, err := s.resolveContextForNotebook(ctx, &domain.Notebook{
		FolderID:              targetFolder.ID,
		Name:                  targetName,
		Owner:                 source.Owner,
		ProjectOverrideID:     source.ProjectOverrideID,
		EnvironmentOverrideID: source.EnvironmentOverrideID,
	})
	if err != nil {
		return nil, fmt.Errorf("resolve destination notebook context: %w", err)
	}
	targetGitPath, err := s.resolveTargetGitPath(ctx, targetCtx, targetFolder.ID, targetName, req.GitPath)
	if err != nil {
		return nil, err
	}
	if targetCtx.EffectiveGitRepoID != nil {
		if err := s.ensureUniqueGitPath(ctx, "", *targetCtx.EffectiveGitRepoID, *targetGitPath); err != nil {
			return nil, err
		}
	}
	if err := s.validateResolvedContext(ctx, targetCtx); err != nil {
		return nil, err
	}

	created, err := s.repo.CreateNotebook(ctx, &domain.Notebook{
		ID:                    domain.NewID(),
		FolderID:              targetFolder.ID,
		Name:                  targetName,
		Description:           source.Description,
		Owner:                 source.Owner,
		GitRepoID:             targetCtx.EffectiveGitRepoID,
		GitPath:               targetGitPath,
		ProjectOverrideID:     source.ProjectOverrideID,
		EnvironmentOverrideID: source.EnvironmentOverrideID,
	})
	if err != nil {
		return nil, fmt.Errorf("create duplicated notebook: %w", err)
	}

	cells, err := s.repo.ListCells(ctx, source.ID)
	if err != nil {
		return nil, fmt.Errorf("list source cells: %w", err)
	}
	for _, cell := range cells {
		copied := cell
		copied.ID = domain.NewID()
		copied.NotebookID = created.ID
		if _, err := s.repo.CreateCell(ctx, &copied); err != nil {
			return nil, fmt.Errorf("copy notebook cell %q: %w", cell.ID, err)
		}
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "DUPLICATE_NOTEBOOK",
		Status:        "ALLOWED",
	})
	return created, nil
}

// DeleteNotebook deletes a notebook. Only the owner or admin can delete.
func (s *Service) DeleteNotebook(ctx context.Context, principal string, isAdmin bool, id string) error {
	if _, err := s.requireNotebookManageAccess(ctx, principal, isAdmin, id); err != nil {
		return err
	}
	if err := s.repo.DeleteNotebook(ctx, id); err != nil {
		return fmt.Errorf("delete notebook: %w", err)
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "DELETE_NOTEBOOK",
		Status:        "ALLOWED",
	})
	return nil
}

// ShareNotebook grants or updates direct notebook access.
func (s *Service) ShareNotebook(ctx context.Context, principal string, isAdmin bool, notebookID string, share domain.NotebookShare) (*domain.NotebookShare, error) {
	if s.notebookShares == nil {
		return nil, domain.ErrValidation("notebook sharing is not configured")
	}
	if _, err := s.requireNotebookManageAccess(ctx, principal, isAdmin, notebookID); err != nil {
		return nil, err
	}
	share.NotebookID = notebookID
	share.PrincipalName = strings.TrimSpace(share.PrincipalName)
	share.Role = strings.TrimSpace(share.Role)
	if share.PrincipalName == "" {
		return nil, domain.ErrValidation("principal_name is required")
	}
	if share.Role == "" {
		share.Role = domain.FolderShareRoleViewer
	}
	created, err := s.notebookShares.Upsert(ctx, &share)
	if err != nil {
		return nil, fmt.Errorf("share notebook: %w", err)
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "SHARE_NOTEBOOK",
		Status:        "ALLOWED",
	})
	return created, nil
}

// UnshareNotebook removes direct notebook access.
func (s *Service) UnshareNotebook(ctx context.Context, principal string, isAdmin bool, notebookID string, principalName string) error {
	if s.notebookShares == nil {
		return domain.ErrValidation("notebook sharing is not configured")
	}
	if _, err := s.requireNotebookManageAccess(ctx, principal, isAdmin, notebookID); err != nil {
		return err
	}
	if err := s.notebookShares.Delete(ctx, notebookID, strings.TrimSpace(principalName)); err != nil {
		return fmt.Errorf("unshare notebook: %w", err)
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "UNSHARE_NOTEBOOK",
		Status:        "ALLOWED",
	})
	return nil
}

// ListNotebookShares returns direct notebook shares.
func (s *Service) ListNotebookShares(ctx context.Context, principal string, isAdmin bool, notebookID string) ([]domain.NotebookShare, error) {
	if s.notebookShares == nil {
		return nil, nil
	}
	if _, err := s.requireNotebookReadAccess(ctx, principal, isAdmin, notebookID); err != nil {
		return nil, err
	}
	items, err := s.notebookShares.ListByNotebook(ctx, notebookID)
	if err != nil {
		return nil, fmt.Errorf("list notebook shares: %w", err)
	}
	return items, nil
}

// CreateCell adds a new cell to a notebook. Owner or admin required.
func (s *Service) CreateCell(ctx context.Context, principal string, isAdmin bool, notebookID string, req domain.CreateCellRequest) (*domain.Cell, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if _, err := s.requireNotebookWriteAccess(ctx, principal, isAdmin, notebookID); err != nil {
		return nil, err
	}

	pos := 0
	if req.Position != nil {
		pos = *req.Position
	} else {
		maxPos, err := s.repo.GetMaxPosition(ctx, notebookID)
		if err != nil {
			return nil, fmt.Errorf("get max position: %w", err)
		}
		pos = maxPos + 1
	}

	cell := &domain.Cell{
		ID:         domain.NewID(),
		NotebookID: notebookID,
		CellType:   req.CellType,
		Name:       req.Name,
		Disabled:   req.Disabled,
		Test:       req.Test,
		VisualSpec: req.VisualSpec,
		Content:    req.Content,
		Position:   pos,
	}
	switch {
	case req.Role != nil:
		cell.Role = *req.Role
	case req.CellType == domain.CellTypeMarkdown:
		cell.Role = domain.CellRoleMarkdown
	default:
		cell.Role = domain.CellRoleTransform
	}

	result, err := s.repo.CreateCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "CREATE_CELL",
		Status:        "ALLOWED",
	})

	return result, nil
}

// UpdateCell updates a cell's content or position. Owner or admin required.
func (s *Service) UpdateCell(ctx context.Context, principal string, isAdmin bool, cellID string, req domain.UpdateCellRequest) (*domain.Cell, error) {
	cell, err := s.repo.GetCell(ctx, cellID)
	if err != nil {
		return nil, err
	}

	if _, err := s.requireNotebookWriteAccess(ctx, principal, isAdmin, cell.NotebookID); err != nil {
		return nil, err
	}

	newRole := cell.Role
	if newRole == "" {
		if cell.CellType == domain.CellTypeMarkdown {
			newRole = domain.CellRoleMarkdown
		} else {
			newRole = domain.CellRoleTransform
		}
	}
	if req.Role != nil {
		newRole = *req.Role
	}
	if req.Test != nil {
		if newRole != domain.CellRoleTest {
			return nil, domain.ErrValidation("test config is only allowed for test cells")
		}
		if err := req.Test.Validate(); err != nil {
			return nil, err
		}
	}
	if newRole == domain.CellRoleTest && req.Test == nil && cell.Test == nil {
		return nil, domain.ErrValidation("test config is required for test cells")
	}
	if req.VisualSpec != nil {
		if cell.CellType != domain.CellTypeSQL {
			return nil, domain.ErrValidation("visual_spec is only allowed for sql cells")
		}
		if err := req.VisualSpec.Validate(); err != nil {
			return nil, err
		}
	}
	if err := validateRoleForCellType(newRole, cell.CellType); err != nil {
		return nil, err
	}
	result, err := s.repo.UpdateCell(ctx, cellID, req)
	if err != nil {
		return nil, err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "UPDATE_CELL",
		Status:        "ALLOWED",
	})

	return result, nil
}

func validateRoleForCellType(role domain.CellRole, cellType domain.CellType) error {
	if cellType == "" {
		return nil
	}
	switch role {
	case domain.CellRoleTransform, domain.CellRoleOutput, domain.CellRoleTest:
		if cellType != domain.CellTypeSQL {
			return domain.ErrValidation("role %q requires cell_type 'sql'", string(role))
		}
	case domain.CellRoleMarkdown:
		if cellType != domain.CellTypeMarkdown {
			return domain.ErrValidation("role %q requires cell_type 'markdown'", string(role))
		}
	default:
		return domain.ErrValidation("invalid cell role %q", string(role))
	}
	return nil
}

// DeleteCell removes a cell. Owner or admin required.
func (s *Service) DeleteCell(ctx context.Context, principal string, isAdmin bool, cellID string) error {
	cell, err := s.repo.GetCell(ctx, cellID)
	if err != nil {
		return err
	}
	if _, err := s.requireNotebookWriteAccess(ctx, principal, isAdmin, cell.NotebookID); err != nil {
		return err
	}
	if err := s.repo.DeleteCell(ctx, cellID); err != nil {
		return err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "DELETE_CELL",
		Status:        "ALLOWED",
	})

	return nil
}

// ReorderCells reorders cells in a notebook. Owner or admin required.
func (s *Service) ReorderCells(ctx context.Context, principal string, isAdmin bool, notebookID string, req domain.ReorderCellsRequest) ([]domain.Cell, error) {
	if _, err := s.requireNotebookWriteAccess(ctx, principal, isAdmin, notebookID); err != nil {
		return nil, err
	}
	if err := s.repo.ReorderCells(ctx, notebookID, req.CellIDs); err != nil {
		return nil, err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "REORDER_CELLS",
		Status:        "ALLOWED",
	})

	return s.repo.ListCells(ctx, notebookID)
}

func (s *Service) resolveContextForNotebook(ctx context.Context, nb *domain.Notebook) (*domain.NotebookContext, error) {
	if nb == nil {
		return nil, domain.ErrValidation("notebook is required")
	}
	resolved := &domain.NotebookContext{
		NotebookID:             nb.ID,
		FolderID:               nb.FolderID,
		EffectiveProjectID:     nb.ProjectOverrideID,
		EffectiveEnvironmentID: nb.EnvironmentOverrideID,
	}
	if s.folders != nil && strings.TrimSpace(nb.FolderID) != "" {
		folder, err := s.folders.GetByID(ctx, nb.FolderID)
		if err != nil {
			return nil, fmt.Errorf("get folder %q: %w", nb.FolderID, err)
		}
		resolved.WorkspaceID = folder.WorkspaceID
		ancestors, err := s.folders.ListAncestors(ctx, nb.FolderID)
		if err != nil {
			return nil, fmt.Errorf("list folder ancestors: %w", err)
		}
		for _, folder := range ancestors {
			if resolved.EffectiveProjectID == nil && folder.DefaultProjectID != nil {
				resolved.EffectiveProjectID = folder.DefaultProjectID
				resolved.ProjectSourceFolderID = &folder.ID
			}
			if resolved.EffectiveEnvironmentID == nil && folder.DefaultEnvironmentID != nil {
				resolved.EffectiveEnvironmentID = folder.DefaultEnvironmentID
				resolved.EnvironmentSourceID = &folder.ID
			}
			if resolved.EffectiveGitRepoID == nil && folder.GitRepoID != nil {
				resolved.EffectiveGitRepoID = folder.GitRepoID
				resolved.EffectiveGitRootPath = folder.GitRootPath
				resolved.GitSourceFolderID = &folder.ID
			}
		}
		if s.workspaces != nil && strings.TrimSpace(resolved.WorkspaceID) != "" {
			workspace, err := s.workspaces.GetByID(ctx, resolved.WorkspaceID)
			if err != nil {
				return nil, fmt.Errorf("get workspace %q: %w", resolved.WorkspaceID, err)
			}
			if resolved.EffectiveProjectID == nil && workspace.DefaultProjectID != nil {
				resolved.EffectiveProjectID = workspace.DefaultProjectID
			}
			if resolved.EffectiveEnvironmentID == nil && workspace.DefaultEnvironmentID != nil {
				resolved.EffectiveEnvironmentID = workspace.DefaultEnvironmentID
			}
			if resolved.EffectiveGitRepoID == nil && workspace.GitRepoID != nil {
				resolved.EffectiveGitRepoID = workspace.GitRepoID
				resolved.EffectiveGitRootPath = workspace.GitRootPath
			}
		}
	}
	if resolved.EffectiveGitRepoID == nil && nb.GitRepoID != nil {
		resolved.EffectiveGitRepoID = nb.GitRepoID
	}
	return resolved, nil
}

func (s *Service) resolveTargetGitPath(ctx context.Context, resolved *domain.NotebookContext, folderID string, notebookName string, requestedPath *string) (*string, error) {
	if resolved == nil || resolved.EffectiveGitRepoID == nil {
		return nil, nil
	}
	if requestedPath != nil && strings.TrimSpace(*requestedPath) != "" {
		pathValue, err := normalizeGitPath(*requestedPath, resolved.EffectiveGitRootPath)
		if err != nil {
			return nil, err
		}
		return &pathValue, nil
	}
	derived, err := s.deriveGitPathFromFolder(ctx, folderID, notebookName, resolved)
	if err != nil {
		return nil, err
	}
	return &derived, nil
}

func (s *Service) deriveGitPathFromFolder(ctx context.Context, folderID, notebookName string, resolved *domain.NotebookContext) (string, error) {
	if s.folders == nil {
		return "", domain.ErrValidation("folder repository is not configured")
	}
	ancestors, err := s.folders.ListAncestors(ctx, folderID)
	if err != nil {
		return "", fmt.Errorf("list destination folder ancestors: %w", err)
	}
	segments := make([]string, 0, len(ancestors)+1)
	for _, folder := range ancestors {
		if resolved.GitSourceFolderID != nil && folder.ID == *resolved.GitSourceFolderID {
			break
		}
		segments = append(segments, sanitizeGitPathSegment(folder.Name))
	}
	for i, j := 0, len(segments)-1; i < j; i, j = i+1, j-1 {
		segments[i], segments[j] = segments[j], segments[i]
	}
	segments = append(segments, notebookFileName(notebookName))
	relativePath := path.Join(segments...)
	if strings.TrimSpace(relativePath) == "" {
		relativePath = notebookFileName(notebookName)
	}
	root := notebookStringValue(resolved.EffectiveGitRootPath)
	if root == "" {
		return relativePath, nil
	}
	return path.Join(root, relativePath), nil
}

func (s *Service) ensureUniqueGitPath(ctx context.Context, notebookID, repoID, gitPath string) error {
	items, _, err := s.repo.ListNotebooks(ctx, nil, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return fmt.Errorf("list notebooks for git collision check: %w", err)
	}
	for _, item := range items {
		if item.ID == notebookID {
			continue
		}
		if notebookStringValue(item.GitRepoID) == repoID && notebookStringValue(item.GitPath) == gitPath {
			return domain.ErrConflict("git path %q is already used by notebook %q", gitPath, item.ID)
		}
	}
	return nil
}

func (s *Service) invalidateNotebookContext(ctx context.Context, notebookID string) error {
	if s.invalidator == nil {
		return nil
	}
	if err := s.invalidator.InvalidateNotebook(ctx, notebookID); err != nil {
		return fmt.Errorf("invalidate notebook runtime context: %w", err)
	}
	return nil
}

func (s *Service) validateNotebookEffectiveContext(ctx context.Context, nb *domain.Notebook) error {
	resolved, err := s.resolveContextForNotebook(ctx, nb)
	if err != nil {
		return fmt.Errorf("resolve notebook context: %w", err)
	}
	return s.validateResolvedContext(ctx, resolved)
}

func (s *Service) validateResolvedContext(ctx context.Context, resolved *domain.NotebookContext) error {
	if resolved == nil {
		return nil
	}
	if resolved.EffectiveProjectID != nil && strings.TrimSpace(*resolved.EffectiveProjectID) != "" && s.projects != nil && strings.TrimSpace(resolved.WorkspaceID) != "" {
		project, err := s.projects.GetByID(ctx, strings.TrimSpace(*resolved.EffectiveProjectID))
		if err != nil {
			return fmt.Errorf("get project %q: %w", strings.TrimSpace(*resolved.EffectiveProjectID), err)
		}
		if strings.TrimSpace(project.WorkspaceID) != strings.TrimSpace(resolved.WorkspaceID) {
			return domain.ErrValidation("effective project must belong to the notebook workspace")
		}
	}
	if resolved.EffectiveEnvironmentID == nil || strings.TrimSpace(*resolved.EffectiveEnvironmentID) == "" {
		return nil
	}
	if resolved.EffectiveProjectID == nil || strings.TrimSpace(*resolved.EffectiveProjectID) == "" {
		return domain.ErrValidation("environment override requires an effective project context")
	}
	if s.environments == nil {
		return nil
	}
	environment, err := s.environments.GetByID(ctx, strings.TrimSpace(*resolved.EffectiveEnvironmentID))
	if err != nil {
		return fmt.Errorf("get environment %q: %w", strings.TrimSpace(*resolved.EffectiveEnvironmentID), err)
	}
	if strings.TrimSpace(environment.ProjectID) != strings.TrimSpace(*resolved.EffectiveProjectID) {
		return domain.ErrValidation("effective environment must belong to the effective project")
	}
	return nil
}

func contextsDiffer(left, right *domain.NotebookContext) bool {
	return notebookStringValue(left.EffectiveProjectID) != notebookStringValue(right.EffectiveProjectID) ||
		notebookStringValue(left.EffectiveEnvironmentID) != notebookStringValue(right.EffectiveEnvironmentID) ||
		notebookStringValue(left.EffectiveGitRepoID) != notebookStringValue(right.EffectiveGitRepoID)
}

func projectOrEnvironmentChanged(left, right *domain.NotebookContext) bool {
	return notebookStringValue(left.EffectiveProjectID) != notebookStringValue(right.EffectiveProjectID) ||
		notebookStringValue(left.EffectiveEnvironmentID) != notebookStringValue(right.EffectiveEnvironmentID)
}

func notebookStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func normalizeGitPath(requestedPath string, gitRootPath *string) (string, error) {
	cleaned := path.Clean(strings.ReplaceAll(strings.TrimSpace(requestedPath), "\\", "/"))
	switch {
	case cleaned == "." || cleaned == "":
		return "", domain.ErrValidation("git path is required")
	case strings.HasPrefix(cleaned, "/"), cleaned == "..", strings.HasPrefix(cleaned, "../"):
		return "", domain.ErrValidation("git path must be relative to the repository root")
	}
	root := notebookStringValue(gitRootPath)
	if root == "" {
		return cleaned, nil
	}
	if cleaned == root || strings.HasPrefix(cleaned, root+"/") {
		return cleaned, nil
	}
	return path.Join(root, cleaned), nil
}

func notebookFileName(name string) string {
	base := sanitizeGitPathSegment(name)
	if base == "" {
		base = "notebook"
	}
	return base + ".yaml"
}

func sanitizeGitPathSegment(name string) string {
	trimmed := strings.TrimSpace(strings.ToLower(name))
	if trimmed == "" {
		return "untitled"
	}
	var builder strings.Builder
	lastDash := false
	for _, r := range trimmed {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				builder.WriteRune('-')
				lastDash = true
			}
		}
	}
	value := strings.Trim(builder.String(), "-")
	if value == "" {
		return "untitled"
	}
	return value
}
