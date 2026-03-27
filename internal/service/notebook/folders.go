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
	notebooks    domain.NotebookRepository
	events       domain.OrchestrationEventRepository
	audit        domain.AuditRepository
}

// NewFolderService creates a new folder service.
func NewFolderService(repo domain.FolderRepository, audit domain.AuditRepository) *FolderService {
	return &FolderService{repo: repo, audit: audit}
}

// SetShareRepository configures inherited folder sharing.
func (s *FolderService) SetShareRepository(folderShares domain.FolderShareRepository) {
	s.folderShares = folderShares
}

// SetContextInvalidation configures queue-backed invalidation for folder context changes.
func (s *FolderService) SetContextInvalidation(notebooks domain.NotebookRepository, events domain.OrchestrationEventRepository) {
	s.notebooks = notebooks
	s.events = events
}

func (s *FolderService) accessResolver(ctx context.Context, principal string, isAdmin bool) (*principalAccessResolver, error) {
	return newPrincipalAccessResolver(ctx, s.repo, s.folderShares, nil, principal, isAdmin)
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
	if s.folderShares == nil {
		return nil, domain.ErrValidation("folder sharing is not configured")
	}
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

// UnshareFolder removes an inherited folder share.
func (s *FolderService) UnshareFolder(ctx context.Context, principal string, isAdmin bool, folderID string, principalName string) error {
	if s.folderShares == nil {
		return domain.ErrValidation("folder sharing is not configured")
	}
	if _, err := s.requireFolderRole(ctx, principal, isAdmin, folderID, roleAllowsManage, "manage"); err != nil {
		return err
	}
	if err := s.folderShares.Delete(ctx, folderID, strings.TrimSpace(principalName)); err != nil {
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
	if s.folderShares == nil {
		return nil, nil
	}
	if _, err := s.requireFolderRole(ctx, principal, isAdmin, folderID, roleAllowsRead, "read"); err != nil {
		return nil, err
	}
	items, err := s.folderShares.ListByFolder(ctx, folderID)
	if err != nil {
		return nil, fmt.Errorf("list folder shares: %w", err)
	}
	return items, nil
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
