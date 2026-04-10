package notebook

import (
	"context"
	"strings"

	"duck-demo/internal/domain"
)

type accessResolver struct {
	workspaces     domain.WorkspaceRepository
	folders        domain.FolderRepository
	folderShares   domain.FolderShareRepository
	auth           domain.AuthorizationService
	notebookShares domain.NotebookShareRepository
}

type principalAccessResolver struct {
	accessResolver
	principal             string
	isAdmin               bool
	workspaceRolesByID    map[string]string
	folderRolesByFolder   map[string]string
	notebookRolesByID     map[string]string
	folderAncestorsByID   map[string][]domain.Folder
}

func newPrincipalAccessResolver(
	ctx context.Context,
	workspaces domain.WorkspaceRepository,
	folders domain.FolderRepository,
	folderShares domain.FolderShareRepository,
	auth domain.AuthorizationService,
	notebookShares domain.NotebookShareRepository,
	principal string,
	isAdmin bool,
) (*principalAccessResolver, error) {
	resolver := &principalAccessResolver{
		accessResolver: accessResolver{
			workspaces:     workspaces,
			folders:        folders,
			folderShares:   folderShares,
			auth:           auth,
			notebookShares: notebookShares,
		},
		principal:           strings.TrimSpace(principal),
		isAdmin:             isAdmin,
		workspaceRolesByID:  map[string]string{},
		folderRolesByFolder: map[string]string{},
		notebookRolesByID:   map[string]string{},
		folderAncestorsByID: map[string][]domain.Folder{},
	}
	if isAdmin || resolver.principal == "" {
		return resolver, nil
	}
	if auth == nil && folderShares != nil {
		items, err := folderShares.ListByPrincipal(ctx, resolver.principal)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			resolver.folderRolesByFolder[item.FolderID] = maxShareRole(resolver.folderRolesByFolder[item.FolderID], item.Role)
		}
	}
	if notebookShares != nil {
		items, err := notebookShares.ListByPrincipal(ctx, resolver.principal)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			resolver.notebookRolesByID[item.NotebookID] = maxShareRole(resolver.notebookRolesByID[item.NotebookID], item.Role)
		}
	}
	return resolver, nil
}

func (r *principalAccessResolver) folderRole(ctx context.Context, folder *domain.Folder) (string, error) {
	if folder == nil {
		return "", nil
	}
	if r.isAdmin {
		return domain.FolderShareRoleManager, nil
	}
	workspaceRole, err := r.workspaceRole(ctx, folder.WorkspaceID)
	if err != nil {
		return "", err
	}
	if workspaceRole == "" {
		return "", nil
	}
	if strings.TrimSpace(folder.Owner) == r.principal {
		return domain.FolderShareRoleManager, nil
	}
	if cached, ok := r.folderRolesByFolder[folder.ID]; ok {
		return cached, nil
	}
	role := ""
	if r.folders != nil && r.auth != nil {
		ancestors, err := r.folderAncestors(ctx, folder.ID)
		if err != nil {
			return "", err
		}
		for _, ancestor := range ancestors {
			allowed, err := r.auth.CheckPrivilege(ctx, r.principal, domain.SecurableFolder, ancestor.ID, domain.PrivManage)
			if err != nil {
				return "", err
			}
			if allowed {
				role = maxShareRole(role, domain.FolderShareRoleManager)
				continue
			}
			allowed, err = r.auth.CheckPrivilege(ctx, r.principal, domain.SecurableFolder, ancestor.ID, domain.PrivModify)
			if err != nil {
				return "", err
			}
			if allowed {
				role = maxShareRole(role, domain.FolderShareRoleEditor)
				continue
			}
			allowed, err = r.auth.CheckPrivilege(ctx, r.principal, domain.SecurableFolder, ancestor.ID, domain.PrivSelect)
			if err != nil {
				return "", err
			}
			if allowed {
				role = maxShareRole(role, domain.FolderShareRoleViewer)
			}
		}
	} else if r.folders != nil {
		ancestors, err := r.folderAncestors(ctx, folder.ID)
		if err != nil {
			return "", err
		}
		for _, ancestor := range ancestors {
			role = maxShareRole(role, r.folderRolesByFolder[ancestor.ID])
		}
	}
	r.folderRolesByFolder[folder.ID] = role
	return role, nil
}

func (r *principalAccessResolver) notebookRole(ctx context.Context, nb *domain.Notebook) (string, error) {
	if nb == nil {
		return "", nil
	}
	if r.isAdmin || strings.TrimSpace(nb.Owner) == r.principal {
		return domain.FolderShareRoleManager, nil
	}
	role := maxShareRole("", r.notebookRolesByID[nb.ID])
	if r.folders != nil && strings.TrimSpace(nb.FolderID) != "" {
		folder, err := r.folders.GetByID(ctx, nb.FolderID)
		if err != nil {
			return "", err
		}
		folderRole, err := r.folderRole(ctx, folder)
		if err != nil {
			return "", err
		}
		role = maxShareRole(role, folderRole)
	}
	return role, nil
}

func (r *principalAccessResolver) folderAncestors(ctx context.Context, folderID string) ([]domain.Folder, error) {
	folderID = strings.TrimSpace(folderID)
	if folderID == "" || r.folders == nil {
		return nil, nil
	}
	if cached, ok := r.folderAncestorsByID[folderID]; ok {
		return cached, nil
	}
	ancestors, err := r.folders.ListAncestors(ctx, folderID)
	if err != nil {
		return nil, err
	}
	r.folderAncestorsByID[folderID] = ancestors
	return ancestors, nil
}

func (r *principalAccessResolver) workspaceRole(ctx context.Context, workspaceID string) (string, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return "", nil
	}
	if r.isAdmin {
		return domain.FolderShareRoleManager, nil
	}
	if cached, ok := r.workspaceRolesByID[workspaceID]; ok {
		return cached, nil
	}
	if r.workspaces == nil {
		return "", nil
	}
	role, err := r.workspaces.GetMemberRole(ctx, workspaceID, r.principal)
	if err != nil {
		return "", err
	}
	r.workspaceRolesByID[workspaceID] = role
	return role, nil
}

func shareRoleLevel(role string) int {
	switch domain.NormalizeShareRole(role) {
	case domain.FolderShareRoleViewer:
		return 1
	case domain.FolderShareRoleEditor:
		return 2
	case domain.FolderShareRoleManager:
		return 3
	default:
		return 0
	}
}

func maxShareRole(current string, candidate string) string {
	if shareRoleLevel(candidate) > shareRoleLevel(current) {
		return domain.NormalizeShareRole(candidate)
	}
	return domain.NormalizeShareRole(current)
}

func roleAllowsRead(role string) bool {
	return shareRoleLevel(role) >= shareRoleLevel(domain.FolderShareRoleViewer)
}

func roleAllowsWrite(role string) bool {
	return shareRoleLevel(role) >= shareRoleLevel(domain.FolderShareRoleEditor)
}

func roleAllowsManage(role string) bool {
	return shareRoleLevel(role) >= shareRoleLevel(domain.FolderShareRoleManager)
}
