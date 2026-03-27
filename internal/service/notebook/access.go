package notebook

import (
	"context"
	"strings"

	"duck-demo/internal/domain"
)

type accessResolver struct {
	folders        domain.FolderRepository
	folderShares   domain.FolderShareRepository
	notebookShares domain.NotebookShareRepository
}

type principalAccessResolver struct {
	accessResolver
	principal             string
	isAdmin               bool
	folderRolesByFolder   map[string]string
	notebookRolesByID     map[string]string
	folderAncestorsByID   map[string][]domain.Folder
}

func newPrincipalAccessResolver(
	ctx context.Context,
	folders domain.FolderRepository,
	folderShares domain.FolderShareRepository,
	notebookShares domain.NotebookShareRepository,
	principal string,
	isAdmin bool,
) (*principalAccessResolver, error) {
	resolver := &principalAccessResolver{
		accessResolver: accessResolver{
			folders:        folders,
			folderShares:   folderShares,
			notebookShares: notebookShares,
		},
		principal:           strings.TrimSpace(principal),
		isAdmin:             isAdmin,
		folderRolesByFolder: map[string]string{},
		notebookRolesByID:   map[string]string{},
		folderAncestorsByID: map[string][]domain.Folder{},
	}
	if isAdmin || resolver.principal == "" {
		return resolver, nil
	}
	if folderShares != nil {
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
	if r.isAdmin || strings.TrimSpace(folder.Owner) == r.principal {
		return domain.FolderShareRoleManager, nil
	}
	role := maxShareRole("", r.folderRolesByFolder[folder.ID])
	if r.folders != nil {
		ancestors, err := r.folderAncestors(ctx, folder.ID)
		if err != nil {
			return "", err
		}
		for _, ancestor := range ancestors {
			role = maxShareRole(role, r.folderRolesByFolder[ancestor.ID])
		}
	}
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
		ancestors, err := r.folderAncestors(ctx, nb.FolderID)
		if err != nil {
			return "", err
		}
		for _, ancestor := range ancestors {
			role = maxShareRole(role, r.folderRolesByFolder[ancestor.ID])
		}
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
