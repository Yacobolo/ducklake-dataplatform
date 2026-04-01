package core

import (
	"context"
	"strings"

	"duck-demo/internal/domain"
)

func ResourceFolderPath(ctx context.Context, deps *Dependencies, principal domain.ContextPrincipal, owner string, folderID string) string {
	if deps == nil || deps.NotebookFolders == nil {
		return ""
	}
	folderID = strings.TrimSpace(folderID)
	owner = strings.TrimSpace(owner)
	if folderID == "" || owner == "" {
		return ""
	}

	folders, err := deps.NotebookFolders.ListFoldersForPrincipal(ctx, principal.Name, principal.IsAdmin, &owner)
	if err != nil {
		return ""
	}

	return folderDisplayPath(folders, folderID)
}

func folderDisplayPath(folders []domain.Folder, folderID string) string {
	byID := make(map[string]domain.Folder, len(folders))
	for i := range folders {
		byID[folders[i].ID] = folders[i]
	}

	path, ok := buildFolderDisplayPath(byID, strings.TrimSpace(folderID), map[string]string{})
	if !ok || path == "" {
		return ""
	}
	return path + "/"
}

func buildFolderDisplayPath(byID map[string]domain.Folder, folderID string, memo map[string]string) (string, bool) {
	if path, ok := memo[folderID]; ok {
		return path, true
	}

	folder, ok := byID[folderID]
	if !ok {
		return "", false
	}

	label := strings.TrimSpace(folder.Name)
	if label == "" {
		label = folder.ID
	}

	parentID := ""
	if folder.ParentFolderID != nil {
		parentID = strings.TrimSpace(*folder.ParentFolderID)
	}
	if parentID == "" {
		memo[folderID] = label
		return label, true
	}

	parent, ok := byID[parentID]
	if !ok {
		memo[folderID] = label
		return label, true
	}
	if parent.SystemRole != nil && *parent.SystemRole == domain.FolderSystemRolePersonalRoot {
		memo[folderID] = label
		return label, true
	}

	parentPath, ok := buildFolderDisplayPath(byID, parentID, memo)
	if !ok || parentPath == "" {
		memo[folderID] = label
		return label, true
	}

	full := parentPath + "/" + label
	memo[folderID] = full
	return full, true
}
