package notebook

import (
	"context"

	"github.com/Yacobolo/quackstack/internal/domain"
)

// ContextResolver computes effective notebook context from overrides and folder inheritance.
type ContextResolver struct {
	notebooks domain.NotebookRepository
	folders   domain.FolderRepository
}

// NewContextResolver creates a notebook context resolver over notebook and folder repositories.
func NewContextResolver(notebooks domain.NotebookRepository, folders domain.FolderRepository) *ContextResolver {
	return &ContextResolver{notebooks: notebooks, folders: folders}
}

// Resolve returns the effective notebook context after applying notebook overrides and folder inheritance.
func (r *ContextResolver) Resolve(ctx context.Context, notebookID string) (*domain.NotebookContext, error) {
	notebook, err := r.notebooks.GetNotebook(ctx, notebookID)
	if err != nil {
		return nil, err
	}
	resolved := &domain.NotebookContext{
		NotebookID: notebook.ID,
		FolderID:   notebook.FolderID,
	}

	ancestors, err := r.folders.ListAncestors(ctx, notebook.FolderID)
	if err != nil {
		return nil, err
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

	if notebook.ProjectOverrideID != nil {
		resolved.EffectiveProjectID = notebook.ProjectOverrideID
		resolved.ProjectSourceFolderID = nil
	}
	if notebook.EnvironmentOverrideID != nil {
		resolved.EffectiveEnvironmentID = notebook.EnvironmentOverrideID
		resolved.EnvironmentSourceID = nil
	}
	return resolved, nil
}
