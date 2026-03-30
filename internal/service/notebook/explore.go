package notebook

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"duck-demo/internal/domain"
)

// ExploreService composes folder-backed and project-backed authored assets into
// a single browse surface for the active folder context.
type ExploreService struct {
	folders        domain.FolderRepository
	folderShares   domain.FolderShareRepository
	auth           domain.AuthorizationService
	notebooks      domain.NotebookRepository
	notebookShares domain.NotebookShareRepository
	dashboards     domain.DashboardRepository
	pipelines      domain.PipelineRepository
	projects       domain.ProjectRepository
	models         domain.ModelRepository
	macros         domain.MacroRepository
	semantics      domain.SemanticModelRepository
}

// NewExploreService creates a new ExploreService.
func NewExploreService(
	folders domain.FolderRepository,
	notebooks domain.NotebookRepository,
	dashboards domain.DashboardRepository,
	pipelines domain.PipelineRepository,
	projects domain.ProjectRepository,
	models domain.ModelRepository,
	macros domain.MacroRepository,
	semantics domain.SemanticModelRepository,
) *ExploreService {
	return &ExploreService{
		folders:    folders,
		notebooks:  notebooks,
		dashboards: dashboards,
		pipelines:  pipelines,
		projects:   projects,
		models:     models,
		macros:     macros,
		semantics:  semantics,
	}
}

// SetAccessRepositories configures optional ACL repositories used for inherited
// folder access and direct notebook shares.
func (s *ExploreService) SetAccessRepositories(folderShares domain.FolderShareRepository, notebookShares domain.NotebookShareRepository) {
	s.folderShares = folderShares
	s.notebookShares = notebookShares
}

// SetAuthorization configures the shared authorization service used for grant-backed folder access.
func (s *ExploreService) SetAuthorization(auth domain.AuthorizationService) {
	s.auth = auth
}

// List returns normalized authored assets visible within the current folder scope.
func (s *ExploreService) List(ctx context.Context, principal string, isAdmin bool, filter domain.ExploreFilter) ([]domain.ExploreItem, error) {
	if s.folders == nil {
		return nil, domain.ErrNotImplemented("explore folders are not configured")
	}
	if s.notebooks == nil {
		return nil, domain.ErrNotImplemented("explore notebooks are not configured")
	}

	kind := normalizeExploreKind(filter.Kind)
	allFolders, err := s.folders.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("list folders: %w", err)
	}
	accessibleFolders, resolver, err := s.accessibleFolders(ctx, principal, isAdmin, allFolders)
	if err != nil {
		return nil, err
	}

	selectedFolder, subtreeFolders, err := s.resolveSelectedFolders(ctx, filter.FolderID, accessibleFolders, allFolders)
	if err != nil {
		return nil, err
	}
	subtreeIDs := make([]string, 0, len(subtreeFolders))
	for _, folder := range subtreeFolders {
		subtreeIDs = append(subtreeIDs, folder.ID)
	}

	items := make([]domain.ExploreItem, 0, 32)
	if kind == domain.ExploreKindAll || kind == domain.ExploreKindNotebook {
		notebooks, err := s.notebooks.ListByFolders(ctx, subtreeIDs)
		if err != nil {
			return nil, fmt.Errorf("list notebooks by folders: %w", err)
		}
		for _, notebook := range notebooks {
			role, roleErr := resolver.notebookRole(ctx, &notebook)
			if roleErr != nil {
				return nil, roleErr
			}
			if !roleAllowsRead(role) {
				continue
			}
			folder := folderByID(subtreeFolders, notebook.FolderID)
			projectBound := folder != nil && s.resolveProjectID(*folder, allFolders) != nil
			items = append(items, domain.ExploreItem{
				Kind:         domain.ExploreKindNotebook,
				Scope:        domain.ExploreScopeFolder,
				ID:           notebook.ID,
				Name:         notebook.Name,
				Owner:        notebook.Owner,
				FolderID:     ptrString(notebook.FolderID),
				UpdatedAt:    notebook.UpdatedAt,
				GitRepoID:    firstNonNil(notebook.GitRepoID, folderGitRepo(folder, allFolders)),
				Shared:       strings.TrimSpace(notebook.Owner) != strings.TrimSpace(principal),
				ProjectBound: projectBound,
			})
		}
	}
	if kind == domain.ExploreKindAll || kind == domain.ExploreKindDashboard {
		if s.dashboards != nil {
			dashboards, err := s.dashboards.ListByFolders(ctx, subtreeIDs)
			if err != nil {
				return nil, fmt.Errorf("list dashboards by folders: %w", err)
			}
			for _, dashboard := range dashboards {
				folder := folderByID(subtreeFolders, dashboard.FolderID)
				items = append(items, domain.ExploreItem{
					Kind:         domain.ExploreKindDashboard,
					Scope:        domain.ExploreScopeFolder,
					ID:           dashboard.ID,
					Name:         dashboard.Name,
					Owner:        dashboard.Owner,
					FolderID:     ptrString(dashboard.FolderID),
					UpdatedAt:    dashboard.UpdatedAt,
					GitRepoID:    folderGitRepo(folder, allFolders),
					Shared:       strings.TrimSpace(dashboard.Owner) != strings.TrimSpace(principal),
					ProjectBound: folder != nil && s.resolveProjectID(*folder, allFolders) != nil,
				})
			}
		}
	}
	if kind == domain.ExploreKindAll || kind == domain.ExploreKindPipeline {
		if s.pipelines != nil {
			pipelines, err := s.pipelines.ListPipelinesByFolders(ctx, subtreeIDs)
			if err != nil {
				return nil, fmt.Errorf("list pipelines by folders: %w", err)
			}
			for _, pipeline := range pipelines {
				folder := folderByID(subtreeFolders, pipeline.FolderID)
				items = append(items, domain.ExploreItem{
					Kind:         domain.ExploreKindPipeline,
					Scope:        domain.ExploreScopeFolder,
					ID:           pipeline.ID,
					Name:         pipeline.Name,
					Owner:        pipeline.CreatedBy,
					FolderID:     ptrString(pipeline.FolderID),
					UpdatedAt:    pipeline.UpdatedAt,
					GitRepoID:    folderGitRepo(folder, allFolders),
					Shared:       strings.TrimSpace(pipeline.CreatedBy) != strings.TrimSpace(principal),
					ProjectBound: folder != nil && s.resolveProjectID(*folder, allFolders) != nil,
				})
			}
		}
	}

	if selectedFolder != nil {
		projectID := s.resolveProjectID(*selectedFolder, allFolders)
		if projectID != nil {
			projectName, err := s.resolveProjectName(ctx, *projectID)
			if err != nil {
				return nil, err
			}
			if projectName != nil {
				if kind == domain.ExploreKindAll || kind == domain.ExploreKindModel {
					models, err := s.models.ListAll(ctx)
					if err != nil {
						return nil, fmt.Errorf("list models: %w", err)
					}
					for _, model := range models {
						if model.ProjectName != *projectName {
							continue
						}
						items = append(items, domain.ExploreItem{
							Kind:         domain.ExploreKindModel,
							Scope:        domain.ExploreScopeProject,
							ID:           model.ID,
							Name:         model.Name,
							Owner:        model.CreatedBy,
							ProjectName:  projectName,
							UpdatedAt:    model.UpdatedAt,
							Shared:       strings.TrimSpace(model.CreatedBy) != strings.TrimSpace(principal),
							ProjectBound: true,
						})
					}
				}
				if kind == domain.ExploreKindAll || kind == domain.ExploreKindMacro {
					macros, err := s.macros.ListAll(ctx)
					if err != nil {
						return nil, fmt.Errorf("list macros: %w", err)
					}
					for _, macro := range macros {
						if strings.TrimSpace(macro.ProjectName) != *projectName {
							continue
						}
						items = append(items, domain.ExploreItem{
							Kind:         domain.ExploreKindMacro,
							Scope:        domain.ExploreScopeProject,
							ID:           macro.Name,
							Name:         macro.Name,
							Owner:        macro.CreatedBy,
							ProjectName:  projectName,
							UpdatedAt:    macro.UpdatedAt,
							Shared:       strings.TrimSpace(macro.CreatedBy) != strings.TrimSpace(principal),
							ProjectBound: true,
						})
					}
				}
				if kind == domain.ExploreKindAll || kind == domain.ExploreKindSemanticModel {
					semanticModels, err := s.semantics.ListAll(ctx)
					if err != nil {
						return nil, fmt.Errorf("list semantic models: %w", err)
					}
					for _, semanticModel := range semanticModels {
						if semanticModel.ProjectName != *projectName {
							continue
						}
						items = append(items, domain.ExploreItem{
							Kind:         domain.ExploreKindSemanticModel,
							Scope:        domain.ExploreScopeProject,
							ID:           semanticModel.ID,
							Name:         semanticModel.Name,
							Owner:        semanticModel.CreatedBy,
							ProjectName:  projectName,
							UpdatedAt:    semanticModel.UpdatedAt,
							Shared:       strings.TrimSpace(semanticModel.CreatedBy) != strings.TrimSpace(principal),
							ProjectBound: true,
						})
					}
				}
			}
		}
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].UpdatedAt.Equal(items[j].UpdatedAt) {
			if items[i].Kind == items[j].Kind {
				return items[i].Name < items[j].Name
			}
			return items[i].Kind < items[j].Kind
		}
		return items[i].UpdatedAt.After(items[j].UpdatedAt)
	})

	return items, nil
}

func (s *ExploreService) accessibleFolders(ctx context.Context, principal string, isAdmin bool, allFolders []domain.Folder) ([]domain.Folder, *principalAccessResolver, error) {
	resolver, err := newPrincipalAccessResolver(ctx, s.folders, s.folderShares, s.auth, s.notebookShares, principal, isAdmin)
	if err != nil {
		return nil, nil, fmt.Errorf("build access resolver: %w", err)
	}
	if isAdmin {
		return slices.Clone(allFolders), resolver, nil
	}

	accessible := make([]domain.Folder, 0, len(allFolders))
	for _, folder := range allFolders {
		role, err := resolver.folderRole(ctx, &folder)
		if err != nil {
			return nil, nil, err
		}
		if roleAllowsRead(role) {
			accessible = append(accessible, folder)
		}
	}
	return accessible, resolver, nil
}

func (s *ExploreService) resolveSelectedFolders(ctx context.Context, selectedFolderID string, accessible []domain.Folder, allFolders []domain.Folder) (*domain.Folder, []domain.Folder, error) {
	if strings.TrimSpace(selectedFolderID) == "" {
		return nil, accessible, nil
	}

	selected := folderByID(accessible, selectedFolderID)
	if selected == nil {
		if _, err := s.folders.GetByID(ctx, selectedFolderID); err == nil {
			return nil, nil, domain.ErrAccessDenied("selected folder is not accessible")
		}
		return nil, nil, domain.ErrNotFound("folder %q not found", selectedFolderID)
	}

	subtree := make([]domain.Folder, 0, len(accessible))
	for _, folder := range accessible {
		if folder.Path == selected.Path || strings.HasPrefix(folder.Path, selected.Path+"/") {
			subtree = append(subtree, folder)
		}
	}
	if len(subtree) == 0 {
		subtree = []domain.Folder{*selected}
	}
	return selected, subtree, nil
}

func (s *ExploreService) resolveProjectID(folder domain.Folder, allFolders []domain.Folder) *string {
	for _, ancestor := range ancestorsForFolder(folder, allFolders) {
		if ancestor.DefaultProjectID != nil && strings.TrimSpace(*ancestor.DefaultProjectID) != "" {
			return ancestor.DefaultProjectID
		}
	}
	return nil
}

func (s *ExploreService) resolveProjectName(ctx context.Context, projectID string) (*string, error) {
	if s.projects == nil || strings.TrimSpace(projectID) == "" {
		return nil, nil
	}
	project, err := s.projects.GetByID(ctx, projectID)
	if err != nil {
		var notFound *domain.NotFoundError
		if strings.Contains(err.Error(), "not found") || errors.As(err, &notFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get project %q: %w", projectID, err)
	}
	return ptrString(project.Name), nil
}

func normalizeExploreKind(kind string) string {
	switch strings.TrimSpace(kind) {
	case "", domain.ExploreKindAll:
		return domain.ExploreKindAll
	case domain.ExploreKindNotebook, domain.ExploreKindModel, domain.ExploreKindMacro,
		domain.ExploreKindDashboard, domain.ExploreKindPipeline, domain.ExploreKindSemanticModel:
		return strings.TrimSpace(kind)
	default:
		return domain.ExploreKindAll
	}
}

func ancestorsForFolder(folder domain.Folder, allFolders []domain.Folder) []domain.Folder {
	ancestors := make([]domain.Folder, 0, folder.Depth+1)
	for _, candidate := range allFolders {
		if candidate.Path == folder.Path || strings.HasPrefix(folder.Path, candidate.Path+"/") {
			ancestors = append(ancestors, candidate)
		}
	}
	sort.Slice(ancestors, func(i, j int) bool {
		return ancestors[i].Depth > ancestors[j].Depth
	})
	return ancestors
}

func folderByID(folders []domain.Folder, id string) *domain.Folder {
	for i := range folders {
		if folders[i].ID == id {
			return &folders[i]
		}
	}
	return nil
}

func folderGitRepo(folder *domain.Folder, allFolders []domain.Folder) *string {
	if folder == nil {
		return nil
	}
	for _, ancestor := range ancestorsForFolder(*folder, allFolders) {
		if ancestor.GitRepoID != nil && strings.TrimSpace(*ancestor.GitRepoID) != "" {
			return ancestor.GitRepoID
		}
	}
	return nil
}

func firstNonNil(values ...*string) *string {
	for _, value := range values {
		if value != nil && strings.TrimSpace(*value) != "" {
			return value
		}
	}
	return nil
}

func ptrString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}
