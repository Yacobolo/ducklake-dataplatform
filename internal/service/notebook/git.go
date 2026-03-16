package notebook

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"duck-demo/internal/declarative"
	"duck-demo/internal/domain"
)

// GitService provides business logic for Git repository operations.
type GitService struct {
	repo          domain.GitRepoRepository
	notebooks     domain.NotebookRepository
	audit         domain.AuditRepository
	models        notebookPromoter
	notebookLinks domain.NotebookModelLinkRepository
}

type notebookPromoter interface {
	PromoteNotebook(ctx context.Context, principal string, req domain.PromoteNotebookRequest) (*domain.Model, error)
}

// NewGitService creates a new GitService.
func NewGitService(repo domain.GitRepoRepository, notebooks domain.NotebookRepository, audit domain.AuditRepository) *GitService {
	return &GitService{repo: repo, notebooks: notebooks, audit: audit}
}

// SetPublishDependencies configures optional model promotion dependencies for declarative notebook publish sync.
func (s *GitService) SetPublishDependencies(models notebookPromoter, notebookLinks domain.NotebookModelLinkRepository) {
	s.models = models
	s.notebookLinks = notebookLinks
}

func canAccessGitRepo(repo *domain.GitRepo, principal string, isAdmin bool) bool {
	return isAdmin || repo.Owner == principal
}

func (s *GitService) requireGitRepoAccess(ctx context.Context, principal string, isAdmin bool, id string) (*domain.GitRepo, error) {
	repo, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if !canAccessGitRepo(repo, principal, isAdmin) {
		return nil, domain.ErrAccessDenied("only the git repo owner or admin can access git repo %q", id)
	}
	return repo, nil
}

// CreateGitRepo registers a new Git repository for notebook sync.
func (s *GitService) CreateGitRepo(ctx context.Context, principal string, req domain.CreateGitRepoRequest) (*domain.GitRepo, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	repo := &domain.GitRepo{
		ID:        domain.NewID(),
		URL:       req.URL,
		Branch:    req.Branch,
		Path:      req.Path,
		AuthToken: req.AuthToken,
		Owner:     principal,
	}

	result, err := s.repo.Create(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("create git repo: %w", err)
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "CREATE_GIT_REPO",
		Status:        "ALLOWED",
	})

	return result, nil
}

// GetGitRepo retrieves a Git repository by ID.
func (s *GitService) GetGitRepo(ctx context.Context, id string) (*domain.GitRepo, error) {
	return s.repo.GetByID(ctx, id)
}

// GetGitRepoForPrincipal retrieves a Git repository visible to the caller.
func (s *GitService) GetGitRepoForPrincipal(ctx context.Context, principal string, isAdmin bool, id string) (*domain.GitRepo, error) {
	return s.requireGitRepoAccess(ctx, principal, isAdmin, id)
}

// ListGitRepos lists registered Git repositories with pagination.
func (s *GitService) ListGitRepos(ctx context.Context, page domain.PageRequest) ([]domain.GitRepo, int64, error) {
	return s.repo.List(ctx, page)
}

// ListGitReposForPrincipal lists Git repositories visible to the caller.
func (s *GitService) ListGitReposForPrincipal(ctx context.Context, principal string, isAdmin bool, page domain.PageRequest) ([]domain.GitRepo, int64, error) {
	listPage := page
	if !isAdmin {
		listPage = domain.PageRequest{MaxResults: domain.MaxMaxResults}
	}

	repos, total, err := s.repo.List(ctx, listPage)
	if err != nil {
		return nil, 0, err
	}
	if isAdmin {
		return repos, total, nil
	}

	filtered := make([]domain.GitRepo, 0, len(repos))
	for _, repo := range repos {
		if repo.Owner == principal {
			filtered = append(filtered, repo)
		}
	}
	start := page.Offset()
	if start > len(filtered) {
		start = len(filtered)
	}
	end := start + page.Limit()
	if end > len(filtered) {
		end = len(filtered)
	}
	return filtered[start:end], int64(len(filtered)), nil
}

// DeleteGitRepo removes a Git repository. Only the owner or admin can delete.
func (s *GitService) DeleteGitRepo(ctx context.Context, principal string, isAdmin bool, id string) error {
	if _, err := s.requireGitRepoAccess(ctx, principal, isAdmin, id); err != nil {
		return err
	}

	if err := s.repo.Delete(ctx, id); err != nil {
		return fmt.Errorf("delete git repo: %w", err)
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "DELETE_GIT_REPO",
		Status:        "ALLOWED",
	})

	return nil
}

// SyncGitRepo triggers a sync from the Git repository to notebooks.
func (s *GitService) SyncGitRepo(ctx context.Context, principal string, isAdmin bool, id string) (*domain.GitSyncResult, error) {
	repo, err := s.requireGitRepoAccess(ctx, principal, isAdmin, id)
	if err != nil {
		return nil, err
	}
	if s.notebooks == nil {
		return nil, domain.ErrValidation("notebook repository is not configured")
	}

	cloneDir, commitSHA, err := s.cloneRepo(ctx, repo)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = os.RemoveAll(cloneDir)
	}()

	root, err := resolveDeclarativeRoot(cloneDir, repo.Path)
	if err != nil {
		return nil, err
	}
	resources, err := declarative.LoadNotebookResources(root)
	if err != nil {
		return nil, err
	}

	existing, _, err := s.notebooks.ListNotebooks(ctx, nil, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return nil, fmt.Errorf("list notebooks: %w", err)
	}
	linked := make(map[string]domain.Notebook)
	for _, nb := range existing {
		if nb.GitRepoID == nil || *nb.GitRepoID != repo.ID || nb.GitPath == nil || *nb.GitPath == "" {
			continue
		}
		if _, exists := linked[*nb.GitPath]; exists {
			return nil, domain.ErrConflict("duplicate synced notebook path %q found for git repo %q", *nb.GitPath, repo.ID)
		}
		linked[*nb.GitPath] = nb
	}

	desired := make(map[string]declarative.NotebookResource, len(resources))
	paths := make([]string, 0, len(resources))
	for _, resource := range resources {
		gitPath := notebookGitPath(resource.Name)
		desired[gitPath] = resource
		paths = append(paths, gitPath)
	}
	sort.Strings(paths)

	result := &domain.GitSyncResult{CommitSHA: commitSHA}
	for _, gitPath := range paths {
		resource := desired[gitPath]
		existingNotebook, found := linked[gitPath]
		if !found {
			notebook, err := s.notebooks.CreateNotebook(ctx, &domain.Notebook{
				Name:        resource.Name,
				Description: stringPtrOrNil(resource.Spec.Description),
				Owner:       repo.Owner,
				GitRepoID:   stringPtr(repo.ID),
				GitPath:     stringPtr(gitPath),
			})
			if err != nil {
				return nil, fmt.Errorf("create synced notebook %q: %w", resource.Name, err)
			}
			if _, err := s.syncNotebookCells(ctx, notebook.ID, resource.Spec.Cells); err != nil {
				return nil, fmt.Errorf("sync cells for notebook %q: %w", resource.Name, err)
			}
			if err := s.reconcileNotebookPublish(ctx, principal, notebook.ID, resource.Spec); err != nil {
				return nil, fmt.Errorf("reconcile publish for notebook %q: %w", resource.Name, err)
			}
			result.NotebooksCreated++
			continue
		}

		metaChanged := syncedNotebookMetadataChanged(existingNotebook, repo, gitPath, resource)
		if metaChanged {
			updated, err := s.notebooks.UpdateNotebookSync(ctx, &domain.Notebook{
				ID:          existingNotebook.ID,
				Name:        resource.Name,
				Description: stringPtrOrNil(resource.Spec.Description),
				Owner:       repo.Owner,
				GitRepoID:   stringPtr(repo.ID),
				GitPath:     stringPtr(gitPath),
			})
			if err != nil {
				return nil, fmt.Errorf("update synced notebook %q: %w", resource.Name, err)
			}
			existingNotebook = *updated
		}
		cellChanged, err := s.syncNotebookCells(ctx, existingNotebook.ID, resource.Spec.Cells)
		if err != nil {
			return nil, fmt.Errorf("sync cells for notebook %q: %w", resource.Name, err)
		}
		if err := s.reconcileNotebookPublish(ctx, principal, existingNotebook.ID, resource.Spec); err != nil {
			return nil, fmt.Errorf("reconcile publish for notebook %q: %w", resource.Name, err)
		}
		if metaChanged || cellChanged {
			result.NotebooksUpdated++
		}
		delete(linked, gitPath)
	}

	for gitPath, nb := range linked {
		if err := s.notebooks.DeleteNotebook(ctx, nb.ID); err != nil {
			return nil, fmt.Errorf("delete removed synced notebook %q: %w", gitPath, err)
		}
		result.NotebooksDeleted++
	}

	if err := s.repo.UpdateSyncStatus(ctx, repo.ID, commitSHA, time.Now().UTC()); err != nil {
		return nil, fmt.Errorf("update git repo sync status: %w", err)
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "SYNC_GIT_REPO",
		Status:        "ALLOWED",
	})

	return result, nil
}

func (s *GitService) cloneRepo(ctx context.Context, repo *domain.GitRepo) (string, string, error) {
	dir, err := os.MkdirTemp("", "duck-git-sync-*")
	if err != nil {
		return "", "", fmt.Errorf("create temp dir: %w", err)
	}
	cloneArgs := []string{"clone", "--depth", "1", "--branch", repo.Branch, "--single-branch", repo.URL, dir}
	if _, err := runGitCommand(ctx, "", repo.AuthToken, cloneArgs...); err != nil {
		_ = os.RemoveAll(dir)
		return "", "", fmt.Errorf("clone git repo %q: %w", repo.URL, err)
	}
	commitSHA, err := runGitCommand(ctx, dir, "", "rev-parse", "HEAD")
	if err != nil {
		_ = os.RemoveAll(dir)
		return "", "", fmt.Errorf("resolve git head: %w", err)
	}
	return dir, strings.TrimSpace(commitSHA), nil
}

func runGitCommand(ctx context.Context, dir, authToken string, args ...string) (string, error) {
	cmdArgs := append([]string{}, args...)
	if strings.TrimSpace(authToken) != "" {
		cmdArgs = append([]string{"-c", "http.extraHeader=" + gitAuthorizationHeader(authToken)}, cmdArgs...)
	}
	// #nosec G204 -- the executable is fixed to git; arguments are controlled clone/rev-parse inputs.
	cmd := exec.CommandContext(ctx, "git", cmdArgs...)
	if dir != "" {
		cmd.Dir = dir
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %s", strings.Join(args, " "), sanitizeGitCommandOutput(string(output), authToken))
	}
	return string(output), nil
}

func gitAuthorizationHeader(authToken string) string {
	encoded := base64.StdEncoding.EncodeToString([]byte("git:" + authToken))
	return "AUTHORIZATION: basic " + encoded
}

func sanitizeGitCommandOutput(output, authToken string) string {
	trimmed := strings.TrimSpace(output)
	if trimmed == "" {
		trimmed = "command failed"
	}
	if authToken == "" {
		return trimmed
	}
	return strings.ReplaceAll(trimmed, authToken, "[REDACTED]")
}

func resolveDeclarativeRoot(cloneDir, repoPath string) (string, error) {
	relPath := strings.TrimSpace(repoPath)
	if relPath == "" || relPath == "." {
		return cloneDir, nil
	}
	if filepath.IsAbs(relPath) {
		return "", domain.ErrValidation("git repo path must be relative")
	}
	clean := filepath.Clean(relPath)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", domain.ErrValidation("git repo path must stay within the repository root")
	}
	root := filepath.Join(cloneDir, clean)
	info, err := os.Stat(root)
	if err != nil {
		return "", fmt.Errorf("resolve declarative root %q: %w", relPath, err)
	}
	if !info.IsDir() {
		return "", domain.ErrValidation("git repo path %q is not a directory", relPath)
	}
	return root, nil
}

func (s *GitService) syncNotebookCells(ctx context.Context, notebookID string, desired []declarative.CellSpec) (bool, error) {
	existing, err := s.notebooks.ListCells(ctx, notebookID)
	if err != nil {
		return false, fmt.Errorf("list notebook cells: %w", err)
	}
	sort.Slice(existing, func(i, j int) bool {
		return existing[i].Position < existing[j].Position
	})

	protectedOutputCellID := ""
	if s.notebookLinks != nil {
		link, err := s.notebookLinks.GetByNotebookID(ctx, notebookID)
		if err == nil && link != nil {
			protectedOutputCellID = link.OutputCellID
		} else if err != nil {
			var notFound *domain.NotFoundError
			if !errors.As(err, &notFound) {
				return false, fmt.Errorf("lookup notebook model link: %w", err)
			}
		}
	}

	changed := false
	for i := 0; i < len(existing) && i < len(desired); i++ {
		if existing[i].CellType != domain.CellType(desired[i].Type) && existing[i].ID == protectedOutputCellID {
			return false, domain.ErrValidation("cannot replace published output cell %q in notebook %q because cell_type changed", existing[i].ID, notebookID)
		}
	}
	for i := len(existing) - 1; i >= len(desired); i-- {
		if existing[i].ID == protectedOutputCellID {
			return false, domain.ErrValidation("cannot delete published output cell %q from notebook %q during sync", existing[i].ID, notebookID)
		}
		if err := s.notebooks.DeleteCell(ctx, existing[i].ID); err != nil {
			return false, err
		}
		changed = true
	}
	for i := 0; i < len(desired); i++ {
		desiredCell := desiredNotebookCell(notebookID, desired[i], i)
		if i >= len(existing) {
			if _, err := s.notebooks.CreateCell(ctx, desiredCell); err != nil {
				return false, err
			}
			changed = true
			continue
		}

		current := existing[i]
		if current.CellType != desiredCell.CellType {
			if current.ID == protectedOutputCellID {
				return false, domain.ErrValidation("cannot replace published output cell %q in notebook %q because cell_type changed", current.ID, notebookID)
			}
			if err := s.notebooks.DeleteCell(ctx, current.ID); err != nil {
				return false, err
			}
			if _, err := s.notebooks.CreateCell(ctx, desiredCell); err != nil {
				return false, err
			}
			changed = true
			continue
		}

		desiredCell.ID = current.ID
		if notebookCellEquals(current, desiredCell) {
			continue
		}
		if _, err := s.notebooks.UpdateCellSync(ctx, desiredCell); err != nil {
			return false, err
		}
		if err := s.notebooks.UpdateCellResult(ctx, current.ID, nil); err != nil {
			return false, err
		}
		changed = true
	}
	return changed, nil
}

func (s *GitService) reconcileNotebookPublish(ctx context.Context, principal, notebookID string, spec declarative.NotebookSpec) error {
	if spec.Publish == nil || spec.Publish.Model == nil {
		if s.notebookLinks != nil {
			if err := s.notebookLinks.DeleteByNotebookID(ctx, notebookID); err != nil {
				var notFound *domain.NotFoundError
				if !errors.As(err, &notFound) {
					return fmt.Errorf("delete notebook publish link: %w", err)
				}
			}
		}
		return nil
	}
	if s.models == nil {
		return domain.ErrValidation("model promotion is not configured")
	}
	cells, err := s.notebooks.ListCells(ctx, notebookID)
	if err != nil {
		return fmt.Errorf("list notebook cells for publish: %w", err)
	}
	outputCellID := ""
	for _, cell := range cells {
		if cell.Name != nil && *cell.Name == spec.Publish.Model.OutputCell {
			outputCellID = cell.ID
			break
		}
	}
	if outputCellID == "" {
		return domain.ErrValidation("publish.model.output_cell %q not found in notebook %q", spec.Publish.Model.OutputCell, notebookID)
	}
	_, err = s.models.PromoteNotebook(ctx, principal, domain.PromoteNotebookRequest{
		NotebookID:      notebookID,
		OutputCellID:    outputCellID,
		ProjectName:     spec.Publish.Model.Project,
		Name:            spec.Publish.Model.Name,
		Materialization: spec.Publish.Model.Materialization,
	})
	return err
}

func desiredNotebookCell(notebookID string, spec declarative.CellSpec, position int) *domain.Cell {
	role := domain.CellRole(spec.Role)
	if role == "" {
		if spec.Type == string(domain.CellTypeMarkdown) {
			role = domain.CellRoleMarkdown
		} else {
			role = domain.CellRoleTransform
		}
	}
	return &domain.Cell{
		NotebookID: notebookID,
		CellType:   domain.CellType(spec.Type),
		Name:       stringPtrOrNil(spec.Name),
		Role:       role,
		Disabled:   spec.Disabled,
		Test:       notebookTestConfigFromSpec(spec.Test),
		VisualSpec: spec.VisualSpec,
		Content:    spec.Content,
		Position:   position,
	}
}

func notebookTestConfigFromSpec(spec *declarative.NotebookTestSpec) *domain.NotebookCellTestConfig {
	if spec == nil {
		return nil
	}
	cfg := &domain.NotebookCellTestConfig{Severity: domain.NotebookTestSeverity(spec.Severity)}
	if cfg.Severity == "" {
		cfg.Severity = domain.NotebookTestSeverityError
	}
	return cfg
}

func notebookCellEquals(existing domain.Cell, desired *domain.Cell) bool {
	return existing.CellType == desired.CellType &&
		stringPtrValue(existing.Name) == stringPtrValue(desired.Name) &&
		existing.Role == desired.Role &&
		existing.Disabled == desired.Disabled &&
		reflect.DeepEqual(existing.Test, desired.Test) &&
		reflect.DeepEqual(existing.VisualSpec, desired.VisualSpec) &&
		existing.Content == desired.Content &&
		existing.Position == desired.Position
}

func syncedNotebookMetadataChanged(existing domain.Notebook, repo *domain.GitRepo, gitPath string, resource declarative.NotebookResource) bool {
	return existing.Name != resource.Name ||
		stringPtrValue(existing.Description) != resource.Spec.Description ||
		existing.Owner != repo.Owner ||
		stringPtrValue(existing.GitRepoID) != repo.ID ||
		stringPtrValue(existing.GitPath) != gitPath
}

func notebookGitPath(name string) string {
	return filepath.ToSlash(filepath.Join("notebooks", name+".yaml"))
}

func stringPtr(value string) *string {
	return &value
}

func stringPtrOrNil(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return &value
}

func stringPtrValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
