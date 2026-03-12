package notebook

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
)

// GitService provides business logic for Git repository operations.
type GitService struct {
	repo  domain.GitRepoRepository
	audit domain.AuditRepository
}

// NewGitService creates a new GitService.
func NewGitService(repo domain.GitRepoRepository, audit domain.AuditRepository) *GitService {
	return &GitService{repo: repo, audit: audit}
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
// Currently stubbed — full go-git integration will be added in a follow-up.
func (s *GitService) SyncGitRepo(ctx context.Context, id string) (*domain.GitSyncResult, error) {
	// Verify the repo exists
	if _, err := s.repo.GetByID(ctx, id); err != nil {
		return nil, err
	}
	return nil, domain.ErrNotImplemented("git sync is not yet implemented")
}
