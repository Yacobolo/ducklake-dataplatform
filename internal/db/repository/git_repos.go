package repository

import (
	"context"
	"database/sql"
	"time"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

var _ domain.GitRepoRepository = (*GitRepoRepo)(nil)

// GitRepoRepo implements domain.GitRepoRepository using sqlc-generated queries.
type GitRepoRepo struct {
	q *dbstore.Queries
}

// NewGitRepoRepo creates a new GitRepoRepo.
func NewGitRepoRepo(db *sql.DB) *GitRepoRepo {
	return &GitRepoRepo{q: dbstore.New(db)}
}

// Create inserts a new git repo.
func (r *GitRepoRepo) Create(ctx context.Context, repo *domain.GitRepo) (*domain.GitRepo, error) {
	row, err := r.q.CreateGitRepo(ctx, dbstore.CreateGitRepoParams{
		ID:            domain.NewID(),
		Url:           repo.URL,
		Branch:        repo.Branch,
		Path:          repo.Path,
		AuthToken:     repo.AuthToken,
		WebhookSecret: mapper.NullStrFromPtr(repo.WebhookSecret),
		Owner:         repo.Owner,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.GitRepoFromDB(row), nil
}

// GetByID returns a git repo by its ID.
func (r *GitRepoRepo) GetByID(ctx context.Context, id string) (*domain.GitRepo, error) {
	row, err := r.q.GetGitRepo(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.GitRepoFromDB(row), nil
}

// List returns a paginated list of git repos.
func (r *GitRepoRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.GitRepo, int64, error) {
	total, err := r.q.CountGitRepos(ctx)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListGitRepos(ctx, dbstore.ListGitReposParams{
		Limit:  int64(page.Limit()),
		Offset: int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	return mapper.GitReposFromDB(rows), total, nil
}

// Delete removes a git repo by ID.
func (r *GitRepoRepo) Delete(ctx context.Context, id string) error {
	return r.q.DeleteGitRepo(ctx, id)
}

// UpdateSyncStatus updates the last commit SHA and sync timestamp for a git repo.
func (r *GitRepoRepo) UpdateSyncStatus(ctx context.Context, id string, commitSHA string, syncedAt time.Time) error {
	return r.q.UpdateGitRepoSyncStatus(ctx, dbstore.UpdateGitRepoSyncStatusParams{
		LastCommit: mapper.NullStrFromStr(commitSHA),
		LastSyncAt: mapper.NullStrFromStr(syncedAt.Format("2006-01-02 15:04:05")),
		ID:         id,
	})
}
