package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"duck-demo/internal/domain"
)

var _ domain.FolderRepository = (*FolderRepo)(nil)

type FolderRepo struct {
	db *sql.DB
}

func NewFolderRepo(db *sql.DB) *FolderRepo {
	return &FolderRepo{db: db}
}

func (r *FolderRepo) Create(ctx context.Context, folder *domain.Folder) (*domain.Folder, error) {
	if folder == nil {
		return nil, domain.ErrValidation("folder is required")
	}
	if strings.TrimSpace(folder.Name) == "" {
		return nil, domain.ErrValidation("folder name is required")
	}
	if strings.TrimSpace(folder.Owner) == "" {
		return nil, domain.ErrValidation("folder owner is required")
	}

	id := folder.ID
	if id == "" {
		id = newID()
	}

	parentPath := ""
	depth := 0
	if folder.ParentFolderID != nil && strings.TrimSpace(*folder.ParentFolderID) != "" {
		parent, err := r.GetByID(ctx, *folder.ParentFolderID)
		if err != nil {
			return nil, err
		}
		parentPath = parent.Path
		depth = parent.Depth + 1
	}
	resolvedPath := folder.Path
	if strings.TrimSpace(resolvedPath) == "" {
		if parentPath == "" {
			resolvedPath = "/" + id
		} else {
			resolvedPath = parentPath + "/" + id
		}
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO folders (
			id, name, owner, parent_folder_id, path, depth, system_role,
			git_repo_id, git_root_path, default_project_id, default_environment_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		id,
		strings.TrimSpace(folder.Name),
		strings.TrimSpace(folder.Owner),
		nullStringPtr(folder.ParentFolderID),
		resolvedPath,
		depth,
		nullStringPtr(folder.SystemRole),
		nullStringPtr(folder.GitRepoID),
		nullStringPtr(folder.GitRootPath),
		nullStringPtr(folder.DefaultProjectID),
		nullStringPtr(folder.DefaultEnvironmentID),
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

func (r *FolderRepo) GetByID(ctx context.Context, id string) (*domain.Folder, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT
			id, name, owner, parent_folder_id, path, depth, system_role,
			git_repo_id, git_root_path, default_project_id, default_environment_id,
			created_at, updated_at
		FROM folders
		WHERE id = ?
	`, id)
	return scanFolder(row)
}

func (r *FolderRepo) ListAll(ctx context.Context) ([]domain.Folder, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			id, name, owner, parent_folder_id, path, depth, system_role,
			git_repo_id, git_root_path, default_project_id, default_environment_id,
			created_at, updated_at
		FROM folders
		ORDER BY owner COLLATE NOCASE ASC, depth ASC, name COLLATE NOCASE ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list all folders: %w", err)
	}
	defer rows.Close()

	out := []domain.Folder{}
	for rows.Next() {
		item, scanErr := scanFolderRows(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate all folders: %w", err)
	}
	return out, nil
}

func (r *FolderRepo) ListByOwner(ctx context.Context, owner string) ([]domain.Folder, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			id, name, owner, parent_folder_id, path, depth, system_role,
			git_repo_id, git_root_path, default_project_id, default_environment_id,
			created_at, updated_at
		FROM folders
		WHERE owner = ?
		ORDER BY depth ASC, name COLLATE NOCASE ASC
	`, strings.TrimSpace(owner))
	if err != nil {
		return nil, fmt.Errorf("list folders by owner: %w", err)
	}
	defer rows.Close()

	out := []domain.Folder{}
	for rows.Next() {
		item, scanErr := scanFolderRows(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate folders by owner: %w", err)
	}
	return out, nil
}

func (r *FolderRepo) Update(ctx context.Context, id string, req domain.UpdateFolderRequest) (*domain.Folder, error) {
	existing, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	name := existing.Name
	if req.Name != nil {
		trimmed := strings.TrimSpace(*req.Name)
		if trimmed == "" {
			return nil, domain.ErrValidation("folder name is required")
		}
		name = trimmed
	}

	gitRepoID := existing.GitRepoID
	if req.GitRepoID != nil {
		gitRepoID = stringPtr(strings.TrimSpace(*req.GitRepoID))
		if strings.TrimSpace(*req.GitRepoID) == "" {
			gitRepoID = nil
		}
	}
	gitRootPath := existing.GitRootPath
	if req.GitRootPath != nil {
		gitRootPath = stringPtr(strings.TrimSpace(*req.GitRootPath))
		if strings.TrimSpace(*req.GitRootPath) == "" {
			gitRootPath = nil
		}
	}
	defaultProjectID := existing.DefaultProjectID
	if req.DefaultProjectID != nil {
		defaultProjectID = stringPtr(strings.TrimSpace(*req.DefaultProjectID))
		if strings.TrimSpace(*req.DefaultProjectID) == "" {
			defaultProjectID = nil
		}
	}
	defaultEnvironmentID := existing.DefaultEnvironmentID
	if req.DefaultEnvironmentID != nil {
		defaultEnvironmentID = stringPtr(strings.TrimSpace(*req.DefaultEnvironmentID))
		if strings.TrimSpace(*req.DefaultEnvironmentID) == "" {
			defaultEnvironmentID = nil
		}
	}

	_, err = r.db.ExecContext(ctx, `
		UPDATE folders
		SET
			name = ?,
			git_repo_id = ?,
			git_root_path = ?,
			default_project_id = ?,
			default_environment_id = ?,
			updated_at = datetime('now')
		WHERE id = ?
	`,
		name,
		nullStringPtr(gitRepoID),
		nullStringPtr(gitRootPath),
		nullStringPtr(defaultProjectID),
		nullStringPtr(defaultEnvironmentID),
		id,
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

func (r *FolderRepo) Move(ctx context.Context, id string, parentFolderID *string) (*domain.Folder, error) {
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	var (
		newParentID *string
		newPath     string
		newDepth    int
	)
	if parentFolderID != nil && strings.TrimSpace(*parentFolderID) != "" {
		parent, err := r.GetByID(ctx, strings.TrimSpace(*parentFolderID))
		if err != nil {
			return nil, err
		}
		if parent.ID == current.ID {
			return nil, domain.ErrValidation("folder cannot be its own parent")
		}
		if parent.Path == current.Path || strings.HasPrefix(parent.Path, current.Path+"/") {
			return nil, domain.ErrValidation("folder cannot be moved into its own subtree")
		}
		newParentID = &parent.ID
		newPath = parent.Path + "/" + current.ID
		newDepth = parent.Depth + 1
	} else {
		newPath = "/" + current.ID
		newDepth = 0
	}
	if newPath == current.Path && ptrTrimmedValue(newParentID) == ptrTrimmedValue(current.ParentFolderID) {
		return current, nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin folder move tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `
		UPDATE folders
		SET parent_folder_id = ?, path = ?, depth = ?, updated_at = datetime('now')
		WHERE id = ?
	`, nullStringPtr(newParentID), newPath, newDepth, current.ID); err != nil {
		return nil, mapDBError(err)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE folders
		SET
			path = ? || substr(path, ?),
			depth = depth + ?,
			updated_at = datetime('now')
		WHERE path LIKE ?
	`, newPath, len(current.Path)+1, newDepth-current.Depth, current.Path+"/%"); err != nil {
		return nil, mapDBError(err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit folder move tx: %w", err)
	}
	return r.GetByID(ctx, id)
}

func (r *FolderRepo) Delete(ctx context.Context, id string) error {
	result, err := r.db.ExecContext(ctx, `DELETE FROM folders WHERE id = ?`, id)
	if err != nil {
		if strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
			return domain.ErrValidation("cannot delete folder %q because it is still referenced", id)
		}
		return mapDBError(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete folder rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return domain.ErrNotFound("folder %s not found", id)
	}
	return nil
}

func (r *FolderRepo) EnsurePersonalRoot(ctx context.Context, owner string) (*domain.Folder, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return nil, domain.ErrValidation("owner is required")
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT
			id, name, owner, parent_folder_id, path, depth, system_role,
			git_repo_id, git_root_path, default_project_id, default_environment_id,
			created_at, updated_at
		FROM folders
		WHERE owner = ? AND system_role = ?
		LIMIT 1
	`, owner, domain.FolderSystemRolePersonalRoot)
	folder, err := scanFolder(row)
	if err == nil {
		return folder, nil
	}
	var notFound *domain.NotFoundError
	if !errors.As(err, &notFound) {
		return nil, err
	}
	role := domain.FolderSystemRolePersonalRoot
	return r.Create(ctx, &domain.Folder{
		Name:       "My notebooks",
		Owner:      owner,
		SystemRole: &role,
	})
}

func (r *FolderRepo) EnsureGitSyncRoot(ctx context.Context, owner string, repo *domain.GitRepo) (*domain.Folder, error) {
	if repo == nil {
		return nil, domain.ErrValidation("git repo is required")
	}
	personalRoot, err := r.EnsurePersonalRoot(ctx, owner)
	if err != nil {
		return nil, err
	}

	row := r.db.QueryRowContext(ctx, `
		SELECT
			id, name, owner, parent_folder_id, path, depth, system_role,
			git_repo_id, git_root_path, default_project_id, default_environment_id,
			created_at, updated_at
		FROM folders
		WHERE owner = ? AND parent_folder_id = ? AND git_repo_id = ?
		LIMIT 1
	`, owner, personalRoot.ID, repo.ID)
	folder, err := scanFolder(row)
	if err == nil {
		return folder, nil
	}
	var notFound *domain.NotFoundError
	if !errors.As(err, &notFound) {
		return nil, err
	}

	parentID := personalRoot.ID
	name := gitSyncFolderName(repo)
	return r.Create(ctx, &domain.Folder{
		Name:           name,
		Owner:          owner,
		ParentFolderID: &parentID,
		GitRepoID:      &repo.ID,
		GitRootPath:    stringPtr(strings.TrimSpace(repo.Path)),
	})
}

func (r *FolderRepo) ListAncestors(ctx context.Context, folderID string) ([]domain.Folder, error) {
	folder, err := r.GetByID(ctx, folderID)
	if err != nil {
		return nil, err
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			id, name, owner, parent_folder_id, path, depth, system_role,
			git_repo_id, git_root_path, default_project_id, default_environment_id,
			created_at, updated_at
		FROM folders
		WHERE ? = path OR ? LIKE path || '/%'
		ORDER BY depth DESC
	`, folder.Path, folder.Path)
	if err != nil {
		return nil, fmt.Errorf("list folder ancestors: %w", err)
	}
	defer rows.Close()

	out := []domain.Folder{}
	for rows.Next() {
		item, scanErr := scanFolderRows(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate folder ancestors: %w", err)
	}
	return out, nil
}

func scanFolder(row interface{ Scan(dest ...any) error }) (*domain.Folder, error) {
	var (
		id                   string
		name                 string
		owner                string
		parentFolderID       sql.NullString
		folderPath           string
		depth                int
		systemRole           sql.NullString
		gitRepoID            sql.NullString
		gitRootPath          sql.NullString
		defaultProjectID     sql.NullString
		defaultEnvironmentID sql.NullString
		createdAtRaw         string
		updatedAtRaw         string
	)
	if err := row.Scan(
		&id,
		&name,
		&owner,
		&parentFolderID,
		&folderPath,
		&depth,
		&systemRole,
		&gitRepoID,
		&gitRootPath,
		&defaultProjectID,
		&defaultEnvironmentID,
		&createdAtRaw,
		&updatedAtRaw,
	); err != nil {
		return nil, mapDBError(err)
	}
	return &domain.Folder{
		ID:                   id,
		Name:                 name,
		Owner:                owner,
		ParentFolderID:       ptrFromNullString(parentFolderID),
		Path:                 folderPath,
		Depth:                depth,
		SystemRole:           ptrFromNullString(systemRole),
		GitRepoID:            ptrFromNullString(gitRepoID),
		GitRootPath:          ptrFromNullString(gitRootPath),
		DefaultProjectID:     ptrFromNullString(defaultProjectID),
		DefaultEnvironmentID: ptrFromNullString(defaultEnvironmentID),
		CreatedAt:            mustParseFolderTime(createdAtRaw),
		UpdatedAt:            mustParseFolderTime(updatedAtRaw),
	}, nil
}

func scanFolderRows(rows *sql.Rows) (*domain.Folder, error) {
	return scanFolder(rows)
}

func gitSyncFolderName(repo *domain.GitRepo) string {
	if repo == nil {
		return "Repo notebooks"
	}
	if trimmed := strings.TrimSpace(repo.Path); trimmed != "" {
		base := path.Base(trimmed)
		if base != "." && base != "/" && base != "" {
			return base
		}
	}
	urlBase := path.Base(strings.TrimSuffix(strings.TrimSpace(repo.URL), ".git"))
	if urlBase != "." && urlBase != "/" && urlBase != "" {
		return urlBase
	}
	return "Repo notebooks"
}

func stringPtr(s string) *string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	v := strings.TrimSpace(s)
	return &v
}

func ptrTrimmedValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func mustParseFolderTime(raw string) time.Time {
	parsed, err := time.Parse("2006-01-02 15:04:05", raw)
	if err != nil {
		return time.Time{}
	}
	return parsed
}
