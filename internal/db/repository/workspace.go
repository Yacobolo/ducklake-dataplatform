package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
)

var _ domain.WorkspaceRepository = (*WorkspaceRepo)(nil)

// WorkspaceRepo persists authoring workspaces and memberships in SQLite.
type WorkspaceRepo struct {
	db *sql.DB
}

// NewWorkspaceRepo constructs a workspace repository backed by SQLite.
func NewWorkspaceRepo(db *sql.DB) *WorkspaceRepo {
	return &WorkspaceRepo{db: db}
}

// Create inserts a workspace record and returns the stored row.
func (r *WorkspaceRepo) Create(ctx context.Context, workspace *domain.Workspace) (*domain.Workspace, error) {
	now := time.Now().UTC()
	id := workspace.ID
	if id == "" {
		id = newID()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO workspaces (
			id, name, kind, owner_group_id, owner_principal, default_project_id,
			default_environment_id, git_repo_id, git_root_path, created_by, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		id,
		workspace.Name,
		workspace.Kind,
		nullableStringValue(workspace.OwnerGroupID),
		nullableStringValue(workspace.OwnerPrincipal),
		nullableStringValue(workspace.DefaultProjectID),
		nullableStringValue(workspace.DefaultEnvironmentID),
		nullableStringValue(workspace.GitRepoID),
		nullableStringValue(workspace.GitRootPath),
		workspace.CreatedBy,
		now.Format(time.RFC3339),
		now.Format(time.RFC3339),
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// GetByID fetches a workspace by ID.
func (r *WorkspaceRepo) GetByID(ctx context.Context, id string) (*domain.Workspace, error) {
	row := r.db.QueryRowContext(ctx, workspaceSelectSQL+` WHERE w.id = ?`, id)
	return scanWorkspace(row)
}

// GetPersonalByPrincipal fetches the personal workspace owned by a principal.
func (r *WorkspaceRepo) GetPersonalByPrincipal(ctx context.Context, principal string) (*domain.Workspace, error) {
	row := r.db.QueryRowContext(ctx, workspaceSelectSQL+` WHERE w.owner_principal = ? AND w.kind = ?`, principal, domain.WorkspaceKindPersonal)
	return scanWorkspace(row)
}

// List returns paginated workspaces across the system.
func (r *WorkspaceRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.Workspace, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM workspaces`).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, workspaceSelectSQL+`
		ORDER BY w.name
		LIMIT ? OFFSET ?`, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]domain.Workspace, 0)
	for rows.Next() {
		item, scanErr := scanWorkspace(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// ListForPrincipal returns paginated workspaces where the principal is a member.
func (r *WorkspaceRepo) ListForPrincipal(ctx context.Context, principal string, page domain.PageRequest) ([]domain.Workspace, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM workspaces w
		JOIN workspace_members m ON m.workspace_id = w.id
		WHERE m.principal_name = ?
	`, principal).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, workspaceSelectSQL+`
		JOIN workspace_members m ON m.workspace_id = w.id
		WHERE m.principal_name = ?
		ORDER BY w.name
		LIMIT ? OFFSET ?`, principal, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]domain.Workspace, 0)
	for rows.Next() {
		item, scanErr := scanWorkspace(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// Update applies mutable workspace fields and returns the stored row.
func (r *WorkspaceRepo) Update(ctx context.Context, id string, req domain.UpdateWorkspaceRequest) (*domain.Workspace, error) {
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	name := current.Name
	if req.Name != nil {
		name = *req.Name
	}
	defaultProjectID := current.DefaultProjectID
	if req.DefaultProjectID != nil {
		defaultProjectID = req.DefaultProjectID
	}
	defaultEnvironmentID := current.DefaultEnvironmentID
	if req.DefaultEnvironmentID != nil {
		defaultEnvironmentID = req.DefaultEnvironmentID
	}
	gitRepoID := current.GitRepoID
	if req.GitRepoID != nil {
		gitRepoID = req.GitRepoID
	}
	gitRootPath := current.GitRootPath
	if req.GitRootPath != nil {
		gitRootPath = req.GitRootPath
	}

	_, err = r.db.ExecContext(ctx, `
		UPDATE workspaces
		SET name = ?, default_project_id = ?, default_environment_id = ?, git_repo_id = ?, git_root_path = ?, updated_at = ?
		WHERE id = ?
	`, name, nullableStringValue(defaultProjectID), nullableStringValue(defaultEnvironmentID), nullableStringValue(gitRepoID), nullableStringValue(gitRootPath), time.Now().UTC().Format(time.RFC3339), id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// Delete removes a workspace by ID.
func (r *WorkspaceRepo) Delete(ctx context.Context, id string) error {
	result, err := r.db.ExecContext(ctx, `DELETE FROM workspaces WHERE id = ?`, id)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("workspace %q not found", id)
	}
	return nil
}

// UpsertMember creates or updates a workspace membership row.
func (r *WorkspaceRepo) UpsertMember(ctx context.Context, member *domain.WorkspaceMember) (*domain.WorkspaceMember, error) {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO workspace_members (workspace_id, principal_name, role, created_at, updated_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT(workspace_id, principal_name) DO UPDATE SET
			role = excluded.role,
			updated_at = CURRENT_TIMESTAMP
	`, member.WorkspaceID, member.PrincipalName, member.Role)
	if err != nil {
		return nil, mapDBError(err)
	}
	role, err := r.GetMemberRole(ctx, member.WorkspaceID, member.PrincipalName)
	if err != nil {
		return nil, err
	}
	return &domain.WorkspaceMember{
		WorkspaceID:   member.WorkspaceID,
		PrincipalName: member.PrincipalName,
		Role:          role,
	}, nil
}

// DeleteMember removes a principal from a workspace membership list.
func (r *WorkspaceRepo) DeleteMember(ctx context.Context, workspaceID string, principalName string) error {
	result, err := r.db.ExecContext(ctx, `DELETE FROM workspace_members WHERE workspace_id = ? AND principal_name = ?`, workspaceID, principalName)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("workspace member %q not found", principalName)
	}
	return nil
}

// ListMembers returns all members for one workspace.
func (r *WorkspaceRepo) ListMembers(ctx context.Context, workspaceID string) ([]domain.WorkspaceMember, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT workspace_id, principal_name, role, created_at, updated_at
		FROM workspace_members
		WHERE workspace_id = ?
		ORDER BY principal_name
	`, workspaceID)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]domain.WorkspaceMember, 0)
	for rows.Next() {
		item, scanErr := scanWorkspaceMember(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// GetMemberRole returns the membership role for a principal in a workspace.
func (r *WorkspaceRepo) GetMemberRole(ctx context.Context, workspaceID string, principalName string) (string, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT role
		FROM workspace_members
		WHERE workspace_id = ? AND principal_name = ?
	`, workspaceID, principalName)
	var role string
	if err := row.Scan(&role); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", mapDBError(err)
	}
	return role, nil
}

const workspaceSelectSQL = `
	SELECT
		w.id, w.name, w.kind, w.owner_group_id, w.owner_principal, w.default_project_id,
		w.default_environment_id, w.git_repo_id, w.git_root_path, w.created_by, w.created_at, w.updated_at
	FROM workspaces w`

func scanWorkspace(row interface{ Scan(dest ...any) error }) (*domain.Workspace, error) {
	var (
		id                   string
		name                 string
		kind                 string
		ownerGroupID         sql.NullString
		ownerPrincipal       sql.NullString
		defaultProjectID     sql.NullString
		defaultEnvironmentID sql.NullString
		gitRepoID            sql.NullString
		gitRootPath          sql.NullString
		createdBy            string
		createdAtRaw         string
		updatedAtRaw         string
	)
	if err := row.Scan(&id, &name, &kind, &ownerGroupID, &ownerPrincipal, &defaultProjectID, &defaultEnvironmentID, &gitRepoID, &gitRootPath, &createdBy, &createdAtRaw, &updatedAtRaw); err != nil {
		return nil, mapDBError(err)
	}
	return &domain.Workspace{
		ID:                   id,
		Name:                 name,
		Kind:                 kind,
		OwnerGroupID:         ptrFromNullString(ownerGroupID),
		OwnerPrincipal:       ptrFromNullString(ownerPrincipal),
		DefaultProjectID:     ptrFromNullString(defaultProjectID),
		DefaultEnvironmentID: ptrFromNullString(defaultEnvironmentID),
		GitRepoID:            ptrFromNullString(gitRepoID),
		GitRootPath:          ptrFromNullString(gitRootPath),
		CreatedBy:            createdBy,
		CreatedAt:            parseSQLiteTimestamp(createdAtRaw),
		UpdatedAt:            parseSQLiteTimestamp(updatedAtRaw),
	}, nil
}

func scanWorkspaceMember(row interface{ Scan(dest ...any) error }) (*domain.WorkspaceMember, error) {
	var (
		workspaceID  string
		principal    string
		role         string
		createdAtRaw string
		updatedAtRaw string
	)
	if err := row.Scan(&workspaceID, &principal, &role, &createdAtRaw, &updatedAtRaw); err != nil {
		return nil, mapDBError(err)
	}
	return &domain.WorkspaceMember{
		WorkspaceID:   workspaceID,
		PrincipalName: principal,
		Role:          role,
		CreatedAt:     parseSQLiteTimestamp(createdAtRaw),
		UpdatedAt:     parseSQLiteTimestamp(updatedAtRaw),
	}, nil
}

func parseSQLiteTimestamp(raw string) time.Time {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed
		}
	}
	return time.Time{}
}

func ensurePersonalWorkspace(ctx context.Context, db *sql.DB, principal string) (*domain.Workspace, error) {
	repo := NewWorkspaceRepo(db)
	workspace, err := repo.GetPersonalByPrincipal(ctx, principal)
	if err == nil {
		return workspace, nil
	}
	if err != nil {
		var notFound *domain.NotFoundError
		if !errors.As(err, &notFound) {
			return nil, err
		}
	}
	created, err := repo.Create(ctx, &domain.Workspace{
		Name:           principal + " workspace",
		Kind:           domain.WorkspaceKindPersonal,
		OwnerPrincipal: &principal,
		CreatedBy:      principal,
	})
	if err != nil {
		return nil, err
	}
	if _, err := repo.UpsertMember(ctx, &domain.WorkspaceMember{
		WorkspaceID:   created.ID,
		PrincipalName: principal,
		Role:          domain.FolderShareRoleManager,
	}); err != nil {
		return nil, err
	}
	return created, nil
}
