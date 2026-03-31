package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

//revive:disable:exported

var _ domain.FolderShareRepository = (*FolderShareRepo)(nil)
var _ domain.NotebookShareRepository = (*NotebookShareRepo)(nil)

type FolderShareRepo struct {
	db *sql.DB
}

func NewFolderShareRepo(db *sql.DB) *FolderShareRepo {
	return &FolderShareRepo{db: db}
}

func (r *FolderShareRepo) Upsert(ctx context.Context, share *domain.FolderShare) (*domain.FolderShare, error) {
	if share == nil {
		return nil, domain.ErrValidation("folder share is required")
	}
	role := strings.TrimSpace(share.Role)
	if role == "" {
		role = domain.FolderShareRoleViewer
	}
	role = domain.NormalizeShareRole(role)
	if role == "" {
		return nil, domain.ErrValidation("unsupported folder share role %q", share.Role)
	}
	folderID := strings.TrimSpace(share.FolderID)
	principalName := strings.TrimSpace(share.PrincipalName)
	if folderID == "" || principalName == "" {
		return nil, domain.ErrValidation("folder_id and principal_name are required")
	}

	shareID := share.ID
	if strings.TrimSpace(shareID) == "" {
		shareID = newID()
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO folder_shares (id, folder_id, principal_name, role)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(folder_id, principal_name) DO UPDATE SET
			role = excluded.role,
			updated_at = datetime('now')
	`, shareID, folderID, principalName, role)
	if err != nil {
		return nil, mapDBError(err)
	}

	items, err := r.ListByFolder(ctx, folderID)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if items[i].PrincipalName == principalName {
			return &items[i], nil
		}
	}
	return nil, domain.ErrNotFound("folder share for %s not found", principalName)
}

func (r *FolderShareRepo) Delete(ctx context.Context, folderID string, principalName string) error {
	result, err := r.db.ExecContext(ctx, `
		DELETE FROM folder_shares
		WHERE folder_id = ? AND principal_name = ?
	`, strings.TrimSpace(folderID), strings.TrimSpace(principalName))
	if err != nil {
		return mapDBError(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete folder share rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return domain.ErrNotFound("folder share for %s not found", principalName)
	}
	return nil
}

func (r *FolderShareRepo) ListByFolder(ctx context.Context, folderID string) ([]domain.FolderShare, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, folder_id, principal_name, role, created_at, updated_at
		FROM folder_shares
		WHERE folder_id = ?
		ORDER BY principal_name COLLATE NOCASE ASC
	`, strings.TrimSpace(folderID))
	if err != nil {
		return nil, fmt.Errorf("list folder shares: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := []domain.FolderShare{}
	for rows.Next() {
		item, scanErr := scanFolderShare(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate folder shares: %w", err)
	}
	return items, nil
}

func (r *FolderShareRepo) ListByPrincipal(ctx context.Context, principalName string) ([]domain.FolderShare, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, folder_id, principal_name, role, created_at, updated_at
		FROM folder_shares
		WHERE principal_name = ?
		ORDER BY updated_at DESC
	`, strings.TrimSpace(principalName))
	if err != nil {
		return nil, fmt.Errorf("list principal folder shares: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := []domain.FolderShare{}
	for rows.Next() {
		item, scanErr := scanFolderShare(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate principal folder shares: %w", err)
	}
	return items, nil
}

type NotebookShareRepo struct {
	db *sql.DB
}

func NewNotebookShareRepo(db *sql.DB) *NotebookShareRepo {
	return &NotebookShareRepo{db: db}
}

func (r *NotebookShareRepo) Upsert(ctx context.Context, share *domain.NotebookShare) (*domain.NotebookShare, error) {
	if share == nil {
		return nil, domain.ErrValidation("notebook share is required")
	}
	role := strings.TrimSpace(share.Role)
	if role == "" {
		role = domain.FolderShareRoleViewer
	}
	role = domain.NormalizeShareRole(role)
	if role == "" {
		return nil, domain.ErrValidation("unsupported notebook share role %q", share.Role)
	}
	notebookID := strings.TrimSpace(share.NotebookID)
	principalName := strings.TrimSpace(share.PrincipalName)
	if notebookID == "" || principalName == "" {
		return nil, domain.ErrValidation("notebook_id and principal_name are required")
	}

	shareID := share.ID
	if strings.TrimSpace(shareID) == "" {
		shareID = newID()
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO notebook_shares (id, notebook_id, principal_name, role)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(notebook_id, principal_name) DO UPDATE SET
			role = excluded.role,
			updated_at = datetime('now')
	`, shareID, notebookID, principalName, role)
	if err != nil {
		return nil, mapDBError(err)
	}

	items, err := r.ListByNotebook(ctx, notebookID)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if items[i].PrincipalName == principalName {
			return &items[i], nil
		}
	}
	return nil, domain.ErrNotFound("notebook share for %s not found", principalName)
}

func (r *NotebookShareRepo) Delete(ctx context.Context, notebookID string, principalName string) error {
	result, err := r.db.ExecContext(ctx, `
		DELETE FROM notebook_shares
		WHERE notebook_id = ? AND principal_name = ?
	`, strings.TrimSpace(notebookID), strings.TrimSpace(principalName))
	if err != nil {
		return mapDBError(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete notebook share rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return domain.ErrNotFound("notebook share for %s not found", principalName)
	}
	return nil
}

func (r *NotebookShareRepo) ListByNotebook(ctx context.Context, notebookID string) ([]domain.NotebookShare, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, notebook_id, principal_name, role, created_at, updated_at
		FROM notebook_shares
		WHERE notebook_id = ?
		ORDER BY principal_name COLLATE NOCASE ASC
	`, strings.TrimSpace(notebookID))
	if err != nil {
		return nil, fmt.Errorf("list notebook shares: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := []domain.NotebookShare{}
	for rows.Next() {
		item, scanErr := scanNotebookShare(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate notebook shares: %w", err)
	}
	return items, nil
}

func (r *NotebookShareRepo) ListByPrincipal(ctx context.Context, principalName string) ([]domain.NotebookShare, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, notebook_id, principal_name, role, created_at, updated_at
		FROM notebook_shares
		WHERE principal_name = ?
		ORDER BY updated_at DESC
	`, strings.TrimSpace(principalName))
	if err != nil {
		return nil, fmt.Errorf("list principal notebook shares: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := []domain.NotebookShare{}
	for rows.Next() {
		item, scanErr := scanNotebookShare(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate principal notebook shares: %w", err)
	}
	return items, nil
}

func scanFolderShare(row interface{ Scan(dest ...any) error }) (*domain.FolderShare, error) {
	var (
		item         domain.FolderShare
		createdAtRaw string
		updatedAtRaw string
	)
	if err := row.Scan(
		&item.ID,
		&item.FolderID,
		&item.PrincipalName,
		&item.Role,
		&createdAtRaw,
		&updatedAtRaw,
	); err != nil {
		return nil, mapDBError(err)
	}
	item.CreatedAt = mustParseFolderTime(createdAtRaw)
	item.UpdatedAt = mustParseFolderTime(updatedAtRaw)
	return &item, nil
}

func scanNotebookShare(row interface{ Scan(dest ...any) error }) (*domain.NotebookShare, error) {
	var (
		item         domain.NotebookShare
		createdAtRaw string
		updatedAtRaw string
	)
	if err := row.Scan(
		&item.ID,
		&item.NotebookID,
		&item.PrincipalName,
		&item.Role,
		&createdAtRaw,
		&updatedAtRaw,
	); err != nil {
		return nil, mapDBError(err)
	}
	item.CreatedAt = mustParseFolderTime(createdAtRaw)
	item.UpdatedAt = mustParseFolderTime(updatedAtRaw)
	return &item, nil
}
