package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"duck-demo/internal/domain"
)

type SavedResourceRepo struct {
	db *sql.DB
}

func NewSavedResourceRepo(db *sql.DB) *SavedResourceRepo {
	return &SavedResourceRepo{db: db}
}

var _ domain.SavedResourceRepository = (*SavedResourceRepo)(nil)

func (r *SavedResourceRepo) Save(ctx context.Context, principalID string, resource domain.ResourceRef) error {
	now := time.Now().UTC()
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO saved_resources (
			principal_id, resource_type, resource_key, display_name, resource_path, section, saved_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(principal_id, resource_type, resource_key) DO UPDATE SET
			display_name = excluded.display_name,
			resource_path = excluded.resource_path,
			section = excluded.section,
			saved_at = excluded.saved_at,
			updated_at = excluded.updated_at
	`, principalID, resource.ResourceType, resource.ResourceKey, resource.DisplayName, resource.ResourcePath, resource.Section, now, now)
	if err != nil {
		return fmt.Errorf("save resource preference: %w", mapDBError(err))
	}
	return nil
}

func (r *SavedResourceRepo) Unsave(ctx context.Context, principalID string, resourceType string, resourceKey string) error {
	_, err := r.db.ExecContext(ctx, `
		DELETE FROM saved_resources
		WHERE principal_id = ? AND resource_type = ? AND resource_key = ?
	`, principalID, resourceType, resourceKey)
	if err != nil {
		return fmt.Errorf("unsave resource preference: %w", mapDBError(err))
	}
	return nil
}

func (r *SavedResourceRepo) ListSaved(ctx context.Context, principalID string, limit int) ([]domain.SavedResource, error) {
	rows, err := r.db.QueryContext(ctx, `
		WITH latest_events AS (
			SELECT resource_type, resource_key, resource_path, accessed_at
			FROM (
				SELECT
					resource_type,
					resource_key,
					resource_path,
					accessed_at,
					ROW_NUMBER() OVER (
						PARTITION BY resource_type, resource_key
						ORDER BY accessed_at DESC, id DESC
					) AS rn
				FROM resource_access_events
				WHERE principal_id = ?
			)
			WHERE rn = 1
		)
		SELECT
			s.resource_type,
			s.resource_key,
			s.display_name,
			COALESCE(s.resource_path, e.resource_path),
			s.section,
			e.accessed_at,
			s.saved_at
		FROM saved_resources s
		LEFT JOIN latest_events e
			ON e.resource_type = s.resource_type
			AND e.resource_key = s.resource_key
		WHERE s.principal_id = ?
		ORDER BY s.saved_at DESC
		LIMIT ?
	`, principalID, principalID, limit)
	if err != nil {
		return nil, fmt.Errorf("list saved resources: %w", mapDBError(err))
	}
	defer rows.Close()

	return scanSavedRows(rows)
}

func scanSavedRows(rows *sql.Rows) ([]domain.SavedResource, error) {
	var items []domain.SavedResource
	for rows.Next() {
		var item domain.SavedResource
		var resourcePath sql.NullString
		var lastAccessedAt sql.NullTime
		if err := rows.Scan(
			&item.ResourceType,
			&item.ResourceKey,
			&item.DisplayName,
			&resourcePath,
			&item.Section,
			&lastAccessedAt,
			&item.SavedAt,
		); err != nil {
			return nil, fmt.Errorf("scan saved resource row: %w", err)
		}
		item.ResourcePath = stringFromNull(resourcePath)
		item.LastAccessedAt = ptrFromNullTime(lastAccessedAt)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate saved resource rows: %w", err)
	}
	return items, nil
}

func stringFromNull(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return v.String
}
