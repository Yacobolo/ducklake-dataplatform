package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
)

// ResourceAccessRepo stores resource access events in SQLite.
type ResourceAccessRepo struct {
	db *sql.DB
}

// NewResourceAccessRepo creates a repository for resource access events.
func NewResourceAccessRepo(db *sql.DB) *ResourceAccessRepo {
	return &ResourceAccessRepo{db: db}
}

var _ domain.ResourceAccessRepository = (*ResourceAccessRepo)(nil)

// TrackVisit appends a resource access event for the principal.
func (r *ResourceAccessRepo) TrackVisit(ctx context.Context, principalID string, resource domain.ResourceRef) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO resource_access_events (
			id, principal_id, resource_type, resource_key, display_name, resource_path, section, accessed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, newID(), principalID, resource.ResourceType, resource.ResourceKey, resource.DisplayName, resource.ResourcePath, resource.Section, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("track resource access event: %w", mapDBError(err))
	}
	return nil
}

// ListRecent returns the latest access event per resource for the principal.
func (r *ResourceAccessRepo) ListRecent(ctx context.Context, principalID string, limit int) ([]domain.ResourceAccessEvent, error) {
	rows, err := r.db.QueryContext(ctx, `
		WITH latest_events AS (
			SELECT resource_type, resource_key, display_name, resource_path, section, accessed_at
			FROM (
				SELECT
					resource_type,
					resource_key,
					display_name,
					resource_path,
					section,
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
			e.resource_type,
			e.resource_key,
			e.display_name,
			e.resource_path,
			e.section,
			e.accessed_at
		FROM latest_events e
		ORDER BY e.accessed_at DESC
		LIMIT ?
	`, principalID, limit)
	if err != nil {
		return nil, fmt.Errorf("list recent resources: %w", mapDBError(err))
	}
	defer func() {
		_ = rows.Close()
	}()

	return scanRecentRows(rows)
}

func scanRecentRows(rows *sql.Rows) ([]domain.ResourceAccessEvent, error) {
	var items []domain.ResourceAccessEvent
	for rows.Next() {
		var item domain.ResourceAccessEvent
		var resourcePath sql.NullString
		if err := rows.Scan(
			&item.ResourceType,
			&item.ResourceKey,
			&item.DisplayName,
			&resourcePath,
			&item.Section,
			&item.AccessedAt,
		); err != nil {
			return nil, fmt.Errorf("scan recent resource row: %w", err)
		}
		item.ResourcePath = stringFromNull(resourcePath)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recent resource rows: %w", err)
	}
	return items, nil
}
