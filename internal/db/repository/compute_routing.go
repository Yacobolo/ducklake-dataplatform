package repository

import (
	"context"
	"database/sql"
	"fmt"

	"duck-demo/internal/domain"
)

var _ domain.ComputeRoutingRepository = (*ComputeRoutingRepo)(nil)

// ComputeRoutingRepo stores global compute routing defaults.
type ComputeRoutingRepo struct {
	db *sql.DB
}

// NewComputeRoutingRepo creates a new ComputeRoutingRepo.
func NewComputeRoutingRepo(db *sql.DB) *ComputeRoutingRepo {
	return &ComputeRoutingRepo{db: db}
}

// GetDefaults returns the current compute routing defaults.
func (r *ComputeRoutingRepo) GetDefaults(ctx context.Context) (*domain.ComputeRoutingDefaults, error) {
	var defaults domain.ComputeRoutingDefaults
	err := r.db.QueryRowContext(ctx, `
		SELECT interactive_mode, scheduled_mode, notebook_mode
		FROM compute_routing_defaults
		WHERE id = 1
	`).Scan(&defaults.InteractiveMode, &defaults.ScheduledMode, &defaults.NotebookMode)
	if err != nil {
		return nil, mapDBError(err)
	}
	norm := defaults.Normalize()
	return &norm, nil
}

// UpdateDefaults upserts the compute routing defaults row.
func (r *ComputeRoutingRepo) UpdateDefaults(ctx context.Context, defaults domain.ComputeRoutingDefaults) (*domain.ComputeRoutingDefaults, error) {
	norm := defaults.Normalize()
	if err := norm.Validate(); err != nil {
		return nil, err
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO compute_routing_defaults (id, interactive_mode, scheduled_mode, notebook_mode, updated_at)
		VALUES (1, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			interactive_mode = excluded.interactive_mode,
			scheduled_mode = excluded.scheduled_mode,
			notebook_mode = excluded.notebook_mode,
			updated_at = CURRENT_TIMESTAMP
	`, norm.InteractiveMode, norm.ScheduledMode, norm.NotebookMode)
	if err != nil {
		return nil, fmt.Errorf("update compute routing defaults: %w", err)
	}

	return r.GetDefaults(ctx)
}
