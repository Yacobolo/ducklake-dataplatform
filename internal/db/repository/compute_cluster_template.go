package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
)

var _ domain.ComputeClusterTemplateRepository = (*ComputeClusterTemplateRepo)(nil)

// ComputeClusterTemplateRepo persists provider-neutral managed compute templates.
type ComputeClusterTemplateRepo struct {
	db *sql.DB
}

// NewComputeClusterTemplateRepo creates a new ComputeClusterTemplateRepo.
func NewComputeClusterTemplateRepo(db *sql.DB) *ComputeClusterTemplateRepo {
	return &ComputeClusterTemplateRepo{db: db}
}

// Create inserts a new managed compute template.
func (r *ComputeClusterTemplateRepo) Create(ctx context.Context, tpl *domain.ComputeClusterTemplate) (*domain.ComputeClusterTemplate, error) {
	if tpl == nil {
		return nil, fmt.Errorf("template is required")
	}

	id := tpl.ID
	if id == "" {
		id = newID()
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO compute_cluster_templates (
			id, name, provider, workload_class, size,
			min_replicas, max_replicas, idle_auto_stop_seconds,
			scaling_policy, storage_profile, result_retention_seconds
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		id,
		tpl.Name,
		tpl.Provider,
		tpl.WorkloadClass,
		tpl.Size,
		tpl.MinReplicas,
		tpl.MaxReplicas,
		tpl.IdleAutoStopSeconds,
		tpl.ScalingPolicy,
		tpl.StorageProfile,
		tpl.ResultRetentionSeconds,
	)
	if err != nil {
		return nil, mapDBError(err)
	}

	return r.GetByID(ctx, id)
}

// GetByID returns a managed compute template by ID.
func (r *ComputeClusterTemplateRepo) GetByID(ctx context.Context, id string) (*domain.ComputeClusterTemplate, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT
			id, name, provider, workload_class, size,
			min_replicas, max_replicas, idle_auto_stop_seconds,
			scaling_policy, storage_profile, result_retention_seconds,
			created_at, updated_at
		FROM compute_cluster_templates
		WHERE id = ?
	`, id)
	return scanComputeClusterTemplate(row)
}

// GetByName returns a managed compute template by name.
func (r *ComputeClusterTemplateRepo) GetByName(ctx context.Context, name string) (*domain.ComputeClusterTemplate, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT
			id, name, provider, workload_class, size,
			min_replicas, max_replicas, idle_auto_stop_seconds,
			scaling_policy, storage_profile, result_retention_seconds,
			created_at, updated_at
		FROM compute_cluster_templates
		WHERE name = ?
	`, name)
	return scanComputeClusterTemplate(row)
}

// List returns a paginated list of managed compute templates.
func (r *ComputeClusterTemplateRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.ComputeClusterTemplate, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM compute_cluster_templates`).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count compute cluster templates: %w", err)
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT
			id, name, provider, workload_class, size,
			min_replicas, max_replicas, idle_auto_stop_seconds,
			scaling_policy, storage_profile, result_retention_seconds,
			created_at, updated_at
		FROM compute_cluster_templates
		ORDER BY name ASC
		LIMIT ? OFFSET ?
	`, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, fmt.Errorf("list compute cluster templates: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	templates := make([]domain.ComputeClusterTemplate, 0)
	for rows.Next() {
		tpl, err := scanComputeClusterTemplate(rows)
		if err != nil {
			return nil, 0, err
		}
		templates = append(templates, *tpl)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate compute cluster templates: %w", err)
	}

	return templates, total, nil
}

// Delete removes a managed compute template by ID.
func (r *ComputeClusterTemplateRepo) Delete(ctx context.Context, id string) error {
	result, err := r.db.ExecContext(ctx, `DELETE FROM compute_cluster_templates WHERE id = ?`, id)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("compute cluster template rows affected: %w", err)
	}
	if rows == 0 {
		return domain.ErrNotFound("compute cluster template %s not found", id)
	}
	return nil
}

type computeClusterTemplateScanner interface {
	Scan(dest ...any) error
}

func scanComputeClusterTemplate(scanner computeClusterTemplateScanner) (*domain.ComputeClusterTemplate, error) {
	var tpl domain.ComputeClusterTemplate
	var createdAtRaw string
	var updatedAtRaw string

	if err := scanner.Scan(
		&tpl.ID,
		&tpl.Name,
		&tpl.Provider,
		&tpl.WorkloadClass,
		&tpl.Size,
		&tpl.MinReplicas,
		&tpl.MaxReplicas,
		&tpl.IdleAutoStopSeconds,
		&tpl.ScalingPolicy,
		&tpl.StorageProfile,
		&tpl.ResultRetentionSeconds,
		&createdAtRaw,
		&updatedAtRaw,
	); err != nil {
		return nil, mapDBError(err)
	}

	tpl.CreatedAt = parseManagedComputeTime(createdAtRaw)
	tpl.UpdatedAt = parseManagedComputeTime(updatedAtRaw)
	return &tpl, nil
}

func parseManagedComputeTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.DateTime, raw)
	if err == nil {
		return parsed
	}
	parsed, err = time.Parse(time.RFC3339Nano, raw)
	if err == nil {
		return parsed
	}
	return time.Time{}
}
