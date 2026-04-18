package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
)

var _ domain.ManagedComputeClusterRepository = (*ManagedComputeClusterRepo)(nil)

// ManagedComputeClusterRepo persists provider-backed clusters bound to logical endpoints.
type ManagedComputeClusterRepo struct {
	db *sql.DB
}

// NewManagedComputeClusterRepo creates a new ManagedComputeClusterRepo.
func NewManagedComputeClusterRepo(db *sql.DB) *ManagedComputeClusterRepo {
	return &ManagedComputeClusterRepo{db: db}
}

// Create inserts a new managed compute cluster row.
func (r *ManagedComputeClusterRepo) Create(ctx context.Context, cluster *domain.ManagedComputeCluster) (*domain.ManagedComputeCluster, error) {
	if cluster == nil {
		return nil, fmt.Errorf("cluster is required")
	}

	id := cluster.ID
	if id == "" {
		id = newID()
	}

	externalID := cluster.ExternalID
	if externalID == "" {
		externalID = id
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO managed_compute_clusters (
			id, name, template_id, endpoint_id, provider, external_id,
			desired_state, observed_state, min_replicas, max_replicas,
			is_draining, last_activity_at, endpoint_url
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		id,
		cluster.Name,
		cluster.TemplateID,
		cluster.EndpointID,
		cluster.Provider,
		externalID,
		cluster.DesiredState,
		cluster.ObservedState,
		cluster.MinReplicas,
		cluster.MaxReplicas,
		boolToInt64(cluster.IsDraining),
		nullSQLiteTime(cluster.LastActivityAt),
		nullStrFromPtr(cluster.EndpointURL),
	)
	if err != nil {
		return nil, mapDBError(err)
	}

	return r.GetByID(ctx, id)
}

// GetByID returns a managed compute cluster by ID.
func (r *ManagedComputeClusterRepo) GetByID(ctx context.Context, id string) (*domain.ManagedComputeCluster, error) {
	row := r.db.QueryRowContext(ctx, managedComputeClusterSelect+` WHERE id = ?`, id)
	return scanManagedComputeCluster(row)
}

// GetByName returns a managed compute cluster by name.
func (r *ManagedComputeClusterRepo) GetByName(ctx context.Context, name string) (*domain.ManagedComputeCluster, error) {
	row := r.db.QueryRowContext(ctx, managedComputeClusterSelect+` WHERE name = ?`, name)
	return scanManagedComputeCluster(row)
}

// GetByEndpointID returns the managed compute cluster bound to an endpoint.
func (r *ManagedComputeClusterRepo) GetByEndpointID(ctx context.Context, endpointID string) (*domain.ManagedComputeCluster, error) {
	row := r.db.QueryRowContext(ctx, managedComputeClusterSelect+` WHERE endpoint_id = ?`, endpointID)
	return scanManagedComputeCluster(row)
}

// List returns a paginated list of managed compute clusters.
func (r *ManagedComputeClusterRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.ManagedComputeCluster, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM managed_compute_clusters`).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count managed compute clusters: %w", err)
	}

	rows, err := r.db.QueryContext(ctx, managedComputeClusterSelect+`
		ORDER BY name ASC
		LIMIT ? OFFSET ?
	`, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, fmt.Errorf("list managed compute clusters: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	clusters := make([]domain.ManagedComputeCluster, 0)
	for rows.Next() {
		cluster, err := scanManagedComputeCluster(rows)
		if err != nil {
			return nil, 0, err
		}
		clusters = append(clusters, *cluster)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate managed compute clusters: %w", err)
	}

	return clusters, total, nil
}

// UpdateDesiredState updates the desired cluster state and draining marker.
func (r *ManagedComputeClusterRepo) UpdateDesiredState(ctx context.Context, id string, desiredState string) error {
	result, err := r.db.ExecContext(ctx, `
		UPDATE managed_compute_clusters
		SET desired_state = ?, is_draining = ?, updated_at = datetime('now')
		WHERE id = ?
	`,
		desiredState,
		boolToInt64(desiredState == domain.ManagedClusterDesiredDraining),
		id,
	)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("managed compute cluster rows affected: %w", err)
	}
	if rows == 0 {
		return domain.ErrNotFound("managed compute cluster %s not found", id)
	}
	return nil
}

// UpdateObservedState updates the observed cluster state and connection snapshot.
func (r *ManagedComputeClusterRepo) UpdateObservedState(ctx context.Context, id string, observedState string, endpointURL *string, lastActivityAt *time.Time) error {
	result, err := r.db.ExecContext(ctx, `
		UPDATE managed_compute_clusters
		SET observed_state = ?, endpoint_url = ?, last_activity_at = ?, updated_at = datetime('now')
		WHERE id = ?
	`,
		observedState,
		nullStrFromPtr(endpointURL),
		nullSQLiteTime(lastActivityAt),
		id,
	)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("managed compute cluster rows affected: %w", err)
	}
	if rows == 0 {
		return domain.ErrNotFound("managed compute cluster %s not found", id)
	}
	return nil
}

// Delete removes a managed compute cluster by ID.
func (r *ManagedComputeClusterRepo) Delete(ctx context.Context, id string) error {
	result, err := r.db.ExecContext(ctx, `DELETE FROM managed_compute_clusters WHERE id = ?`, id)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("managed compute cluster rows affected: %w", err)
	}
	if rows == 0 {
		return domain.ErrNotFound("managed compute cluster %s not found", id)
	}
	return nil
}

const managedComputeClusterSelect = `
	SELECT
		id, name, template_id, endpoint_id, provider, external_id,
		desired_state, observed_state, min_replicas, max_replicas,
		is_draining, last_activity_at, endpoint_url, created_at, updated_at
	FROM managed_compute_clusters
`

type managedComputeClusterScanner interface {
	Scan(dest ...any) error
}

func scanManagedComputeCluster(scanner managedComputeClusterScanner) (*domain.ManagedComputeCluster, error) {
	var cluster domain.ManagedComputeCluster
	var isDraining int64
	var endpointURL sql.NullString
	var lastActivityRaw sql.NullString
	var createdAtRaw string
	var updatedAtRaw string

	if err := scanner.Scan(
		&cluster.ID,
		&cluster.Name,
		&cluster.TemplateID,
		&cluster.EndpointID,
		&cluster.Provider,
		&cluster.ExternalID,
		&cluster.DesiredState,
		&cluster.ObservedState,
		&cluster.MinReplicas,
		&cluster.MaxReplicas,
		&isDraining,
		&lastActivityRaw,
		&endpointURL,
		&createdAtRaw,
		&updatedAtRaw,
	); err != nil {
		return nil, mapDBError(err)
	}

	cluster.IsDraining = isDraining != 0
	if endpointURL.Valid {
		cluster.EndpointURL = &endpointURL.String
	}
	if lastActivityRaw.Valid {
		t := parseManagedComputeTime(lastActivityRaw.String)
		cluster.LastActivityAt = &t
	}
	cluster.CreatedAt = parseManagedComputeTime(createdAtRaw)
	cluster.UpdatedAt = parseManagedComputeTime(updatedAtRaw)

	return &cluster, nil
}

func nullSQLiteTime(value *time.Time) sql.NullString {
	if value == nil || value.IsZero() {
		return sql.NullString{}
	}
	return sql.NullString{
		String: value.UTC().Format(time.DateTime),
		Valid:  true,
	}
}
