package repository

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// Compile-time check.
var _ domain.VolumeRepository = (*VolumeRepo)(nil)

// VolumeRepo implements VolumeRepository backed by SQLite.
type VolumeRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

// NewVolumeRepo creates a new VolumeRepo.
func NewVolumeRepo(db *sql.DB) *VolumeRepo {
	return &VolumeRepo{q: dbstore.New(db), db: db}
}

// Create inserts a new volume into the database.
func (r *VolumeRepo) Create(ctx context.Context, vol *domain.Volume) (*domain.Volume, error) {
	row, err := r.q.CreateVolume(ctx, dbstore.CreateVolumeParams{
		ID:              newID(),
		Name:            vol.Name,
		SchemaName:      vol.SchemaName,
		CatalogName:     vol.CatalogName,
		VolumeType:      vol.VolumeType,
		StorageLocation: vol.StorageLocation,
		Comment:         vol.Comment,
		Owner:           vol.Owner,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return volumeFromDB(row), nil
}

// GetByName returns a volume by schema and name.
func (r *VolumeRepo) GetByName(ctx context.Context, schemaName, name string) (*domain.Volume, error) {
	row, err := r.q.GetVolumeByName(ctx, dbstore.GetVolumeByNameParams{
		SchemaName: schemaName,
		Name:       name,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return volumeFromDB(row), nil
}

// List returns a paginated list of volumes in a schema.
func (r *VolumeRepo) List(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.Volume, int64, error) {
	total, err := r.q.CountVolumes(ctx, schemaName)
	if err != nil {
		return nil, 0, fmt.Errorf("count volumes: %w", err)
	}

	rows, err := r.q.ListVolumes(ctx, dbstore.ListVolumesParams{
		SchemaName: schemaName,
		Limit:      int64(page.Limit()),
		Offset:     int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("list volumes: %w", err)
	}

	vols := make([]domain.Volume, 0, len(rows))
	for _, row := range rows {
		vols = append(vols, *volumeFromDB(row))
	}
	return vols, total, nil
}

// Update applies partial updates to a volume by ID.
func (r *VolumeRepo) Update(ctx context.Context, id string, req domain.UpdateVolumeRequest) (*domain.Volume, error) {
	// Fetch current to merge fields.
	var current dbstore.Volume
	row := r.db.QueryRowContext(ctx, "SELECT id, name, schema_name, catalog_name, volume_type, storage_location, comment, owner, created_at, updated_at FROM volumes WHERE id = ?", id)
	if err := row.Scan(
		&current.ID, &current.Name, &current.SchemaName, &current.CatalogName,
		&current.VolumeType, &current.StorageLocation, &current.Comment,
		&current.Owner, &current.CreatedAt, &current.UpdatedAt,
	); err != nil {
		return nil, mapDBError(err)
	}

	name := current.Name
	if req.NewName != nil {
		name = *req.NewName
	}
	comment := current.Comment
	if req.Comment != nil {
		comment = *req.Comment
	}
	owner := current.Owner
	if req.Owner != nil {
		owner = *req.Owner
	}

	if err := r.q.UpdateVolume(ctx, dbstore.UpdateVolumeParams{
		Name:    name,
		Comment: comment,
		Owner:   owner,
		ID:      id,
	}); err != nil {
		return nil, mapDBError(err)
	}

	// Re-fetch to return updated state.
	updated, err := r.q.GetVolumeByName(ctx, dbstore.GetVolumeByNameParams{
		SchemaName: current.SchemaName,
		Name:       name,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return volumeFromDB(updated), nil
}

// Delete removes a volume by ID.
func (r *VolumeRepo) Delete(ctx context.Context, id string) error {
	return mapDBError(r.q.DeleteVolume(ctx, id))
}

// volumeFromDB converts a dbstore.Volume row to domain.Volume.
func volumeFromDB(row dbstore.Volume) *domain.Volume {
	createdAt, err := time.Parse("2006-01-02 15:04:05", row.CreatedAt)
	if err != nil {
		slog.Default().Warn("failed to parse volume created_at", "value", row.CreatedAt, "error", err)
	}
	updatedAt, err := time.Parse("2006-01-02 15:04:05", row.UpdatedAt)
	if err != nil {
		slog.Default().Warn("failed to parse volume updated_at", "value", row.UpdatedAt, "error", err)
	}
	return &domain.Volume{
		ID:              row.ID,
		Name:            row.Name,
		SchemaName:      row.SchemaName,
		CatalogName:     row.CatalogName,
		VolumeType:      row.VolumeType,
		StorageLocation: row.StorageLocation,
		Comment:         row.Comment,
		Owner:           row.Owner,
		CreatedAt:       createdAt,
		UpdatedAt:       updatedAt,
	}
}
