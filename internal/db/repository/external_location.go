package repository

import (
	"context"
	"database/sql"
	"time"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// Compile-time check.
var _ domain.ExternalLocationRepository = (*ExternalLocationRepo)(nil)

// ExternalLocationRepo implements ExternalLocationRepository.
type ExternalLocationRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

// NewExternalLocationRepo creates a new ExternalLocationRepo.
func NewExternalLocationRepo(db *sql.DB) *ExternalLocationRepo {
	return &ExternalLocationRepo{q: dbstore.New(db), db: db}
}

// Create inserts a new external location into the database.
func (r *ExternalLocationRepo) Create(ctx context.Context, loc *domain.ExternalLocation) (*domain.ExternalLocation, error) {
	row, err := r.q.CreateExternalLocation(ctx, dbstore.CreateExternalLocationParams{
		ID:             newID(),
		Name:           loc.Name,
		Url:            loc.URL,
		CredentialName: loc.CredentialName,
		StorageType:    string(loc.StorageType),
		Comment:        loc.Comment,
		Owner:          loc.Owner,
		ReadOnly:       boolToInt(loc.ReadOnly),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return fromDBLocation(row), nil
}

// GetByID returns an external location by its ID.
func (r *ExternalLocationRepo) GetByID(ctx context.Context, id string) (*domain.ExternalLocation, error) {
	row, err := r.q.GetExternalLocation(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return fromDBLocation(row), nil
}

// GetByName returns an external location by its name.
func (r *ExternalLocationRepo) GetByName(ctx context.Context, name string) (*domain.ExternalLocation, error) {
	row, err := r.q.GetExternalLocationByName(ctx, name)
	if err != nil {
		return nil, mapDBError(err)
	}
	return fromDBLocation(row), nil
}

// List returns a paginated list of external locations.
func (r *ExternalLocationRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.ExternalLocation, int64, error) {
	total, err := r.q.CountExternalLocations(ctx)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListExternalLocations(ctx, dbstore.ListExternalLocationsParams{
		Limit:  int64(page.Limit()),
		Offset: int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	locs := make([]domain.ExternalLocation, 0, len(rows))
	for _, row := range rows {
		locs = append(locs, *fromDBLocation(row))
	}
	return locs, total, nil
}

// Update applies partial updates to an external location by ID.
func (r *ExternalLocationRepo) Update(ctx context.Context, id string, req domain.UpdateExternalLocationRequest) (*domain.ExternalLocation, error) {
	// Fetch current to fill in defaults for COALESCE
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	urlVal := current.URL
	if req.URL != nil {
		urlVal = *req.URL
	}
	credName := current.CredentialName
	if req.CredentialName != nil {
		credName = *req.CredentialName
	}
	comment := current.Comment
	if req.Comment != nil {
		comment = *req.Comment
	}
	owner := current.Owner
	if req.Owner != nil {
		owner = *req.Owner
	}
	readOnly := boolToInt(current.ReadOnly)
	if req.ReadOnly != nil {
		readOnly = boolToInt(*req.ReadOnly)
	}

	err = r.q.UpdateExternalLocation(ctx, dbstore.UpdateExternalLocationParams{
		Url:            urlVal,
		CredentialName: credName,
		Comment:        comment,
		Owner:          owner,
		ReadOnly:       readOnly,
		ID:             id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// Delete removes an external location by ID.
func (r *ExternalLocationRepo) Delete(ctx context.Context, id string) error {
	return mapDBError(r.q.DeleteExternalLocation(ctx, id))
}

func fromDBLocation(row dbstore.ExternalLocation) *domain.ExternalLocation {
	createdAt, _ := time.Parse("2006-01-02 15:04:05", row.CreatedAt)
	updatedAt, _ := time.Parse("2006-01-02 15:04:05", row.UpdatedAt)
	return &domain.ExternalLocation{
		ID:             row.ID,
		Name:           row.Name,
		URL:            row.Url,
		CredentialName: row.CredentialName,
		StorageType:    domain.StorageType(row.StorageType),
		Comment:        row.Comment,
		Owner:          row.Owner,
		ReadOnly:       row.ReadOnly != 0,
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
	}
}
