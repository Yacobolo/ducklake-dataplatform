package repository

import (
	"context"
	"database/sql"
	"fmt"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

// CatalogRegistrationRepo implements domain.CatalogRegistrationRepository
// using the control plane SQLite database via sqlc-generated queries.
type CatalogRegistrationRepo struct {
	db *sql.DB
	q  *dbstore.Queries
}

// NewCatalogRegistrationRepo creates a new CatalogRegistrationRepo.
func NewCatalogRegistrationRepo(db *sql.DB) *CatalogRegistrationRepo {
	return &CatalogRegistrationRepo{db: db, q: dbstore.New(db)}
}

// Compile-time interface check.
var _ domain.CatalogRegistrationRepository = (*CatalogRegistrationRepo)(nil)

// Create persists a new catalog registration.
func (r *CatalogRegistrationRepo) Create(ctx context.Context, reg *domain.CatalogRegistration) (*domain.CatalogRegistration, error) {
	row, err := r.q.CreateCatalog(ctx, dbstore.CreateCatalogParams{
		ID:            newID(),
		Name:          reg.Name,
		MetastoreType: string(reg.MetastoreType),
		Dsn:           reg.DSN,
		DataPath:      reg.DataPath,
		Status:        string(reg.Status),
		StatusMessage: mapper.NullStrFromStr(reg.StatusMessage),
		IsDefault:     boolToInt(reg.IsDefault),
		Comment:       mapper.NullStrFromStr(reg.Comment),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.CatalogRegistrationFromDB(row), nil
}

// GetByID returns a catalog registration by ID.
func (r *CatalogRegistrationRepo) GetByID(ctx context.Context, id string) (*domain.CatalogRegistration, error) {
	row, err := r.q.GetCatalogByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.CatalogRegistrationFromDB(row), nil
}

// GetByName returns a catalog registration by name.
func (r *CatalogRegistrationRepo) GetByName(ctx context.Context, name string) (*domain.CatalogRegistration, error) {
	row, err := r.q.GetCatalogByName(ctx, name)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.CatalogRegistrationFromDB(row), nil
}

// List returns a paginated list of catalog registrations.
func (r *CatalogRegistrationRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.CatalogRegistration, int64, error) {
	total, err := r.q.CountCatalogs(ctx)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListCatalogs(ctx, dbstore.ListCatalogsParams{
		Limit:  int64(page.Limit()),
		Offset: int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	result := make([]domain.CatalogRegistration, len(rows))
	for i, row := range rows {
		result[i] = *mapper.CatalogRegistrationFromDB(row)
	}
	return result, total, nil
}

// Update updates a catalog registration.
func (r *CatalogRegistrationRepo) Update(ctx context.Context, id string, req domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error) {
	// We need to pass proper COALESCE-friendly values
	params := dbstore.UpdateCatalogParams{
		ID: id,
	}
	if req.Comment != nil {
		params.Comment = sql.NullString{String: *req.Comment, Valid: true}
	}
	if req.DataPath != nil {
		params.DataPath = *req.DataPath
	}
	if req.DSN != nil {
		params.Dsn = *req.DSN
	}

	row, err := r.q.UpdateCatalog(ctx, params)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.CatalogRegistrationFromDB(row), nil
}

// Delete removes a catalog registration.
func (r *CatalogRegistrationRepo) Delete(ctx context.Context, id string) error {
	return r.q.DeleteCatalog(ctx, id)
}

// UpdateStatus updates the status and status message of a catalog registration.
func (r *CatalogRegistrationRepo) UpdateStatus(ctx context.Context, id string, status domain.CatalogStatus, message string) error {
	return r.q.UpdateCatalogStatus(ctx, dbstore.UpdateCatalogStatusParams{
		ID:            id,
		Status:        string(status),
		StatusMessage: mapper.NullStrFromStr(message),
	})
}

// GetDefault returns the default catalog registration.
func (r *CatalogRegistrationRepo) GetDefault(ctx context.Context) (*domain.CatalogRegistration, error) {
	row, err := r.q.GetDefaultCatalog(ctx)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.CatalogRegistrationFromDB(row), nil
}

// SetDefault clears the current default and sets the given catalog as default.
// Both operations run in a single transaction to prevent race conditions that
// could result in zero or multiple defaults.
func (r *CatalogRegistrationRepo) SetDefault(ctx context.Context, id string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin set-default tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	qtx := r.q.WithTx(tx)
	if err := qtx.ClearDefaultCatalog(ctx); err != nil {
		return fmt.Errorf("clear default catalog: %w", err)
	}
	if err := qtx.SetDefaultCatalog(ctx, id); err != nil {
		return fmt.Errorf("set default catalog: %w", err)
	}
	return tx.Commit()
}
