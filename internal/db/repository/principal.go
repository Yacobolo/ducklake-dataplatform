package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

// PrincipalRepo implements domain.PrincipalRepository using SQLite.
type PrincipalRepo struct {
	q *dbstore.Queries
}

// NewPrincipalRepo creates a new PrincipalRepo.
func NewPrincipalRepo(db *sql.DB) *PrincipalRepo {
	return &PrincipalRepo{q: dbstore.New(db)}
}

// Create inserts a new principal into the database.
func (r *PrincipalRepo) Create(ctx context.Context, p *domain.Principal) (*domain.Principal, error) {
	if p.ExternalID != nil {
		row, err := r.q.CreatePrincipalWithExternalID(ctx, dbstore.CreatePrincipalWithExternalIDParams{
			ID:             newID(),
			Name:           p.Name,
			Type:           p.Type,
			IsAdmin:        boolToInt(p.IsAdmin),
			ExternalID:     mapper.NullStrFromPtr(p.ExternalID),
			ExternalIssuer: mapper.NullStrFromPtr(p.ExternalIssuer),
		})
		if err != nil {
			return nil, mapDBError(err)
		}
		return mapper.PrincipalFromDB(row), nil
	}
	row, err := r.q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		ID:      newID(),
		Name:    p.Name,
		Type:    p.Type,
		IsAdmin: boolToInt(p.IsAdmin),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.PrincipalFromDB(row), nil
}

// GetByID returns a principal by its ID.
func (r *PrincipalRepo) GetByID(ctx context.Context, id string) (*domain.Principal, error) {
	row, err := r.q.GetPrincipal(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.PrincipalFromDB(row), nil
}

// GetByName returns a principal by its name.
func (r *PrincipalRepo) GetByName(ctx context.Context, name string) (*domain.Principal, error) {
	row, err := r.q.GetPrincipalByName(ctx, name)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.PrincipalFromDB(row), nil
}

// GetByExternalID returns a principal by its external identity provider mapping.
func (r *PrincipalRepo) GetByExternalID(ctx context.Context, issuer, externalID string) (*domain.Principal, error) {
	row, err := r.q.GetPrincipalByExternalID(ctx, dbstore.GetPrincipalByExternalIDParams{
		ExternalIssuer: mapper.NullStrFromStr(issuer),
		ExternalID:     mapper.NullStrFromStr(externalID),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.PrincipalFromDB(row), nil
}

// List returns a paginated list of principals.
func (r *PrincipalRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.Principal, int64, error) {
	total, err := r.q.CountPrincipals(ctx)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListPrincipalsPaginated(ctx, dbstore.ListPrincipalsPaginatedParams{
		Limit:  int64(page.Limit()),
		Offset: int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	return mapper.PrincipalsFromDB(rows), total, nil
}

// Delete removes a principal by ID.
func (r *PrincipalRepo) Delete(ctx context.Context, id string) error {
	return r.q.DeletePrincipal(ctx, id)
}

// SetAdmin updates the admin status of a principal.
func (r *PrincipalRepo) SetAdmin(ctx context.Context, id string, isAdmin bool) error {
	return r.q.SetAdmin(ctx, dbstore.SetAdminParams{
		IsAdmin: boolToInt(isAdmin),
		ID:      id,
	})
}

// BindExternalID sets the external identity on an existing principal.
func (r *PrincipalRepo) BindExternalID(ctx context.Context, id string, externalID string, externalIssuer string) error {
	return r.q.BindExternalID(ctx, dbstore.BindExternalIDParams{
		ExternalID:     mapper.NullStrFromStr(externalID),
		ExternalIssuer: mapper.NullStrFromStr(externalIssuer),
		ID:             id,
	})
}
