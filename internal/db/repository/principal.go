package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

type PrincipalRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

func NewPrincipalRepo(db *sql.DB) *PrincipalRepo {
	return &PrincipalRepo{q: dbstore.New(db), db: db}
}

func (r *PrincipalRepo) Create(ctx context.Context, p *domain.Principal) (*domain.Principal, error) {
	row, err := r.q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name:    p.Name,
		Type:    p.Type,
		IsAdmin: boolToInt(p.IsAdmin),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.PrincipalFromDB(row), nil
}

func (r *PrincipalRepo) GetByID(ctx context.Context, id int64) (*domain.Principal, error) {
	row, err := r.q.GetPrincipal(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.PrincipalFromDB(row), nil
}

func (r *PrincipalRepo) GetByName(ctx context.Context, name string) (*domain.Principal, error) {
	row, err := r.q.GetPrincipalByName(ctx, name)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.PrincipalFromDB(row), nil
}

func (r *PrincipalRepo) List(ctx context.Context) ([]domain.Principal, error) {
	rows, err := r.q.ListPrincipals(ctx)
	if err != nil {
		return nil, err
	}
	return mapper.PrincipalsFromDB(rows), nil
}

func (r *PrincipalRepo) Delete(ctx context.Context, id int64) error {
	return r.q.DeletePrincipal(ctx, id)
}

func (r *PrincipalRepo) SetAdmin(ctx context.Context, id int64, isAdmin bool) error {
	return r.q.SetAdmin(ctx, dbstore.SetAdminParams{
		IsAdmin: boolToInt(isAdmin),
		ID:      id,
	})
}
