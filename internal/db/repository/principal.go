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

func (r *PrincipalRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.Principal, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM principals`).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.db.QueryContext(ctx,
		`SELECT id, name, type, is_admin, created_at FROM principals ORDER BY id LIMIT ? OFFSET ?`,
		page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var principals []domain.Principal
	for rows.Next() {
		var p dbstore.Principal
		if err := rows.Scan(&p.ID, &p.Name, &p.Type, &p.IsAdmin, &p.CreatedAt); err != nil {
			return nil, 0, err
		}
		principals = append(principals, *mapper.PrincipalFromDB(p))
	}
	return principals, total, rows.Err()
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
