package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/db/catalog"
	"duck-demo/domain"
	"duck-demo/internal/mapper"
)

type GrantRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

func NewGrantRepo(db *sql.DB) *GrantRepo {
	return &GrantRepo{q: dbstore.New(db), db: db}
}

func (r *GrantRepo) Grant(ctx context.Context, g *domain.PrivilegeGrant) (*domain.PrivilegeGrant, error) {
	grantedBy := sql.NullInt64{}
	if g.GrantedBy != nil {
		grantedBy = sql.NullInt64{Int64: *g.GrantedBy, Valid: true}
	}
	row, err := r.q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID:   g.PrincipalID,
		PrincipalType: g.PrincipalType,
		SecurableType: g.SecurableType,
		SecurableID:   g.SecurableID,
		Privilege:     g.Privilege,
		GrantedBy:     grantedBy,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.GrantFromDB(row), nil
}

func (r *GrantRepo) Revoke(ctx context.Context, g *domain.PrivilegeGrant) error {
	return r.q.RevokePrivilege(ctx, dbstore.RevokePrivilegeParams{
		PrincipalID:   g.PrincipalID,
		PrincipalType: g.PrincipalType,
		SecurableType: g.SecurableType,
		SecurableID:   g.SecurableID,
		Privilege:     g.Privilege,
	})
}

func (r *GrantRepo) ListForPrincipal(ctx context.Context, principalID int64, principalType string) ([]domain.PrivilegeGrant, error) {
	rows, err := r.q.ListGrantsForPrincipal(ctx, dbstore.ListGrantsForPrincipalParams{
		PrincipalID:   principalID,
		PrincipalType: principalType,
	})
	if err != nil {
		return nil, err
	}
	return mapper.GrantsFromDB(rows), nil
}

func (r *GrantRepo) ListForSecurable(ctx context.Context, securableType string, securableID int64) ([]domain.PrivilegeGrant, error) {
	rows, err := r.q.ListGrantsForSecurable(ctx, dbstore.ListGrantsForSecurableParams{
		SecurableType: securableType,
		SecurableID:   securableID,
	})
	if err != nil {
		return nil, err
	}
	return mapper.GrantsFromDB(rows), nil
}
