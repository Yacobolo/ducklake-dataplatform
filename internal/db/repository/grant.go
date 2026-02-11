package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

type GrantRepo struct {
	q *dbstore.Queries
}

func NewGrantRepo(db *sql.DB) *GrantRepo {
	return &GrantRepo{q: dbstore.New(db)}
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

func (r *GrantRepo) ListForPrincipal(ctx context.Context, principalID int64, principalType string, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
	total, err := r.q.CountGrantsForPrincipal(ctx, dbstore.CountGrantsForPrincipalParams{
		PrincipalID:   principalID,
		PrincipalType: principalType,
	})
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListGrantsForPrincipalPaginated(ctx, dbstore.ListGrantsForPrincipalPaginatedParams{
		PrincipalID:   principalID,
		PrincipalType: principalType,
		Limit:         int64(page.Limit()),
		Offset:        int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	return mapper.GrantsFromDB(rows), total, nil
}

func (r *GrantRepo) ListForSecurable(ctx context.Context, securableType string, securableID int64, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
	total, err := r.q.CountGrantsForSecurable(ctx, dbstore.CountGrantsForSecurableParams{
		SecurableType: securableType,
		SecurableID:   securableID,
	})
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListGrantsForSecurablePaginated(ctx, dbstore.ListGrantsForSecurablePaginatedParams{
		SecurableType: securableType,
		SecurableID:   securableID,
		Limit:         int64(page.Limit()),
		Offset:        int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	return mapper.GrantsFromDB(rows), total, nil
}

func (r *GrantRepo) HasPrivilege(ctx context.Context, principalID int64, principalType, securableType string, securableID int64, privilege string) (bool, error) {
	cnt, err := r.q.CheckDirectGrantAny(ctx, dbstore.CheckDirectGrantAnyParams{
		PrincipalID:   principalID,
		PrincipalType: principalType,
		SecurableType: securableType,
		SecurableID:   securableID,
		Privilege:     privilege,
	})
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}
