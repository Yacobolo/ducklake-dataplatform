package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
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

func (r *GrantRepo) ListForPrincipal(ctx context.Context, principalID int64, principalType string, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM privilege_grants WHERE principal_id = ? AND principal_type = ?`,
		principalID, principalType).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.db.QueryContext(ctx,
		`SELECT id, principal_id, principal_type, securable_type, securable_id, privilege, granted_by, granted_at
		FROM privilege_grants WHERE principal_id = ? AND principal_type = ? ORDER BY id LIMIT ? OFFSET ?`,
		principalID, principalType, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var grants []domain.PrivilegeGrant
	for rows.Next() {
		var g dbstore.PrivilegeGrant
		if err := rows.Scan(&g.ID, &g.PrincipalID, &g.PrincipalType, &g.SecurableType, &g.SecurableID, &g.Privilege, &g.GrantedBy, &g.GrantedAt); err != nil {
			return nil, 0, err
		}
		grants = append(grants, *mapper.GrantFromDB(g))
	}
	return grants, total, rows.Err()
}

func (r *GrantRepo) ListForSecurable(ctx context.Context, securableType string, securableID int64, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM privilege_grants WHERE securable_type = ? AND securable_id = ?`,
		securableType, securableID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.db.QueryContext(ctx,
		`SELECT id, principal_id, principal_type, securable_type, securable_id, privilege, granted_by, granted_at
		FROM privilege_grants WHERE securable_type = ? AND securable_id = ? ORDER BY id LIMIT ? OFFSET ?`,
		securableType, securableID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var grants []domain.PrivilegeGrant
	for rows.Next() {
		var g dbstore.PrivilegeGrant
		if err := rows.Scan(&g.ID, &g.PrincipalID, &g.PrincipalType, &g.SecurableType, &g.SecurableID, &g.Privilege, &g.GrantedBy, &g.GrantedAt); err != nil {
			return nil, 0, err
		}
		grants = append(grants, *mapper.GrantFromDB(g))
	}
	return grants, total, rows.Err()
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
