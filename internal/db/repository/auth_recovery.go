//nolint:revive
package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/cuestore"
	"duck-demo/internal/domain"
)

type AuthRecoveryRepo struct {
	q *dbstore.Queries
}

func NewAuthRecoveryRepo(db *sql.DB) *AuthRecoveryRepo {
	return &AuthRecoveryRepo{q: dbstore.New(db)}
}

var _ domain.AuthRecoveryRepository = (*AuthRecoveryRepo)(nil)

func (r *AuthRecoveryRepo) Create(ctx context.Context, code *domain.AuthRecoveryCode) (*domain.AuthRecoveryCode, error) {
	row, err := r.q.CreateAuthRecoveryCode(ctx, dbstore.CreateAuthRecoveryCodeParams{
		ID:          newID(),
		PrincipalID: code.PrincipalID,
		CodeHash:    code.CodeHash,
		ExpiresAt:   code.ExpiresAt,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return authRecoveryCodeFromDB(row), nil
}

func (r *AuthRecoveryRepo) ListByPrincipal(ctx context.Context, principalID string) ([]domain.AuthRecoveryCode, error) {
	rows, err := r.q.ListAuthRecoveryCodesByPrincipal(ctx, principalID)
	if err != nil {
		return nil, mapDBError(err)
	}
	out := make([]domain.AuthRecoveryCode, len(rows))
	for i, row := range rows {
		out[i] = *authRecoveryCodeFromDB(row)
	}
	return out, nil
}

func (r *AuthRecoveryRepo) GetUnusedByHash(ctx context.Context, codeHash string) (*domain.AuthRecoveryCode, error) {
	row, err := r.q.GetUnusedAuthRecoveryCodeByHash(ctx, codeHash)
	if err != nil {
		return nil, mapDBError(err)
	}
	return authRecoveryCodeFromDB(row), nil
}

func (r *AuthRecoveryRepo) MarkUsed(ctx context.Context, id string) error {
	return mapDBError(r.q.MarkAuthRecoveryCodeUsed(ctx, id))
}

func (r *AuthRecoveryRepo) DeleteExpired(ctx context.Context) (int64, error) {
	count, err := r.q.DeleteExpiredAuthRecoveryCodes(ctx)
	if err != nil {
		return 0, mapDBError(err)
	}
	return count, nil
}

func authRecoveryCodeFromDB(row dbstore.AuthRecoveryCode) *domain.AuthRecoveryCode {
	return &domain.AuthRecoveryCode{
		ID:          row.ID,
		PrincipalID: row.PrincipalID,
		CodeHash:    row.CodeHash,
		UsedAt:      ptrFromNullTime(row.UsedAt),
		ExpiresAt:   row.ExpiresAt,
		CreatedAt:   row.CreatedAt,
	}
}
