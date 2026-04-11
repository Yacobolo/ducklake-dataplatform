//nolint:revive
package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/cuestore"
	"duck-demo/internal/domain"
)

type LocalCredentialRepo struct {
	q *dbstore.Queries
}

func NewLocalCredentialRepo(db *sql.DB) *LocalCredentialRepo {
	return &LocalCredentialRepo{q: dbstore.New(db)}
}

var _ domain.LocalCredentialRepository = (*LocalCredentialRepo)(nil)

func (r *LocalCredentialRepo) Upsert(ctx context.Context, credential *domain.LocalCredential) error {
	return mapDBError(r.q.UpsertLocalCredential(ctx, dbstore.UpsertLocalCredentialParams{
		PrincipalID:        credential.PrincipalID,
		Username:           credential.Username,
		PasswordHash:       credential.PasswordHash,
		MustChangePassword: boolToInt(credential.MustChangePassword),
	}))
}

func (r *LocalCredentialRepo) GetByUsername(ctx context.Context, username string) (*domain.LocalCredential, error) {
	row, err := r.q.GetLocalCredentialByUsername(ctx, username)
	if err != nil {
		return nil, mapDBError(err)
	}
	return localCredentialFromDB(row), nil
}

func (r *LocalCredentialRepo) GetByPrincipalID(ctx context.Context, principalID string) (*domain.LocalCredential, error) {
	row, err := r.q.GetLocalCredentialByPrincipalID(ctx, principalID)
	if err != nil {
		return nil, mapDBError(err)
	}
	return localCredentialFromDB(row), nil
}

func (r *LocalCredentialRepo) Delete(ctx context.Context, principalID string) error {
	return mapDBError(r.q.DeleteLocalCredential(ctx, principalID))
}

func localCredentialFromDB(row dbstore.LocalCredential) *domain.LocalCredential {
	return &domain.LocalCredential{
		PrincipalID:        row.PrincipalID,
		Username:           row.Username,
		PasswordHash:       row.PasswordHash,
		PasswordChangedAt:  row.PasswordChangedAt,
		MustChangePassword: row.MustChangePassword != 0,
		CreatedAt:          row.CreatedAt,
		UpdatedAt:          row.UpdatedAt,
	}
}
