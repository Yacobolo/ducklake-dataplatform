//nolint:revive
package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

type WebauthnCredentialRepo struct {
	q *dbstore.Queries
}

func NewWebauthnCredentialRepo(db *sql.DB) *WebauthnCredentialRepo {
	return &WebauthnCredentialRepo{q: dbstore.New(db)}
}

var _ domain.WebAuthnCredentialRepository = (*WebauthnCredentialRepo)(nil)

func (r *WebauthnCredentialRepo) Create(ctx context.Context, credential *domain.WebAuthnCredential) (*domain.WebAuthnCredential, error) {
	row, err := r.q.CreateWebauthnCredential(ctx, dbstore.CreateWebauthnCredentialParams{
		ID:             newID(),
		PrincipalID:    credential.PrincipalID,
		CredentialID:   credential.CredentialID,
		PublicKey:      credential.PublicKey,
		SignCount:      credential.SignCount,
		Transports:     mapper.NullStrFromPtr(credential.Transports),
		BackupEligible: boolToInt(credential.BackupEligible),
		BackupState:    boolToInt(credential.BackupState),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return webauthnCredentialFromDB(row), nil
}

func (r *WebauthnCredentialRepo) ListByPrincipal(ctx context.Context, principalID string) ([]domain.WebAuthnCredential, error) {
	rows, err := r.q.ListWebauthnCredentialsByPrincipal(ctx, principalID)
	if err != nil {
		return nil, mapDBError(err)
	}
	out := make([]domain.WebAuthnCredential, len(rows))
	for i, row := range rows {
		out[i] = *webauthnCredentialFromDB(row)
	}
	return out, nil
}

func (r *WebauthnCredentialRepo) GetByCredentialID(ctx context.Context, credentialID string) (*domain.WebAuthnCredential, error) {
	row, err := r.q.GetWebauthnCredentialByCredentialID(ctx, credentialID)
	if err != nil {
		return nil, mapDBError(err)
	}
	return webauthnCredentialFromDB(row), nil
}

func (r *WebauthnCredentialRepo) UpdateCounter(ctx context.Context, credentialID string, signCount int64) error {
	return mapDBError(r.q.UpdateWebauthnCredentialCounter(ctx, dbstore.UpdateWebauthnCredentialCounterParams{
		SignCount:    signCount,
		CredentialID: credentialID,
	}))
}

func (r *WebauthnCredentialRepo) Delete(ctx context.Context, id string) error {
	return mapDBError(r.q.DeleteWebauthnCredential(ctx, id))
}

func webauthnCredentialFromDB(row dbstore.WebauthnCredential) *domain.WebAuthnCredential {
	return &domain.WebAuthnCredential{
		ID:             row.ID,
		PrincipalID:    row.PrincipalID,
		CredentialID:   row.CredentialID,
		PublicKey:      row.PublicKey,
		SignCount:      row.SignCount,
		Transports:     ptrFromNullString(row.Transports),
		BackupEligible: row.BackupEligible != 0,
		BackupState:    row.BackupState != 0,
		CreatedAt:      row.CreatedAt,
		LastUsedAt:     ptrFromNullTime(row.LastUsedAt),
	}
}
