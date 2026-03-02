//nolint:revive
package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

type AuthIdentityRepo struct {
	q *dbstore.Queries
}

func NewAuthIdentityRepo(db *sql.DB) *AuthIdentityRepo {
	return &AuthIdentityRepo{q: dbstore.New(db)}
}

var _ domain.AuthIdentityRepository = (*AuthIdentityRepo)(nil)

func (r *AuthIdentityRepo) Create(ctx context.Context, identity *domain.AuthIdentity) (*domain.AuthIdentity, error) {
	row, err := r.q.CreateAuthIdentity(ctx, dbstore.CreateAuthIdentityParams{
		ID:            newID(),
		PrincipalID:   identity.PrincipalID,
		Provider:      identity.Provider,
		Issuer:        mapper.NullStrFromPtr(identity.Issuer),
		Subject:       identity.Subject,
		Email:         mapper.NullStrFromPtr(identity.Email),
		EmailVerified: boolToInt(identity.EmailVerified),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return authIdentityFromDB(row), nil
}

func (r *AuthIdentityRepo) GetByProviderSubject(ctx context.Context, provider string, issuer *string, subject string) (*domain.AuthIdentity, error) {
	row, err := r.q.GetAuthIdentityByProviderSubject(ctx, dbstore.GetAuthIdentityByProviderSubjectParams{
		Provider: provider,
		Issuer:   mapper.NullStrFromPtr(issuer),
		Subject:  subject,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return authIdentityFromDB(row), nil
}

func (r *AuthIdentityRepo) ListByPrincipal(ctx context.Context, principalID string) ([]domain.AuthIdentity, error) {
	rows, err := r.q.ListAuthIdentitiesByPrincipal(ctx, principalID)
	if err != nil {
		return nil, mapDBError(err)
	}
	out := make([]domain.AuthIdentity, len(rows))
	for i, row := range rows {
		out[i] = *authIdentityFromDB(row)
	}
	return out, nil
}

func (r *AuthIdentityRepo) Delete(ctx context.Context, id string) error {
	return mapDBError(r.q.DeleteAuthIdentity(ctx, id))
}

func authIdentityFromDB(row dbstore.AuthIdentity) *domain.AuthIdentity {
	return &domain.AuthIdentity{
		ID:            row.ID,
		PrincipalID:   row.PrincipalID,
		Provider:      row.Provider,
		Issuer:        ptrFromNullString(row.Issuer),
		Subject:       row.Subject,
		Email:         ptrFromNullString(row.Email),
		EmailVerified: row.EmailVerified != 0,
		CreatedAt:     row.CreatedAt,
		UpdatedAt:     row.UpdatedAt,
	}
}

func ptrFromNullString(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	s := v.String
	return &s
}
