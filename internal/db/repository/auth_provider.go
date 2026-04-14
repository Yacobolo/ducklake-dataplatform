//nolint:revive
package repository

import (
	"context"
	"database/sql"

	dbstore "github.com/Yacobolo/quackstack/internal/db/dbstore"
	"github.com/Yacobolo/quackstack/internal/db/mapper"
	"github.com/Yacobolo/quackstack/internal/domain"
)

type AuthProviderRepo struct {
	q *dbstore.Queries
}

func NewAuthProviderRepo(db *sql.DB) *AuthProviderRepo {
	return &AuthProviderRepo{q: dbstore.New(db)}
}

var _ domain.AuthProviderRepository = (*AuthProviderRepo)(nil)

func (r *AuthProviderRepo) Get(ctx context.Context) (*domain.AuthProviderConfig, error) {
	row, err := r.q.GetAuthProviderConfig(ctx)
	if err != nil {
		return nil, mapDBError(err)
	}
	return &domain.AuthProviderConfig{
		OIDCEnabled:         row.OidcEnabled != 0,
		OIDCIssuerURL:       ptrFromNullString(row.OidcIssuerUrl),
		OIDCJWKSURL:         ptrFromNullString(row.OidcJwksUrl),
		OIDCAudience:        ptrFromNullString(row.OidcAudience),
		OIDCClientID:        ptrFromNullString(row.OidcClientID),
		OIDCClientSecretEnc: ptrFromNullString(row.OidcClientSecretEnc),
		OIDCScopes:          ptrFromNullString(row.OidcScopes),
		CreatedAt:           row.CreatedAt,
		UpdatedAt:           row.UpdatedAt,
	}, nil
}

func (r *AuthProviderRepo) Upsert(ctx context.Context, cfg *domain.AuthProviderConfig) error {
	return mapDBError(r.q.UpsertAuthProviderConfig(ctx, dbstore.UpsertAuthProviderConfigParams{
		OidcEnabled:         boolToInt(cfg.OIDCEnabled),
		OidcIssuerUrl:       mapper.NullStrFromPtr(cfg.OIDCIssuerURL),
		OidcJwksUrl:         mapper.NullStrFromPtr(cfg.OIDCJWKSURL),
		OidcAudience:        mapper.NullStrFromPtr(cfg.OIDCAudience),
		OidcClientID:        mapper.NullStrFromPtr(cfg.OIDCClientID),
		OidcClientSecretEnc: mapper.NullStrFromPtr(cfg.OIDCClientSecretEnc),
		OidcScopes:          mapper.NullStrFromPtr(cfg.OIDCScopes),
	}))
}
