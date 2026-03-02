//nolint:revive
package repository

import (
	"context"
	"database/sql"
	"time"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

type AuthSessionRepo struct {
	q *dbstore.Queries
}

func NewAuthSessionRepo(db *sql.DB) *AuthSessionRepo {
	return &AuthSessionRepo{q: dbstore.New(db)}
}

var _ domain.AuthSessionRepository = (*AuthSessionRepo)(nil)

func (r *AuthSessionRepo) Create(ctx context.Context, session *domain.AuthSession) (*domain.AuthSession, error) {
	row, err := r.q.CreateAuthSession(ctx, dbstore.CreateAuthSessionParams{
		ID:            newID(),
		PrincipalID:   session.PrincipalID,
		SessionHash:   session.SessionHash,
		AuthMethod:    session.AuthMethod,
		UserAgent:     mapper.NullStrFromPtr(session.UserAgent),
		IpAddress:     mapper.NullStrFromPtr(session.IPAddress),
		ExpiresAt:     session.ExpiresAt,
		IdleExpiresAt: session.IdleExpiresAt,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return authSessionFromDB(row), nil
}

func (r *AuthSessionRepo) GetActiveByHash(ctx context.Context, sessionHash string) (*domain.AuthSession, error) {
	row, err := r.q.GetActiveAuthSessionByHash(ctx, sessionHash)
	if err != nil {
		return nil, mapDBError(err)
	}
	return authSessionFromDB(row), nil
}

func (r *AuthSessionRepo) Touch(ctx context.Context, sessionID string, idleExpiresAt time.Time) error {
	return mapDBError(r.q.TouchAuthSession(ctx, dbstore.TouchAuthSessionParams{
		IdleExpiresAt: idleExpiresAt,
		ID:            sessionID,
	}))
}

func (r *AuthSessionRepo) Revoke(ctx context.Context, sessionID string) error {
	return mapDBError(r.q.RevokeAuthSession(ctx, sessionID))
}

func (r *AuthSessionRepo) RevokeByHash(ctx context.Context, sessionHash string) error {
	return mapDBError(r.q.RevokeAuthSessionByHash(ctx, sessionHash))
}

func (r *AuthSessionRepo) DeleteExpiredOrRevoked(ctx context.Context) (int64, error) {
	count, err := r.q.DeleteExpiredOrRevokedAuthSessions(ctx)
	if err != nil {
		return 0, mapDBError(err)
	}
	return count, nil
}

func authSessionFromDB(row dbstore.AuthSession) *domain.AuthSession {
	return &domain.AuthSession{
		ID:            row.ID,
		PrincipalID:   row.PrincipalID,
		SessionHash:   row.SessionHash,
		AuthMethod:    row.AuthMethod,
		UserAgent:     ptrFromNullString(row.UserAgent),
		IPAddress:     ptrFromNullString(row.IpAddress),
		ExpiresAt:     row.ExpiresAt,
		IdleExpiresAt: row.IdleExpiresAt,
		LastSeenAt:    row.LastSeenAt,
		RevokedAt:     ptrFromNullTime(row.RevokedAt),
		CreatedAt:     row.CreatedAt,
		UpdatedAt:     row.UpdatedAt,
	}
}

func ptrFromNullTime(v sql.NullTime) *time.Time {
	if !v.Valid {
		return nil
	}
	t := v.Time
	return &t
}
