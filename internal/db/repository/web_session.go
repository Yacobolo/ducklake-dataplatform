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

type WebSessionRepo struct {
	q *dbstore.Queries
}

func NewWebSessionRepo(db *sql.DB) *WebSessionRepo {
	return &WebSessionRepo{q: dbstore.New(db)}
}

var _ domain.AuthSessionRepository = (*WebSessionRepo)(nil)

func (r *WebSessionRepo) Create(ctx context.Context, session *domain.AuthSession) (*domain.AuthSession, error) {
	row, err := r.q.CreateWebSession(ctx, dbstore.CreateWebSessionParams{
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
	return webSessionFromDB(row), nil
}

func (r *WebSessionRepo) GetActiveByHash(ctx context.Context, sessionHash string) (*domain.AuthSession, error) {
	row, err := r.q.GetActiveWebSessionByHash(ctx, sessionHash)
	if err != nil {
		return nil, mapDBError(err)
	}
	return webSessionFromDB(row), nil
}

func (r *WebSessionRepo) Touch(ctx context.Context, sessionID string, idleExpiresAt time.Time) error {
	return mapDBError(r.q.TouchWebSession(ctx, dbstore.TouchWebSessionParams{
		IdleExpiresAt: idleExpiresAt,
		ID:            sessionID,
	}))
}

func (r *WebSessionRepo) Revoke(ctx context.Context, sessionID string) error {
	return mapDBError(r.q.RevokeWebSession(ctx, sessionID))
}

func (r *WebSessionRepo) RevokeByHash(ctx context.Context, sessionHash string) error {
	return mapDBError(r.q.RevokeWebSessionByHash(ctx, sessionHash))
}

func (r *WebSessionRepo) RevokeAllForPrincipal(ctx context.Context, principalID string) error {
	return mapDBError(r.q.RevokeWebSessionsByPrincipal(ctx, principalID))
}

func (r *WebSessionRepo) DeleteExpiredOrRevoked(ctx context.Context) (int64, error) {
	count, err := r.q.DeleteExpiredOrRevokedWebSessions(ctx)
	if err != nil {
		return 0, mapDBError(err)
	}
	return count, nil
}

func webSessionFromDB(row dbstore.WebSession) *domain.AuthSession {
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
