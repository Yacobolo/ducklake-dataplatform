package auth

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestSessionService_CreateResolveRevoke(t *testing.T) {
	principals := newStubPrincipalRepo()
	p, err := principals.Create(context.Background(), &domain.Principal{Name: "alice", Type: "user", IsAdmin: true})
	require.NoError(t, err)

	repo := newInMemorySessionRepo()
	svc := NewSessionService(principals, repo, &stubAuditRepo{}, 5*time.Minute, time.Hour)

	token, session, err := svc.CreateForPrincipal(context.Background(), p.ID, "local", "ua", "127.0.0.1")
	require.NoError(t, err)
	assert.NotEmpty(t, token)
	require.NotNil(t, session)

	resolvedPrincipal, resolvedSession, err := svc.Resolve(context.Background(), token)
	require.NoError(t, err)
	assert.Equal(t, p.ID, resolvedPrincipal.ID)
	assert.Equal(t, session.ID, resolvedSession.ID)

	require.NoError(t, svc.Revoke(context.Background(), token))
	_, _, err = svc.Resolve(context.Background(), token)
	require.Error(t, err)
}

func TestSessionService_ReapExpired(t *testing.T) {
	principals := newStubPrincipalRepo()
	p, err := principals.Create(context.Background(), &domain.Principal{Name: "bob", Type: "user"})
	require.NoError(t, err)

	repo := newInMemorySessionRepo()
	svc := NewSessionService(principals, repo, &stubAuditRepo{}, 2*time.Minute, 3*time.Minute)

	_, session, err := svc.CreateForPrincipal(context.Background(), p.ID, "local", "ua", "127.0.0.1")
	require.NoError(t, err)

	repo.sessionsByID[session.ID].ExpiresAt = time.Now().Add(-time.Minute)
	count, err := svc.ReapExpired(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

type inMemorySessionRepo struct {
	sessionsByID   map[string]*domain.AuthSession
	sessionsByHash map[string]*domain.AuthSession
	nextID         int
}

func newInMemorySessionRepo() *inMemorySessionRepo {
	return &inMemorySessionRepo{
		sessionsByID:   map[string]*domain.AuthSession{},
		sessionsByHash: map[string]*domain.AuthSession{},
		nextID:         1,
	}
}

func (r *inMemorySessionRepo) Create(_ context.Context, session *domain.AuthSession) (*domain.AuthSession, error) {
	cp := *session
	cp.ID = fmt.Sprintf("s-%d", r.nextID)
	r.nextID++
	now := time.Now()
	cp.CreatedAt = now
	cp.UpdatedAt = now
	cp.LastSeenAt = now
	r.sessionsByID[cp.ID] = &cp
	r.sessionsByHash[cp.SessionHash] = &cp
	return &cp, nil
}

func (r *inMemorySessionRepo) GetActiveByHash(_ context.Context, sessionHash string) (*domain.AuthSession, error) {
	s, ok := r.sessionsByHash[sessionHash]
	if !ok {
		return nil, domain.ErrNotFound("session not found")
	}
	if s.RevokedAt != nil || !s.ExpiresAt.After(time.Now()) || !s.IdleExpiresAt.After(time.Now()) {
		return nil, domain.ErrNotFound("session inactive")
	}
	cp := *s
	return &cp, nil
}

func (r *inMemorySessionRepo) Touch(_ context.Context, sessionID string, idleExpiresAt time.Time) error {
	s, ok := r.sessionsByID[sessionID]
	if !ok {
		return domain.ErrNotFound("session not found")
	}
	s.IdleExpiresAt = idleExpiresAt
	s.LastSeenAt = time.Now()
	return nil
}

func (r *inMemorySessionRepo) Revoke(_ context.Context, sessionID string) error {
	s, ok := r.sessionsByID[sessionID]
	if !ok {
		return domain.ErrNotFound("session not found")
	}
	now := time.Now()
	s.RevokedAt = &now
	return nil
}

func (r *inMemorySessionRepo) RevokeByHash(_ context.Context, sessionHash string) error {
	s, ok := r.sessionsByHash[sessionHash]
	if !ok {
		return domain.ErrNotFound("session not found")
	}
	now := time.Now()
	s.RevokedAt = &now
	return nil
}

func (r *inMemorySessionRepo) RevokeAllForPrincipal(_ context.Context, principalID string) error {
	now := time.Now()
	for _, s := range r.sessionsByID {
		if s.PrincipalID == principalID {
			s.RevokedAt = &now
		}
	}
	return nil
}

func (r *inMemorySessionRepo) DeleteExpiredOrRevoked(_ context.Context) (int64, error) {
	var deleted int64
	now := time.Now()
	for id, s := range r.sessionsByID {
		if s.RevokedAt != nil || !s.ExpiresAt.After(now) || !s.IdleExpiresAt.After(now) {
			delete(r.sessionsByHash, s.SessionHash)
			delete(r.sessionsByID, id)
			deleted++
		}
	}
	return deleted, nil
}
