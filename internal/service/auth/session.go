//nolint:revive
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"duck-demo/internal/domain"
)

type SessionService struct {
	principals    domain.PrincipalRepository
	sessions      domain.AuthSessionRepository
	audit         domain.AuditRepository
	idleTTL       time.Duration
	absoluteTTL   time.Duration
	touchInterval time.Duration
	createdTotal  atomic.Int64
	resolvedTotal atomic.Int64
	resolveFailed atomic.Int64
	revokedTotal  atomic.Int64
	revokedAll    atomic.Int64
	reapedTotal   atomic.Int64
}

type WebSessionStats struct {
	CreatedTotal       int64 `json:"created_total"`
	ResolvedTotal      int64 `json:"resolved_total"`
	ResolveFailed      int64 `json:"resolve_failed_total"`
	RevokedTotal       int64 `json:"revoked_total"`
	RevokedAllTotal    int64 `json:"revoked_all_total"`
	ReapedTotal        int64 `json:"reaped_total"`
	ActiveSessions     int64 `json:"active_sessions"`
	IdleTTLSeconds     int64 `json:"idle_ttl_seconds"`
	AbsoluteTTLSeconds int64 `json:"absolute_ttl_seconds"`
}

func NewSessionService(
	principals domain.PrincipalRepository,
	sessions domain.AuthSessionRepository,
	audit domain.AuditRepository,
	idleTTL time.Duration,
	absoluteTTL time.Duration,
) *SessionService {
	if idleTTL <= 0 {
		idleTTL = 30 * time.Minute
	}
	if absoluteTTL <= 0 {
		absoluteTTL = 24 * time.Hour
	}
	if absoluteTTL < idleTTL {
		absoluteTTL = idleTTL
	}

	touchInterval := idleTTL / 5
	if touchInterval < time.Minute {
		touchInterval = time.Minute
	}

	return &SessionService{
		principals:    principals,
		sessions:      sessions,
		audit:         audit,
		idleTTL:       idleTTL,
		absoluteTTL:   absoluteTTL,
		touchInterval: touchInterval,
	}
}

func (s *SessionService) CreateForPrincipal(ctx context.Context, principalID, authMethod, userAgent, ipAddress string) (string, *domain.AuthSession, error) {
	token, err := newSessionToken()
	if err != nil {
		return "", nil, fmt.Errorf("generate session token: %w", err)
	}
	now := time.Now()
	session, err := s.sessions.Create(ctx, &domain.AuthSession{
		PrincipalID:   strings.TrimSpace(principalID),
		SessionHash:   hashSessionToken(token),
		AuthMethod:    strings.TrimSpace(authMethod),
		UserAgent:     optionalPtr(userAgent),
		IPAddress:     optionalPtr(ipAddress),
		ExpiresAt:     now.Add(s.absoluteTTL),
		IdleExpiresAt: now.Add(s.idleTTL),
	})
	if err != nil {
		return "", nil, fmt.Errorf("create auth session: %w", err)
	}
	s.createdTotal.Add(1)

	if s.audit != nil {
		name := callerName(ctx)
		if s.principals != nil {
			if p, getErr := s.principals.GetByID(ctx, session.PrincipalID); getErr == nil {
				name = p.Name
			}
		}
		_ = s.audit.Insert(ctx, &domain.AuditEntry{PrincipalName: name, Action: "AUTH_WEB_SESSION_CREATE", Status: "ALLOWED"})
	}

	return token, session, nil
}

func (s *SessionService) Resolve(ctx context.Context, token string) (*domain.Principal, *domain.AuthSession, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		s.resolveFailed.Add(1)
		return nil, nil, domain.ErrAccessDenied("missing web session token")
	}

	session, err := s.sessions.GetActiveByHash(ctx, hashSessionToken(token))
	if err != nil {
		s.resolveFailed.Add(1)
		return nil, nil, domain.ErrAccessDenied("invalid web session")
	}

	now := time.Now()
	touched := false
	if session.RevokedAt != nil || !session.ExpiresAt.After(now) || !session.IdleExpiresAt.After(now) {
		s.resolveFailed.Add(1)
		return nil, nil, domain.ErrAccessDenied("expired web session")
	}

	if now.Sub(session.LastSeenAt) >= s.touchInterval {
		newIdle := now.Add(s.idleTTL)
		if err := s.sessions.Touch(ctx, session.ID, newIdle); err != nil {
			return nil, nil, fmt.Errorf("touch web session: %w", err)
		}
		session.LastSeenAt = now
		session.IdleExpiresAt = newIdle
		touched = true
	}

	principal, err := s.principals.GetByID(ctx, session.PrincipalID)
	if err != nil {
		s.resolveFailed.Add(1)
		return nil, nil, fmt.Errorf("resolve principal for session: %w", err)
	}
	if touched && s.audit != nil {
		_ = s.audit.Insert(ctx, &domain.AuditEntry{PrincipalName: principal.Name, Action: "AUTH_WEB_SESSION_TOUCH", Status: "ALLOWED"})
	}
	s.resolvedTotal.Add(1)

	return principal, session, nil
}

func (s *SessionService) Revoke(ctx context.Context, token string) error {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil
	}

	hash := hashSessionToken(token)
	principalName := callerName(ctx)
	if session, err := s.sessions.GetActiveByHash(ctx, hash); err == nil && s.principals != nil {
		if p, getErr := s.principals.GetByID(ctx, session.PrincipalID); getErr == nil {
			principalName = p.Name
		}
	}

	if err := s.sessions.RevokeByHash(ctx, hash); err != nil {
		return fmt.Errorf("revoke web session: %w", err)
	}
	s.revokedTotal.Add(1)

	if s.audit != nil {
		_ = s.audit.Insert(ctx, &domain.AuditEntry{PrincipalName: principalName, Action: "AUTH_WEB_SESSION_REVOKE", Status: "ALLOWED"})
	}

	return nil
}

func (s *SessionService) RevokeAll(ctx context.Context, principalID string) error {
	if err := s.sessions.RevokeAllForPrincipal(ctx, strings.TrimSpace(principalID)); err != nil {
		return fmt.Errorf("revoke all web sessions: %w", err)
	}
	s.revokedAll.Add(1)
	if s.audit != nil {
		name := strings.TrimSpace(principalID)
		if s.principals != nil {
			if p, err := s.principals.GetByID(ctx, strings.TrimSpace(principalID)); err == nil {
				name = p.Name
			}
		}
		_ = s.audit.Insert(ctx, &domain.AuditEntry{PrincipalName: name, Action: "AUTH_WEB_SESSION_REVOKE_ALL", Status: "ALLOWED"})
	}
	return nil
}

func (s *SessionService) ReapExpired(ctx context.Context) (int64, error) {
	count, err := s.sessions.DeleteExpiredOrRevoked(ctx)
	if err != nil {
		return 0, fmt.Errorf("reap web sessions: %w", err)
	}
	if count > 0 && s.audit != nil {
		_ = s.audit.Insert(ctx, &domain.AuditEntry{PrincipalName: "system", Action: "AUTH_WEB_SESSION_REAP", Status: "ALLOWED"})
	}
	if count > 0 {
		s.reapedTotal.Add(count)
	}
	return count, nil
}

func (s *SessionService) Stats(ctx context.Context) (*WebSessionStats, error) {
	active, err := s.sessions.CountActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("count active web sessions: %w", err)
	}
	return &WebSessionStats{
		CreatedTotal:       s.createdTotal.Load(),
		ResolvedTotal:      s.resolvedTotal.Load(),
		ResolveFailed:      s.resolveFailed.Load(),
		RevokedTotal:       s.revokedTotal.Load(),
		RevokedAllTotal:    s.revokedAll.Load(),
		ReapedTotal:        s.reapedTotal.Load(),
		ActiveSessions:     active,
		IdleTTLSeconds:     int64(s.idleTTL.Seconds()),
		AbsoluteTTLSeconds: int64(s.absoluteTTL.Seconds()),
	}, nil
}

func newSessionToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func hashSessionToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func optionalPtr(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
