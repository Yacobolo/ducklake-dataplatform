//nolint:revive
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
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
		return nil, nil, domain.ErrAccessDenied("missing web session token")
	}

	session, err := s.sessions.GetActiveByHash(ctx, hashSessionToken(token))
	if err != nil {
		return nil, nil, domain.ErrAccessDenied("invalid web session")
	}

	now := time.Now()
	if session.RevokedAt != nil || !session.ExpiresAt.After(now) || !session.IdleExpiresAt.After(now) {
		return nil, nil, domain.ErrAccessDenied("expired web session")
	}

	if now.Sub(session.LastSeenAt) >= s.touchInterval {
		newIdle := now.Add(s.idleTTL)
		if err := s.sessions.Touch(ctx, session.ID, newIdle); err != nil {
			return nil, nil, fmt.Errorf("touch web session: %w", err)
		}
		session.LastSeenAt = now
		session.IdleExpiresAt = newIdle
	}

	principal, err := s.principals.GetByID(ctx, session.PrincipalID)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve principal for session: %w", err)
	}

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

	if s.audit != nil {
		_ = s.audit.Insert(ctx, &domain.AuditEntry{PrincipalName: principalName, Action: "AUTH_WEB_SESSION_REVOKE", Status: "ALLOWED"})
	}

	return nil
}

func (s *SessionService) RevokeAll(ctx context.Context, principalID string) error {
	if err := s.sessions.RevokeAllForPrincipal(ctx, strings.TrimSpace(principalID)); err != nil {
		return fmt.Errorf("revoke all web sessions: %w", err)
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
	return count, nil
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
