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

	"github.com/golang-jwt/jwt/v5"

	"duck-demo/internal/domain"
)

type Service struct {
	principals    domain.PrincipalRepository
	credentials   domain.LocalCredentialRepository
	loginAttempts domain.AuthLoginAttemptRepository
	setupState    domain.SetupStateRepository
	providers     domain.AuthProviderRepository
	audit         domain.AuditRepository
	jwtSecret     string
	passwordParms argon2Params
}

func NewService(
	principals domain.PrincipalRepository,
	credentials domain.LocalCredentialRepository,
	loginAttempts domain.AuthLoginAttemptRepository,
	setupState domain.SetupStateRepository,
	providers domain.AuthProviderRepository,
	audit domain.AuditRepository,
	jwtSecret string,
) *Service {
	return &Service{
		principals:    principals,
		credentials:   credentials,
		loginAttempts: loginAttempts,
		setupState:    setupState,
		providers:     providers,
		audit:         audit,
		jwtSecret:     jwtSecret,
		passwordParms: defaultArgon2Params,
	}
}

type BootstrapRequest struct {
	Username       string
	Password       string
	PrincipalName  string
	BootstrapToken string
}

type LoginResult struct {
	Token     string
	Principal *domain.Principal
}

func (s *Service) Bootstrap(ctx context.Context, req BootstrapRequest) (*LoginResult, error) {
	username := strings.ToLower(strings.TrimSpace(req.Username))
	principalName := strings.ToLower(strings.TrimSpace(req.PrincipalName))
	if username == "" {
		return nil, domain.ErrValidation("username is required")
	}
	if len(req.Password) < 12 {
		return nil, domain.ErrValidation("password must be at least 12 characters")
	}
	if principalName == "" {
		principalName = username
	}

	state, err := s.setupState.Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("load setup state: %w", err)
	}

	_, total, err := s.principals.List(ctx, domain.PageRequest{MaxResults: 1})
	if err != nil {
		return nil, fmt.Errorf("check principals: %w", err)
	}

	requireToken := state.SetupCompleted || total > 0
	if requireToken {
		if !s.validateBootstrapToken(state, req.BootstrapToken) {
			return nil, domain.ErrAccessDenied("valid bootstrap token required")
		}
		if err := s.setupState.ClearBootstrapToken(ctx); err != nil {
			return nil, fmt.Errorf("consume bootstrap token: %w", err)
		}
	}

	ph, err := hashPassword(req.Password, s.passwordParms)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	p, err := s.principals.Create(ctx, &domain.Principal{
		Name:    principalName,
		Type:    "user",
		IsAdmin: true,
	})
	if err != nil {
		return nil, err
	}

	err = s.credentials.Upsert(ctx, &domain.LocalCredential{
		PrincipalID:        p.ID,
		Username:           username,
		PasswordHash:       ph,
		MustChangePassword: false,
	})
	if err != nil {
		return nil, err
	}

	_ = s.setupState.Complete(ctx, p.ID)

	token, err := s.issueJWT(p)
	if err != nil {
		return nil, err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: p.Name,
		Action:        "AUTH_BOOTSTRAP_COMPLETE",
		Status:        "ALLOWED",
	})

	return &LoginResult{Token: token, Principal: p}, nil
}

func (s *Service) Login(ctx context.Context, username, password, ipAddress string) (*LoginResult, error) {
	p, err := s.AuthenticateLocal(ctx, username, password, ipAddress)
	if err != nil {
		return nil, err
	}

	token, err := s.issueJWT(p)
	if err != nil {
		return nil, err
	}

	return &LoginResult{Token: token, Principal: p}, nil
}

func (s *Service) AuthenticateLocal(ctx context.Context, username, password, ipAddress string) (*domain.Principal, error) {
	username = strings.ToLower(strings.TrimSpace(username))
	if username == "" || password == "" {
		return nil, domain.ErrValidation("username and password are required")
	}

	if s.isRateLimited(ctx, username, ipAddress) {
		reason := "rate_limited"
		s.recordLoginAttempt(ctx, username, ipAddress, false, &reason)
		return nil, domain.ErrAccessDenied("too many failed login attempts")
	}

	cred, err := s.credentials.GetByUsername(ctx, username)
	if err != nil {
		reason := "unknown_user"
		s.recordLoginAttempt(ctx, username, ipAddress, false, &reason)
		return nil, domain.ErrAccessDenied("invalid username or password")
	}

	ok, err := verifyPassword(password, cred.PasswordHash)
	if err != nil || !ok {
		reason := "bad_password"
		s.recordLoginAttempt(ctx, username, ipAddress, false, &reason)
		return nil, domain.ErrAccessDenied("invalid username or password")
	}

	p, err := s.principals.GetByID(ctx, cred.PrincipalID)
	if err != nil {
		return nil, err
	}
	s.recordLoginAttempt(ctx, username, ipAddress, true, nil)

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: p.Name,
		Action:        "AUTH_LOCAL_LOGIN",
		Status:        "ALLOWED",
	})

	return p, nil
}

func (s *Service) isRateLimited(ctx context.Context, username, ipAddress string) bool {
	if s.loginAttempts == nil {
		return false
	}
	windowStart := time.Now().Add(-15 * time.Minute)
	if username != "" {
		if count, err := s.loginAttempts.CountRecentFailedByUsername(ctx, username, windowStart); err == nil && count >= 5 {
			return true
		}
	}
	if strings.TrimSpace(ipAddress) != "" {
		if count, err := s.loginAttempts.CountRecentFailedByIP(ctx, ipAddress, windowStart); err == nil && count >= 20 {
			return true
		}
	}
	return false
}

func (s *Service) recordLoginAttempt(ctx context.Context, username, ipAddress string, success bool, reason *string) {
	if s.loginAttempts == nil {
		return
	}
	var usernamePtr *string
	if strings.TrimSpace(username) != "" {
		u := strings.TrimSpace(username)
		usernamePtr = &u
	}
	var ipPtr *string
	if strings.TrimSpace(ipAddress) != "" {
		ip := strings.TrimSpace(ipAddress)
		ipPtr = &ip
	}
	_ = s.loginAttempts.Insert(ctx, &domain.AuthLoginAttempt{
		Username:  usernamePtr,
		IPAddress: ipPtr,
		Success:   success,
		Reason:    reason,
	})
}

func (s *Service) CreateBootstrapToken(ctx context.Context, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate bootstrap token: %w", err)
	}
	token := hex.EncodeToString(raw)
	hash := sha256.Sum256([]byte(token))
	if err := s.setupState.SetBootstrapToken(ctx, hex.EncodeToString(hash[:]), time.Now().Add(ttl)); err != nil {
		return "", err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: callerName(ctx),
		Action:        "AUTH_BOOTSTRAP_TOKEN_CREATE",
		Status:        "ALLOWED",
	})
	return token, nil
}

func (s *Service) GetOIDCProvider(ctx context.Context) (*domain.AuthProviderConfig, error) {
	return s.providers.Get(ctx)
}

func (s *Service) UpsertOIDCProvider(ctx context.Context, cfg *domain.AuthProviderConfig) error {
	if cfg == nil {
		return domain.ErrValidation("provider config is required")
	}
	if cfg.OIDCEnabled {
		if cfg.OIDCIssuerURL == nil || strings.TrimSpace(*cfg.OIDCIssuerURL) == "" {
			return domain.ErrValidation("oidc issuer_url is required when oidc is enabled")
		}
	}
	return s.providers.Upsert(ctx, cfg)
}

func (s *Service) validateBootstrapToken(state *domain.SetupState, token string) bool {
	if state == nil || state.BootstrapTokenHash == nil || state.BootstrapTokenExpiresAt == nil {
		return false
	}
	if state.BootstrapTokenExpiresAt.Before(time.Now()) {
		return false
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return hex.EncodeToString(sum[:]) == *state.BootstrapTokenHash
}

func (s *Service) issueJWT(p *domain.Principal) (string, error) {
	if strings.TrimSpace(s.jwtSecret) == "" {
		return "", domain.ErrValidation("local auth requires JWT_SECRET")
	}
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":   p.Name,
		"iat":   now.Unix(),
		"exp":   now.Add(24 * time.Hour).Unix(),
		"admin": p.IsAdmin,
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := t.SignedString([]byte(s.jwtSecret))
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	return signed, nil
}

func callerName(ctx context.Context) string {
	p, ok := domain.PrincipalFromContext(ctx)
	if !ok || strings.TrimSpace(p.Name) == "" {
		return "system"
	}
	return p.Name
}
