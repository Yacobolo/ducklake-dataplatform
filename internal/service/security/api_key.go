// Package security implements authentication, authorization, and access control services.
package security

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/Yacobolo/quackstack/internal/domain"
	servicepolicy "github.com/Yacobolo/quackstack/internal/service/policy"
)

// APIKeyService provides API key management operations.
type APIKeyService struct {
	repo       domain.APIKeyRepository
	principals domain.PrincipalRepository
	audit      domain.AuditRepository
}

// NewAPIKeyService creates a new APIKeyService.
func NewAPIKeyService(repo domain.APIKeyRepository, principals domain.PrincipalRepository, audit domain.AuditRepository) *APIKeyService {
	return &APIKeyService{repo: repo, principals: principals, audit: audit}
}

// Create generates a new API key for the given principal.
// Non-admin users can only create keys for themselves.
// Returns the raw key (shown once) and the created key metadata.
func (s *APIKeyService) Create(ctx context.Context, req domain.CreateAPIKeyRequest) (string, *domain.APIKey, error) {
	caller, err := servicepolicy.RequirePrincipalOrAdmin(ctx, req.PrincipalID, "non-admin users can only create API keys for themselves")
	if err != nil {
		return "", nil, err
	}

	if err := req.Validate(); err != nil {
		return "", nil, err
	}
	if _, err := s.principals.GetByID(ctx, req.PrincipalID); err != nil {
		return "", nil, err
	}

	// Generate a cryptographically secure random key.
	rawBytes := make([]byte, 32)
	if _, err := rand.Read(rawBytes); err != nil {
		return "", nil, fmt.Errorf("generate key: %w", err)
	}
	rawKey := hex.EncodeToString(rawBytes)

	// Hash for storage.
	hash := sha256.Sum256([]byte(rawKey))
	hashStr := hex.EncodeToString(hash[:])

	key := &domain.APIKey{
		PrincipalID: req.PrincipalID,
		Name:        req.Name,
		KeyPrefix:   rawKey[:8],
		KeyHash:     hashStr,
		ExpiresAt:   req.ExpiresAt,
	}

	if err := s.repo.Create(ctx, key); err != nil {
		return "", nil, err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: caller.Name,
		Action:        fmt.Sprintf("CREATE_API_KEY(name=%s)", req.Name),
		Status:        "ALLOWED",
	})

	return rawKey, key, nil
}

// List returns API keys for a principal (without raw key values).
// If principalID is nil or empty, defaults to the caller's own keys.
// Non-admin users can only list their own keys.
func (s *APIKeyService) List(ctx context.Context, principalID *string, page domain.PageRequest) ([]domain.APIKey, int64, error) {
	caller, err := servicepolicy.RequireAuthenticatedPrincipal(ctx)
	if err != nil {
		return nil, 0, err
	}

	// Default to caller's own keys
	if principalID == nil || *principalID == "" {
		return s.repo.ListByPrincipal(ctx, caller.ID, page)
	}

	// Non-admin can only list own keys
	if _, err := servicepolicy.RequirePrincipalOrAdmin(ctx, *principalID, "can only list your own API keys"); err != nil {
		return nil, 0, err
	}

	return s.repo.ListByPrincipal(ctx, *principalID, page)
}

// Delete removes an API key by ID.
// The caller must be the key owner or an admin.
func (s *APIKeyService) Delete(ctx context.Context, id string) error {
	caller, err := servicepolicy.RequireAuthenticatedPrincipal(ctx)
	if err != nil {
		return err
	}

	// Look up the API key to verify ownership.
	key, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}

	// Allow deletion only if the caller owns the key or is an admin.
	if _, err := servicepolicy.RequirePrincipalOrAdmin(ctx, key.PrincipalID, "only the key owner or an admin can delete this API key"); err != nil {
		return err
	}

	if err := s.repo.Delete(ctx, id); err != nil {
		return err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: caller.Name,
		Action:        fmt.Sprintf("DELETE_API_KEY(id=%s)", id),
		Status:        "ALLOWED",
	})
	return nil
}

// CleanupExpired removes all expired API keys. Requires admin privileges.
func (s *APIKeyService) CleanupExpired(ctx context.Context) (int64, error) {
	if err := requireAdmin(ctx); err != nil {
		return 0, err
	}
	count, err := s.repo.DeleteExpired(ctx)
	if err != nil {
		return 0, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: callerName(ctx),
		Action:        fmt.Sprintf("CLEANUP_EXPIRED_API_KEYS(count=%d)", count),
		Status:        "ALLOWED",
	})
	return count, nil
}
