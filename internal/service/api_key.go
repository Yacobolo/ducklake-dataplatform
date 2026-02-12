package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"duck-demo/internal/domain"
)

// APIKeyService provides API key management operations.
type APIKeyService struct {
	repo  domain.APIKeyRepository
	audit domain.AuditRepository
}

// NewAPIKeyService creates a new APIKeyService.
func NewAPIKeyService(repo domain.APIKeyRepository, audit domain.AuditRepository) *APIKeyService {
	return &APIKeyService{repo: repo, audit: audit}
}

// Create generates a new API key for the given principal.
// Non-admin users can only create keys for themselves.
// Returns the raw key (shown once) and the created key metadata.
func (s *APIKeyService) Create(ctx context.Context, principalID int64, name string, expiresAt *time.Time) (string, *domain.APIKey, error) {
	caller, ok := domain.PrincipalFromContext(ctx)
	if !ok {
		return "", nil, domain.ErrAccessDenied("authentication required")
	}

	if name == "" {
		return "", nil, domain.ErrValidation("api key name is required")
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
		PrincipalID: principalID,
		Name:        name,
		KeyPrefix:   rawKey[:8],
		KeyHash:     hashStr,
		ExpiresAt:   expiresAt,
	}

	if err := s.repo.Create(ctx, key); err != nil {
		return "", nil, err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: caller.Name,
		Action:        fmt.Sprintf("CREATE_API_KEY(name=%s)", name),
		Status:        "ALLOWED",
	})

	return rawKey, key, nil
}

// List returns API keys for a principal (without raw key values).
func (s *APIKeyService) List(ctx context.Context, principalID int64, page domain.PageRequest) ([]domain.APIKey, int64, error) {
	return s.repo.ListByPrincipal(ctx, principalID, page)
}

// Delete removes an API key by ID.
func (s *APIKeyService) Delete(ctx context.Context, id int64) error {
	caller := callerName(ctx)
	if err := s.repo.Delete(ctx, id); err != nil {
		return err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: caller,
		Action:        fmt.Sprintf("DELETE_API_KEY(id=%d)", id),
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
