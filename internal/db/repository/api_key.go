package repository

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

// APIKeyRepo implements both domain.APIKeyRepository and middleware.APIKeyLookup using sqlc queries.
type APIKeyRepo struct {
	q *dbstore.Queries
}

// NewAPIKeyRepo creates a new APIKeyRepo.
func NewAPIKeyRepo(db *sql.DB) *APIKeyRepo {
	return &APIKeyRepo{q: dbstore.New(db)}
}

// Compile-time check that APIKeyRepo implements domain.APIKeyRepository.
var _ domain.APIKeyRepository = (*APIKeyRepo)(nil)

// LookupPrincipalByAPIKeyHash returns the principal name associated with the given API key hash.
// This implements the middleware.APIKeyLookup interface.
func (r *APIKeyRepo) LookupPrincipalByAPIKeyHash(ctx context.Context, keyHash string) (string, error) {
	row, err := r.q.GetAPIKeyByHash(ctx, keyHash)
	if err != nil {
		return "", mapDBError(err)
	}
	return row.PrincipalName, nil
}

// Create inserts a new API key into the database.
func (r *APIKeyRepo) Create(ctx context.Context, key *domain.APIKey) error {
	params := dbstore.CreateAPIKeyParams{
		ID:          newID(),
		KeyHash:     key.KeyHash,
		PrincipalID: key.PrincipalID,
		Name:        key.Name,
	}
	if key.ExpiresAt != nil {
		params.ExpiresAt = mapper.NullStrFromStr(key.ExpiresAt.Format(time.DateTime))
	}
	row, err := r.q.CreateAPIKey(ctx, params)
	if err != nil {
		return mapDBError(err)
	}
	key.ID = row.ID
	key.CreatedAt = parseTimeStr(row.CreatedAt)
	return nil
}

// GetByHash returns the API key and associated principal for a given hash.
func (r *APIKeyRepo) GetByHash(ctx context.Context, hash string) (*domain.APIKey, *domain.Principal, error) {
	row, err := r.q.GetAPIKeyByHash(ctx, hash)
	if err != nil {
		return nil, nil, mapDBError(err)
	}
	apiKey := apiKeyFromHashRow(row)
	// We only have the principal name from the join; create a minimal principal.
	principal := &domain.Principal{
		ID:   row.PrincipalID,
		Name: row.PrincipalName,
	}
	return apiKey, principal, nil
}

// ListByPrincipal returns a paginated list of API keys for a principal.
func (r *APIKeyRepo) ListByPrincipal(ctx context.Context, principalID string, page domain.PageRequest) ([]domain.APIKey, int64, error) {
	total, err := r.q.CountAPIKeysForPrincipal(ctx, principalID)
	if err != nil {
		return nil, 0, err
	}
	rows, err := r.q.ListAPIKeysForPrincipalPaginated(ctx, dbstore.ListAPIKeysForPrincipalPaginatedParams{
		PrincipalID: principalID,
		Limit:       int64(page.Limit()),
		Offset:      int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}
	keys := make([]domain.APIKey, len(rows))
	for i, row := range rows {
		keys[i] = apiKeyFromDB(row)
	}
	return keys, total, nil
}

// GetByID returns an API key by its ID.
func (r *APIKeyRepo) GetByID(ctx context.Context, id string) (*domain.APIKey, error) {
	row, err := r.q.GetAPIKeyByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	key := apiKeyFromDB(row)
	return &key, nil
}

// Delete removes an API key by ID.
func (r *APIKeyRepo) Delete(ctx context.Context, id string) error {
	return r.q.DeleteAPIKey(ctx, id)
}

// DeleteExpired removes all expired API keys and returns the count deleted.
func (r *APIKeyRepo) DeleteExpired(ctx context.Context) (int64, error) {
	result, err := r.q.DeleteExpiredKeys(ctx)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return count, nil
}

// --- helpers ---

func parseTimeStr(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.DateTime, s)
	if err != nil {
		slog.Default().Warn("failed to parse time", "value", s, "error", err)
		return time.Time{}
	}
	return t
}

func apiKeyFromDB(row dbstore.ApiKey) domain.APIKey {
	key := domain.APIKey{
		ID:          row.ID,
		PrincipalID: row.PrincipalID,
		Name:        row.Name,
		KeyHash:     row.KeyHash,
		CreatedAt:   parseTimeStr(row.CreatedAt),
	}
	if row.KeyHash != "" && len(row.KeyHash) >= 8 {
		key.KeyPrefix = row.KeyHash[:8]
	}
	if row.ExpiresAt.Valid {
		t := parseTimeStr(row.ExpiresAt.String)
		key.ExpiresAt = &t
	}
	return key
}

func apiKeyFromHashRow(row dbstore.GetAPIKeyByHashRow) *domain.APIKey {
	key := &domain.APIKey{
		ID:          row.ID,
		PrincipalID: row.PrincipalID,
		Name:        row.Name,
		KeyHash:     row.KeyHash,
		CreatedAt:   parseTimeStr(row.CreatedAt),
	}
	if row.KeyHash != "" && len(row.KeyHash) >= 8 {
		key.KeyPrefix = row.KeyHash[:8]
	}
	if row.ExpiresAt.Valid {
		t := parseTimeStr(row.ExpiresAt.String)
		key.ExpiresAt = &t
	}
	return key
}
