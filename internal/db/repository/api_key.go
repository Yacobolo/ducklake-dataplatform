package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
)

// APIKeyRepo implements middleware.APIKeyLookup using sqlc queries.
type APIKeyRepo struct {
	q *dbstore.Queries
}

// NewAPIKeyRepo creates a new APIKeyRepo.
func NewAPIKeyRepo(db *sql.DB) *APIKeyRepo {
	return &APIKeyRepo{q: dbstore.New(db)}
}

// LookupPrincipalByAPIKeyHash returns the principal name associated with the given API key hash.
func (r *APIKeyRepo) LookupPrincipalByAPIKeyHash(ctx context.Context, keyHash string) (string, error) {
	row, err := r.q.GetAPIKeyByHash(ctx, keyHash)
	if err != nil {
		return "", mapDBError(err)
	}
	return row.PrincipalName, nil
}
