//nolint:revive
package repository

import (
	"context"
	"database/sql"
	"time"

	dbstore "github.com/Yacobolo/quackstack/internal/db/dbstore"
	"github.com/Yacobolo/quackstack/internal/db/mapper"
	"github.com/Yacobolo/quackstack/internal/domain"
)

type AuthLoginAttemptRepo struct {
	q *dbstore.Queries
}

func NewAuthLoginAttemptRepo(db *sql.DB) *AuthLoginAttemptRepo {
	return &AuthLoginAttemptRepo{q: dbstore.New(db)}
}

var _ domain.AuthLoginAttemptRepository = (*AuthLoginAttemptRepo)(nil)

func (r *AuthLoginAttemptRepo) Insert(ctx context.Context, attempt *domain.AuthLoginAttempt) error {
	return mapDBError(r.q.InsertAuthLoginAttempt(ctx, dbstore.InsertAuthLoginAttemptParams{
		ID:        newID(),
		Username:  mapper.NullStrFromPtr(attempt.Username),
		IpAddress: mapper.NullStrFromPtr(attempt.IPAddress),
		Success:   boolToInt(attempt.Success),
		Reason:    mapper.NullStrFromPtr(attempt.Reason),
	}))
}

func (r *AuthLoginAttemptRepo) CountRecentFailedByUsername(ctx context.Context, username string, since time.Time) (int64, error) {
	count, err := r.q.CountRecentFailedAuthLoginAttemptsByUsername(ctx, dbstore.CountRecentFailedAuthLoginAttemptsByUsernameParams{
		Username:  mapper.NullStrFromStr(username),
		CreatedAt: since,
	})
	if err != nil {
		return 0, mapDBError(err)
	}
	return count, nil
}

func (r *AuthLoginAttemptRepo) CountRecentFailedByIP(ctx context.Context, ipAddress string, since time.Time) (int64, error) {
	count, err := r.q.CountRecentFailedAuthLoginAttemptsByIP(ctx, dbstore.CountRecentFailedAuthLoginAttemptsByIPParams{
		IpAddress: mapper.NullStrFromStr(ipAddress),
		CreatedAt: since,
	})
	if err != nil {
		return 0, mapDBError(err)
	}
	return count, nil
}
