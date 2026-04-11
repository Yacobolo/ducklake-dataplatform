//nolint:revive
package repository

import (
	"context"
	"database/sql"
	"time"

	dbstore "duck-demo/internal/db/cuestore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

type SetupStateRepo struct {
	q *dbstore.Queries
}

func NewSetupStateRepo(db *sql.DB) *SetupStateRepo {
	return &SetupStateRepo{q: dbstore.New(db)}
}

var _ domain.SetupStateRepository = (*SetupStateRepo)(nil)

func (r *SetupStateRepo) Get(ctx context.Context) (*domain.SetupState, error) {
	row, err := r.q.GetSetupState(ctx)
	if err != nil {
		return nil, mapDBError(err)
	}
	return &domain.SetupState{
		SetupCompleted:          row.SetupCompleted != 0,
		SetupCompletedAt:        ptrFromNullTime(row.SetupCompletedAt),
		SetupCompletedBy:        ptrFromNullString(row.SetupCompletedBy),
		BootstrapTokenHash:      ptrFromNullString(row.BootstrapTokenHash),
		BootstrapTokenExpiresAt: ptrFromNullTime(row.BootstrapTokenExpiresAt),
		CreatedAt:               row.CreatedAt,
		UpdatedAt:               row.UpdatedAt,
	}, nil
}

func (r *SetupStateRepo) Complete(ctx context.Context, principalID string) error {
	return mapDBError(r.q.CompleteSetupState(ctx, mapper.NullStrFromStr(principalID)))
}

func (r *SetupStateRepo) SetBootstrapToken(ctx context.Context, tokenHash string, expiresAt time.Time) error {
	return mapDBError(r.q.SetSetupBootstrapToken(ctx, dbstore.SetSetupBootstrapTokenParams{
		BootstrapTokenHash:      mapper.NullStrFromStr(tokenHash),
		BootstrapTokenExpiresAt: sql.NullTime{Time: expiresAt, Valid: true},
	}))
}

func (r *SetupStateRepo) ClearBootstrapToken(ctx context.Context) error {
	return mapDBError(r.q.ClearSetupBootstrapToken(ctx))
}
