package repository

import (
	"context"
	"database/sql"
	"time"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// TableStatisticsRepo implements domain.TableStatisticsRepository.
type TableStatisticsRepo struct {
	q *dbstore.Queries
}

func NewTableStatisticsRepo(db *sql.DB) *TableStatisticsRepo {
	return &TableStatisticsRepo{q: dbstore.New(db)}
}

func (r *TableStatisticsRepo) Upsert(ctx context.Context, securableName string, stats *domain.TableStatistics) error {
	return r.q.UpsertTableStatistics(ctx, dbstore.UpsertTableStatisticsParams{
		TableSecurableName: securableName,
		RowCount:           nullInt64(stats.RowCount),
		SizeBytes:          nullInt64(stats.SizeBytes),
		ColumnCount:        nullInt64(stats.ColumnCount),
		ProfiledBy:         sql.NullString{String: stats.ProfiledBy, Valid: stats.ProfiledBy != ""},
	})
}

func (r *TableStatisticsRepo) Get(ctx context.Context, securableName string) (*domain.TableStatistics, error) {
	row, err := r.q.GetTableStatistics(ctx, securableName)
	if err == sql.ErrNoRows {
		return nil, nil // No stats yet, not an error
	}
	if err != nil {
		return nil, err
	}

	var stats domain.TableStatistics
	if row.RowCount.Valid {
		stats.RowCount = &row.RowCount.Int64
	}
	if row.SizeBytes.Valid {
		stats.SizeBytes = &row.SizeBytes.Int64
	}
	if row.ColumnCount.Valid {
		stats.ColumnCount = &row.ColumnCount.Int64
	}
	if row.LastProfiledAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.LastProfiledAt.String)
		stats.LastProfiledAt = &t
	}
	if row.ProfiledBy.Valid {
		stats.ProfiledBy = row.ProfiledBy.String
	}

	return &stats, nil
}

func (r *TableStatisticsRepo) Delete(ctx context.Context, securableName string) error {
	return r.q.DeleteTableStatistics(ctx, securableName)
}

func nullInt64(p *int64) sql.NullInt64 {
	if p == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *p, Valid: true}
}
