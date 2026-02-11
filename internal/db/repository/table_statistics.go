package repository

import (
	"context"
	"database/sql"
	"time"

	"duck-demo/internal/domain"
)

// TableStatisticsRepo implements domain.TableStatisticsRepository.
type TableStatisticsRepo struct {
	db *sql.DB
}

func NewTableStatisticsRepo(db *sql.DB) *TableStatisticsRepo {
	return &TableStatisticsRepo{db: db}
}

func (r *TableStatisticsRepo) Upsert(ctx context.Context, securableName string, stats *domain.TableStatistics) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO table_statistics (table_securable_name, row_count, size_bytes, column_count, last_profiled_at, profiled_by)
		 VALUES (?, ?, ?, ?, datetime('now'), ?)
		 ON CONFLICT(table_securable_name)
		 DO UPDATE SET row_count = excluded.row_count,
		               size_bytes = excluded.size_bytes,
		               column_count = excluded.column_count,
		               last_profiled_at = datetime('now'),
		               profiled_by = excluded.profiled_by`,
		securableName, stats.RowCount, stats.SizeBytes, stats.ColumnCount, stats.ProfiledBy)
	return err
}

func (r *TableStatisticsRepo) Get(ctx context.Context, securableName string) (*domain.TableStatistics, error) {
	var stats domain.TableStatistics
	var rowCount, sizeBytes, columnCount sql.NullInt64
	var lastProfiledAt sql.NullString
	var profiledBy sql.NullString

	err := r.db.QueryRowContext(ctx,
		`SELECT row_count, size_bytes, column_count, last_profiled_at, profiled_by
		 FROM table_statistics WHERE table_securable_name = ?`, securableName).
		Scan(&rowCount, &sizeBytes, &columnCount, &lastProfiledAt, &profiledBy)
	if err == sql.ErrNoRows {
		return nil, nil // No stats yet, not an error
	}
	if err != nil {
		return nil, err
	}

	if rowCount.Valid {
		stats.RowCount = &rowCount.Int64
	}
	if sizeBytes.Valid {
		stats.SizeBytes = &sizeBytes.Int64
	}
	if columnCount.Valid {
		stats.ColumnCount = &columnCount.Int64
	}
	if lastProfiledAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", lastProfiledAt.String)
		stats.LastProfiledAt = &t
	}
	if profiledBy.Valid {
		stats.ProfiledBy = profiledBy.String
	}

	return &stats, nil
}

func (r *TableStatisticsRepo) Delete(ctx context.Context, securableName string) error {
	_, err := r.db.ExecContext(ctx,
		`DELETE FROM table_statistics WHERE table_securable_name = ?`, securableName)
	return err
}
