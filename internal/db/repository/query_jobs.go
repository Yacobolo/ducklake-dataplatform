package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"duck-demo/internal/domain"
)

var _ domain.QueryJobRepository = (*QueryJobRepo)(nil)

// QueryJobRepo stores asynchronous query lifecycle state in SQLite.
type QueryJobRepo struct {
	db *sql.DB
}

// NewQueryJobRepo creates a new QueryJobRepo.
func NewQueryJobRepo(db *sql.DB) *QueryJobRepo {
	return &QueryJobRepo{db: db}
}

// Create inserts a new query job.
func (r *QueryJobRepo) Create(ctx context.Context, job *domain.QueryJob) (*domain.QueryJob, error) {
	if job == nil {
		return nil, domain.ErrValidation("query job is required")
	}
	if job.ID == "" {
		job.ID = domain.NewID()
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO query_jobs (id, principal_name, request_id, sql_text, status)
		VALUES (?, ?, ?, ?, ?)
	`, job.ID, job.PrincipalName, job.RequestID, job.SQLText, string(job.Status))
	if err != nil {
		return nil, mapDBError(err)
	}

	return r.GetByID(ctx, job.ID)
}

// GetByID returns a query job by ID.
func (r *QueryJobRepo) GetByID(ctx context.Context, id string) (*domain.QueryJob, error) {
	return r.getOne(ctx, `
		SELECT id, principal_name, request_id, sql_text, status, columns_json, rows_json, row_count,
		       error_message, created_at, started_at, completed_at, updated_at
		FROM query_jobs WHERE id = ?
	`, id)
}

// GetByRequestID returns a query job by principal + request id.
func (r *QueryJobRepo) GetByRequestID(ctx context.Context, principalName, requestID string) (*domain.QueryJob, error) {
	return r.getOne(ctx, `
		SELECT id, principal_name, request_id, sql_text, status, columns_json, rows_json, row_count,
		       error_message, created_at, started_at, completed_at, updated_at
		FROM query_jobs
		WHERE principal_name = ? AND request_id = ?
	`, principalName, requestID)
}

// MarkRunning updates a queued job to running.
func (r *QueryJobRepo) MarkRunning(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE query_jobs
		SET status = ?, started_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, string(domain.QueryJobStatusRunning), id)
	if err != nil {
		return mapDBError(err)
	}
	return nil
}

// MarkSucceeded stores query result and marks job successful.
func (r *QueryJobRepo) MarkSucceeded(ctx context.Context, id string, columns []string, rows [][]interface{}, rowCount int) error {
	columnsJSON, err := json.Marshal(columns)
	if err != nil {
		return fmt.Errorf("marshal columns: %w", err)
	}
	rowsJSON, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("marshal rows: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
		UPDATE query_jobs
		SET status = ?, columns_json = ?, rows_json = ?, row_count = ?, error_message = NULL,
		    completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, string(domain.QueryJobStatusSucceeded), string(columnsJSON), string(rowsJSON), rowCount, id)
	if err != nil {
		return mapDBError(err)
	}
	return nil
}

// MarkFailed marks a job as failed with an error message.
func (r *QueryJobRepo) MarkFailed(ctx context.Context, id string, message string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE query_jobs
		SET status = ?, error_message = ?, completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, string(domain.QueryJobStatusFailed), message, id)
	if err != nil {
		return mapDBError(err)
	}
	return nil
}

// MarkCanceled marks a job as canceled.
func (r *QueryJobRepo) MarkCanceled(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE query_jobs
		SET status = ?,
		    error_message = CASE WHEN error_message IS NULL OR error_message = '' THEN 'query canceled' ELSE error_message END,
		    completed_at = CURRENT_TIMESTAMP,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, string(domain.QueryJobStatusCanceled), id)
	if err != nil {
		return mapDBError(err)
	}
	return nil
}

// Delete removes a query job.
func (r *QueryJobRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM query_jobs WHERE id = ?`, id)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		return domain.ErrNotFound("query job %q not found", id)
	}
	return nil
}

func (r *QueryJobRepo) getOne(ctx context.Context, stmt string, args ...interface{}) (*domain.QueryJob, error) {
	var (
		job                    domain.QueryJob
		status                 string
		columnsJSON, rowsJSON  sql.NullString
		errorMessage           sql.NullString
		startedAt, completedAt sql.NullTime
		createdAt, updatedAt   time.Time
	)

	err := r.db.QueryRowContext(ctx, stmt, args...).Scan(
		&job.ID,
		&job.PrincipalName,
		&job.RequestID,
		&job.SQLText,
		&status,
		&columnsJSON,
		&rowsJSON,
		&job.RowCount,
		&errorMessage,
		&createdAt,
		&startedAt,
		&completedAt,
		&updatedAt,
	)
	if err != nil {
		return nil, mapDBError(err)
	}

	job.Status = domain.QueryJobStatus(status)
	job.CreatedAt = createdAt
	job.UpdatedAt = updatedAt
	if errorMessage.Valid {
		msg := errorMessage.String
		job.ErrorMessage = &msg
	}
	if startedAt.Valid {
		t := startedAt.Time
		job.StartedAt = &t
	}
	if completedAt.Valid {
		t := completedAt.Time
		job.CompletedAt = &t
	}
	if columnsJSON.Valid && columnsJSON.String != "" {
		if err := json.Unmarshal([]byte(columnsJSON.String), &job.Columns); err != nil {
			return nil, fmt.Errorf("unmarshal columns: %w", err)
		}
	}
	if rowsJSON.Valid && rowsJSON.String != "" {
		if err := json.Unmarshal([]byte(rowsJSON.String), &job.Rows); err != nil {
			return nil, fmt.Errorf("unmarshal rows: %w", err)
		}
	}

	return &job, nil
}
