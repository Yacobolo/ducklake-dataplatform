//nolint:revive // repository constructors and methods intentionally exported for wiring.
package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"duck-demo/internal/domain"
)

const (
	claimLockRetryLimit = 5
	claimRetryStepDelay = 10 * time.Millisecond
	processingLeaseTTL  = 5 * time.Minute
)

var (
	_ domain.OrchestrationEventRepository = (*OrchestrationEventRepo)(nil)
	_ domain.BackfillRepository           = (*BackfillRepo)(nil)
)

type OrchestrationEventRepo struct {
	db *sql.DB
}

func NewOrchestrationEventRepo(db *sql.DB) *OrchestrationEventRepo {
	return &OrchestrationEventRepo{db: db}
}

func (r *OrchestrationEventRepo) Enqueue(ctx context.Context, event *domain.OrchestrationEvent) (*domain.OrchestrationEvent, error) {
	if event == nil {
		return nil, domain.ErrValidation("event is required")
	}
	id := event.ID
	if id == "" {
		id = newID()
	}
	if event.Status == "" {
		event.Status = domain.OrchestrationEventStatusPending
	}
	if event.AvailableAt.IsZero() {
		event.AvailableAt = time.Now().UTC()
	}
	payloadJSON, err := json.Marshal(event.PayloadJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal payload_json: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO orchestration_events
		(id, event_type, asset_id, partition_key, payload_json, status, attempt_count, available_at, last_error, idempotency_key)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, id, event.EventType, nullStrFromPtr(event.AssetID), nullStrFromPtr(event.PartitionKey), string(payloadJSON), event.Status, event.AttemptCount, event.AvailableAt, nullStrFromPtr(event.LastError), nullStrFromPtr(event.IdempotencyKey))
	if err != nil {
		mappedErr := mapDBError(err)
		var conflictErr *domain.ConflictError
		if errors.As(mappedErr, &conflictErr) && event.IdempotencyKey != nil && *event.IdempotencyKey != "" {
			existing, lookupErr := r.getByIdempotencyKey(ctx, *event.IdempotencyKey)
			if lookupErr == nil {
				return existing, nil
			}
		}
		return nil, mappedErr
	}
	event.ID = id
	return r.getByID(ctx, id)
}

func (r *OrchestrationEventRepo) ClaimNextPending(ctx context.Context, now time.Time) (*domain.OrchestrationEvent, error) {
	nowUTC := now.UTC()
	staleProcessingBefore := nowUTC.Add(-processingLeaseTTL)
	lockRetries := 0
	for {
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			if claimRetryableLockError(err) {
				if waitErr := waitClaimRetry(ctx, lockRetries); waitErr == nil {
					lockRetries++
					continue
				} else {
					return nil, waitErr
				}
			}
			return nil, fmt.Errorf("begin claim transaction: %w", err)
		}

		row := tx.QueryRowContext(ctx, `
			SELECT id, event_type, asset_id, partition_key, payload_json, status, attempt_count,
			       available_at, last_error, idempotency_key, created_at, updated_at
			FROM orchestration_events
			WHERE (status = ? AND available_at <= ?)
			   OR (status = ? AND updated_at <= ?)
			ORDER BY CASE
				WHEN status = ? THEN available_at
				ELSE updated_at
			END ASC,
			created_at ASC
			LIMIT 1
		`,
			domain.OrchestrationEventStatusPending,
			nowUTC,
			domain.OrchestrationEventStatusProcessing,
			staleProcessingBefore,
			domain.OrchestrationEventStatusPending,
		)

		event, scanErr := scanOrchestrationEvent(row)
		if scanErr != nil {
			_ = tx.Rollback()
			if claimRetryableLockError(scanErr) {
				if waitErr := waitClaimRetry(ctx, lockRetries); waitErr == nil {
					lockRetries++
					continue
				} else {
					return nil, waitErr
				}
			}
			var notFoundErr *domain.NotFoundError
			if errors.As(scanErr, &notFoundErr) {
				return nil, domain.ErrNotFound("no pending orchestration event")
			}
			if errors.Is(scanErr, sql.ErrNoRows) {
				return nil, domain.ErrNotFound("no pending orchestration event")
			}
			return nil, fmt.Errorf("scan pending orchestration event: %w", scanErr)
		}

		result, execErr := tx.ExecContext(ctx, `
			UPDATE orchestration_events
			SET status = ?, attempt_count = attempt_count + 1, updated_at = CURRENT_TIMESTAMP
			WHERE id = ? AND (
				status = ?
				OR (status = ? AND updated_at <= ?)
			)
		`,
			domain.OrchestrationEventStatusProcessing,
			event.ID,
			domain.OrchestrationEventStatusPending,
			domain.OrchestrationEventStatusProcessing,
			staleProcessingBefore,
		)
		if execErr != nil {
			_ = tx.Rollback()
			if claimRetryableLockError(execErr) {
				if waitErr := waitClaimRetry(ctx, lockRetries); waitErr == nil {
					lockRetries++
					continue
				} else {
					return nil, waitErr
				}
			}
			return nil, fmt.Errorf("claim orchestration event %s: %w", event.ID, mapDBError(execErr))
		}

		rowsAffected, affectedErr := result.RowsAffected()
		if affectedErr != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("read claim result for orchestration event %s: %w", event.ID, affectedErr)
		}
		if rowsAffected == 0 {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return nil, fmt.Errorf("rollback failed claim transaction: %w", rollbackErr)
			}
			lockRetries = 0
			continue
		}

		if commitErr := tx.Commit(); commitErr != nil {
			if claimRetryableLockError(commitErr) {
				if waitErr := waitClaimRetry(ctx, lockRetries); waitErr == nil {
					lockRetries++
					continue
				} else {
					return nil, waitErr
				}
			}
			return nil, fmt.Errorf("commit claim transaction for orchestration event %s: %w", event.ID, commitErr)
		}
		lockRetries = 0

		return r.getByID(ctx, event.ID)
	}
}

func claimRetryableLockError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") ||
		strings.Contains(message, "database table is locked") ||
		strings.Contains(message, "database schema is locked") ||
		strings.Contains(message, "database is busy")
}

func waitClaimRetry(ctx context.Context, retries int) error {
	if retries >= claimLockRetryLimit {
		return domain.ErrConflict("orchestration queue is contended")
	}
	delay := time.Duration(retries+1) * claimRetryStepDelay
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *OrchestrationEventRepo) MarkProcessed(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE orchestration_events
		SET status = ?,
		    last_error = CASE
				WHEN status = ? THEN last_error
				ELSE NULL
			END,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, domain.OrchestrationEventStatusProcessed, domain.OrchestrationEventStatusFailed, id)
	return mapDBError(err)
}

func (r *OrchestrationEventRepo) MarkFailed(ctx context.Context, id string, errMsg string, retryAt *time.Time) error {
	status := domain.OrchestrationEventStatusFailed
	availableAt := sql.NullTime{}
	if retryAt != nil {
		status = domain.OrchestrationEventStatusPending
		availableAt = sql.NullTime{Time: retryAt.UTC(), Valid: true}
	}
	_, err := r.db.ExecContext(ctx, `
		UPDATE orchestration_events
		SET status = ?, last_error = ?, available_at = COALESCE(?, available_at), updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, status, errMsg, availableAt, id)
	return mapDBError(err)
}

func (r *OrchestrationEventRepo) List(ctx context.Context, filter domain.OrchestrationEventFilter) ([]domain.OrchestrationEvent, int64, error) {
	status := ""
	if filter.Status != nil {
		status = *filter.Status
	}
	var total int64
	if err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM orchestration_events
		WHERE (? = '' OR status = ?)
	`, status, status).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, event_type, asset_id, partition_key, payload_json, status, attempt_count,
		       available_at, last_error, idempotency_key, created_at, updated_at
		FROM orchestration_events
		WHERE (? = '' OR status = ?)
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`, status, status, filter.Page.Limit(), filter.Page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.OrchestrationEvent, 0)
	for rows.Next() {
		e, scanErr := scanOrchestrationEvent(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		out = append(out, *e)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

func (r *OrchestrationEventRepo) getByID(ctx context.Context, id string) (*domain.OrchestrationEvent, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, event_type, asset_id, partition_key, payload_json, status, attempt_count,
		       available_at, last_error, idempotency_key, created_at, updated_at
		FROM orchestration_events
		WHERE id = ?
	`, id)
	return scanOrchestrationEvent(row)
}

func (r *OrchestrationEventRepo) getByIdempotencyKey(ctx context.Context, key string) (*domain.OrchestrationEvent, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, event_type, asset_id, partition_key, payload_json, status, attempt_count,
		       available_at, last_error, idempotency_key, created_at, updated_at
		FROM orchestration_events
		WHERE idempotency_key = ?
	`, key)
	return scanOrchestrationEvent(row)
}

type BackfillRepo struct {
	db *sql.DB
}

func NewBackfillRepo(db *sql.DB) *BackfillRepo {
	return &BackfillRepo{db: db}
}

func (r *BackfillRepo) CreateRequest(ctx context.Context, req *domain.BackfillRequest) (*domain.BackfillRequest, error) {
	if req == nil {
		return nil, domain.ErrValidation("backfill request is required")
	}
	id := req.ID
	if id == "" {
		id = newID()
	}
	if req.Status == "" {
		req.Status = domain.BackfillStatusPending
	}
	if req.MaxParallelism <= 0 {
		req.MaxParallelism = 1
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO backfill_requests
		(id, asset_id, partition_from, partition_to, status, requested_by, max_parallelism, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, id, req.AssetID, req.PartitionFrom, req.PartitionTo, req.Status, req.RequestedBy, req.MaxParallelism, nullStrFromPtr(req.ErrorMessage))
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetRequestByID(ctx, id)
}

func (r *BackfillRepo) GetRequestByID(ctx context.Context, id string) (*domain.BackfillRequest, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, asset_id, partition_from, partition_to, status, requested_by, max_parallelism,
		       error_message, created_at, started_at, finished_at
		FROM backfill_requests
		WHERE id = ?
	`, id)
	return scanBackfillRequest(row)
}

func (r *BackfillRepo) ListRequests(ctx context.Context, filter domain.BackfillFilter) ([]domain.BackfillRequest, int64, error) {
	assetID := ""
	if filter.AssetID != nil {
		assetID = *filter.AssetID
	}
	status := ""
	if filter.Status != nil {
		status = *filter.Status
	}
	var total int64
	if err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM backfill_requests
		WHERE (? = '' OR asset_id = ?) AND (? = '' OR status = ?)
	`, assetID, assetID, status, status).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, asset_id, partition_from, partition_to, status, requested_by, max_parallelism,
		       error_message, created_at, started_at, finished_at
		FROM backfill_requests
		WHERE (? = '' OR asset_id = ?) AND (? = '' OR status = ?)
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`, assetID, assetID, status, status, filter.Page.Limit(), filter.Page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]domain.BackfillRequest, 0)
	for rows.Next() {
		req, scanErr := scanBackfillRequest(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		out = append(out, *req)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

func (r *BackfillRepo) UpdateRequestStatus(ctx context.Context, id string, status string, errMsg *string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE backfill_requests
		SET status = ?, error_message = ?,
		    started_at = CASE WHEN ? = 'RUNNING' AND started_at IS NULL THEN CURRENT_TIMESTAMP ELSE started_at END,
		    finished_at = CASE WHEN ? IN ('SUCCESS','FAILED','CANCELLED') THEN CURRENT_TIMESTAMP ELSE finished_at END
		WHERE id = ?
	`, status, nullStrFromPtr(errMsg), status, status, id)
	return mapDBError(err)
}

func (r *BackfillRepo) CreateSlice(ctx context.Context, slice *domain.BackfillSlice) (*domain.BackfillSlice, error) {
	if slice == nil {
		return nil, domain.ErrValidation("backfill slice is required")
	}
	id := slice.ID
	if id == "" {
		id = newID()
	}
	if slice.Status == "" {
		slice.Status = domain.BackfillStatusPending
	}
	if slice.MaxAttempts <= 0 {
		slice.MaxAttempts = 1
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO backfill_slices
		(id, request_id, asset_id, partition_key, status, run_id, attempt_count, max_attempts, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, id, slice.RequestID, slice.AssetID, slice.PartitionKey, slice.Status, nullStrFromPtr(slice.RunID), slice.AttemptCount, slice.MaxAttempts, nullStrFromPtr(slice.ErrorMessage))
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.getSliceByID(ctx, id)
}

func (r *BackfillRepo) ListSlicesByRequest(ctx context.Context, requestID string) ([]domain.BackfillSlice, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, request_id, asset_id, partition_key, status, run_id, attempt_count, max_attempts,
		       error_message, created_at, started_at, finished_at
		FROM backfill_slices
		WHERE request_id = ?
		ORDER BY partition_key ASC
	`, requestID)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]domain.BackfillSlice, 0)
	for rows.Next() {
		slice, scanErr := scanBackfillSlice(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, *slice)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *BackfillRepo) UpdateSliceStatus(ctx context.Context, id string, status string, runID *string, errMsg *string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE backfill_slices
		SET status = ?, run_id = COALESCE(?, run_id), error_message = ?,
		    started_at = CASE WHEN ? = 'RUNNING' AND started_at IS NULL THEN CURRENT_TIMESTAMP ELSE started_at END,
		    finished_at = CASE WHEN ? IN ('SUCCESS','FAILED','CANCELLED') THEN CURRENT_TIMESTAMP ELSE finished_at END
		WHERE id = ?
	`, status, nullStrFromPtr(runID), nullStrFromPtr(errMsg), status, status, id)
	return mapDBError(err)
}

func (r *BackfillRepo) getSliceByID(ctx context.Context, id string) (*domain.BackfillSlice, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, request_id, asset_id, partition_key, status, run_id, attempt_count, max_attempts,
		       error_message, created_at, started_at, finished_at
		FROM backfill_slices
		WHERE id = ?
	`, id)
	return scanBackfillSlice(row)
}

func scanOrchestrationEvent(scanner interface{ Scan(dest ...any) error }) (*domain.OrchestrationEvent, error) {
	var (
		e                                         domain.OrchestrationEvent
		assetID, partitionKey, lastError, idemKey sql.NullString
		payloadJSON                               string
	)
	if err := scanner.Scan(&e.ID, &e.EventType, &assetID, &partitionKey, &payloadJSON, &e.Status, &e.AttemptCount, &e.AvailableAt, &lastError, &idemKey, &e.CreatedAt, &e.UpdatedAt); err != nil {
		return nil, mapDBError(err)
	}
	if assetID.Valid {
		s := assetID.String
		e.AssetID = &s
	}
	if partitionKey.Valid {
		s := partitionKey.String
		e.PartitionKey = &s
	}
	if lastError.Valid {
		s := lastError.String
		e.LastError = &s
	}
	if idemKey.Valid {
		s := idemKey.String
		e.IdempotencyKey = &s
	}
	if payloadJSON != "" {
		if err := json.Unmarshal([]byte(payloadJSON), &e.PayloadJSON); err != nil {
			return nil, fmt.Errorf("unmarshal payload_json: %w", err)
		}
	}
	if e.PayloadJSON == nil {
		e.PayloadJSON = map[string]any{}
	}
	return &e, nil
}

func scanBackfillRequest(scanner interface{ Scan(dest ...any) error }) (*domain.BackfillRequest, error) {
	var (
		req                   domain.BackfillRequest
		errMsg                sql.NullString
		startedAt, finishedAt sql.NullTime
	)
	if err := scanner.Scan(&req.ID, &req.AssetID, &req.PartitionFrom, &req.PartitionTo, &req.Status, &req.RequestedBy, &req.MaxParallelism, &errMsg, &req.CreatedAt, &startedAt, &finishedAt); err != nil {
		return nil, mapDBError(err)
	}
	if errMsg.Valid {
		s := errMsg.String
		req.ErrorMessage = &s
	}
	if startedAt.Valid {
		t := startedAt.Time
		req.StartedAt = &t
	}
	if finishedAt.Valid {
		t := finishedAt.Time
		req.FinishedAt = &t
	}
	return &req, nil
}

func scanBackfillSlice(scanner interface{ Scan(dest ...any) error }) (*domain.BackfillSlice, error) {
	var (
		slice                 domain.BackfillSlice
		runID, errMsg         sql.NullString
		startedAt, finishedAt sql.NullTime
	)
	if err := scanner.Scan(&slice.ID, &slice.RequestID, &slice.AssetID, &slice.PartitionKey, &slice.Status, &runID, &slice.AttemptCount, &slice.MaxAttempts, &errMsg, &slice.CreatedAt, &startedAt, &finishedAt); err != nil {
		return nil, mapDBError(err)
	}
	if runID.Valid {
		s := runID.String
		slice.RunID = &s
	}
	if errMsg.Valid {
		s := errMsg.String
		slice.ErrorMessage = &s
	}
	if startedAt.Valid {
		t := startedAt.Time
		slice.StartedAt = &t
	}
	if finishedAt.Valid {
		t := finishedAt.Time
		slice.FinishedAt = &t
	}
	return &slice, nil
}
