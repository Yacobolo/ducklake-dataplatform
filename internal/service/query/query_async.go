package query

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"duck-demo/internal/domain"
)

const (
	defaultMaxAsyncAttempts = 3
	heartbeatInterval       = 1 * time.Second
)

// SubmitAsync creates an asynchronous query job and starts background execution.
func (s *QueryService) SubmitAsync(ctx context.Context, principalName, sqlQuery, requestID string) (*domain.QueryJob, error) {
	if !s.asyncEnabled {
		return nil, domain.ErrNotImplemented("async query queue is disabled")
	}
	if s.jobRepo == nil {
		return nil, domain.ErrNotImplemented("async query jobs are not configured")
	}
	if strings.TrimSpace(sqlQuery) == "" {
		return nil, domain.ErrValidation("sql query is required")
	}
	if requestID == "" {
		requestID = uuid.NewString()
	}

	existing, err := s.jobRepo.GetByRequestID(ctx, principalName, requestID)
	if err == nil {
		return existing, nil
	}
	if _, ok := err.(*domain.NotFoundError); !ok {
		return nil, fmt.Errorf("lookup query job by request id: %w", err)
	}

	job, err := s.jobRepo.Create(ctx, &domain.QueryJob{
		PrincipalName: principalName,
		RequestID:     requestID,
		SQLText:       sqlQuery,
		Status:        domain.QueryJobStatusQueued,
		MaxAttempts:   defaultMaxAsyncAttempts,
	})
	if err != nil {
		return nil, fmt.Errorf("create query job: %w", err)
	}

	go s.runAsyncJob(job.ID, principalName, sqlQuery, job.MaxAttempts)
	return job, nil
}

// GetAsyncJob returns query job state for the given principal and job id.
func (s *QueryService) GetAsyncJob(ctx context.Context, principalName, jobID string) (*domain.QueryJob, error) {
	if !s.asyncEnabled {
		return nil, domain.ErrNotImplemented("async query queue is disabled")
	}
	if s.jobRepo == nil {
		return nil, domain.ErrNotImplemented("async query jobs are not configured")
	}
	job, err := s.jobRepo.GetByID(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if job.PrincipalName != principalName {
		return nil, domain.ErrNotFound("query job %q not found", jobID)
	}
	return job, nil
}

// CancelAsyncJob cancels a queued or running query job.
func (s *QueryService) CancelAsyncJob(ctx context.Context, principalName, jobID string) error {
	if !s.asyncEnabled {
		return domain.ErrNotImplemented("async query queue is disabled")
	}
	job, err := s.GetAsyncJob(ctx, principalName, jobID)
	if err != nil {
		return err
	}
	if job.Status == domain.QueryJobStatusSucceeded || job.Status == domain.QueryJobStatusFailed || job.Status == domain.QueryJobStatusCanceled {
		s.logAudit(ctx, principalName, "QUERY_JOB_CANCEL", nil, nil, nil, "ALLOWED", "", 0, nil)
		return nil
	}

	if cancelRaw, ok := s.jobCancels.Load(jobID); ok {
		if cancelFn, ok := cancelRaw.(context.CancelFunc); ok {
			cancelFn()
		}
	}

	if err := s.jobRepo.MarkCanceled(ctx, jobID); err != nil {
		s.logAudit(ctx, principalName, "QUERY_JOB_CANCEL", nil, nil, nil, "ERROR", err.Error(), 0, nil)
		return err
	}

	s.logAudit(ctx, principalName, "QUERY_JOB_CANCEL", nil, nil, nil, "ALLOWED", "", 0, nil)
	return nil
}

// DeleteAsyncJob removes a query job after canceling execution if needed.
func (s *QueryService) DeleteAsyncJob(ctx context.Context, principalName, jobID string) error {
	if !s.asyncEnabled {
		return domain.ErrNotImplemented("async query queue is disabled")
	}
	if err := s.CancelAsyncJob(ctx, principalName, jobID); err != nil {
		return err
	}
	if err := s.jobRepo.Delete(ctx, jobID); err != nil {
		s.logAudit(ctx, principalName, "QUERY_JOB_DELETE", nil, nil, nil, "ERROR", err.Error(), 0, nil)
		return err
	}

	s.logAudit(ctx, principalName, "QUERY_JOB_DELETE", nil, nil, nil, "ALLOWED", "", 0, nil)
	return nil
}

func (s *QueryService) runAsyncJob(jobID, principalName, sqlQuery string, maxAttempts int) {
	ctx, cancel := context.WithCancel(context.Background())
	s.jobCancels.Store(jobID, cancel)
	defer s.jobCancels.Delete(jobID)
	defer cancel()

	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAsyncAttempts
	}
	attempt := 0

	for {
		attempt++
		_ = s.jobRepo.MarkRunning(ctx, jobID, attempt)

		hbDone := make(chan struct{})
		go s.heartbeatLoop(ctx, jobID, hbDone)

		result, err := s.Execute(ctx, principalName, sqlQuery)
		close(hbDone)

		if err == nil {
			_ = s.jobRepo.MarkSucceeded(context.Background(), jobID, result.Columns, result.Rows, result.RowCount)
			return
		}

		if ctx.Err() == context.Canceled {
			_ = s.jobRepo.MarkCanceled(context.Background(), jobID)
			return
		}

		if attempt >= maxAttempts || !isRetryableQueryError(err) {
			_ = s.jobRepo.MarkFailed(context.Background(), jobID, err.Error())
			return
		}

		nextRetryAt := time.Now().Add(time.Duration(attempt) * 200 * time.Millisecond)
		_ = s.jobRepo.MarkRetrying(context.Background(), jobID, attempt, nextRetryAt, err.Error())

		select {
		case <-ctx.Done():
			_ = s.jobRepo.MarkCanceled(context.Background(), jobID)
			return
		case <-time.After(time.Until(nextRetryAt)):
		}
	}
}

func (s *QueryService) heartbeatLoop(ctx context.Context, jobID string, done <-chan struct{}) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			_ = s.jobRepo.Heartbeat(context.Background(), jobID, t)
		}
	}
}

func isRetryableQueryError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	retryHints := []string{"timeout", "temporarily", "temporary", "connection reset", "eof", "broken pipe"}
	for _, hint := range retryHints {
		if strings.Contains(msg, hint) {
			return true
		}
	}
	return false
}
