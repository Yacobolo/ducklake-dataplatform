package query

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"duck-demo/internal/domain"
)

// SubmitAsync creates an asynchronous query job and starts background execution.
func (s *QueryService) SubmitAsync(ctx context.Context, principalName, sqlQuery, requestID string) (*domain.QueryJob, error) {
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
	})
	if err != nil {
		return nil, fmt.Errorf("create query job: %w", err)
	}

	go s.runAsyncJob(job.ID, principalName, sqlQuery)
	return job, nil
}

// GetAsyncJob returns query job state for the given principal and job id.
func (s *QueryService) GetAsyncJob(ctx context.Context, principalName, jobID string) (*domain.QueryJob, error) {
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

func (s *QueryService) runAsyncJob(jobID, principalName, sqlQuery string) {
	ctx, cancel := context.WithCancel(context.Background())
	s.jobCancels.Store(jobID, cancel)
	defer s.jobCancels.Delete(jobID)
	defer cancel()

	_ = s.jobRepo.MarkRunning(ctx, jobID)

	result, err := s.Execute(ctx, principalName, sqlQuery)
	if err != nil {
		if ctx.Err() == context.Canceled {
			_ = s.jobRepo.MarkCanceled(context.Background(), jobID)
			return
		}
		_ = s.jobRepo.MarkFailed(context.Background(), jobID, err.Error())
		return
	}

	_ = s.jobRepo.MarkSucceeded(context.Background(), jobID, result.Columns, result.Rows, result.RowCount)
}
