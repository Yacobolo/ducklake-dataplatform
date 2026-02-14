package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"duck-demo/internal/domain"
)

// validVariableName matches valid SQL variable names: starts with letter or underscore,
// followed by letters, digits, or underscores.
var validVariableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// isValidVariableName checks whether name is a safe SQL variable identifier.
func isValidVariableName(name string) bool {
	return validVariableName.MatchString(name)
}

// executeRun processes a pipeline run in a background goroutine.
// It resolves the DAG, executes jobs level-by-level, and updates status.
func (s *Service) executeRun(ctx context.Context, runID string, jobs []domain.PipelineJob,
	levels [][]string, params map[string]string, principal string) {

	logger := s.logger.With("run_id", runID)

	// Clean up the cancel func when done.
	defer s.runCancels.Delete(runID)

	// Recover from panics.
	defer func() {
		if r := recover(); r != nil {
			errMsg := fmt.Sprintf("panic: %v", r)
			logger.Error("pipeline run panicked", "error", errMsg)
			_ = s.runs.UpdateRunFinished(ctx, runID, domain.PipelineRunStatusFailed, &errMsg)
		}
	}()

	// Mark run as started.
	if err := s.runs.UpdateRunStarted(ctx, runID); err != nil {
		logger.Error("failed to update run started", "error", err)
		return
	}

	// Build job ID → job map and job ID → job run ID map.
	jobByID := make(map[string]domain.PipelineJob, len(jobs))
	for _, j := range jobs {
		jobByID[j.ID] = j
	}

	// Get all job runs for this run.
	jobRuns, err := s.runs.ListJobRunsByRun(ctx, runID)
	if err != nil {
		errMsg := fmt.Sprintf("list job runs: %v", err)
		_ = s.runs.UpdateRunFinished(ctx, runID, domain.PipelineRunStatusFailed, &errMsg)
		return
	}
	jobRunByJobID := make(map[string]string, len(jobRuns))
	for _, jr := range jobRuns {
		jobRunByJobID[jr.JobID] = jr.ID
	}

	runFailed := false
	cancelled := false

	// Execute level by level.
	for _, level := range levels {
		if runFailed || cancelled {
			// Skip remaining levels — mark jobs as skipped/cancelled.
			status := domain.PipelineJobRunStatusSkipped
			if cancelled {
				status = domain.PipelineJobRunStatusCancelled
			}
			for _, jobID := range level {
				jrID := jobRunByJobID[jobID]
				_ = s.runs.UpdateJobRunFinished(ctx, jrID, status, nil)
			}
			continue
		}

		// Execute jobs in this level sequentially (parallel execution is a future enhancement).
		for _, jobID := range level {
			// Check for cancellation before each job.
			if ctx.Err() != nil {
				cancelled = true
				jrID := jobRunByJobID[jobID]
				_ = s.runs.UpdateJobRunFinished(ctx, jrID, domain.PipelineJobRunStatusCancelled, nil)
				continue
			}

			// Skip remaining jobs in this level if a prior job failed.
			if runFailed {
				jrID := jobRunByJobID[jobID]
				_ = s.runs.UpdateJobRunFinished(ctx, jrID, domain.PipelineJobRunStatusSkipped, nil)
				continue
			}

			job := jobByID[jobID]
			jrID := jobRunByJobID[jobID]

			if err := s.executeJob(ctx, job, jrID, params, principal, logger); err != nil {
				runFailed = true
				continue
			}
		}
	}

	// Finalize run status.
	switch {
	case cancelled:
		errMsg := "run was cancelled"
		_ = s.runs.UpdateRunFinished(ctx, runID, domain.PipelineRunStatusCancelled, &errMsg)
	case runFailed:
		errMsg := "one or more jobs failed"
		_ = s.runs.UpdateRunFinished(ctx, runID, domain.PipelineRunStatusFailed, &errMsg)
	default:
		_ = s.runs.UpdateRunFinished(ctx, runID, domain.PipelineRunStatusSuccess, nil)
	}
}

// executeJob executes a single pipeline job on a pinned DuckDB connection.
func (s *Service) executeJob(ctx context.Context, job domain.PipelineJob,
	jobRunID string, params map[string]string, principal string, logger *slog.Logger) error {

	logger = logger.With("job_id", job.ID, "job_name", job.Name)

	var lastErr error
	maxAttempts := job.RetryCount + 1

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 1s, 2s, 4s... — interruptible by cancellation.
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second //nolint:gosec // attempt is always >= 1 here
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			logger.Info("retrying job", "attempt", attempt+1)
		}

		lastErr = s.executeJobAttempt(ctx, job, params, principal, logger)
		if lastErr == nil {
			break
		}
		logger.Warn("job attempt failed", "attempt", attempt+1, "error", lastErr)
	}

	if lastErr != nil {
		errMsg := lastErr.Error()
		_ = s.runs.UpdateJobRunFinished(ctx, jobRunID, domain.PipelineJobRunStatusFailed, &errMsg)
		return lastErr
	}

	_ = s.runs.UpdateJobRunFinished(ctx, jobRunID, domain.PipelineJobRunStatusSuccess, nil)
	return nil
}

// executeJobAttempt runs one attempt of a job on a fresh pinned connection.
func (s *Service) executeJobAttempt(ctx context.Context, job domain.PipelineJob,
	params map[string]string, principal string, logger *slog.Logger) error {

	// Acquire a pinned connection for job isolation.
	conn, err := s.duckDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Inject parameters via SET VARIABLE.
	for k, v := range params {
		if !isValidVariableName(k) {
			return fmt.Errorf("set variable: %w",
				domain.ErrValidation("invalid variable name: %s", k))
		}
		escaped := strings.ReplaceAll(v, "'", "''")
		setSQL := fmt.Sprintf("SET VARIABLE %s = '%s'", k, escaped)
		if err := s.execOnConn(ctx, conn, principal, setSQL); err != nil {
			return fmt.Errorf("set variable %s: %w", k, err)
		}
	}

	// Get SQL blocks from the notebook.
	blocks, err := s.notebooks.GetSQLBlocks(ctx, job.NotebookID)
	if err != nil {
		return fmt.Errorf("get notebook SQL: %w", err)
	}

	// Execute each SQL block.
	for i, block := range blocks {
		if err := s.execOnConn(ctx, conn, principal, block); err != nil {
			return fmt.Errorf("execute block %d: %w", i+1, err)
		}
	}

	logger.Info("job completed successfully")
	return nil
}

// execOnConn executes a SQL statement on a pinned connection and drains the result.
func (s *Service) execOnConn(ctx context.Context, conn *sql.Conn, principal, query string) error {
	rows, err := s.engine.QueryOnConn(ctx, conn, principal, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	return rows.Err()
}
