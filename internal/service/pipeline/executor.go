package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"duck-demo/internal/domain"
)

// executeRun processes a pipeline run in a background goroutine.
// It resolves the DAG, executes jobs level-by-level, and updates status.
func (s *PipelineService) executeRun(runID string, jobs []domain.PipelineJob,
	levels [][]string, params map[string]string, principal string) {

	ctx := context.Background()
	logger := s.logger.With("run_id", runID)

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

	// Execute level by level.
	for _, level := range levels {
		if runFailed {
			// Skip remaining levels — mark jobs as skipped.
			for _, jobID := range level {
				jrID := jobRunByJobID[jobID]
				_ = s.runs.UpdateJobRunFinished(ctx, jrID, domain.PipelineJobRunStatusSkipped, nil)
			}
			continue
		}

		// Execute jobs in this level sequentially (parallel execution is a future enhancement).
		for _, jobID := range level {
			job := jobByID[jobID]
			jrID := jobRunByJobID[jobID]

			if err := s.executeJob(ctx, job, jrID, params, principal, logger); err != nil {
				runFailed = true
				// Continue marking remaining jobs in this level as skipped.
				continue
			}
		}
	}

	// Finalize run status.
	if runFailed {
		errMsg := "one or more jobs failed"
		_ = s.runs.UpdateRunFinished(ctx, runID, domain.PipelineRunStatusFailed, &errMsg)
	} else {
		_ = s.runs.UpdateRunFinished(ctx, runID, domain.PipelineRunStatusSuccess, nil)
	}
}

// executeJob executes a single pipeline job on a pinned DuckDB connection.
func (s *PipelineService) executeJob(ctx context.Context, job domain.PipelineJob,
	jobRunID string, params map[string]string, principal string, logger *slog.Logger) error {

	logger = logger.With("job_id", job.ID, "job_name", job.Name)

	var lastErr error
	maxAttempts := job.RetryCount + 1

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 1s, 2s, 4s...
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second
			time.Sleep(backoff)
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
func (s *PipelineService) executeJobAttempt(ctx context.Context, job domain.PipelineJob,
	params map[string]string, principal string, logger *slog.Logger) error {

	// Acquire a pinned connection for job isolation.
	conn, err := s.duckDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	// Inject parameters via SET VARIABLE.
	for k, v := range params {
		setSQL := fmt.Sprintf("SET VARIABLE %s = '%s'", k, v)
		rows, err := s.engine.QueryOnConn(ctx, conn, principal, setSQL)
		if err != nil {
			return fmt.Errorf("set variable %s: %w", k, err)
		}
		rows.Close()
	}

	// Get SQL blocks from the notebook.
	blocks, err := s.notebooks.GetSQLBlocks(ctx, job.NotebookID)
	if err != nil {
		return fmt.Errorf("get notebook SQL: %w", err)
	}

	// Execute each SQL block.
	for i, block := range blocks {
		rows, err := s.engine.QueryOnConn(ctx, conn, principal, block)
		if err != nil {
			return fmt.Errorf("execute block %d: %w", i+1, err)
		}
		rows.Close()
	}

	logger.Info("job completed successfully")
	return nil
}
