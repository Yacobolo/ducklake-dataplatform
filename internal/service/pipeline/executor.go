package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
)

// validVariableName matches valid SQL variable names: starts with letter or underscore,
// followed by letters, digits, or underscores.
var validVariableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// isValidVariableName checks whether name is a safe SQL variable identifier.
func isValidVariableName(name string) bool {
	return validVariableName.MatchString(name)
}

// executeJob executes a single pipeline job on a pinned DuckDB connection.
func (s *Service) executeJob(ctx context.Context, pipelineDef *domain.Pipeline, job domain.PipelineJob,
	runID string, jobRunID string, params map[string]string, principal string, logger *slog.Logger) error {

	logger = logger.With("job_id", job.ID, "job_name", job.Name)
	persistCtx := context.Background()
	effectiveComputeEndpointID := effectiveComputeEndpointID(pipelineDef, job)
	retryCount := effectiveJobRetryCount(pipelineDef, job)
	maxAttempts := retryCount + 1

	runCtx := ctx
	if timeoutSeconds := effectiveJobTimeoutSeconds(pipelineDef, job); timeoutSeconds != nil && *timeoutSeconds > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(*timeoutSeconds)*time.Second)
		defer cancel()
		runCtx = timeoutCtx
	}

	if jobRunID != "" {
		if err := s.runs.UpdateJobRunStarted(persistCtx, jobRunID, effectiveComputeEndpointID, 1); err != nil {
			return fmt.Errorf("mark job run started: %w", err)
		}
		s.logJobRunEvent(persistCtx, runID, jobRunID, domain.PipelineRunEventStarted, pipelineMessagePtr("job started"), nil, map[string]any{"job_id": job.ID})
	}

	var lastErr error
	attemptCount := 0
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 1s, 2s, 4s... — interruptible by cancellation.
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second //nolint:gosec // attempt is always >= 1 here
			select {
			case <-runCtx.Done():
				lastErr = runCtx.Err()
				attemptCount = attempt
				break
			case <-time.After(backoff):
			}
			if lastErr != nil {
				break
			}
			logger.Info("retrying job", "attempt", attempt+1)
			if jobRunID != "" {
				msg := fmt.Sprintf("retrying job attempt %d", attempt+1)
				s.logJobRunEvent(persistCtx, runID, jobRunID, domain.PipelineRunEventRetried, &msg, classifyPipelineErrorCode(lastErr), map[string]any{"attempt": attempt + 1})
			}
		}

		attemptCount = attempt + 1
		lastErr = s.executeJobAttempt(runCtx, pipelineDef, job, params, principal, logger)
		if lastErr == nil {
			break
		}
		logger.Warn("job attempt failed", "attempt", attempt+1, "error", lastErr)
	}

	if lastErr != nil {
		if jobRunID != "" {
			errMsg := lastErr.Error()
			status := domain.PipelineJobRunStatusFailed
			if errors.Is(lastErr, context.Canceled) {
				status = domain.PipelineJobRunStatusCancelled
			}
			errorCode := classifyPipelineErrorCode(lastErr)
			_ = s.runs.UpdateJobRunFinished(persistCtx, jobRunID, status, &errMsg, errorCode, attemptCount)
			s.logJobRunEvent(persistCtx, runID, jobRunID, eventTypeForJobStatus(status), &errMsg, errorCode, map[string]any{"attempt_count": attemptCount})
		}
		return lastErr
	}

	if jobRunID != "" {
		_ = s.runs.UpdateJobRunFinished(persistCtx, jobRunID, domain.PipelineJobRunStatusSuccess, nil, nil, attemptCount)
		s.logJobRunEvent(persistCtx, runID, jobRunID, domain.PipelineRunEventSucceeded, pipelineMessagePtr("job completed successfully"), nil, map[string]any{"attempt_count": attemptCount})
	}
	return nil
}

// executeJobAttempt runs one attempt of a job on a fresh pinned connection.
func (s *Service) executeJobAttempt(ctx context.Context, pipelineDef *domain.Pipeline, job domain.PipelineJob,
	params map[string]string, principal string, logger *slog.Logger) error {

	// Handle MODEL_RUN jobs via the model runner.
	if job.JobType == domain.PipelineJobTypeModelRun {
		return s.executeModelRunJob(ctx, job, params, principal, logger)
	}

	endpoint, err := s.jobComputeEndpoint(ctx, effectiveComputeEndpointID(pipelineDef, job))
	if err != nil {
		return err
	}

	execCells, err := s.loadExecutableCells(ctx, job.NotebookID)
	if err != nil {
		return err
	}
	if endpoint != nil && strings.EqualFold(endpoint.Type, "REMOTE") {
		return s.executeRemoteNotebookJob(ctx, *endpoint, execCells, params, principal, logger)
	}

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

	// Execute each executable cell.
	for i, cell := range execCells {
		if cell.Role == domain.CellRoleTest {
			hasRows, err := s.queryHasRows(ctx, conn, principal, cell.SQL)
			if err != nil {
				return fmt.Errorf("execute test cell %s: %w", cell.ID, err)
			}
			severity := domain.NotebookTestSeverityError
			if cell.Test != nil && cell.Test.Severity != "" {
				severity = cell.Test.Severity
			}
			if hasRows && severity == domain.NotebookTestSeverityError {
				return fmt.Errorf("execute cell %d: error-severity notebook test failed", i+1)
			}
			continue
		}

		if err := s.execOnConn(ctx, conn, principal, cell.SQL); err != nil {
			return fmt.Errorf("execute cell %d: %w", i+1, err)
		}
	}

	logger.Info("job completed successfully")
	return nil
}

func (s *Service) loadExecutableCells(ctx context.Context, notebookID string) ([]domain.NotebookExecutableCell, error) {
	execCells, err := s.notebooks.GetExecutableCells(ctx, notebookID)
	if err == nil {
		return execCells, nil
	}
	if !errors.As(err, new(*domain.NotImplementedError)) {
		return nil, fmt.Errorf("get notebook executable SQL: %w", err)
	}

	blocks, blockErr := s.notebooks.GetSQLBlocks(ctx, notebookID)
	if blockErr != nil {
		return nil, fmt.Errorf("get notebook SQL: %w", blockErr)
	}
	execCells = make([]domain.NotebookExecutableCell, 0, len(blocks))
	for _, block := range blocks {
		execCells = append(execCells, domain.NotebookExecutableCell{SQL: block, Role: domain.CellRoleTransform})
	}
	return execCells, nil
}

func (s *Service) jobComputeEndpoint(ctx context.Context, computeEndpointID *string) (*domain.ComputeEndpoint, error) {
	if computeEndpointID == nil || strings.TrimSpace(*computeEndpointID) == "" {
		return nil, nil
	}
	if s.computeRepo == nil {
		return nil, domain.ErrValidation("compute_endpoint_id is configured but compute endpoint repository is unavailable")
	}
	endpoint, err := s.computeRepo.GetByID(ctx, strings.TrimSpace(*computeEndpointID))
	if err != nil {
		return nil, fmt.Errorf("resolve compute endpoint: %w", err)
	}
	return endpoint, nil
}

func (s *Service) executeRemoteNotebookJob(ctx context.Context, endpoint domain.ComputeEndpoint, execCells []domain.NotebookExecutableCell,
	params map[string]string, principal string, logger *slog.Logger) error {

	execCtx := domain.WithComputeExecutionRequest(ctx, domain.ComputeExecutionRequest{
		Mode:                  domain.ComputeModeSharedEndpoint,
		EndpointName:          endpoint.Name,
		WorkloadType:          domain.ComputeWorkloadNotebook,
		AuthoritativeEndpoint: true,
		FallbackLocal:         false,
	})

	script, err := compileRemoteNotebookScript(execCells, params)
	if err != nil {
		return err
	}
	if err := s.execQuery(execCtx, principal, script); err != nil {
		return fmt.Errorf("execute notebook job on compute endpoint %s: %w", endpoint.Name, err)
	}

	logger.Info("remote notebook job completed successfully", "compute_endpoint", endpoint.Name)
	return nil
}

func (s *Service) queryHasRows(ctx context.Context, conn *sql.Conn, principal, query string) (bool, error) {
	rows, err := s.engine.QueryOnConn(ctx, conn, principal, query)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	hasRows := rows.Next()
	if err := rows.Err(); err != nil {
		return false, err
	}
	return hasRows, nil
}

// executeModelRunJob triggers a synchronous model run via the ModelRunner interface.
func (s *Service) executeModelRunJob(ctx context.Context, job domain.PipelineJob,
	params map[string]string, principal string, logger *slog.Logger) error {

	if s.modelRunner == nil {
		return fmt.Errorf("model runner not configured")
	}

	targetCatalog := params["target_catalog"]
	targetSchema := params["target_schema"]
	if targetCatalog == "" {
		targetCatalog = "main"
	}
	if targetSchema == "" {
		targetSchema = "main"
	}

	req := domain.TriggerModelRunRequest{
		TargetCatalog: targetCatalog,
		TargetSchema:  targetSchema,
		Selector:      job.ModelSelector,
		TriggerType:   domain.ModelTriggerTypePipeline,
		Variables:     params,
	}

	logger.Info("triggering model run", "selector", job.ModelSelector)
	if err := s.modelRunner.TriggerRunSync(ctx, principal, req); err != nil {
		return fmt.Errorf("model run: %w", err)
	}

	logger.Info("model run job completed successfully")
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

func (s *Service) execQuery(ctx context.Context, principal, query string) error {
	rows, err := s.engine.Query(ctx, principal, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	return rows.Err()
}

func remoteNotebookTestFailureSQL(query string) string {
	return fmt.Sprintf(
		"SELECT CAST(COUNT(*) || ' error-severity notebook test failed' AS INTEGER) AS __pipeline_test_failure FROM (%s) AS __pipeline_test_result HAVING COUNT(*) > 0",
		query,
	)
}

func compileRemoteNotebookScript(execCells []domain.NotebookExecutableCell, params map[string]string) (string, error) {
	var builder strings.Builder
	for key, value := range params {
		if !isValidVariableName(key) {
			return "", fmt.Errorf("set variable: %w", domain.ErrValidation("invalid variable name: %s", key))
		}
		escaped := strings.ReplaceAll(value, "'", "''")
		appendSQLStatement(&builder, fmt.Sprintf("SET VARIABLE %s = '%s'", key, escaped))
	}

	for _, cell := range execCells {
		sqlToRun := cell.SQL
		if cell.Role == domain.CellRoleTest {
			severity := domain.NotebookTestSeverityError
			if cell.Test != nil && cell.Test.Severity != "" {
				severity = cell.Test.Severity
			}
			if severity == domain.NotebookTestSeverityError {
				sqlToRun = remoteNotebookTestFailureSQL(cell.SQL)
			} else {
				sqlToRun = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS __pipeline_test_warn", cell.SQL)
			}
		}
		appendSQLStatement(&builder, sqlToRun)
	}

	return strings.TrimSpace(builder.String()), nil
}

func appendSQLStatement(builder *strings.Builder, sqlText string) {
	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return
	}
	builder.WriteString(trimmed)
	if !strings.HasSuffix(trimmed, ";") {
		builder.WriteString(";")
	}
	builder.WriteString("\n")
}

func effectiveComputeEndpointID(pipelineDef *domain.Pipeline, job domain.PipelineJob) *string {
	if job.ComputeEndpointID != nil && strings.TrimSpace(*job.ComputeEndpointID) != "" {
		return job.ComputeEndpointID
	}
	if pipelineDef != nil {
		return pipelineDef.DefaultComputeEndpointID
	}
	return nil
}

func effectiveJobTimeoutSeconds(pipelineDef *domain.Pipeline, job domain.PipelineJob) *int64 {
	if job.TimeoutSeconds != nil {
		return job.TimeoutSeconds
	}
	if pipelineDef != nil {
		return pipelineDef.DefaultTimeoutSeconds
	}
	return nil
}

func effectiveJobRetryCount(pipelineDef *domain.Pipeline, job domain.PipelineJob) int {
	if job.RetryCount > 0 {
		return job.RetryCount
	}
	if pipelineDef != nil && pipelineDef.DefaultRetryCount != nil && *pipelineDef.DefaultRetryCount > 0 {
		return *pipelineDef.DefaultRetryCount
	}
	return job.RetryCount
}

func eventTypeForJobStatus(status string) string {
	switch status {
	case domain.PipelineJobRunStatusCancelled:
		return domain.PipelineRunEventCancelled
	case domain.PipelineJobRunStatusFailed:
		return domain.PipelineRunEventFailed
	default:
		return domain.PipelineRunEventSucceeded
	}
}
