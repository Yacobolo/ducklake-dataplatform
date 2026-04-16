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
func (s *Service) executeJob(ctx context.Context, job domain.PipelineJob,
	jobRunID string, params map[string]string, principal string, logger *slog.Logger) error {

	logger = logger.With("job_id", job.ID, "job_name", job.Name)
	persistCtx := context.Background()

	if jobRunID != "" {
		if err := s.runs.UpdateJobRunStarted(persistCtx, jobRunID); err != nil {
			return fmt.Errorf("mark job run started: %w", err)
		}
	}

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
		if jobRunID != "" {
			errMsg := lastErr.Error()
			status := domain.PipelineJobRunStatusFailed
			if errors.Is(lastErr, context.Canceled) {
				status = domain.PipelineJobRunStatusCancelled
			}
			_ = s.runs.UpdateJobRunFinished(persistCtx, jobRunID, status, &errMsg)
		}
		return lastErr
	}

	if jobRunID != "" {
		_ = s.runs.UpdateJobRunFinished(persistCtx, jobRunID, domain.PipelineJobRunStatusSuccess, nil)
	}
	return nil
}

// executeJobAttempt runs one attempt of a job on a fresh pinned connection.
func (s *Service) executeJobAttempt(ctx context.Context, job domain.PipelineJob,
	params map[string]string, principal string, logger *slog.Logger) error {

	// Handle MODEL_RUN jobs via the model runner.
	if job.JobType == domain.PipelineJobTypeModelRun {
		return s.executeModelRunJob(ctx, job, params, principal, logger)
	}

	endpoint, err := s.jobComputeEndpoint(ctx, job)
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

func (s *Service) jobComputeEndpoint(ctx context.Context, job domain.PipelineJob) (*domain.ComputeEndpoint, error) {
	if job.ComputeEndpointID == nil || strings.TrimSpace(*job.ComputeEndpointID) == "" {
		return nil, nil
	}
	if s.computeRepo == nil {
		return nil, domain.ErrValidation("compute_endpoint_id is configured but compute endpoint repository is unavailable")
	}
	endpoint, err := s.computeRepo.GetByID(ctx, strings.TrimSpace(*job.ComputeEndpointID))
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

	for k, v := range params {
		if !isValidVariableName(k) {
			return fmt.Errorf("set variable: %w", domain.ErrValidation("invalid variable name: %s", k))
		}
		escaped := strings.ReplaceAll(v, "'", "''")
		setSQL := fmt.Sprintf("SET VARIABLE %s = '%s'", k, escaped)
		if err := s.execQuery(execCtx, principal, setSQL); err != nil {
			return fmt.Errorf("set variable %s on compute endpoint %s: %w", k, endpoint.Name, err)
		}
	}

	for i, cell := range execCells {
		sqlToRun := cell.SQL
		if cell.Role == domain.CellRoleTest {
			severity := domain.NotebookTestSeverityError
			if cell.Test != nil && cell.Test.Severity != "" {
				severity = cell.Test.Severity
			}
			if severity == domain.NotebookTestSeverityError {
				sqlToRun = remoteNotebookTestFailureSQL(cell.SQL)
			}
		}

		if err := s.execQuery(execCtx, principal, sqlToRun); err != nil {
			return fmt.Errorf("execute cell %d on compute endpoint %s: %w", i+1, endpoint.Name, err)
		}
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
