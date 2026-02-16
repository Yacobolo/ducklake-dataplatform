package model

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"duck-demo/internal/domain"
)

var validVariableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ExecutionConfig holds the target resolution for a model run.
type ExecutionConfig struct {
	TargetCatalog string
	TargetSchema  string
	Variables     map[string]string
}

// executeRun processes a model run in a background goroutine.
func (s *Service) executeRun(ctx context.Context, runID string,
	_ []domain.Model, tiers [][]DAGNode, config ExecutionConfig, principal string) {

	logger := s.logger.With("run_id", runID)

	defer s.runCancels.Delete(runID)

	defer func() {
		if r := recover(); r != nil {
			errMsg := fmt.Sprintf("panic: %v", r)
			logger.Error("model run panicked", "error", errMsg)
			_ = s.runs.UpdateRunFinished(ctx, runID, domain.ModelRunStatusFailed, &errMsg)
		}
	}()

	if err := s.runs.UpdateRunStarted(ctx, runID); err != nil {
		logger.Error("failed to update run started", "error", err)
		return
	}

	// Build step map
	steps, err := s.runs.ListStepsByRun(ctx, runID)
	if err != nil {
		errMsg := fmt.Sprintf("list steps: %v", err)
		_ = s.runs.UpdateRunFinished(ctx, runID, domain.ModelRunStatusFailed, &errMsg)
		return
	}
	stepByModelID := make(map[string]string, len(steps))
	for _, st := range steps {
		stepByModelID[st.ModelID] = st.ID
	}

	runFailed := false
	cancelled := false

	for _, tier := range tiers {
		if runFailed || cancelled {
			status := domain.ModelRunStatusSkipped
			if cancelled {
				status = domain.ModelRunStatusCancelled
			}
			for _, node := range tier {
				stepID := stepByModelID[node.Model.ID]
				_ = s.runs.UpdateStepFinished(ctx, stepID, status, nil, nil)
			}
			continue
		}

		for _, node := range tier {
			if ctx.Err() != nil {
				cancelled = true
				stepID := stepByModelID[node.Model.ID]
				_ = s.runs.UpdateStepFinished(ctx, stepID, domain.ModelRunStatusCancelled, nil, nil)
				continue
			}

			if runFailed {
				stepID := stepByModelID[node.Model.ID]
				_ = s.runs.UpdateStepFinished(ctx, stepID, domain.ModelRunStatusSkipped, nil, nil)
				continue
			}

			stepID := stepByModelID[node.Model.ID]
			if err := s.runs.UpdateStepStarted(ctx, stepID); err != nil {
				logger.Error("failed to update step started", "model", node.Model.QualifiedName(), "error", err)
			}

			rowsAffected, err := s.executeSingleModel(ctx, node.Model, config, principal, logger)
			if err != nil {
				runFailed = true
				errMsg := err.Error()
				_ = s.runs.UpdateStepFinished(ctx, stepID, domain.ModelRunStatusFailed, nil, &errMsg)
				continue
			}

			// Post-materialization: contract validation and tests
			if err := s.postMaterialize(ctx, node.Model, config, stepID, principal, logger); err != nil {
				runFailed = true
				errMsg := err.Error()
				_ = s.runs.UpdateStepFinished(ctx, stepID, domain.ModelRunStatusFailed, rowsAffected, &errMsg)
				continue
			}

			_ = s.runs.UpdateStepFinished(ctx, stepID, domain.ModelRunStatusSuccess, rowsAffected, nil)
		}
	}

	switch {
	case cancelled:
		errMsg := "run was cancelled"
		_ = s.runs.UpdateRunFinished(ctx, runID, domain.ModelRunStatusCancelled, &errMsg)
	case runFailed:
		errMsg := "one or more models failed"
		_ = s.runs.UpdateRunFinished(ctx, runID, domain.ModelRunStatusFailed, &errMsg)
	default:
		_ = s.runs.UpdateRunFinished(ctx, runID, domain.ModelRunStatusSuccess, nil)
	}
}

// loadMacros creates all macros on the connection before model execution.
func (s *Service) loadMacros(ctx context.Context, conn *sql.Conn, principal string) error {
	if s.macros == nil {
		return nil
	}

	macros, err := s.macros.ListAll(ctx)
	if err != nil {
		s.logger.Warn("failed to load macros", "error", err)
		return nil // non-fatal: continue without macros
	}

	for _, m := range macros {
		var paramList string
		if len(m.Parameters) > 0 {
			paramList = strings.Join(m.Parameters, ", ")
		}

		var ddl string
		switch m.MacroType {
		case domain.MacroTypeTable:
			ddl = fmt.Sprintf("CREATE OR REPLACE MACRO %s(%s) AS TABLE %s",
				quoteIdent(m.Name), paramList, m.Body)
		default:
			ddl = fmt.Sprintf("CREATE OR REPLACE MACRO %s(%s) AS %s",
				quoteIdent(m.Name), paramList, m.Body)
		}

		if err := s.execOnConn(ctx, conn, principal, ddl); err != nil {
			s.logger.Warn("failed to create macro", "macro", m.Name, "error", err)
			// Non-fatal: continue with other macros.
		}
	}
	return nil
}

// executeSingleModel materializes one model on a pinned DuckDB connection.
func (s *Service) executeSingleModel(ctx context.Context, model *domain.Model,
	config ExecutionConfig, principal string, logger *slog.Logger) (*int64, error) {

	logger = logger.With("model", model.QualifiedName(), "materialization", model.Materialization)

	conn, err := s.duckDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Load macros before materialization.
	if err := s.loadMacros(ctx, conn, principal); err != nil {
		return nil, err
	}

	if err := s.injectVariables(ctx, conn, config, model, principal); err != nil {
		return nil, err
	}

	switch model.Materialization {
	case domain.MaterializationView:
		if err := s.materializeView(ctx, conn, model, config, principal); err != nil {
			return nil, err
		}
		logger.Info("view materialized")
		return nil, nil
	case domain.MaterializationTable:
		n, err := s.materializeTable(ctx, conn, model, config, principal)
		if err != nil {
			return nil, err
		}
		logger.Info("table materialized", "rows", n)
		return &n, nil
	case domain.MaterializationIncremental:
		n, err := s.materializeIncremental(ctx, conn, model, config, principal)
		if err != nil {
			return nil, err
		}
		logger.Info("incremental materialized", "rows", n)
		return &n, nil
	default:
		return nil, fmt.Errorf("unsupported materialization: %s", model.Materialization)
	}
}

func (s *Service) injectVariables(ctx context.Context, conn *sql.Conn,
	config ExecutionConfig, model *domain.Model, principal string) error {
	vars := map[string]string{
		"target_catalog": config.TargetCatalog,
		"target_schema":  config.TargetSchema,
		"model_name":     model.Name,
		"project_name":   model.ProjectName,
	}
	for k, v := range config.Variables {
		vars[k] = v
	}
	for k, v := range vars {
		if !validVariableName.MatchString(k) {
			return fmt.Errorf("set variable: %w",
				domain.ErrValidation("invalid variable name: %s", k))
		}
		escaped := strings.ReplaceAll(v, "'", "''")
		setSQL := fmt.Sprintf("SET VARIABLE %s = '%s'", k, escaped)
		if err := s.execOnConn(ctx, conn, principal, setSQL); err != nil {
			return fmt.Errorf("set variable %s: %w", k, err)
		}
	}
	return nil
}

func (s *Service) materializeView(ctx context.Context, conn *sql.Conn,
	model *domain.Model, config ExecutionConfig, principal string) error {
	ddl := fmt.Sprintf("CREATE OR REPLACE VIEW %s.%s AS (%s)",
		quoteIdent(config.TargetSchema), quoteIdent(model.Name), model.SQL)
	return s.execOnConn(ctx, conn, principal, ddl)
}

func (s *Service) materializeTable(ctx context.Context, conn *sql.Conn,
	model *domain.Model, config ExecutionConfig, principal string) (int64, error) {
	ddl := fmt.Sprintf("CREATE OR REPLACE TABLE %s.%s AS (%s)",
		quoteIdent(config.TargetSchema), quoteIdent(model.Name), model.SQL)
	// Execute and count rows via a separate count query
	if err := s.execOnConn(ctx, conn, principal, ddl); err != nil {
		return 0, err
	}
	// Count rows in the materialized table
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s",
		quoteIdent(config.TargetSchema), quoteIdent(model.Name))
	rows, err := s.engine.QueryOnConn(ctx, conn, principal, countSQL)
	if err != nil {
		return 0, nil // non-fatal: count failure doesn't fail materialization
	}
	defer func() { _ = rows.Close() }()
	var count int64
	if rows.Next() {
		_ = rows.Scan(&count)
	}
	if err := rows.Err(); err != nil {
		s.logger.Warn("row count error", "error", err)
	}
	return count, nil
}

func (s *Service) execOnConn(ctx context.Context, conn *sql.Conn, principal, query string) error {
	rows, err := s.engine.QueryOnConn(ctx, conn, principal, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	return rows.Err()
}

// postMaterialize runs contract validation and tests after a model is materialized.
// It acquires its own connection to avoid lifecycle issues with the materialization connection.
func (s *Service) postMaterialize(ctx context.Context, model *domain.Model,
	config ExecutionConfig, stepID, principal string, logger *slog.Logger) error {

	conn, err := s.duckDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for post-materialization: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Contract validation
	if err := s.validateContract(ctx, conn, model, config, principal); err != nil {
		logger.Error("contract validation failed", "model", model.QualifiedName(), "error", err)
		return err
	}

	// Test execution
	if s.tests != nil {
		anyFailed, err := s.executeTests(ctx, conn, model, config, stepID, principal)
		if err != nil {
			logger.Error("test execution error", "model", model.QualifiedName(), "error", err)
			return fmt.Errorf("test execution for %s: %w", model.QualifiedName(), err)
		}
		if anyFailed {
			return fmt.Errorf("one or more tests failed for %s", model.QualifiedName())
		}
	}

	return nil
}

func (s *Service) materializeIncremental(ctx context.Context, conn *sql.Conn,
	model *domain.Model, config ExecutionConfig, principal string) (int64, error) {

	targetFQN := quoteIdent(config.TargetSchema) + "." + quoteIdent(model.Name)

	// Check if target table exists
	exists, err := s.tableExists(ctx, conn, config.TargetSchema, model.Name, principal)
	if err != nil {
		return 0, fmt.Errorf("check table existence: %w", err)
	}

	if !exists {
		// First run: full refresh
		return s.materializeTable(ctx, conn, model, config, principal)
	}

	// Incremental: MERGE INTO
	uniqueKeys := model.Config.UniqueKey
	if len(uniqueKeys) == 0 {
		return 0, domain.ErrValidation("incremental model %s requires unique_key in config", model.Name)
	}

	// Build ON clause
	onParts := make([]string, len(uniqueKeys))
	for i, key := range uniqueKeys {
		qk := quoteIdent(key)
		onParts[i] = fmt.Sprintf("target.%s = source.%s", qk, qk)
	}
	onClause := strings.Join(onParts, " AND ")

	mergeSQL := fmt.Sprintf(
		"MERGE INTO %s AS target USING (%s) AS source ON %s WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
		targetFQN, model.SQL, onClause)

	if err := s.execOnConn(ctx, conn, principal, mergeSQL); err != nil {
		return 0, err
	}

	// Count total rows
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", targetFQN)
	rows, err := s.engine.QueryOnConn(ctx, conn, principal, countSQL)
	if err != nil {
		return 0, nil
	}
	defer func() { _ = rows.Close() }()
	var count int64
	if rows.Next() {
		_ = rows.Scan(&count)
	}
	if err := rows.Err(); err != nil {
		s.logger.Warn("row count error", "error", err)
	}
	return count, nil
}

func (s *Service) tableExists(ctx context.Context, conn *sql.Conn, schema, table, principal string) (bool, error) {
	checkSQL := fmt.Sprintf(
		"SELECT 1 FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'",
		strings.ReplaceAll(schema, "'", "''"),
		strings.ReplaceAll(table, "'", "''"),
	)
	rows, err := s.engine.QueryOnConn(ctx, conn, principal, checkSQL)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	exists := rows.Next()
	if err := rows.Err(); err != nil {
		return false, err
	}
	return exists, nil
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// resolveEphemeralModels injects ephemeral model SQL as CTEs into downstream models.
// Returns only the materializable models (non-ephemeral).
func resolveEphemeralModels(models []domain.Model) []domain.Model {
	// Index ephemeral models
	ephemeral := make(map[string]*domain.Model)
	for i, m := range models {
		if m.Materialization == domain.MaterializationEphemeral {
			ephemeral[m.QualifiedName()] = &models[i]
		}
	}
	if len(ephemeral) == 0 {
		return models
	}

	// For each non-ephemeral model, inject dependent ephemeral CTEs
	var result []domain.Model
	for _, m := range models {
		if m.Materialization == domain.MaterializationEphemeral {
			continue // skip ephemeral models from execution
		}

		// Collect ephemeral deps (recursive)
		ctes := collectEphemeralCTEs(&m, ephemeral, make(map[string]bool))
		if len(ctes) > 0 {
			// Prepend CTEs to the model SQL
			var cteParts []string
			for _, cte := range ctes {
				cteParts = append(cteParts, fmt.Sprintf("%s AS (%s)", quoteIdent(cte.Name), cte.SQL))
			}
			m.SQL = "WITH " + strings.Join(cteParts, ", ") + " " + m.SQL
		}
		result = append(result, m)
	}
	return result
}

// collectEphemeralCTEs recursively collects ephemeral model CTEs needed by a model.
func collectEphemeralCTEs(model *domain.Model, ephemeral map[string]*domain.Model, visited map[string]bool) []domain.Model {
	var ctes []domain.Model
	for _, dep := range model.DependsOn {
		if visited[dep] {
			continue
		}
		if eph, ok := ephemeral[dep]; ok {
			visited[dep] = true
			// Recursively resolve ephemeral deps of this ephemeral
			nested := collectEphemeralCTEs(eph, ephemeral, visited)
			ctes = append(ctes, nested...)
			ctes = append(ctes, *eph)
		}
	}
	return ctes
}
