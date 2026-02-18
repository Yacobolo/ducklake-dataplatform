package model

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/sqlrewrite"
)

var validVariableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ExecutionConfig holds the target resolution for a model run.
type ExecutionConfig struct {
	TargetCatalog string
	TargetSchema  string
	Variables     map[string]string
	FullRefresh   bool
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

	if err := s.verifyMacrosLoadable(ctx, principal); err != nil {
		errMsg := fmt.Sprintf("macro validation failed: %v", err)
		for _, st := range stepsOrEmpty(ctx, s, runID) {
			_ = s.runs.UpdateStepFinished(ctx, st.ID, domain.ModelRunStatusFailed, nil, &errMsg)
		}
		_ = s.runs.UpdateRunFinished(ctx, runID, domain.ModelRunStatusFailed, &errMsg)
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
	stepMetaByModelID := make(map[string]domain.ModelRunStep, len(steps))
	for _, st := range steps {
		stepByModelID[st.ModelID] = st.ID
		stepMetaByModelID[st.ModelID] = st
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

			execModel := *node.Model
			stepMeta, ok := stepMetaByModelID[node.Model.ID]
			if !ok || stepMeta.CompiledSQL == nil || strings.TrimSpace(*stepMeta.CompiledSQL) == "" {
				runFailed = true
				errMsg := fmt.Sprintf("missing compiled SQL artifact for %s", node.Model.QualifiedName())
				_ = s.runs.UpdateStepFinished(ctx, stepID, domain.ModelRunStatusFailed, nil, &errMsg)
				continue
			}
			execModel.SQL = *stepMeta.CompiledSQL

			rowsAffected, err := s.executeSingleModel(ctx, &execModel, config, principal, logger)
			if err != nil {
				runFailed = true
				errMsg := err.Error()
				_ = s.runs.UpdateStepFinished(ctx, stepID, domain.ModelRunStatusFailed, nil, &errMsg)
				continue
			}

			// Post-materialization: contract validation and tests
			if err := s.postMaterialize(ctx, &execModel, config, stepID, principal, logger); err != nil {
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

func (s *Service) verifyMacrosLoadable(ctx context.Context, principal string) error {
	if s.macros == nil {
		return nil
	}
	conn, err := s.duckDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for macro validation: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if err := s.loadMacros(ctx, conn, principal); err != nil {
		return err
	}
	return nil
}

func stepsOrEmpty(ctx context.Context, s *Service, runID string) []domain.ModelRunStep {
	steps, err := s.runs.ListStepsByRun(ctx, runID)
	if err != nil {
		return nil
	}
	return steps
}

// loadMacros creates all macros on the connection before model execution.
func (s *Service) loadMacros(ctx context.Context, conn *sql.Conn, principal string) error {
	if s.macros == nil {
		return nil
	}

	macros, err := s.macros.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list macros: %w", err)
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
			return fmt.Errorf("create macro %q: %w", m.Name, err)
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
	case domain.MaterializationSeed:
		n, err := s.materializeSeed(ctx, conn, model, config, principal)
		if err != nil {
			return nil, err
		}
		logger.Info("seed materialized", "rows", n)
		return &n, nil
	case domain.MaterializationSnapshot:
		n, err := s.materializeSnapshot(ctx, conn, model, config, principal)
		if err != nil {
			return nil, err
		}
		logger.Info("snapshot materialized", "rows", n)
		return &n, nil
	default:
		return nil, fmt.Errorf("unsupported materialization: %s", model.Materialization)
	}
}

func (s *Service) materializeSeed(ctx context.Context, conn *sql.Conn,
	model *domain.Model, config ExecutionConfig, principal string) (int64, error) {
	return s.materializeTable(ctx, conn, model, config, principal)
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
	relation := relationFQN(config.TargetCatalog, config.TargetSchema, model.Name)
	ddl := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS (%s)", relation, model.SQL)
	return s.execOnConn(ctx, conn, principal, ddl)
}

func (s *Service) materializeTable(ctx context.Context, conn *sql.Conn,
	model *domain.Model, config ExecutionConfig, principal string) (int64, error) {
	relation := relationFQN(config.TargetCatalog, config.TargetSchema, model.Name)
	ddl := fmt.Sprintf("CREATE OR REPLACE TABLE %s AS (%s)", relation, model.SQL)
	// Execute and count rows via a separate count query
	if err := s.execOnConn(ctx, conn, principal, ddl); err != nil {
		return 0, err
	}
	// Count rows in the materialized table
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", relation)
	rows, err := s.engine.QueryOnConn(ctx, conn, principal, countSQL)
	if err != nil {
		return 0, fmt.Errorf("count materialized rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var count int64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, fmt.Errorf("scan materialized row count: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		s.logger.Warn("row count error", "error", err)
	}
	return count, nil
}

func (s *Service) execOnConn(ctx context.Context, conn *sql.Conn, principal, query string) error {
	stmtType, err := sqlrewrite.ClassifyStatement(query)
	if err != nil {
		return fmt.Errorf("classify statement: %w", err)
	}

	if canDirectExecOnConn(stmtType, query) {
		if _, err := conn.ExecContext(ctx, query); err != nil {
			return err
		}
		return nil
	}

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
	if err := s.validateContract(ctx, conn, model, config); err != nil {
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
	if config.FullRefresh {
		return s.materializeTable(ctx, conn, model, config, principal)
	}

	targetFQN := relationFQN(config.TargetCatalog, config.TargetSchema, model.Name)

	// Check if target table exists
	exists, err := s.tableExists(ctx, conn, config.TargetCatalog, config.TargetSchema, model.Name, principal)
	if err != nil {
		return 0, fmt.Errorf("check table existence: %w", err)
	}

	if !exists {
		// First run: full refresh
		return s.materializeTable(ctx, conn, model, config, principal)
	}

	if err := s.enforceIncrementalSchemaPolicy(ctx, conn, model, config, principal); err != nil {
		return 0, err
	}

	// Incremental: MERGE INTO
	uniqueKeys := model.Config.UniqueKey
	if len(uniqueKeys) == 0 {
		return 0, domain.ErrValidation("incremental model %s requires unique_key in config", model.Name)
	}
	strategy := resolveIncrementalStrategy(model.Config.IncrementalStrategy)

	// Build ON clause
	onParts := make([]string, len(uniqueKeys))
	for i, key := range uniqueKeys {
		qk := quoteIdent(key)
		onParts[i] = fmt.Sprintf("target.%s = source.%s", qk, qk)
	}
	onClause := strings.Join(onParts, " AND ")

	switch strategy {
	case "merge":
		mergeSQL := fmt.Sprintf(
			"MERGE INTO %s AS target USING (%s) AS source ON %s WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
			targetFQN, model.SQL, onClause)

		if err := s.execOnConn(ctx, conn, principal, mergeSQL); err != nil {
			return 0, err
		}
	case "delete_insert", "delete+insert":
		deleteSQL := fmt.Sprintf(
			"DELETE FROM %s AS target USING (%s) AS source WHERE %s",
			targetFQN, model.SQL, onClause,
		)
		if err := s.execOnConn(ctx, conn, principal, deleteSQL); err != nil {
			return 0, err
		}

		insertSQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM (%s)", targetFQN, model.SQL)
		if err := s.execOnConn(ctx, conn, principal, insertSQL); err != nil {
			return 0, err
		}
	default:
		return 0, domain.ErrValidation("unsupported incremental_strategy %q for model %s", model.Config.IncrementalStrategy, model.Name)
	}

	// Count total rows
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", targetFQN)
	rows, err := s.engine.QueryOnConn(ctx, conn, principal, countSQL)
	if err != nil {
		return 0, fmt.Errorf("count incremental rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var count int64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, fmt.Errorf("scan incremental row count: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		s.logger.Warn("row count error", "error", err)
	}
	return count, nil
}

func (s *Service) materializeSnapshot(ctx context.Context, conn *sql.Conn,
	model *domain.Model, config ExecutionConfig, principal string) (int64, error) {
	targetFQN := relationFQN(config.TargetCatalog, config.TargetSchema, model.Name)
	exists, err := s.tableExists(ctx, conn, config.TargetCatalog, config.TargetSchema, model.Name, principal)
	if err != nil {
		return 0, fmt.Errorf("check snapshot table existence: %w", err)
	}

	if config.FullRefresh || !exists {
		createSQL := fmt.Sprintf(
			"CREATE OR REPLACE TABLE %s AS SELECT src.*, CURRENT_TIMESTAMP AS dbt_valid_from, CAST(NULL AS TIMESTAMP) AS dbt_valid_to, TRUE AS dbt_is_current FROM (%s) AS src",
			targetFQN,
			model.SQL,
		)
		if err := s.execOnConn(ctx, conn, principal, createSQL); err != nil {
			return 0, err
		}
		return s.countRows(ctx, conn, principal, targetFQN)
	}

	uniqueKeys := model.Config.UniqueKey
	if len(uniqueKeys) == 0 {
		return 0, domain.ErrValidation("snapshot model %s requires unique_key in config", model.Name)
	}

	sourceCols, err := s.queryColumns(ctx, conn, principal, fmt.Sprintf("SELECT * FROM (%s) AS source WHERE 1=0", model.SQL))
	if err != nil {
		return 0, fmt.Errorf("load snapshot source columns for %s: %w", model.QualifiedName(), err)
	}
	if len(sourceCols) == 0 {
		return 0, domain.ErrValidation("snapshot model %s compiled SQL returned no columns", model.Name)
	}

	keySet := make(map[string]struct{}, len(uniqueKeys))
	for _, k := range uniqueKeys {
		keySet[strings.ToLower(strings.TrimSpace(k))] = struct{}{}
	}

	nonKeyCols := make([]string, 0, len(sourceCols))
	for _, col := range sourceCols {
		if _, ok := keySet[col]; ok {
			continue
		}
		nonKeyCols = append(nonKeyCols, col)
	}
	if len(nonKeyCols) == 0 {
		return 0, domain.ErrValidation("snapshot model %s requires at least one non-unique_key column", model.Name)
	}

	onParts := make([]string, len(uniqueKeys))
	for i, key := range uniqueKeys {
		qk := quoteIdent(key)
		onParts[i] = fmt.Sprintf("target.%s = source.%s", qk, qk)
	}
	onClause := strings.Join(onParts, " AND ")

	changeParts := make([]string, len(nonKeyCols))
	for i, col := range nonKeyCols {
		qc := quoteIdent(col)
		changeParts[i] = fmt.Sprintf("target.%s IS DISTINCT FROM source.%s", qc, qc)
	}
	changeClause := strings.Join(changeParts, " OR ")

	updateSQL := fmt.Sprintf(
		"UPDATE %s AS target SET dbt_valid_to = CURRENT_TIMESTAMP, dbt_is_current = FALSE FROM (%s) AS source WHERE target.dbt_is_current = TRUE AND %s AND (%s)",
		targetFQN,
		model.SQL,
		onClause,
		changeClause,
	)
	if err := s.execOnConn(ctx, conn, principal, updateSQL); err != nil {
		return 0, err
	}

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s SELECT source.*, CURRENT_TIMESTAMP AS dbt_valid_from, CAST(NULL AS TIMESTAMP) AS dbt_valid_to, TRUE AS dbt_is_current FROM (%s) AS source LEFT JOIN %s AS target ON %s AND target.dbt_is_current = TRUE WHERE target.%s IS NULL OR (%s)",
		targetFQN,
		model.SQL,
		targetFQN,
		onClause,
		quoteIdent(uniqueKeys[0]),
		changeClause,
	)
	if err := s.execOnConn(ctx, conn, principal, insertSQL); err != nil {
		return 0, err
	}

	return s.countRows(ctx, conn, principal, targetFQN)
}

func (s *Service) countRows(ctx context.Context, conn *sql.Conn, principal, relation string) (int64, error) {
	rows, err := s.engine.QueryOnConn(ctx, conn, principal, fmt.Sprintf("SELECT COUNT(*) FROM %s", relation))
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

func resolveIncrementalStrategy(strategy string) string {
	norm := strings.ToLower(strings.TrimSpace(strategy))
	if norm == "" {
		return "merge"
	}
	if norm == "delete+insert" {
		return "delete_insert"
	}
	return norm
}

func resolveSchemaChangePolicy(policy string) string {
	norm := strings.ToLower(strings.TrimSpace(policy))
	if norm == "" {
		return "ignore"
	}
	return norm
}

func (s *Service) enforceIncrementalSchemaPolicy(
	ctx context.Context,
	conn *sql.Conn,
	model *domain.Model,
	config ExecutionConfig,
	principal string,
) error {
	policy := resolveSchemaChangePolicy(model.Config.OnSchemaChange)
	if policy == "ignore" {
		return nil
	}
	if policy != "fail" {
		return domain.ErrValidation("unsupported on_schema_change %q for model %s", model.Config.OnSchemaChange, model.Name)
	}

	targetCols, err := s.targetTableColumns(ctx, conn, config, model, principal)
	if err != nil {
		return fmt.Errorf("load target columns for %s: %w", model.QualifiedName(), err)
	}
	sourceCols, err := s.queryColumns(ctx, conn, principal, fmt.Sprintf("SELECT * FROM (%s) AS source WHERE 1=0", model.SQL))
	if err != nil {
		return fmt.Errorf("load source columns for %s: %w", model.QualifiedName(), err)
	}

	if !sameColumns(targetCols, sourceCols) {
		return domain.ErrValidation(
			"schema change detected for incremental model %s (target=%v source=%v) with on_schema_change=fail",
			model.QualifiedName(), targetCols, sourceCols,
		)
	}
	return nil
}

func (s *Service) targetTableColumns(
	ctx context.Context,
	conn *sql.Conn,
	config ExecutionConfig,
	model *domain.Model,
	principal string,
) ([]string, error) {
	query := fmt.Sprintf(
		"SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position",
		strings.ReplaceAll(config.TargetSchema, "'", "''"),
		strings.ReplaceAll(model.Name, "'", "''"),
	)
	rows, err := s.engine.QueryOnConn(ctx, conn, principal, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	cols := make([]string, 0)
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, strings.ToLower(strings.TrimSpace(col)))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

func (s *Service) queryColumns(ctx context.Context, conn *sql.Conn, principal, query string) ([]string, error) {
	rows, err := s.engine.QueryOnConn(ctx, conn, principal, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		out = append(out, strings.ToLower(strings.TrimSpace(c)))
	}
	return out, nil
}

func sameColumns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *Service) tableExists(ctx context.Context, conn *sql.Conn, catalog, schema, table, principal string) (bool, error) {
	var checkSQL string
	if catalog != "" {
		checkSQL = fmt.Sprintf(
			"SELECT 1 FROM information_schema.tables WHERE table_catalog = '%s' AND table_schema = '%s' AND table_name = '%s'",
			strings.ReplaceAll(catalog, "'", "''"),
			strings.ReplaceAll(schema, "'", "''"),
			strings.ReplaceAll(table, "'", "''"),
		)
	} else {
		checkSQL = fmt.Sprintf(
			"SELECT 1 FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'",
			strings.ReplaceAll(schema, "'", "''"),
			strings.ReplaceAll(table, "'", "''"),
		)
	}
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

func relationFQN(catalog, schema, name string) string {
	if catalog == "" {
		return quoteIdent(schema) + "." + quoteIdent(name)
	}
	return quoteIdent(catalog) + "." + quoteIdent(schema) + "." + quoteIdent(name)
}

func canDirectExecOnConn(stmtType sqlrewrite.StatementType, query string) bool {
	norm := strings.ToUpper(strings.TrimSpace(query))

	if strings.HasPrefix(norm, "SET VARIABLE ") {
		return true
	}

	if stmtType != sqlrewrite.StmtDDL {
		return false
	}

	allowedDDLPrefixes := []string{
		"CREATE OR REPLACE VIEW ",
		"CREATE OR REPLACE TABLE ",
		"CREATE TEMP TABLE ",
		"DROP TABLE ",
		"CREATE OR REPLACE MACRO ",
	}

	for _, p := range allowedDDLPrefixes {
		if strings.HasPrefix(norm, p) {
			return true
		}
	}

	return false
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
