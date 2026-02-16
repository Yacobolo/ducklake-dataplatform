package model

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

// generateTestSQL builds the SQL query for a model test.
// The test passes if 0 rows are returned.
func generateTestSQL(test domain.ModelTest, targetSchema, modelName string) (string, error) {
	fqn := quoteIdent(targetSchema) + "." + quoteIdent(modelName)
	col := quoteIdent(test.Column)

	switch test.TestType {
	case domain.TestTypeNotNull:
		return fmt.Sprintf("SELECT * FROM %s WHERE %s IS NULL LIMIT 1", fqn, col), nil
	case domain.TestTypeUnique:
		return fmt.Sprintf("SELECT %s, COUNT(*) AS cnt FROM %s GROUP BY %s HAVING cnt > 1 LIMIT 1", col, fqn, col), nil
	case domain.TestTypeAcceptedValues:
		if len(test.Config.Values) == 0 {
			return "", fmt.Errorf("accepted_values test requires values")
		}
		vals := make([]string, len(test.Config.Values))
		for i, v := range test.Config.Values {
			vals[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		}
		return fmt.Sprintf("SELECT * FROM %s WHERE %s NOT IN (%s) LIMIT 1",
			fqn, col, strings.Join(vals, ", ")), nil
	case domain.TestTypeRelationships:
		toFqn := quoteIdent(targetSchema) + "." + quoteIdent(test.Config.ToModel)
		toCol := quoteIdent(test.Config.ToColumn)
		return fmt.Sprintf(
			"SELECT a.%s FROM %s a LEFT JOIN %s b ON a.%s = b.%s WHERE b.%s IS NULL LIMIT 1",
			col, fqn, toFqn, col, toCol, toCol), nil
	case domain.TestTypeCustomSQL:
		return test.Config.SQL, nil
	default:
		return "", fmt.Errorf("unknown test type: %s", test.TestType)
	}
}

// executeTests runs all tests for a model after materialization.
// Returns whether any test failed and any error encountered.
func (s *Service) executeTests(ctx context.Context, conn *sql.Conn,
	model *domain.Model, config ExecutionConfig, stepID, principal string) (bool, error) {

	tests, err := s.tests.ListByModel(ctx, model.ID)
	if err != nil {
		return false, fmt.Errorf("list tests for %s: %w", model.QualifiedName(), err)
	}
	if len(tests) == 0 {
		return false, nil
	}

	anyFailed := false
	for _, test := range tests {
		testSQL, err := generateTestSQL(test, config.TargetSchema, model.Name)
		if err != nil {
			// Record error result
			s.recordTestResult(ctx, stepID, test, domain.TestResultError, nil, err.Error())
			anyFailed = true
			continue
		}

		hasRows, queryErr := s.runTestQuery(ctx, conn, principal, testSQL)
		if queryErr != nil {
			s.recordTestResult(ctx, stepID, test, domain.TestResultError, nil, queryErr.Error())
			anyFailed = true
			continue
		}

		if hasRows {
			var rowCount int64 = 1
			s.recordTestResult(ctx, stepID, test, domain.TestResultFail, &rowCount, "")
			anyFailed = true
		} else {
			var rowCount int64
			s.recordTestResult(ctx, stepID, test, domain.TestResultPass, &rowCount, "")
		}
	}

	return anyFailed, nil
}

// runTestQuery executes a test query and returns whether any rows were returned.
func (s *Service) runTestQuery(ctx context.Context, conn *sql.Conn, principal, testSQL string) (bool, error) {
	rows, err := s.engine.QueryOnConn(ctx, conn, principal, testSQL)
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

func (s *Service) recordTestResult(ctx context.Context, stepID string, test domain.ModelTest, status string, rowsReturned *int64, errMsg string) {
	result := &domain.ModelTestResult{
		RunStepID:    stepID,
		TestID:       test.ID,
		TestName:     test.Name,
		Status:       status,
		RowsReturned: rowsReturned,
	}
	if errMsg != "" {
		result.ErrorMessage = &errMsg
	}
	if s.testResults != nil {
		if _, err := s.testResults.Create(ctx, result); err != nil {
			s.logger.Warn("failed to record test result", "test", test.Name, "error", err)
		}
	}
}
