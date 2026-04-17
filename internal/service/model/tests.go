package model

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
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
		testSQL, err := generateTestSQL(test, effectiveSchema(config.TargetSchema, model.Config.Schema), model.Name)
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

// executeNotebookCellTests runs notebook test-role cells for models linked from notebooks.
// Returns true if any error-severity test cell failed.
func (s *Service) executeNotebookCellTests(ctx context.Context, conn *sql.Conn, model *domain.Model, stepID, principal string) (bool, error) {
	if s.notebookLinks == nil || s.notebooks == nil {
		return false, nil
	}

	link, err := s.notebookLinks.GetByModelID(ctx, model.ID)
	if err != nil {
		if errors.As(err, new(*domain.NotFoundError)) {
			return false, nil
		}
		return false, fmt.Errorf("get notebook-model link for %s: %w", model.QualifiedName(), err)
	}

	cells, err := s.notebooks.ListCells(ctx, link.NotebookID)
	if err != nil {
		return false, fmt.Errorf("list notebook cells: %w", err)
	}

	anyFailed := false
	for _, cell := range cells {
		if cell.CellType != domain.CellTypeSQL || cell.Disabled || cell.Role != domain.CellRoleTest {
			continue
		}
		hasRows, queryErr := s.runTestQuery(ctx, conn, principal, cell.Content)
		if queryErr != nil {
			return false, fmt.Errorf("execute notebook test cell %s: %w", cell.ID, queryErr)
		}
		severity := domain.NotebookTestSeverityError
		if cell.Test != nil && cell.Test.Severity != "" {
			severity = cell.Test.Severity
		}
		if hasRows && severity == domain.NotebookTestSeverityError {
			anyFailed = true
		}

		if s.testResults != nil {
			status := domain.TestResultPass
			if hasRows {
				status = domain.TestResultFail
			}
			rowsReturned := int64(0)
			if hasRows {
				rowsReturned = 1
			}
			testName := cell.ID
			if cell.Name != nil && *cell.Name != "" {
				testName = *cell.Name
			}
			if _, err := s.testResults.Create(ctx, &domain.ModelTestResult{
				ID:           domain.NewID(),
				RunStepID:    stepID,
				TestID:       cell.ID,
				TestName:     "notebook:" + testName,
				Status:       status,
				RowsReturned: &rowsReturned,
			}); err != nil {
				return false, fmt.Errorf("persist notebook test result for cell %s: %w", cell.ID, err)
			}
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
