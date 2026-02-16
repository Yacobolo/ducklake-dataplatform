package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// Compile-time checks.
var _ domain.ModelTestRepository = (*ModelTestRepo)(nil)
var _ domain.ModelTestResultRepository = (*ModelTestResultRepo)(nil)

// ModelTestRepo implements ModelTestRepository using SQLite.
type ModelTestRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

// NewModelTestRepo creates a new ModelTestRepo.
func NewModelTestRepo(db *sql.DB) *ModelTestRepo {
	return &ModelTestRepo{q: dbstore.New(db), db: db}
}

// Create inserts a new model test.
func (r *ModelTestRepo) Create(ctx context.Context, test *domain.ModelTest) (*domain.ModelTest, error) {
	configJSON, err := json.Marshal(test.Config)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}

	row, err := r.q.CreateModelTest(ctx, dbstore.CreateModelTestParams{
		ID:         newID(),
		ModelID:    test.ModelID,
		Name:       test.Name,
		TestType:   test.TestType,
		ColumnName: test.Column,
		Config:     string(configJSON),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return modelTestFromDB(row), nil
}

// GetByID returns a model test by its ID.
func (r *ModelTestRepo) GetByID(ctx context.Context, id string) (*domain.ModelTest, error) {
	row, err := r.q.GetModelTestByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return modelTestFromDB(row), nil
}

// ListByModel returns all tests for a model.
func (r *ModelTestRepo) ListByModel(ctx context.Context, modelID string) ([]domain.ModelTest, error) {
	rows, err := r.q.ListModelTestsByModel(ctx, modelID)
	if err != nil {
		return nil, err
	}

	tests := make([]domain.ModelTest, 0, len(rows))
	for _, row := range rows {
		tests = append(tests, *modelTestFromDB(row))
	}
	return tests, nil
}

// Delete removes a model test by ID.
func (r *ModelTestRepo) Delete(ctx context.Context, id string) error {
	return mapDBError(r.q.DeleteModelTest(ctx, id))
}

// === Private mapper ===

func modelTestFromDB(row dbstore.ModelTest) *domain.ModelTest {
	createdAt, err := time.Parse("2006-01-02 15:04:05", row.CreatedAt)
	if err != nil {
		slog.Default().Warn("failed to parse model_test created_at", "value", row.CreatedAt, "error", err)
	}

	var config domain.ModelTestConfig
	_ = json.Unmarshal([]byte(row.Config), &config)

	return &domain.ModelTest{
		ID:        row.ID,
		ModelID:   row.ModelID,
		Name:      row.Name,
		TestType:  row.TestType,
		Column:    row.ColumnName,
		Config:    config,
		CreatedAt: createdAt,
	}
}

// === ModelTestResultRepo ===

// ModelTestResultRepo implements ModelTestResultRepository using SQLite.
type ModelTestResultRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

// NewModelTestResultRepo creates a new ModelTestResultRepo.
func NewModelTestResultRepo(db *sql.DB) *ModelTestResultRepo {
	return &ModelTestResultRepo{q: dbstore.New(db), db: db}
}

// Create inserts a new model test result.
func (r *ModelTestResultRepo) Create(ctx context.Context, result *domain.ModelTestResult) (*domain.ModelTestResult, error) {
	var rowsReturned sql.NullInt64
	if result.RowsReturned != nil {
		rowsReturned = sql.NullInt64{Int64: *result.RowsReturned, Valid: true}
	}

	row, err := r.q.CreateModelTestResult(ctx, dbstore.CreateModelTestResultParams{
		ID:           newID(),
		RunStepID:    result.RunStepID,
		TestID:       result.TestID,
		TestName:     result.TestName,
		Status:       result.Status,
		RowsReturned: rowsReturned,
		ErrorMessage: nullStrFromPtr(result.ErrorMessage),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return modelTestResultFromDB(row), nil
}

// ListByStep returns all test results for a run step.
func (r *ModelTestResultRepo) ListByStep(ctx context.Context, runStepID string) ([]domain.ModelTestResult, error) {
	rows, err := r.q.ListModelTestResultsByStep(ctx, runStepID)
	if err != nil {
		return nil, err
	}

	results := make([]domain.ModelTestResult, 0, len(rows))
	for _, row := range rows {
		results = append(results, *modelTestResultFromDB(row))
	}
	return results, nil
}

// === Private mapper ===

func modelTestResultFromDB(row dbstore.ModelTestResult) *domain.ModelTestResult {
	createdAt, _ := time.Parse("2006-01-02 15:04:05", row.CreatedAt)

	var rowsReturned *int64
	if row.RowsReturned.Valid {
		rowsReturned = &row.RowsReturned.Int64
	}

	var errMsg *string
	if row.ErrorMessage.Valid {
		errMsg = &row.ErrorMessage.String
	}

	return &domain.ModelTestResult{
		ID:           row.ID,
		RunStepID:    row.RunStepID,
		TestID:       row.TestID,
		TestName:     row.TestName,
		Status:       row.Status,
		RowsReturned: rowsReturned,
		ErrorMessage: errMsg,
		CreatedAt:    createdAt,
	}
}
