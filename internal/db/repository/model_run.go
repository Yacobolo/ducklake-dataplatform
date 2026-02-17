package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// Compile-time check.
var _ domain.ModelRunRepository = (*ModelRunRepo)(nil)

// ModelRunRepo implements ModelRunRepository using SQLite.
type ModelRunRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

// NewModelRunRepo creates a new ModelRunRepo.
func NewModelRunRepo(db *sql.DB) *ModelRunRepo {
	return &ModelRunRepo{q: dbstore.New(db), db: db}
}

// CreateRun inserts a new model run.
func (r *ModelRunRepo) CreateRun(ctx context.Context, run *domain.ModelRun) (*domain.ModelRun, error) {
	varsJSON, err := json.Marshal(run.Variables)
	if err != nil {
		return nil, fmt.Errorf("marshal variables: %w", err)
	}

	row, err := r.q.CreateModelRun(ctx, dbstore.CreateModelRunParams{
		ID:                 newID(),
		Status:             run.Status,
		TriggerType:        run.TriggerType,
		TriggeredBy:        run.TriggeredBy,
		TargetCatalog:      run.TargetCatalog,
		TargetSchema:       run.TargetSchema,
		ModelSelector:      run.ModelSelector,
		Variables:          string(varsJSON),
		FullRefresh:        boolToInt64(run.FullRefresh),
		CompileManifest:    ptrToStr(run.CompileManifest),
		CompileDiagnostics: marshalCompileDiagnostics(run.CompileDiagnostics),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return modelRunFromDB(row), nil
}

// GetRunByID returns a model run by its ID.
func (r *ModelRunRepo) GetRunByID(ctx context.Context, id string) (*domain.ModelRun, error) {
	row, err := r.q.GetModelRunByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return modelRunFromDB(row), nil
}

// ListRuns returns a filtered, paginated list of model runs.
func (r *ModelRunRepo) ListRuns(ctx context.Context, filter domain.ModelRunFilter) ([]domain.ModelRun, int64, error) {
	statusFilter := ""
	if filter.Status != nil {
		statusFilter = *filter.Status
	}

	total, err := r.q.CountModelRuns(ctx, dbstore.CountModelRunsParams{
		Column1: statusFilter,
		Status:  statusFilter,
	})
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListModelRuns(ctx, dbstore.ListModelRunsParams{
		Column1: statusFilter,
		Status:  statusFilter,
		Limit:   int64(filter.Page.Limit()),
		Offset:  int64(filter.Page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	runs := make([]domain.ModelRun, 0, len(rows))
	for _, row := range rows {
		runs = append(runs, *modelRunFromDB(row))
	}
	return runs, total, nil
}

// UpdateRunStarted marks a model run as started.
func (r *ModelRunRepo) UpdateRunStarted(ctx context.Context, id string) error {
	return mapDBError(r.q.UpdateModelRunStarted(ctx, id))
}

// UpdateRunFinished marks a model run as finished with a final status.
func (r *ModelRunRepo) UpdateRunFinished(ctx context.Context, id string, status string, errMsg *string) error {
	return mapDBError(r.q.UpdateModelRunFinished(ctx, dbstore.UpdateModelRunFinishedParams{
		Status:       status,
		ErrorMessage: nullStrFromPtr(errMsg),
		ID:           id,
	}))
}

// CreateStep inserts a new model run step.
func (r *ModelRunRepo) CreateStep(ctx context.Context, step *domain.ModelRunStep) (*domain.ModelRunStep, error) {
	dependsOnJSON, err := json.Marshal(step.DependsOn)
	if err != nil {
		return nil, fmt.Errorf("marshal depends_on: %w", err)
	}
	varsUsedJSON, err := json.Marshal(step.VarsUsed)
	if err != nil {
		return nil, fmt.Errorf("marshal vars_used: %w", err)
	}
	macrosUsedJSON, err := json.Marshal(step.MacrosUsed)
	if err != nil {
		return nil, fmt.Errorf("marshal macros_used: %w", err)
	}

	row, err := r.q.CreateModelRunStep(ctx, dbstore.CreateModelRunStepParams{
		ID:           newID(),
		RunID:        step.RunID,
		ModelID:      step.ModelID,
		ModelName:    step.ModelName,
		CompiledSql:  nullStrFromPtr(step.CompiledSQL),
		CompiledHash: nullStrFromPtr(step.CompiledHash),
		DependsOn:    string(dependsOnJSON),
		VarsUsed:     string(varsUsedJSON),
		MacrosUsed:   string(macrosUsedJSON),
		Status:       step.Status,
		Tier:         int64(step.Tier),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return modelRunStepFromDB(row), nil
}

// ListStepsByRun returns all steps for a model run.
func (r *ModelRunRepo) ListStepsByRun(ctx context.Context, runID string) ([]domain.ModelRunStep, error) {
	rows, err := r.q.ListModelRunStepsByRun(ctx, runID)
	if err != nil {
		return nil, err
	}

	steps := make([]domain.ModelRunStep, 0, len(rows))
	for _, row := range rows {
		steps = append(steps, *modelRunStepFromDB(row))
	}
	return steps, nil
}

// UpdateStepStarted marks a model run step as started.
func (r *ModelRunRepo) UpdateStepStarted(ctx context.Context, id string) error {
	return mapDBError(r.q.UpdateModelRunStepStarted(ctx, id))
}

// UpdateStepFinished marks a model run step as finished with a final status.
func (r *ModelRunRepo) UpdateStepFinished(ctx context.Context, id string, status string, rowsAffected *int64, errMsg *string) error {
	var ra sql.NullInt64
	if rowsAffected != nil {
		ra = sql.NullInt64{Int64: *rowsAffected, Valid: true}
	}

	return mapDBError(r.q.UpdateModelRunStepFinished(ctx, dbstore.UpdateModelRunStepFinishedParams{
		Status:       status,
		RowsAffected: ra,
		ErrorMessage: nullStrFromPtr(errMsg),
		ID:           id,
	}))
}

// === Private mappers ===

func modelRunFromDB(row dbstore.ModelRun) *domain.ModelRun {
	createdAt, _ := time.Parse("2006-01-02 15:04:05", row.CreatedAt)

	var vars map[string]string
	_ = json.Unmarshal([]byte(row.Variables), &vars)
	if vars == nil {
		vars = map[string]string{}
	}

	var startedAt *time.Time
	if row.StartedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.StartedAt.String)
		startedAt = &t
	}

	var finishedAt *time.Time
	if row.FinishedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.FinishedAt.String)
		finishedAt = &t
	}

	var errMsg *string
	if row.ErrorMessage.Valid {
		errMsg = &row.ErrorMessage.String
	}

	return &domain.ModelRun{
		ID:                 row.ID,
		Status:             row.Status,
		TriggerType:        row.TriggerType,
		TriggeredBy:        row.TriggeredBy,
		TargetCatalog:      row.TargetCatalog,
		TargetSchema:       row.TargetSchema,
		ModelSelector:      row.ModelSelector,
		Variables:          vars,
		FullRefresh:        row.FullRefresh != 0,
		CompileManifest:    strPtrOrNil(strings.TrimSpace(row.CompileManifest)),
		CompileDiagnostics: unmarshalCompileDiagnostics(row.CompileDiagnostics),
		StartedAt:          startedAt,
		FinishedAt:         finishedAt,
		ErrorMessage:       errMsg,
		CreatedAt:          createdAt,
	}
}

func marshalCompileDiagnostics(d *domain.ModelCompileDiagnostics) string {
	if d == nil {
		return "{}"
	}
	b, err := json.Marshal(d)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func unmarshalCompileDiagnostics(raw string) *domain.ModelCompileDiagnostics {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out domain.ModelCompileDiagnostics
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	if len(out.Warnings) == 0 && len(out.Errors) == 0 {
		return nil
	}
	return &out
}

func boolToInt64(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func strPtrOrNil(v string) *string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return &v
}

func modelRunStepFromDB(row dbstore.ModelRunStep) *domain.ModelRunStep {
	createdAt, _ := time.Parse("2006-01-02 15:04:05", row.CreatedAt)

	var startedAt *time.Time
	if row.StartedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.StartedAt.String)
		startedAt = &t
	}

	var finishedAt *time.Time
	if row.FinishedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.FinishedAt.String)
		finishedAt = &t
	}

	var errMsg *string
	if row.ErrorMessage.Valid {
		errMsg = &row.ErrorMessage.String
	}

	var rowsAffected *int64
	if row.RowsAffected.Valid {
		rowsAffected = &row.RowsAffected.Int64
	}

	dependsOn := make([]string, 0)
	if row.DependsOn != "" {
		_ = json.Unmarshal([]byte(row.DependsOn), &dependsOn)
	}
	varsUsed := make([]string, 0)
	if row.VarsUsed != "" {
		_ = json.Unmarshal([]byte(row.VarsUsed), &varsUsed)
	}
	macrosUsed := make([]string, 0)
	if row.MacrosUsed != "" {
		_ = json.Unmarshal([]byte(row.MacrosUsed), &macrosUsed)
	}

	var compiledSQL *string
	if row.CompiledSql.Valid {
		compiledSQL = &row.CompiledSql.String
	}
	var compiledHash *string
	if row.CompiledHash.Valid {
		compiledHash = &row.CompiledHash.String
	}

	return &domain.ModelRunStep{
		ID:           row.ID,
		RunID:        row.RunID,
		ModelID:      row.ModelID,
		ModelName:    row.ModelName,
		CompiledSQL:  compiledSQL,
		CompiledHash: compiledHash,
		DependsOn:    dependsOn,
		VarsUsed:     varsUsed,
		MacrosUsed:   macrosUsed,
		Status:       row.Status,
		Tier:         int(row.Tier),
		RowsAffected: rowsAffected,
		StartedAt:    startedAt,
		FinishedAt:   finishedAt,
		ErrorMessage: errMsg,
		CreatedAt:    createdAt,
	}
}
