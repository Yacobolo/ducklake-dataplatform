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

// Compile-time check.
var _ domain.ModelRepository = (*ModelRepo)(nil)

// ModelRepo implements ModelRepository using SQLite.
type ModelRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

// NewModelRepo creates a new ModelRepo.
func NewModelRepo(db *sql.DB) *ModelRepo {
	return &ModelRepo{q: dbstore.New(db), db: db}
}

// Create inserts a new transformation model.
func (r *ModelRepo) Create(ctx context.Context, m *domain.Model) (*domain.Model, error) {
	tagsJSON, err := json.Marshal(m.Tags)
	if err != nil {
		return nil, fmt.Errorf("marshal tags: %w", err)
	}
	depsJSON, err := json.Marshal(m.DependsOn)
	if err != nil {
		return nil, fmt.Errorf("marshal depends_on: %w", err)
	}
	configJSON, err := json.Marshal(m.Config)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	contractJSON := "{}"
	if m.Contract != nil {
		b, err := json.Marshal(m.Contract)
		if err != nil {
			return nil, fmt.Errorf("marshal contract: %w", err)
		}
		contractJSON = string(b)
	}

	var freshnessMaxLag sql.NullInt64
	var freshnessCron sql.NullString
	if m.Freshness != nil {
		if m.Freshness.MaxLagSeconds > 0 {
			freshnessMaxLag = sql.NullInt64{Int64: m.Freshness.MaxLagSeconds, Valid: true}
		}
		if m.Freshness.CronSchedule != "" {
			freshnessCron = sql.NullString{String: m.Freshness.CronSchedule, Valid: true}
		}
	}

	row, err := r.q.CreateModel(ctx, dbstore.CreateModelParams{
		ID:              newID(),
		ProjectName:     m.ProjectName,
		Name:            m.Name,
		SqlBody:         m.SQL,
		Materialization: m.Materialization,
		Description:     m.Description,
		Owner:           m.Owner,
		Tags:            string(tagsJSON),
		DependsOn:       string(depsJSON),
		Config:          string(configJSON),
		CreatedBy:       m.CreatedBy,
		Contract:        contractJSON,
		FreshnessMaxLag: freshnessMaxLag,
		FreshnessCron:   freshnessCron,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return modelFromDB(row), nil
}

// GetByID returns a model by its ID.
func (r *ModelRepo) GetByID(ctx context.Context, id string) (*domain.Model, error) {
	row, err := r.q.GetModelByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return modelFromDB(row), nil
}

// GetByName returns a model by project name and model name.
func (r *ModelRepo) GetByName(ctx context.Context, projectName, name string) (*domain.Model, error) {
	row, err := r.q.GetModelByName(ctx, dbstore.GetModelByNameParams{
		ProjectName: projectName,
		Name:        name,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return modelFromDB(row), nil
}

// List returns a paginated list of models, optionally filtered by project.
func (r *ModelRepo) List(ctx context.Context, projectName *string, page domain.PageRequest) ([]domain.Model, int64, error) {
	projectFilter := ""
	if projectName != nil {
		projectFilter = *projectName
	}

	total, err := r.q.CountModels(ctx, dbstore.CountModelsParams{
		Column1:     projectFilter,
		ProjectName: projectFilter,
	})
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListModels(ctx, dbstore.ListModelsParams{
		Column1:     projectFilter,
		ProjectName: projectFilter,
		Limit:       int64(page.Limit()),
		Offset:      int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	models := make([]domain.Model, 0, len(rows))
	for _, row := range rows {
		models = append(models, *modelFromDB(row))
	}
	return models, total, nil
}

// Update applies partial updates to a model using read-modify-write.
func (r *ModelRepo) Update(ctx context.Context, id string, req domain.UpdateModelRequest) (*domain.Model, error) {
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	sqlBody := current.SQL
	if req.SQL != nil {
		sqlBody = *req.SQL
	}
	materialization := current.Materialization
	if req.Materialization != nil {
		materialization = *req.Materialization
	}
	description := current.Description
	if req.Description != nil {
		description = *req.Description
	}

	tags := current.Tags
	if req.Tags != nil {
		tags = req.Tags
	}
	tagsJSON, err := json.Marshal(tags)
	if err != nil {
		return nil, fmt.Errorf("marshal tags: %w", err)
	}

	config := current.Config
	if req.Config != nil {
		config = *req.Config
	}
	configJSON, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}

	contract := current.Contract
	if req.Contract != nil {
		contract = req.Contract
	}
	contractJSON, err := json.Marshal(contract)
	if err != nil {
		return nil, fmt.Errorf("marshal contract: %w", err)
	}

	freshness := current.Freshness
	if req.Freshness != nil {
		freshness = req.Freshness
	}
	var freshnessMaxLag sql.NullInt64
	var freshnessCron sql.NullString
	if freshness != nil {
		if freshness.MaxLagSeconds > 0 {
			freshnessMaxLag = sql.NullInt64{Int64: freshness.MaxLagSeconds, Valid: true}
		}
		if freshness.CronSchedule != "" {
			freshnessCron = sql.NullString{String: freshness.CronSchedule, Valid: true}
		}
	}

	err = r.q.UpdateModel(ctx, dbstore.UpdateModelParams{
		SqlBody:         sqlBody,
		Materialization: materialization,
		Description:     description,
		Tags:            string(tagsJSON),
		Config:          string(configJSON),
		Contract:        string(contractJSON),
		FreshnessMaxLag: freshnessMaxLag,
		FreshnessCron:   freshnessCron,
		ID:              id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// Delete removes a model by ID.
func (r *ModelRepo) Delete(ctx context.Context, id string) error {
	return mapDBError(r.q.DeleteModel(ctx, id))
}

// ListAll returns all models ordered by project and name.
func (r *ModelRepo) ListAll(ctx context.Context) ([]domain.Model, error) {
	rows, err := r.q.ListAllModels(ctx)
	if err != nil {
		return nil, err
	}

	models := make([]domain.Model, 0, len(rows))
	for _, row := range rows {
		models = append(models, *modelFromDB(row))
	}
	return models, nil
}

// UpdateDependencies updates a model's dependency list.
func (r *ModelRepo) UpdateDependencies(ctx context.Context, id string, deps []string) error {
	depsJSON, err := json.Marshal(deps)
	if err != nil {
		return fmt.Errorf("marshal depends_on: %w", err)
	}
	return mapDBError(r.q.UpdateModelDependencies(ctx, dbstore.UpdateModelDependenciesParams{
		DependsOn: string(depsJSON),
		ID:        id,
	}))
}

// === Private mappers ===

func modelFromDB(row dbstore.Model) *domain.Model {
	createdAt, err := time.Parse("2006-01-02 15:04:05", row.CreatedAt)
	if err != nil {
		slog.Default().Warn("failed to parse model created_at", "value", row.CreatedAt, "error", err)
	}
	updatedAt, err := time.Parse("2006-01-02 15:04:05", row.UpdatedAt)
	if err != nil {
		slog.Default().Warn("failed to parse model updated_at", "value", row.UpdatedAt, "error", err)
	}

	var tags []string
	_ = json.Unmarshal([]byte(row.Tags), &tags)
	if tags == nil {
		tags = []string{}
	}

	var deps []string
	_ = json.Unmarshal([]byte(row.DependsOn), &deps)
	if deps == nil {
		deps = []string{}
	}

	var config domain.ModelConfig
	_ = json.Unmarshal([]byte(row.Config), &config)

	var contract *domain.ModelContract
	if row.Contract != "" && row.Contract != "{}" {
		contract = &domain.ModelContract{}
		_ = json.Unmarshal([]byte(row.Contract), contract)
	}

	var freshness *domain.FreshnessPolicy
	if row.FreshnessMaxLag.Valid || row.FreshnessCron.Valid {
		freshness = &domain.FreshnessPolicy{}
		if row.FreshnessMaxLag.Valid {
			freshness.MaxLagSeconds = row.FreshnessMaxLag.Int64
		}
		if row.FreshnessCron.Valid {
			freshness.CronSchedule = row.FreshnessCron.String
		}
	}

	return &domain.Model{
		ID:              row.ID,
		ProjectName:     row.ProjectName,
		Name:            row.Name,
		SQL:             row.SqlBody,
		Materialization: row.Materialization,
		Description:     row.Description,
		Owner:           row.Owner,
		Tags:            tags,
		DependsOn:       deps,
		Config:          config,
		Contract:        contract,
		Freshness:       freshness,
		CreatedBy:       row.CreatedBy,
		CreatedAt:       createdAt,
		UpdatedAt:       updatedAt,
	}
}
