package model

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
)

type sourceDefinitionRepoStub struct {
	listByProjectFn func(context.Context, string) ([]domain.SourceDefinition, error)
}

func (s sourceDefinitionRepoStub) Create(context.Context, *domain.SourceDefinition) (*domain.SourceDefinition, error) {
	panic("unexpected call")
}
func (s sourceDefinitionRepoStub) GetByName(context.Context, string, string, string) (*domain.SourceDefinition, error) {
	panic("unexpected call")
}
func (s sourceDefinitionRepoStub) ListByProject(ctx context.Context, projectName string) ([]domain.SourceDefinition, error) {
	return s.listByProjectFn(ctx, projectName)
}
func (s sourceDefinitionRepoStub) Update(context.Context, string, *domain.SourceDefinition) (*domain.SourceDefinition, error) {
	panic("unexpected call")
}
func (s sourceDefinitionRepoStub) Delete(context.Context, string) error {
	panic("unexpected call")
}

type seedRepoStub struct {
	listByProjectFn func(context.Context, string) ([]domain.Seed, error)
}

func (s seedRepoStub) Create(context.Context, *domain.Seed) (*domain.Seed, error) {
	panic("unexpected call")
}
func (s seedRepoStub) GetByName(context.Context, string, string) (*domain.Seed, error) {
	panic("unexpected call")
}
func (s seedRepoStub) ListByProject(ctx context.Context, projectName string) ([]domain.Seed, error) {
	return s.listByProjectFn(ctx, projectName)
}
func (s seedRepoStub) Update(context.Context, string, *domain.Seed) (*domain.Seed, error) {
	panic("unexpected call")
}
func (s seedRepoStub) Delete(context.Context, string) error {
	panic("unexpected call")
}

type compilationScopeModelRepoStub struct {
	listAllFn func(context.Context) ([]domain.Model, error)
}

func (s compilationScopeModelRepoStub) Create(context.Context, *domain.Model) (*domain.Model, error) {
	panic("unexpected call")
}
func (s compilationScopeModelRepoStub) CreateWithNotebookLink(context.Context, *domain.Model, string, string) (*domain.Model, error) {
	panic("unexpected call")
}
func (s compilationScopeModelRepoStub) GetByID(context.Context, string) (*domain.Model, error) {
	panic("unexpected call")
}
func (s compilationScopeModelRepoStub) GetByName(context.Context, string, string) (*domain.Model, error) {
	panic("unexpected call")
}
func (s compilationScopeModelRepoStub) List(context.Context, *string, domain.PageRequest) ([]domain.Model, int64, error) {
	panic("unexpected call")
}
func (s compilationScopeModelRepoStub) Update(context.Context, string, domain.UpdateModelRequest) (*domain.Model, error) {
	panic("unexpected call")
}
func (s compilationScopeModelRepoStub) Delete(context.Context, string) error {
	panic("unexpected call")
}
func (s compilationScopeModelRepoStub) ListAll(ctx context.Context) ([]domain.Model, error) {
	return s.listAllFn(ctx)
}
func (s compilationScopeModelRepoStub) UpdateDependencies(context.Context, string, []string) error {
	panic("unexpected call")
}

type modelRunRepoStub struct {
	listRunsFn func(context.Context, domain.ModelRunFilter) ([]domain.ModelRun, int64, error)
}

func (s modelRunRepoStub) CreateRun(context.Context, *domain.ModelRun) (*domain.ModelRun, error) {
	panic("unexpected call")
}
func (s modelRunRepoStub) GetRunByID(context.Context, string) (*domain.ModelRun, error) {
	panic("unexpected call")
}
func (s modelRunRepoStub) ListRuns(ctx context.Context, filter domain.ModelRunFilter) ([]domain.ModelRun, int64, error) {
	return s.listRunsFn(ctx, filter)
}
func (s modelRunRepoStub) UpdateRunBuild(context.Context, string, string) error {
	panic("unexpected call")
}
func (s modelRunRepoStub) UpdateRunStarted(context.Context, string) error {
	panic("unexpected call")
}
func (s modelRunRepoStub) UpdateRunFinished(context.Context, string, string, *string) error {
	panic("unexpected call")
}
func (s modelRunRepoStub) CreateStep(context.Context, *domain.ModelRunStep) (*domain.ModelRunStep, error) {
	panic("unexpected call")
}
func (s modelRunRepoStub) ListStepsByRun(context.Context, string) ([]domain.ModelRunStep, error) {
	panic("unexpected call")
}
func (s modelRunRepoStub) UpdateStepStarted(context.Context, string) error {
	panic("unexpected call")
}
func (s modelRunRepoStub) UpdateStepFinished(context.Context, string, string, *int64, *string) error {
	panic("unexpected call")
}

func TestLoadSourceRegistry_UsesPersistedSourcesAndOverrides(t *testing.T) {
	svc := &Service{
		sources: sourceDefinitionRepoStub{
			listByProjectFn: func(_ context.Context, projectName string) ([]domain.SourceDefinition, error) {
				switch projectName {
				case "analytics":
					return []domain.SourceDefinition{
						{ProjectName: "analytics", SourceName: "raw", TableName: "orders", RelationRef: "prod.raw_orders"},
					}, nil
				case "shared_lib":
					return []domain.SourceDefinition{
						{ProjectName: "shared_lib", SourceName: "calendar", TableName: "dates", RelationRef: "foundation.calendar_dates"},
					}, nil
				default:
					return nil, nil
				}
			},
		},
	}

	registry, warnings, err := svc.loadSourceRegistry(context.Background(), &resolvedRunContext{
		project:            &domain.Project{Name: "analytics"},
		sourceOverrides:    map[string]string{"raw.orders": "sandbox.raw_orders"},
		dependencyProjects: []string{"shared_lib"},
	})
	require.NoError(t, err)
	assert.Empty(t, warnings)
	require.Contains(t, registry, "analytics:raw.orders")
	require.Contains(t, registry, "shared_lib:calendar.dates")
	assert.Equal(t, `"sandbox"."raw_orders"`, registry["analytics:raw.orders"].relation)
	assert.Equal(t, `"foundation"."calendar_dates"`, registry["shared_lib:calendar.dates"].relation)
}

func TestLatestSuccessfulRunHashes_FiltersByProjectAndEnvironment(t *testing.T) {
	svc := &Service{
		runs: modelRunRepoStub{
			listRunsFn: func(_ context.Context, filter domain.ModelRunFilter) ([]domain.ModelRun, int64, error) {
				require.NotNil(t, filter.Status)
				assert.Equal(t, domain.ModelRunStatusSuccess, *filter.Status)
				return []domain.ModelRun{
					{
						ProjectName:     "other",
						EnvironmentName: "prod",
						CompileManifest: strPtrOrNil(`{"version":1,"models":[{"model_name":"other.model","compiled_hash":"sha256:nope"}]}`),
					},
					{
						ProjectName:     "analytics",
						EnvironmentName: "prod",
						CompileManifest: strPtrOrNil(`{"version":1,"models":[{"model_name":"analytics.model","compiled_hash":"sha256:yes"}]}`),
					},
				}, 2, nil
			},
		},
	}

	hashes, err := svc.latestSuccessfulRunHashes(context.Background(), "analytics", "prod")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"analytics.model": "sha256:yes"}, hashes)
}

func TestLoadSourceRegistry_WarnsWhenRelationRefMissing(t *testing.T) {
	svc := &Service{
		sources: sourceDefinitionRepoStub{
			listByProjectFn: func(_ context.Context, projectName string) ([]domain.SourceDefinition, error) {
				require.Equal(t, "analytics", projectName)
				return []domain.SourceDefinition{
					{ProjectName: "analytics", SourceName: "raw", TableName: "orders"},
				}, nil
			},
		},
	}

	registry, warnings, err := svc.loadSourceRegistry(context.Background(), &resolvedRunContext{
		project: &domain.Project{Name: "analytics"},
	})
	require.NoError(t, err)
	assert.Empty(t, registry)
	assert.Equal(t, []string{"source raw.orders in project analytics has no relation reference"}, warnings)
}

func TestLoadCompilationModelScope_IncludesSeedResources(t *testing.T) {
	svc := &Service{
		models: compilationScopeModelRepoStub{
			listAllFn: func(context.Context) ([]domain.Model, error) {
				return []domain.Model{
					{
						ID:              "model-1",
						ProjectName:     "analytics",
						Name:            "stg_orders",
						SQL:             "select 1",
						Materialization: domain.MaterializationTable,
					},
				}, nil
			},
		},
		seeds: seedRepoStub{
			listByProjectFn: func(_ context.Context, projectName string) ([]domain.Seed, error) {
				require.Equal(t, "analytics", projectName)
				return []domain.Seed{
					{
						ID:          "seed-1",
						ProjectName: "analytics",
						Name:        "seed_orders",
						InputRef:    "fixtures/orders.csv",
						Format:      "csv",
						Delimiter:   ",",
						HasHeader:   true,
						ColumnTypes: map[string]string{"order_id": "INTEGER"},
						Tags:        []string{"seed"},
					},
				}, nil
			},
		},
	}

	scope, warnings, err := svc.loadCompilationModelScope(context.Background(), &resolvedRunContext{
		project:            &domain.Project{Name: "analytics"},
		allowedRefProjects: map[string]struct{}{"analytics": {}},
	})
	require.NoError(t, err)
	assert.Empty(t, warnings)
	require.Len(t, scope, 2)

	var seedModel *domain.Model
	for i := range scope {
		if scope[i].Name == "seed_orders" {
			seedModel = &scope[i]
			break
		}
	}
	require.NotNil(t, seedModel)
	assert.Equal(t, domain.MaterializationSeed, seedModel.Materialization)
	assert.Contains(t, seedModel.SQL, "read_csv_auto")
	assert.Contains(t, seedModel.SQL, "fixtures/orders.csv")
	assert.Contains(t, seedModel.SQL, "types={'order_id': 'INTEGER'}")
}

func TestLoadCompilationModelScope_RejectsSeedNameConflicts(t *testing.T) {
	svc := &Service{
		models: compilationScopeModelRepoStub{
			listAllFn: func(context.Context) ([]domain.Model, error) {
				return []domain.Model{
					{
						ID:              "model-1",
						ProjectName:     "analytics",
						Name:            "seed_orders",
						SQL:             "select 1",
						Materialization: domain.MaterializationTable,
					},
				}, nil
			},
		},
		seeds: seedRepoStub{
			listByProjectFn: func(_ context.Context, projectName string) ([]domain.Seed, error) {
				require.Equal(t, "analytics", projectName)
				return []domain.Seed{
					{
						ID:          "seed-1",
						ProjectName: "analytics",
						Name:        "seed_orders",
						InputRef:    "fixtures/orders.csv",
						Format:      "csv",
						Delimiter:   ",",
						HasHeader:   true,
					},
				}, nil
			},
		},
	}

	_, _, err := svc.loadCompilationModelScope(context.Background(), &resolvedRunContext{
		project:            &domain.Project{Name: "analytics"},
		allowedRefProjects: map[string]struct{}{"analytics": {}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conflicts with an existing model")
}
