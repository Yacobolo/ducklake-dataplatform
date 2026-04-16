package model

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
)

type runtimeProjectRepoStub struct {
	getByNameFn func(context.Context, string) (*domain.Project, error)
}

func (s runtimeProjectRepoStub) Create(context.Context, *domain.Project) (*domain.Project, error) {
	panic("unexpected call")
}
func (s runtimeProjectRepoStub) GetByID(context.Context, string) (*domain.Project, error) {
	panic("unexpected call")
}
func (s runtimeProjectRepoStub) GetByName(ctx context.Context, name string) (*domain.Project, error) {
	return s.getByNameFn(ctx, name)
}
func (s runtimeProjectRepoStub) List(context.Context, domain.PageRequest) ([]domain.Project, int64, error) {
	panic("unexpected call")
}
func (s runtimeProjectRepoStub) ListByWorkspace(context.Context, string, domain.PageRequest) ([]domain.Project, int64, error) {
	panic("unexpected call")
}
func (s runtimeProjectRepoStub) ListByProduct(context.Context, string, domain.PageRequest) ([]domain.Project, int64, error) {
	panic("unexpected call")
}
func (s runtimeProjectRepoStub) Update(context.Context, string, domain.UpdateProjectRequest) (*domain.Project, error) {
	panic("unexpected call")
}
func (s runtimeProjectRepoStub) Delete(context.Context, string) error {
	panic("unexpected call")
}

type runtimeEnvironmentRepoStub struct {
	getByNameFn     func(context.Context, string, string) (*domain.Environment, error)
	listByProjectFn func(context.Context, string, domain.PageRequest) ([]domain.Environment, int64, error)
}

func (s runtimeEnvironmentRepoStub) Create(context.Context, *domain.Environment) (*domain.Environment, error) {
	panic("unexpected call")
}
func (s runtimeEnvironmentRepoStub) GetByID(context.Context, string) (*domain.Environment, error) {
	panic("unexpected call")
}
func (s runtimeEnvironmentRepoStub) GetByName(ctx context.Context, projectID, name string) (*domain.Environment, error) {
	return s.getByNameFn(ctx, projectID, name)
}
func (s runtimeEnvironmentRepoStub) ListByProject(ctx context.Context, projectID string, page domain.PageRequest) ([]domain.Environment, int64, error) {
	return s.listByProjectFn(ctx, projectID, page)
}
func (s runtimeEnvironmentRepoStub) Update(context.Context, string, domain.UpdateEnvironmentRequest) (*domain.Environment, error) {
	panic("unexpected call")
}
func (s runtimeEnvironmentRepoStub) Delete(context.Context, string) error {
	panic("unexpected call")
}

type runtimeProjectDependencyRepoStub struct {
	listByProjectFn func(context.Context, string) ([]domain.ProjectDependency, error)
}

func (s runtimeProjectDependencyRepoStub) Create(context.Context, *domain.ProjectDependency) (*domain.ProjectDependency, error) {
	panic("unexpected call")
}
func (s runtimeProjectDependencyRepoStub) ListByProject(ctx context.Context, projectID string) ([]domain.ProjectDependency, error) {
	return s.listByProjectFn(ctx, projectID)
}
func (s runtimeProjectDependencyRepoStub) Delete(context.Context, string, string) error {
	panic("unexpected call")
}

func TestResolveExecutionContext_MergesDeferredEnvironmentAndRequestOverrides(t *testing.T) {
	project := &domain.Project{ID: "proj-1", Name: "analytics"}
	deferredName := "prod"
	envByName := map[string]*domain.Environment{
		"dev": {
			ID:                 "env-dev",
			ProjectID:          project.ID,
			ProjectName:        project.Name,
			Name:               "dev",
			Kind:               domain.EnvironmentKindDevelopment,
			TargetCatalog:      "dev_catalog",
			TargetSchema:       "dev_schema",
			DeferToEnvironment: &deferredName,
			Variables:          map[string]string{"owner": "dev"},
			SourceOverrides:    map[string]string{"raw.orders": "dev.raw_orders"},
		},
		"prod": {
			ID:              "env-prod",
			ProjectID:       project.ID,
			ProjectName:     project.Name,
			Name:            "prod",
			Kind:            domain.EnvironmentKindProduction,
			TargetCatalog:   "prod_catalog",
			TargetSchema:    "prod_schema",
			Variables:       map[string]string{"owner": "prod", "region": "eu"},
			SourceOverrides: map[string]string{"raw.orders": "prod.raw_orders"},
		},
	}

	svc := &Service{
		projects: runtimeProjectRepoStub{
			getByNameFn: func(_ context.Context, name string) (*domain.Project, error) {
				require.Equal(t, project.Name, name)
				return project, nil
			},
		},
		environments: runtimeEnvironmentRepoStub{
			getByNameFn: func(_ context.Context, projectID, name string) (*domain.Environment, error) {
				require.Equal(t, project.ID, projectID)
				return envByName[name], nil
			},
			listByProjectFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.Environment, int64, error) {
				return nil, 0, nil
			},
		},
		projectDeps: runtimeProjectDependencyRepoStub{
			listByProjectFn: func(_ context.Context, projectID string) ([]domain.ProjectDependency, error) {
				require.Equal(t, project.ID, projectID)
				return []domain.ProjectDependency{{DependencyProject: "shared_lib"}}, nil
			},
		},
	}

	runCtx, err := svc.resolveExecutionContext(context.Background(), project.Name, "dev", domain.TriggerModelRunRequest{
		Variables: map[string]string{"owner": "request"},
	})
	require.NoError(t, err)
	require.NotNil(t, runCtx)
	assert.Equal(t, "dev_catalog", runCtx.targetCatalog)
	assert.Equal(t, "dev_schema", runCtx.targetSchema)
	assert.Equal(t, "prod", runCtx.stateEnvironment.Name)
	assert.Equal(t, "request", runCtx.variables["owner"])
	assert.Equal(t, "eu", runCtx.variables["region"])
	assert.Equal(t, "dev.raw_orders", runCtx.sourceOverrides["raw.orders"])
	assert.Contains(t, runCtx.allowedRefProjects, "analytics")
	assert.Contains(t, runCtx.allowedRefProjects, "shared_lib")
}
