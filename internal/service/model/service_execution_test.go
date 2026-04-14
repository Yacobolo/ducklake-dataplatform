package model

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
)

type executionModelRepoStub struct {
	listFn func(context.Context, *string, domain.PageRequest) ([]domain.Model, int64, error)
}

func (s executionModelRepoStub) Create(context.Context, *domain.Model) (*domain.Model, error) {
	panic("unexpected call")
}

func (s executionModelRepoStub) CreateWithNotebookLink(context.Context, *domain.Model, string, string) (*domain.Model, error) {
	panic("unexpected call")
}

func (s executionModelRepoStub) GetByID(context.Context, string) (*domain.Model, error) {
	panic("unexpected call")
}

func (s executionModelRepoStub) GetByName(context.Context, string, string) (*domain.Model, error) {
	panic("unexpected call")
}

func (s executionModelRepoStub) List(ctx context.Context, projectName *string, page domain.PageRequest) ([]domain.Model, int64, error) {
	if s.listFn != nil {
		return s.listFn(ctx, projectName, page)
	}
	panic("unexpected call")
}

func (s executionModelRepoStub) Update(context.Context, string, domain.UpdateModelRequest) (*domain.Model, error) {
	panic("unexpected call")
}

func (s executionModelRepoStub) Delete(context.Context, string) error {
	panic("unexpected call")
}

func (s executionModelRepoStub) ListAll(context.Context) ([]domain.Model, error) {
	panic("unexpected call")
}

func (s executionModelRepoStub) UpdateDependencies(context.Context, string, []string) error {
	panic("unexpected call")
}

func TestSelectDefaultDevelopmentEnvironment_PrefersNamedDev(t *testing.T) {
	t.Parallel()

	environment, err := selectDefaultDevelopmentEnvironment([]domain.Environment{
		{Name: "alice", Kind: domain.EnvironmentKindDevelopment},
		{Name: "dev", Kind: domain.EnvironmentKindDevelopment},
	})
	require.NoError(t, err)
	require.NotNil(t, environment)
	assert.Equal(t, "dev", environment.Name)
}

func TestSelectDefaultDevelopmentEnvironment_RequiresDisambiguation(t *testing.T) {
	t.Parallel()

	environment, err := selectDefaultDevelopmentEnvironment([]domain.Environment{
		{Name: "alice", Kind: domain.EnvironmentKindDevelopment},
		{Name: "bob", Kind: domain.EnvironmentKindDevelopment},
	})
	require.Error(t, err)
	assert.Nil(t, environment)
	assert.Contains(t, err.Error(), "environment_name is required")
}

func TestSelectDefaultDevelopmentEnvironment_RequiresDevelopmentEnvironment(t *testing.T) {
	t.Parallel()

	environment, err := selectDefaultDevelopmentEnvironment([]domain.Environment{
		{Name: "staging", Kind: domain.EnvironmentKindStaging},
		{Name: "prod", Kind: domain.EnvironmentKindProduction},
	})
	require.Error(t, err)
	assert.Nil(t, environment)
	assert.Contains(t, err.Error(), "no development environment")
}

func TestLoadProjectModels_FiltersToProject(t *testing.T) {
	t.Parallel()

	svc := &Service{
		models: executionModelRepoStub{
			listFn: func(_ context.Context, projectName *string, page domain.PageRequest) ([]domain.Model, int64, error) {
				require.NotNil(t, projectName)
				assert.Equal(t, "orders", *projectName)
				assert.Equal(t, domain.MaxMaxResults, page.MaxResults)
				return []domain.Model{{ProjectName: "orders", Name: "stg_orders"}}, 1, nil
			},
		},
	}

	models, err := svc.loadProjectModels(context.Background(), "orders")
	require.NoError(t, err)
	require.Len(t, models, 1)
	assert.Equal(t, "orders", models[0].ProjectName)
	assert.Equal(t, "stg_orders", models[0].Name)
}
