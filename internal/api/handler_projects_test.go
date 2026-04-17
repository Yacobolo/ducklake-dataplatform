package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
)

type mockProjectControlService struct {
	createDependencyForProjectFn func(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateProjectDependencyRequest) (*domain.ProjectDependency, error)
	listDependenciesForProjectFn func(ctx context.Context, principal string, isAdmin bool, projectID string, page domain.PageRequest) ([]domain.ProjectDependency, int64, error)
	createSourceForProjectFn     func(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateSourceDefinitionRequest) (*domain.SourceDefinition, error)
	createSeedForProjectFn       func(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateSeedRequest) (*domain.Seed, error)
}

func (m *mockProjectControlService) CreateProject(context.Context, string, domain.CreateProjectRequest) (*domain.Project, error) {
	panic("not implemented")
}
func (m *mockProjectControlService) ListProjectsForPrincipal(context.Context, string, bool, string, domain.PageRequest) ([]domain.Project, int64, error) {
	panic("not implemented")
}
func (m *mockProjectControlService) GetProjectForPrincipal(context.Context, string, bool, string) (*domain.Project, error) {
	panic("not implemented")
}
func (m *mockProjectControlService) UpdateProjectForPrincipal(context.Context, string, bool, string, domain.UpdateProjectRequest) (*domain.Project, error) {
	panic("not implemented")
}
func (m *mockProjectControlService) DeleteProjectForPrincipal(context.Context, string, bool, string) error {
	panic("not implemented")
}
func (m *mockProjectControlService) CreateDependencyForProject(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateProjectDependencyRequest) (*domain.ProjectDependency, error) {
	return m.createDependencyForProjectFn(ctx, principal, isAdmin, projectID, req)
}
func (m *mockProjectControlService) ListDependenciesForProject(ctx context.Context, principal string, isAdmin bool, projectID string, page domain.PageRequest) ([]domain.ProjectDependency, int64, error) {
	return m.listDependenciesForProjectFn(ctx, principal, isAdmin, projectID, page)
}
func (m *mockProjectControlService) DeleteDependencyForProject(context.Context, string, bool, string, string) error {
	panic("not implemented")
}
func (m *mockProjectControlService) CreateSourceForProject(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateSourceDefinitionRequest) (*domain.SourceDefinition, error) {
	return m.createSourceForProjectFn(ctx, principal, isAdmin, projectID, req)
}
func (m *mockProjectControlService) ListSourcesForProject(context.Context, string, bool, string, domain.PageRequest) ([]domain.SourceDefinition, int64, error) {
	panic("not implemented")
}
func (m *mockProjectControlService) GetSourceForProject(context.Context, string, bool, string, string, string) (*domain.SourceDefinition, error) {
	panic("not implemented")
}
func (m *mockProjectControlService) UpdateSourceForProject(context.Context, string, bool, string, string, string, domain.UpdateSourceDefinitionRequest) (*domain.SourceDefinition, error) {
	panic("not implemented")
}
func (m *mockProjectControlService) DeleteSourceForProject(context.Context, string, bool, string, string, string) error {
	panic("not implemented")
}
func (m *mockProjectControlService) CreateSeedForProject(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateSeedRequest) (*domain.Seed, error) {
	return m.createSeedForProjectFn(ctx, principal, isAdmin, projectID, req)
}
func (m *mockProjectControlService) ListSeedsForProject(context.Context, string, bool, string, domain.PageRequest) ([]domain.Seed, int64, error) {
	panic("not implemented")
}
func (m *mockProjectControlService) GetSeedForProject(context.Context, string, bool, string, string) (*domain.Seed, error) {
	panic("not implemented")
}
func (m *mockProjectControlService) UpdateSeedForProject(context.Context, string, bool, string, string, domain.UpdateSeedRequest) (*domain.Seed, error) {
	panic("not implemented")
}
func (m *mockProjectControlService) DeleteSeedForProject(context.Context, string, bool, string, string) error {
	panic("not implemented")
}
func (m *mockProjectControlService) CreateEnvironmentForProject(context.Context, string, bool, string, domain.CreateEnvironmentRequest) (*domain.Environment, error) {
	panic("not implemented")
}
func (m *mockProjectControlService) ListEnvironmentsForProject(context.Context, string, bool, string, domain.PageRequest) ([]domain.Environment, int64, error) {
	panic("not implemented")
}
func (m *mockProjectControlService) UpdateEnvironmentForProject(context.Context, string, bool, string, string, domain.UpdateEnvironmentRequest) (*domain.Environment, error) {
	panic("not implemented")
}
func (m *mockProjectControlService) DeleteEnvironmentForProject(context.Context, string, bool, string, string) error {
	panic("not implemented")
}
func (m *mockProjectControlService) CreateBuildForProject(context.Context, string, bool, string, domain.CreateBuildRequest) (*domain.Build, error) {
	panic("not implemented")
}
func (m *mockProjectControlService) ListBuildsForProject(context.Context, string, bool, string, domain.PageRequest) ([]domain.Build, int64, error) {
	panic("not implemented")
}

func projectHandlerTestCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		Name:    "alice",
		IsAdmin: true,
	})
}

func TestHandler_CreateProjectDependency_MapsRequestAndResponse(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	handler := &APIHandler{
		projectsCtl: &mockProjectControlService{
			createDependencyForProjectFn: func(_ context.Context, principal string, isAdmin bool, projectID string, req domain.CreateProjectDependencyRequest) (*domain.ProjectDependency, error) {
				assert.Equal(t, "alice", principal)
				assert.True(t, isAdmin)
				assert.Equal(t, "prj-1", projectID)
				assert.Equal(t, "shared_lib", req.DependencyProject)
				assert.Equal(t, "library", req.DependencyKind)
				assert.Equal(t, 3, req.Position)
				return &domain.ProjectDependency{
					ID:                "dep-1",
					ProjectID:         projectID,
					ProjectName:       "analytics",
					DependencyProject: req.DependencyProject,
					DependencyKind:    req.DependencyKind,
					Position:          req.Position,
					CreatedAt:         fixed,
					UpdatedAt:         fixed,
				}, nil
			},
		},
	}

	dependencyKind := "library"
	position := int32(3)
	resp, err := handler.CreateProjectDependency(projectHandlerTestCtx(), GenCreateProjectDependencyRequest{
		ProjectId: "prj-1",
		Body: &GenCreateProjectDependencyJSONBody{
			DependencyProject: "shared_lib",
			DependencyKind:    &dependencyKind,
			Position:          &position,
		},
	})
	require.NoError(t, err)

	created, ok := resp.(GenCreateProjectDependency201JSONResponse)
	require.True(t, ok)
	assert.Equal(t, "shared_lib", created.Body.DependencyProject)
	assert.Equal(t, "prj-1", created.Body.ProjectId)
	require.NotNil(t, created.Body.ProjectName)
	assert.Equal(t, "analytics", *created.Body.ProjectName)
}

func TestHandler_CreateProjectSource_MapsFreshnessPolicy(t *testing.T) {
	t.Parallel()

	handler := &APIHandler{
		projectsCtl: &mockProjectControlService{
			createSourceForProjectFn: func(_ context.Context, principal string, isAdmin bool, projectID string, req domain.CreateSourceDefinitionRequest) (*domain.SourceDefinition, error) {
				assert.Equal(t, "alice", principal)
				assert.True(t, isAdmin)
				assert.Equal(t, "prj-1", projectID)
				assert.Equal(t, "raw", req.SourceName)
				assert.Equal(t, "orders", req.TableName)
				assert.Equal(t, "lake.raw.orders", req.RelationRef)
				require.NotNil(t, req.Freshness)
				assert.Equal(t, "loaded_at", req.Freshness.TimestampColumn)
				assert.Equal(t, int64(3600), req.Freshness.MaxLagSeconds)
				return &domain.SourceDefinition{
					ID:          "src-1",
					ProjectName: "analytics",
					SourceName:  req.SourceName,
					TableName:   req.TableName,
					RelationRef: req.RelationRef,
					Description: req.Description,
					Freshness:   req.Freshness,
				}, nil
			},
		},
	}

	timestampColumn := "loaded_at"
	maxLagSeconds := int32(3600)
	description := "orders source"
	resp, err := handler.CreateProjectSource(projectHandlerTestCtx(), GenCreateProjectSourceRequest{
		ProjectId: "prj-1",
		Body: &GenCreateProjectSourceJSONBody{
			SourceName:  "raw",
			TableName:   "orders",
			RelationRef: "lake.raw.orders",
			Description: &description,
			FreshnessPolicy: &SourceFreshnessPolicy{
				TimestampColumn: &timestampColumn,
				MaxLagSeconds:   &maxLagSeconds,
			},
		},
	})
	require.NoError(t, err)

	created, ok := resp.(GenCreateProjectSource201JSONResponse)
	require.True(t, ok)
	assert.Equal(t, "src-1", created.Body.Id)
	assert.Equal(t, "analytics", created.Body.ProjectName)
	require.NotNil(t, created.Body.FreshnessPolicy)
	require.NotNil(t, created.Body.FreshnessPolicy.TimestampColumn)
	assert.Equal(t, "loaded_at", *created.Body.FreshnessPolicy.TimestampColumn)
}

func TestHandler_CreateProjectSeed_MapsRequestAndResponse(t *testing.T) {
	t.Parallel()

	handler := &APIHandler{
		projectsCtl: &mockProjectControlService{
			createSeedForProjectFn: func(_ context.Context, principal string, isAdmin bool, projectID string, req domain.CreateSeedRequest) (*domain.Seed, error) {
				assert.Equal(t, "alice", principal)
				assert.True(t, isAdmin)
				assert.Equal(t, "prj-1", projectID)
				assert.Equal(t, "seed_orders", req.Name)
				assert.Equal(t, "fixtures/orders.csv", req.InputRef)
				assert.Equal(t, "csv", req.Format)
				require.NotNil(t, req.Delimiter)
				assert.Equal(t, ",", *req.Delimiter)
				require.NotNil(t, req.HasHeader)
				assert.True(t, *req.HasHeader)
				assert.Equal(t, map[string]string{"order_id": "INTEGER"}, req.ColumnTypes)
				assert.Equal(t, []string{"seed", "finance"}, req.Tags)
				return &domain.Seed{
					ID:          "seed-1",
					ProjectName: "analytics",
					Name:        req.Name,
					InputRef:    req.InputRef,
					Format:      req.Format,
					Delimiter:   ",",
					HasHeader:   true,
					ColumnTypes: req.ColumnTypes,
					Tags:        req.Tags,
				}, nil
			},
		},
	}

	format := SeedFormatCsv
	delimiter := ","
	hasHeader := true
	tags := []string{"seed", "finance"}
	resp, err := handler.CreateProjectSeed(projectHandlerTestCtx(), GenCreateProjectSeedRequest{
		ProjectId: "prj-1",
		Body: &GenCreateProjectSeedJSONBody{
			Name:        "seed_orders",
			InputRef:    "fixtures/orders.csv",
			Format:      &format,
			Delimiter:   &delimiter,
			HasHeader:   &hasHeader,
			ColumnTypes: &map[string]any{"order_id": "INTEGER"},
			Tags:        &tags,
		},
	})
	require.NoError(t, err)

	created, ok := resp.(GenCreateProjectSeed201JSONResponse)
	require.True(t, ok)
	assert.Equal(t, "analytics", created.Body.ProjectName)
	assert.Equal(t, "seed_orders", created.Body.Name)
	require.NotNil(t, created.Body.HasHeader)
	assert.True(t, *created.Body.HasHeader)
	require.NotNil(t, created.Body.Tags)
	assert.Equal(t, []string{"seed", "finance"}, *created.Body.Tags)
}

func TestHandler_ListProjectDependencies_PaginatesResponse(t *testing.T) {
	t.Parallel()

	handler := &APIHandler{
		projectsCtl: &mockProjectControlService{
			listDependenciesForProjectFn: func(_ context.Context, principal string, isAdmin bool, projectID string, page domain.PageRequest) ([]domain.ProjectDependency, int64, error) {
				assert.Equal(t, "alice", principal)
				assert.True(t, isAdmin)
				assert.Equal(t, "prj-1", projectID)
				assert.Equal(t, 5, page.Limit())
				return []domain.ProjectDependency{
					{ID: "dep-1", ProjectID: projectID, ProjectName: "analytics", DependencyProject: "shared_lib", DependencyKind: "library", Position: 1},
				}, 7, nil
			},
		},
	}

	maxResults := int32(5)
	resp, err := handler.ListProjectDependencies(projectHandlerTestCtx(), GenListProjectDependenciesRequest{
		ProjectId: "prj-1",
		Params:    GenListProjectDependenciesParams{MaxResults: &maxResults},
	})
	require.NoError(t, err)

	list, ok := resp.(GenListProjectDependencies200JSONResponse)
	require.True(t, ok)
	require.Len(t, list.Body.Data, 1)
	assert.Equal(t, "shared_lib", list.Body.Data[0].DependencyProject)
	require.NotNil(t, list.Body.NextPageToken)
}
