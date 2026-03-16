package project

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

type fakeProjectRepo struct {
	createFn func(ctx context.Context, p *domain.Project) (*domain.Project, error)
	getFn    func(ctx context.Context, name string) (*domain.Project, error)
	listFn   func(ctx context.Context, page domain.PageRequest) ([]domain.Project, int64, error)
}

func (f *fakeProjectRepo) Create(ctx context.Context, p *domain.Project) (*domain.Project, error) {
	return f.createFn(ctx, p)
}

func (f *fakeProjectRepo) GetByID(context.Context, string) (*domain.Project, error) {
	panic("GetByID not implemented")
}

func (f *fakeProjectRepo) GetByName(ctx context.Context, name string) (*domain.Project, error) {
	return f.getFn(ctx, name)
}

func (f *fakeProjectRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.Project, int64, error) {
	if f.listFn != nil {
		return f.listFn(ctx, page)
	}
	return nil, 0, nil
}

func (f *fakeProjectRepo) ListByProduct(context.Context, string, domain.PageRequest) ([]domain.Project, int64, error) {
	panic("ListByProduct not implemented")
}

func (f *fakeProjectRepo) Update(context.Context, string, domain.UpdateProjectRequest) (*domain.Project, error) {
	panic("Update not implemented")
}

func (f *fakeProjectRepo) Delete(context.Context, string) error {
	panic("Delete not implemented")
}

type fakeEnvironmentRepo struct {
	createFn func(ctx context.Context, e *domain.Environment) (*domain.Environment, error)
	getFn    func(ctx context.Context, projectID, name string) (*domain.Environment, error)
	listFn   func(ctx context.Context, projectID string, page domain.PageRequest) ([]domain.Environment, int64, error)
}

func (f *fakeEnvironmentRepo) Create(ctx context.Context, e *domain.Environment) (*domain.Environment, error) {
	return f.createFn(ctx, e)
}

func (f *fakeEnvironmentRepo) GetByID(context.Context, string) (*domain.Environment, error) {
	panic("GetByID not implemented")
}

func (f *fakeEnvironmentRepo) GetByName(ctx context.Context, projectID, name string) (*domain.Environment, error) {
	return f.getFn(ctx, projectID, name)
}

func (f *fakeEnvironmentRepo) ListByProject(ctx context.Context, projectID string, page domain.PageRequest) ([]domain.Environment, int64, error) {
	return f.listFn(ctx, projectID, page)
}

func (f *fakeEnvironmentRepo) Update(context.Context, string, domain.UpdateEnvironmentRequest) (*domain.Environment, error) {
	panic("Update not implemented")
}

func (f *fakeEnvironmentRepo) Delete(context.Context, string) error {
	panic("Delete not implemented")
}

type fakeBuildRepo struct {
	createFn func(ctx context.Context, b *domain.Build) (*domain.Build, error)
	listFn   func(ctx context.Context, projectID string, page domain.PageRequest) ([]domain.Build, int64, error)
}

func (f *fakeBuildRepo) Create(ctx context.Context, b *domain.Build) (*domain.Build, error) {
	return f.createFn(ctx, b)
}

func (f *fakeBuildRepo) GetByID(context.Context, string) (*domain.Build, error) {
	panic("GetByID not implemented")
}

func (f *fakeBuildRepo) ListByProject(ctx context.Context, projectID string, page domain.PageRequest) ([]domain.Build, int64, error) {
	return f.listFn(ctx, projectID, page)
}

func (f *fakeBuildRepo) UpdateState(context.Context, string, string) error {
	panic("UpdateState not implemented")
}

type fakeTeamRepo struct {
	getByIDFn func(ctx context.Context, id string) (*domain.Team, error)
}

func (f *fakeTeamRepo) Create(context.Context, *domain.Team) (*domain.Team, error) {
	panic("Create not implemented")
}

func (f *fakeTeamRepo) GetByID(ctx context.Context, id string) (*domain.Team, error) {
	return f.getByIDFn(ctx, id)
}

func (f *fakeTeamRepo) GetByDomainAndName(context.Context, string, string) (*domain.Team, error) {
	panic("GetByDomainAndName not implemented")
}

func (f *fakeTeamRepo) List(context.Context, domain.PageRequest) ([]domain.Team, int64, error) {
	panic("List not implemented")
}

func (f *fakeTeamRepo) Update(context.Context, string, string, *domain.Team) (*domain.Team, error) {
	panic("Update not implemented")
}

func (f *fakeTeamRepo) Delete(context.Context, string, string) error {
	panic("Delete not implemented")
}

type fakeDataProductRepo struct {
	getByIDFn func(ctx context.Context, id string) (*domain.DataProduct, error)
}

func (f *fakeDataProductRepo) Create(context.Context, *domain.DataProduct) (*domain.DataProduct, error) {
	panic("Create not implemented")
}

func (f *fakeDataProductRepo) GetByID(ctx context.Context, productID string) (*domain.DataProduct, error) {
	return f.getByIDFn(ctx, productID)
}

func (f *fakeDataProductRepo) GetBySlug(context.Context, string) (*domain.DataProductDetail, error) {
	panic("GetBySlug not implemented")
}

func (f *fakeDataProductRepo) List(context.Context, domain.DataProductFilter) ([]domain.DataProductListItem, int64, error) {
	panic("List not implemented")
}

func (f *fakeDataProductRepo) Update(context.Context, *domain.DataProduct) (*domain.DataProduct, error) {
	panic("Update not implemented")
}

func (f *fakeDataProductRepo) Delete(context.Context, string) error {
	panic("Delete not implemented")
}

func (f *fakeDataProductRepo) CreateVersion(context.Context, *domain.DataProductVersion) (*domain.DataProductVersion, error) {
	panic("CreateVersion not implemented")
}

func (f *fakeDataProductRepo) GetVersionByNumber(context.Context, string, int) (*domain.DataProductVersion, error) {
	panic("GetVersionByNumber not implemented")
}

func (f *fakeDataProductRepo) ListVersions(context.Context, string) ([]domain.DataProductVersion, error) {
	panic("ListVersions not implemented")
}

func (f *fakeDataProductRepo) DeleteVersion(context.Context, string) error {
	panic("DeleteVersion not implemented")
}

func (f *fakeDataProductRepo) UpdateVersionReleaseState(context.Context, string, string) error {
	panic("UpdateVersionReleaseState not implemented")
}

func (f *fakeDataProductRepo) UpdatePublicationIntent(context.Context, string, string) error {
	panic("UpdatePublicationIntent not implemented")
}

func (f *fakeDataProductRepo) UpsertStatus(context.Context, *domain.DataProductStatus) error {
	panic("UpsertStatus not implemented")
}

func (f *fakeDataProductRepo) GetStatus(context.Context, string) (*domain.DataProductStatus, error) {
	panic("GetStatus not implemented")
}

func (f *fakeDataProductRepo) AddOutput(context.Context, *domain.ProductOutput) error {
	panic("AddOutput not implemented")
}

func (f *fakeDataProductRepo) ListOutputs(context.Context, string) ([]domain.ProductOutput, error) {
	panic("ListOutputs not implemented")
}

func (f *fakeDataProductRepo) ReplaceOutputs(context.Context, string, []domain.ProductOutput) error {
	panic("ReplaceOutputs not implemented")
}

func (f *fakeDataProductRepo) AddSemanticEntrypoint(context.Context, *domain.ProductSemanticEntrypoint) error {
	panic("AddSemanticEntrypoint not implemented")
}

func (f *fakeDataProductRepo) ListSemanticEntrypoints(context.Context, string) ([]domain.ProductSemanticEntrypoint, error) {
	panic("ListSemanticEntrypoints not implemented")
}

func (f *fakeDataProductRepo) ReplaceSemanticEntrypoints(context.Context, string, []domain.ProductSemanticEntrypoint) error {
	panic("ReplaceSemanticEntrypoints not implemented")
}

func (f *fakeDataProductRepo) AddDependency(context.Context, *domain.ProductDependency) error {
	panic("AddDependency not implemented")
}

func (f *fakeDataProductRepo) ListDependencies(context.Context, string) ([]domain.DataProductListItem, error) {
	panic("ListDependencies not implemented")
}

func (f *fakeDataProductRepo) AddSubscription(context.Context, *domain.ProductSubscription) (*domain.ProductSubscription, error) {
	panic("AddSubscription not implemented")
}

func (f *fakeDataProductRepo) ListSubscriptions(context.Context, string) ([]domain.ProductSubscription, error) {
	panic("ListSubscriptions not implemented")
}

func (f *fakeDataProductRepo) AddEvent(context.Context, *domain.ProductEvent) (*domain.ProductEvent, error) {
	panic("AddEvent not implemented")
}

func (f *fakeDataProductRepo) ListEvents(context.Context, string, domain.PageRequest) ([]domain.ProductEvent, int64, error) {
	panic("ListEvents not implemented")
}

func (f *fakeDataProductRepo) CountDependents(context.Context, string) (int64, error) {
	panic("CountDependents not implemented")
}

func (f *fakeDataProductRepo) ListOrphanAssets(context.Context) ([]domain.OrphanResource, error) {
	panic("ListOrphanAssets not implemented")
}

func (f *fakeDataProductRepo) ListOrphanSemanticModels(context.Context) ([]domain.OrphanResource, error) {
	panic("ListOrphanSemanticModels not implemented")
}

func (f *fakeDataProductRepo) GetByAssetID(context.Context, string) (*domain.DataProductListItem, error) {
	panic("GetByAssetID not implemented")
}

func TestService_CreateProject_DefaultsBranchAndValidatesAttachedProduct(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true})
	ownerTeamID := "team-1"
	productID := "prod-1"
	projects := &fakeProjectRepo{
		createFn: func(_ context.Context, p *domain.Project) (*domain.Project, error) {
			require.NotNil(t, p.OwnerTeamID)
			assert.Equal(t, ownerTeamID, *p.OwnerTeamID)
			require.NotNil(t, p.ProductID)
			assert.Equal(t, productID, *p.ProductID)
			assert.Equal(t, "main", p.DefaultBranch)
			return p, nil
		},
	}
	teams := &fakeTeamRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Team, error) {
			return &domain.Team{ID: id, Name: "Analytics"}, nil
		},
	}
	products := &fakeDataProductRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.DataProduct, error) {
			return &domain.DataProduct{ID: id, OwnerTeamID: ownerTeamID}, nil
		},
	}
	svc := NewService(projects, &fakeEnvironmentRepo{}, &fakeBuildRepo{}, teams, products)

	project, err := svc.CreateProject(ctx, "admin", domain.CreateProjectRequest{
		Name:        "analytics-authoring",
		Kind:        domain.ProjectKindShared,
		OwnerTeamID: &ownerTeamID,
		ProductID:   &productID,
	})
	require.NoError(t, err)
	assert.Equal(t, "analytics-authoring", project.Name)
}

func TestService_CreateProject_PersonalRequiresCreatingPrincipal(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})
	ownerPrincipal := "bob"
	svc := NewService(&fakeProjectRepo{}, &fakeEnvironmentRepo{}, &fakeBuildRepo{}, &fakeTeamRepo{}, &fakeDataProductRepo{})

	_, err := svc.CreateProject(ctx, "alice", domain.CreateProjectRequest{
		Name:           "alice-personal",
		Kind:           domain.ProjectKindPersonal,
		OwnerPrincipal: &ownerPrincipal,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "personal projects must be owned by the creating principal")
}

func TestService_CreateEnvironment_PersonalProjectRejectsNonDev(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true})
	projects := &fakeProjectRepo{
		getFn: func(_ context.Context, name string) (*domain.Project, error) {
			return &domain.Project{ID: "proj-1", Name: name, Kind: domain.ProjectKindPersonal}, nil
		},
	}
	envs := &fakeEnvironmentRepo{
		createFn: func(context.Context, *domain.Environment) (*domain.Environment, error) {
			t.Fatal("Create should not be called")
			return nil, nil
		},
	}
	svc := NewService(projects, envs, &fakeBuildRepo{}, &fakeTeamRepo{}, &fakeDataProductRepo{})

	_, err := svc.CreateEnvironment(ctx, "admin", "alice-personal", domain.CreateEnvironmentRequest{
		Name:          "prod",
		Kind:          domain.EnvironmentKindProduction,
		TargetCatalog: "alice_dev",
		TargetSchema:  "alice",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "personal projects only support development environments")
}

func TestService_CreateBuild_UsesProjectEnvironmentAndProduct(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true})
	productID := "prod-1"
	project := &domain.Project{
		ID:            "project-1",
		Name:          "analytics-authoring",
		Kind:          domain.ProjectKindShared,
		ProductID:     &productID,
		DefaultBranch: "main",
	}
	environment := &domain.Environment{
		ID:            "env-1",
		ProjectID:     project.ID,
		ProjectName:   project.Name,
		Name:          "prod",
		Kind:          domain.EnvironmentKindProduction,
		TargetCatalog: "analytics",
		TargetSchema:  "mart",
	}
	projects := &fakeProjectRepo{
		getFn: func(_ context.Context, name string) (*domain.Project, error) {
			assert.Equal(t, project.Name, name)
			return project, nil
		},
	}
	environments := &fakeEnvironmentRepo{
		getFn: func(_ context.Context, projectIDArg, name string) (*domain.Environment, error) {
			assert.Equal(t, project.ID, projectIDArg)
			assert.Equal(t, environment.Name, name)
			return environment, nil
		},
	}
	builds := &fakeBuildRepo{
		createFn: func(_ context.Context, b *domain.Build) (*domain.Build, error) {
			assert.Equal(t, project.ID, b.ProjectID)
			require.NotNil(t, b.ProductID)
			assert.Equal(t, productID, *b.ProductID)
			assert.Equal(t, environment.ID, b.EnvironmentID)
			assert.Equal(t, domain.BuildStateReady, b.State)
			assert.Equal(t, "refs/heads/main", b.GitRef)
			assert.Equal(t, "analytics", b.TargetCatalog)
			assert.Equal(t, "mart", b.TargetSchema)
			assert.Equal(t, "admin", b.CreatedBy)
			return b, nil
		},
	}
	svc := NewService(projects, environments, builds, &fakeTeamRepo{}, &fakeDataProductRepo{})

	build, err := svc.CreateBuild(ctx, "admin", "analytics-authoring", domain.CreateBuildRequest{
		EnvironmentName: "prod",
		GitRef:          "refs/heads/main",
		TargetCatalog:   "analytics",
		TargetSchema:    "mart",
		CompileManifest: `{"version":1}`,
	})
	require.NoError(t, err)
	assert.Equal(t, "refs/heads/main", build.GitRef)
}

func TestService_ListBuilds_ByProject(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true})
	project := &domain.Project{ID: "project-1", Name: "analytics-authoring"}
	projects := &fakeProjectRepo{
		getFn: func(_ context.Context, name string) (*domain.Project, error) {
			assert.Equal(t, project.Name, name)
			return project, nil
		},
	}
	builds := &fakeBuildRepo{
		listFn: func(_ context.Context, projectID string, page domain.PageRequest) ([]domain.Build, int64, error) {
			assert.Equal(t, project.ID, projectID)
			assert.Equal(t, 10, page.Limit())
			return []domain.Build{{ID: "build-1", ProjectID: projectID, GitRef: "refs/heads/main"}}, 1, nil
		},
	}
	svc := NewService(projects, &fakeEnvironmentRepo{}, builds, &fakeTeamRepo{}, &fakeDataProductRepo{})

	items, total, err := svc.ListBuilds(ctx, project.Name, domain.PageRequest{MaxResults: 10})
	require.NoError(t, err)
	assert.EqualValues(t, 1, total)
	require.Len(t, items, 1)
	assert.Equal(t, "build-1", items[0].ID)
}
