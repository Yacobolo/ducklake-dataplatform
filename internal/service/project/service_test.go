package project

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
)

type fakeProjectRepo struct {
	createFn          func(ctx context.Context, p *domain.Project) (*domain.Project, error)
	getFn             func(ctx context.Context, name string) (*domain.Project, error)
	getByIDFn         func(ctx context.Context, id string) (*domain.Project, error)
	listFn            func(ctx context.Context, page domain.PageRequest) ([]domain.Project, int64, error)
	listByWorkspaceFn func(ctx context.Context, workspaceID string, page domain.PageRequest) ([]domain.Project, int64, error)
}

func (f *fakeProjectRepo) Create(ctx context.Context, p *domain.Project) (*domain.Project, error) {
	return f.createFn(ctx, p)
}

func (f *fakeProjectRepo) GetByID(ctx context.Context, id string) (*domain.Project, error) {
	if f.getByIDFn != nil {
		return f.getByIDFn(ctx, id)
	}
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

func (f *fakeProjectRepo) ListByWorkspace(ctx context.Context, workspaceID string, page domain.PageRequest) ([]domain.Project, int64, error) {
	if f.listByWorkspaceFn != nil {
		return f.listByWorkspaceFn(ctx, workspaceID, page)
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
	createFn  func(ctx context.Context, e *domain.Environment) (*domain.Environment, error)
	getByIDFn func(ctx context.Context, id string) (*domain.Environment, error)
	getFn     func(ctx context.Context, projectID, name string) (*domain.Environment, error)
	listFn    func(ctx context.Context, projectID string, page domain.PageRequest) ([]domain.Environment, int64, error)
}

func (f *fakeEnvironmentRepo) Create(ctx context.Context, e *domain.Environment) (*domain.Environment, error) {
	return f.createFn(ctx, e)
}

func (f *fakeEnvironmentRepo) GetByID(ctx context.Context, id string) (*domain.Environment, error) {
	if f.getByIDFn != nil {
		return f.getByIDFn(ctx, id)
	}
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
	createFn            func(ctx context.Context, b *domain.Build) (*domain.Build, error)
	getByIDFn           func(ctx context.Context, id string) (*domain.Build, error)
	listFn              func(ctx context.Context, projectID string, page domain.PageRequest) ([]domain.Build, int64, error)
	listByEnvironmentFn func(ctx context.Context, projectID string, environmentID string, page domain.PageRequest) ([]domain.Build, int64, error)
}

func (f *fakeBuildRepo) Create(ctx context.Context, b *domain.Build) (*domain.Build, error) {
	return f.createFn(ctx, b)
}

func (f *fakeBuildRepo) GetByID(ctx context.Context, id string) (*domain.Build, error) {
	if f.getByIDFn != nil {
		return f.getByIDFn(ctx, id)
	}
	panic("GetByID not implemented")
}

func (f *fakeBuildRepo) ListByProject(ctx context.Context, projectID string, page domain.PageRequest) ([]domain.Build, int64, error) {
	return f.listFn(ctx, projectID, page)
}

func (f *fakeBuildRepo) ListByEnvironment(ctx context.Context, projectID string, environmentID string, page domain.PageRequest) ([]domain.Build, int64, error) {
	if f.listByEnvironmentFn != nil {
		return f.listByEnvironmentFn(ctx, projectID, environmentID, page)
	}
	return f.listFn(ctx, projectID, page)
}

func (f *fakeBuildRepo) UpdateState(context.Context, string, string) error {
	panic("UpdateState not implemented")
}

type fakeWorkspaceRepo struct {
	getByIDFn          func(ctx context.Context, id string) (*domain.Workspace, error)
	getRoleFn          func(ctx context.Context, workspaceID string, principal string) (string, error)
	getPersonalFn      func(ctx context.Context, principal string) (*domain.Workspace, error)
	createFn           func(ctx context.Context, workspace *domain.Workspace) (*domain.Workspace, error)
	listFn             func(ctx context.Context, page domain.PageRequest) ([]domain.Workspace, int64, error)
	listForPrincipalFn func(ctx context.Context, principal string, page domain.PageRequest) ([]domain.Workspace, int64, error)
	updateFn           func(ctx context.Context, id string, req domain.UpdateWorkspaceRequest) (*domain.Workspace, error)
	deleteFn           func(ctx context.Context, id string) error
	upsertMemberFn     func(ctx context.Context, member *domain.WorkspaceMember) (*domain.WorkspaceMember, error)
	deleteMemberFn     func(ctx context.Context, workspaceID string, principalName string) error
	listMembersFn      func(ctx context.Context, workspaceID string) ([]domain.WorkspaceMember, error)
}

func (f *fakeWorkspaceRepo) Create(ctx context.Context, workspace *domain.Workspace) (*domain.Workspace, error) {
	if f.createFn != nil {
		return f.createFn(ctx, workspace)
	}
	panic("Create not implemented")
}

func (f *fakeWorkspaceRepo) GetByID(ctx context.Context, id string) (*domain.Workspace, error) {
	if f.getByIDFn != nil {
		return f.getByIDFn(ctx, id)
	}
	panic("GetByID not implemented")
}

func (f *fakeWorkspaceRepo) GetPersonalByPrincipal(ctx context.Context, principal string) (*domain.Workspace, error) {
	if f.getPersonalFn != nil {
		return f.getPersonalFn(ctx, principal)
	}
	panic("GetPersonalByPrincipal not implemented")
}

func (f *fakeWorkspaceRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.Workspace, int64, error) {
	if f.listFn != nil {
		return f.listFn(ctx, page)
	}
	panic("List not implemented")
}

func (f *fakeWorkspaceRepo) ListForPrincipal(ctx context.Context, principal string, page domain.PageRequest) ([]domain.Workspace, int64, error) {
	if f.listForPrincipalFn != nil {
		return f.listForPrincipalFn(ctx, principal, page)
	}
	panic("ListForPrincipal not implemented")
}

func (f *fakeWorkspaceRepo) Update(ctx context.Context, id string, req domain.UpdateWorkspaceRequest) (*domain.Workspace, error) {
	if f.updateFn != nil {
		return f.updateFn(ctx, id, req)
	}
	panic("Update not implemented")
}

func (f *fakeWorkspaceRepo) Delete(ctx context.Context, id string) error {
	if f.deleteFn != nil {
		return f.deleteFn(ctx, id)
	}
	panic("Delete not implemented")
}

func (f *fakeWorkspaceRepo) UpsertMember(ctx context.Context, member *domain.WorkspaceMember) (*domain.WorkspaceMember, error) {
	if f.upsertMemberFn != nil {
		return f.upsertMemberFn(ctx, member)
	}
	panic("UpsertMember not implemented")
}

func (f *fakeWorkspaceRepo) DeleteMember(ctx context.Context, workspaceID string, principalName string) error {
	if f.deleteMemberFn != nil {
		return f.deleteMemberFn(ctx, workspaceID, principalName)
	}
	panic("DeleteMember not implemented")
}

func (f *fakeWorkspaceRepo) ListMembers(ctx context.Context, workspaceID string) ([]domain.WorkspaceMember, error) {
	if f.listMembersFn != nil {
		return f.listMembersFn(ctx, workspaceID)
	}
	panic("ListMembers not implemented")
}

func (f *fakeWorkspaceRepo) GetMemberRole(ctx context.Context, workspaceID string, principal string) (string, error) {
	if f.getRoleFn != nil {
		return f.getRoleFn(ctx, workspaceID, principal)
	}
	panic("GetMemberRole not implemented")
}

type fakeProjectDependencyRepo struct {
	createFn  func(ctx context.Context, dep *domain.ProjectDependency) (*domain.ProjectDependency, error)
	getByIDFn func(ctx context.Context, id string) (*domain.ProjectDependency, error)
	listFn    func(ctx context.Context, projectID string) ([]domain.ProjectDependency, error)
	deleteFn  func(ctx context.Context, projectID string, dependencyID string) error
}

func (f *fakeProjectDependencyRepo) Create(ctx context.Context, dep *domain.ProjectDependency) (*domain.ProjectDependency, error) {
	return f.createFn(ctx, dep)
}

func (f *fakeProjectDependencyRepo) GetByID(ctx context.Context, id string) (*domain.ProjectDependency, error) {
	if f.getByIDFn != nil {
		return f.getByIDFn(ctx, id)
	}
	panic("GetByID not implemented")
}

func (f *fakeProjectDependencyRepo) ListByProject(ctx context.Context, projectID string) ([]domain.ProjectDependency, error) {
	return f.listFn(ctx, projectID)
}

func (f *fakeProjectDependencyRepo) Delete(ctx context.Context, projectID string, dependencyID string) error {
	return f.deleteFn(ctx, projectID, dependencyID)
}

type fakeSourceDefinitionRepo struct {
	createFn func(ctx context.Context, source *domain.SourceDefinition) (*domain.SourceDefinition, error)
	getFn    func(ctx context.Context, projectName, sourceName, tableName string) (*domain.SourceDefinition, error)
	listFn   func(ctx context.Context, projectName string) ([]domain.SourceDefinition, error)
	updateFn func(ctx context.Context, id string, source *domain.SourceDefinition) (*domain.SourceDefinition, error)
	deleteFn func(ctx context.Context, id string) error
}

func (f *fakeSourceDefinitionRepo) Create(ctx context.Context, source *domain.SourceDefinition) (*domain.SourceDefinition, error) {
	return f.createFn(ctx, source)
}

func (f *fakeSourceDefinitionRepo) GetByName(ctx context.Context, projectName, sourceName, tableName string) (*domain.SourceDefinition, error) {
	return f.getFn(ctx, projectName, sourceName, tableName)
}

func (f *fakeSourceDefinitionRepo) ListByProject(ctx context.Context, projectName string) ([]domain.SourceDefinition, error) {
	return f.listFn(ctx, projectName)
}

func (f *fakeSourceDefinitionRepo) Update(ctx context.Context, id string, source *domain.SourceDefinition) (*domain.SourceDefinition, error) {
	return f.updateFn(ctx, id, source)
}

func (f *fakeSourceDefinitionRepo) Delete(ctx context.Context, id string) error {
	return f.deleteFn(ctx, id)
}

type fakeSeedRepo struct {
	createFn func(ctx context.Context, seed *domain.Seed) (*domain.Seed, error)
	getFn    func(ctx context.Context, projectName, name string) (*domain.Seed, error)
	listFn   func(ctx context.Context, projectName string) ([]domain.Seed, error)
	updateFn func(ctx context.Context, id string, seed *domain.Seed) (*domain.Seed, error)
	deleteFn func(ctx context.Context, id string) error
}

func (f *fakeSeedRepo) Create(ctx context.Context, seed *domain.Seed) (*domain.Seed, error) {
	return f.createFn(ctx, seed)
}

func (f *fakeSeedRepo) GetByName(ctx context.Context, projectName, name string) (*domain.Seed, error) {
	return f.getFn(ctx, projectName, name)
}

func (f *fakeSeedRepo) ListByProject(ctx context.Context, projectName string) ([]domain.Seed, error) {
	return f.listFn(ctx, projectName)
}

func (f *fakeSeedRepo) Update(ctx context.Context, id string, seed *domain.Seed) (*domain.Seed, error) {
	return f.updateFn(ctx, id, seed)
}

func (f *fakeSeedRepo) Delete(ctx context.Context, id string) error {
	return f.deleteFn(ctx, id)
}

func TestService_CreateProject_DefaultsBranchAndCopiesWorkspaceOwnership(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true})
	ownerGroupID := "group-1"
	workspaceID := "workspace-1"
	workspaces := &fakeWorkspaceRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Workspace, error) {
			assert.Equal(t, workspaceID, id)
			return &domain.Workspace{ID: id, Kind: domain.WorkspaceKindShared, OwnerGroupID: &ownerGroupID}, nil
		},
		getRoleFn: func(_ context.Context, id string, principal string) (string, error) {
			assert.Equal(t, workspaceID, id)
			assert.Equal(t, "admin", principal)
			return domain.FolderShareRoleManager, nil
		},
	}
	projects := &fakeProjectRepo{
		createFn: func(_ context.Context, p *domain.Project) (*domain.Project, error) {
			assert.Equal(t, workspaceID, p.WorkspaceID)
			require.NotNil(t, p.OwnerGroupID)
			assert.Equal(t, ownerGroupID, *p.OwnerGroupID)
			assert.Equal(t, "main", p.DefaultBranch)
			return p, nil
		},
	}
	svc := NewService(workspaces, projects, &fakeEnvironmentRepo{}, nil, nil, nil, nil, nil, &fakeBuildRepo{}, nil)

	project, err := svc.CreateProject(ctx, "admin", domain.CreateProjectRequest{
		WorkspaceID: workspaceID,
		Name:        "analytics-authoring",
		Kind:        domain.ProjectKindShared,
	})
	require.NoError(t, err)
	assert.Equal(t, "analytics-authoring", project.Name)
}

func TestService_CreateProject_RejectsWorkspaceKindMismatch(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})
	workspaceID := "workspace-1"
	ownerPrincipal := "alice"
	workspaces := &fakeWorkspaceRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Workspace, error) {
			assert.Equal(t, workspaceID, id)
			return &domain.Workspace{ID: id, Kind: domain.WorkspaceKindPersonal, OwnerPrincipal: &ownerPrincipal}, nil
		},
		getRoleFn: func(_ context.Context, id string, principal string) (string, error) {
			assert.Equal(t, workspaceID, id)
			assert.Equal(t, "alice", principal)
			return domain.FolderShareRoleManager, nil
		},
	}
	svc := NewService(workspaces, &fakeProjectRepo{}, &fakeEnvironmentRepo{}, nil, nil, nil, nil, nil, &fakeBuildRepo{}, nil)

	_, err := svc.CreateProject(ctx, "alice", domain.CreateProjectRequest{
		WorkspaceID: workspaceID,
		Name:        "alice-personal",
		Kind:        domain.ProjectKindShared,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "project kind must match workspace kind")
}

func TestService_CreateEnvironment_PersonalProjectRejectsNonDev(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true})
	projects := &fakeProjectRepo{
		getFn: func(_ context.Context, name string) (*domain.Project, error) {
			return &domain.Project{ID: "proj-1", WorkspaceID: "workspace-1", Name: name, Kind: domain.ProjectKindPersonal}, nil
		},
	}
	envs := &fakeEnvironmentRepo{
		createFn: func(context.Context, *domain.Environment) (*domain.Environment, error) {
			t.Fatal("Create should not be called")
			return nil, nil
		},
	}
	svc := NewService(&fakeWorkspaceRepo{}, projects, envs, nil, nil, nil, nil, nil, &fakeBuildRepo{}, nil)

	_, err := svc.CreateEnvironment(ctx, "admin", "alice-personal", domain.CreateEnvironmentRequest{
		Name:          "prod",
		Kind:          domain.EnvironmentKindProduction,
		TargetCatalog: "alice_dev",
		TargetSchema:  "alice",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "personal projects only support development environments")
}

func TestService_CreateBuild_UsesProjectEnvironmentDefaults(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true})
	project := &domain.Project{
		ID:            "project-1",
		WorkspaceID:   "workspace-1",
		Name:          "analytics-authoring",
		Kind:          domain.ProjectKindShared,
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
			assert.Equal(t, environment.ID, b.EnvironmentID)
			assert.Equal(t, domain.BuildStateReady, b.State)
			assert.Equal(t, "refs/heads/main", b.GitRef)
			assert.Equal(t, "analytics", b.TargetCatalog)
			assert.Equal(t, "mart", b.TargetSchema)
			assert.Equal(t, "admin", b.CreatedBy)
			return b, nil
		},
	}
	svc := NewService(&fakeWorkspaceRepo{}, projects, environments, nil, nil, nil, nil, nil, builds, nil)

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
	project := &domain.Project{ID: "project-1", WorkspaceID: "workspace-1", Name: "analytics-authoring"}
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
	svc := NewService(&fakeWorkspaceRepo{}, projects, &fakeEnvironmentRepo{}, nil, nil, nil, nil, nil, builds, nil)

	items, total, err := svc.ListBuilds(ctx, project.Name, domain.PageRequest{MaxResults: 10})
	require.NoError(t, err)
	assert.EqualValues(t, 1, total)
	require.Len(t, items, 1)
	assert.Equal(t, "build-1", items[0].ID)
}

func TestService_GetEnvironmentForProject_RejectsCrossProjectEnvironment(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true})
	project := &domain.Project{ID: "project-1", WorkspaceID: "workspace-1", Name: "analytics-authoring"}
	projects := &fakeProjectRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Project, error) {
			assert.Equal(t, project.ID, id)
			return project, nil
		},
	}
	environments := &fakeEnvironmentRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Environment, error) {
			assert.Equal(t, "env-1", id)
			return &domain.Environment{ID: id, ProjectID: "project-2", Name: "prod"}, nil
		},
	}

	svc := NewService(&fakeWorkspaceRepo{}, projects, environments, nil, nil, nil, nil, nil, &fakeBuildRepo{}, nil)

	_, err := svc.GetEnvironmentForProject(ctx, "admin", true, project.ID, "env-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "environment does not belong to project")
}

func TestService_GetBuildForProject_RejectsCrossProjectBuild(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true})
	project := &domain.Project{ID: "project-1", WorkspaceID: "workspace-1", Name: "analytics-authoring"}
	projects := &fakeProjectRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Project, error) {
			assert.Equal(t, project.ID, id)
			return project, nil
		},
	}
	builds := &fakeBuildRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Build, error) {
			assert.Equal(t, "build-1", id)
			return &domain.Build{ID: id, ProjectID: "project-2", GitRef: "refs/heads/main"}, nil
		},
	}

	svc := NewService(&fakeWorkspaceRepo{}, projects, &fakeEnvironmentRepo{}, nil, nil, nil, nil, nil, builds, nil)

	_, err := svc.GetBuildForProject(ctx, "admin", true, project.ID, "build-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "build does not belong to project")
}

func TestService_CreateDependencyForProject_PersistsDeclaredDependency(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true})
	project := &domain.Project{ID: "project-1", WorkspaceID: "workspace-1", Name: "analytics-authoring"}
	projects := &fakeProjectRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Project, error) {
			assert.Equal(t, project.ID, id)
			return project, nil
		},
		getFn: func(_ context.Context, name string) (*domain.Project, error) {
			assert.Equal(t, "shared_lib", name)
			return &domain.Project{ID: "project-2", Name: name, Kind: domain.ProjectKindLibrary}, nil
		},
	}
	deps := &fakeProjectDependencyRepo{
		createFn: func(_ context.Context, dep *domain.ProjectDependency) (*domain.ProjectDependency, error) {
			assert.Equal(t, project.ID, dep.ProjectID)
			assert.Equal(t, project.Name, dep.ProjectName)
			assert.Equal(t, "shared_lib", dep.DependencyProject)
			assert.Equal(t, "library", dep.DependencyKind)
			assert.Equal(t, 1, dep.Position)
			return dep, nil
		},
	}

	svc := NewService(&fakeWorkspaceRepo{}, projects, &fakeEnvironmentRepo{}, deps, nil, nil, nil, nil, &fakeBuildRepo{}, nil)

	created, err := svc.CreateDependencyForProject(ctx, "admin", true, project.ID, domain.CreateProjectDependencyRequest{
		DependencyProject: "shared_lib",
		DependencyKind:    "library",
		Position:          1,
	})
	require.NoError(t, err)
	assert.Equal(t, "shared_lib", created.DependencyProject)
}

func TestService_CreateSourceForProject_PersistsSourceDefinition(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true})
	project := &domain.Project{ID: "project-1", WorkspaceID: "workspace-1", Name: "analytics-authoring"}
	projects := &fakeProjectRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Project, error) {
			assert.Equal(t, project.ID, id)
			return project, nil
		},
	}
	sources := &fakeSourceDefinitionRepo{
		createFn: func(_ context.Context, source *domain.SourceDefinition) (*domain.SourceDefinition, error) {
			assert.Equal(t, project.Name, source.ProjectName)
			assert.Equal(t, "raw", source.SourceName)
			assert.Equal(t, "orders", source.TableName)
			assert.Equal(t, "lake.raw.orders", source.RelationRef)
			require.NotNil(t, source.Freshness)
			assert.Equal(t, "loaded_at", source.Freshness.TimestampColumn)
			return source, nil
		},
	}

	svc := NewService(&fakeWorkspaceRepo{}, projects, &fakeEnvironmentRepo{}, nil, sources, nil, nil, nil, &fakeBuildRepo{}, nil)

	created, err := svc.CreateSourceForProject(ctx, "admin", true, project.ID, domain.CreateSourceDefinitionRequest{
		SourceName:  "raw",
		TableName:   "orders",
		RelationRef: "lake.raw.orders",
		Freshness: &domain.SourceFreshnessPolicy{
			TimestampColumn: "loaded_at",
			MaxLagSeconds:   3600,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "raw", created.SourceName)
}

func TestService_CreateSeedForProject_DefaultsProjectAndCsvFormat(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true})
	project := &domain.Project{ID: "project-1", WorkspaceID: "workspace-1", Name: "analytics-authoring"}
	projects := &fakeProjectRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Project, error) {
			assert.Equal(t, project.ID, id)
			return project, nil
		},
	}
	seeds := &fakeSeedRepo{
		createFn: func(_ context.Context, seed *domain.Seed) (*domain.Seed, error) {
			assert.Equal(t, project.Name, seed.ProjectName)
			assert.Equal(t, "seed_orders", seed.Name)
			assert.Equal(t, "csv", seed.Format)
			assert.Equal(t, ",", seed.Delimiter)
			assert.True(t, seed.HasHeader)
			assert.Equal(t, map[string]string{"order_id": "INTEGER"}, seed.ColumnTypes)
			assert.Equal(t, []string{"finance"}, seed.Tags)
			return seed, nil
		},
	}

	svc := NewService(&fakeWorkspaceRepo{}, projects, &fakeEnvironmentRepo{}, nil, nil, seeds, nil, nil, &fakeBuildRepo{}, nil)

	created, err := svc.CreateSeedForProject(ctx, "admin", true, project.ID, domain.CreateSeedRequest{
		Name:        "seed_orders",
		InputRef:    "fixtures/orders.csv",
		ColumnTypes: map[string]string{"order_id": "INTEGER"},
		Tags:        []string{"finance"},
	})
	require.NoError(t, err)
	assert.Equal(t, "seed_orders", created.Name)
}
