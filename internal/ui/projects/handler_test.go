package projects

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Yacobolo/quackstack/internal/domain"
	macrosvc "github.com/Yacobolo/quackstack/internal/service/macro"
	modelsvc "github.com/Yacobolo/quackstack/internal/service/model"
	projectsvc "github.com/Yacobolo/quackstack/internal/service/project"
	workspacesvc "github.com/Yacobolo/quackstack/internal/service/workspace"
	"github.com/Yacobolo/quackstack/internal/ui/core"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testAuditRepo struct{}

func (testAuditRepo) Insert(_ context.Context, _ *domain.AuditEntry) error { return nil }
func (testAuditRepo) List(_ context.Context, _ domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	return nil, 0, nil
}

type fakeWorkspaceRepo struct {
	items   map[string]*domain.Workspace
	members map[string]map[string]string
}

func newFakeWorkspaceRepo() *fakeWorkspaceRepo {
	return &fakeWorkspaceRepo{items: map[string]*domain.Workspace{}, members: map[string]map[string]string{}}
}

func (r *fakeWorkspaceRepo) Create(_ context.Context, workspace *domain.Workspace) (*domain.Workspace, error) {
	item := *workspace
	item.ID = fmt.Sprintf("ws-%d", len(r.items)+1)
	r.items[item.ID] = &item
	return &item, nil
}

func (r *fakeWorkspaceRepo) GetByID(_ context.Context, id string) (*domain.Workspace, error) {
	item, ok := r.items[id]
	if !ok {
		return nil, domain.ErrNotFound("workspace %q not found", id)
	}
	copy := *item
	return &copy, nil
}

func (r *fakeWorkspaceRepo) GetPersonalByPrincipal(_ context.Context, principal string) (*domain.Workspace, error) {
	for _, item := range r.items {
		if item.OwnerPrincipal != nil && *item.OwnerPrincipal == principal {
			copy := *item
			return &copy, nil
		}
	}
	return nil, domain.ErrNotFound("personal workspace not found")
}

func (r *fakeWorkspaceRepo) List(_ context.Context, page domain.PageRequest) ([]domain.Workspace, int64, error) {
	items := make([]domain.Workspace, 0, len(r.items))
	for _, item := range r.items {
		items = append(items, *item)
	}
	return items, int64(len(items)), nil
}

func (r *fakeWorkspaceRepo) ListForPrincipal(_ context.Context, principal string, _ domain.PageRequest) ([]domain.Workspace, int64, error) {
	items := make([]domain.Workspace, 0)
	for id, item := range r.items {
		if role := r.members[id][principal]; role != "" {
			items = append(items, *item)
		}
	}
	return items, int64(len(items)), nil
}

func (r *fakeWorkspaceRepo) Update(_ context.Context, id string, _ domain.UpdateWorkspaceRequest) (*domain.Workspace, error) {
	return r.GetByID(context.Background(), id)
}
func (r *fakeWorkspaceRepo) Delete(_ context.Context, id string) error {
	delete(r.items, id)
	return nil
}
func (r *fakeWorkspaceRepo) UpsertMember(_ context.Context, member *domain.WorkspaceMember) (*domain.WorkspaceMember, error) {
	if r.members[member.WorkspaceID] == nil {
		r.members[member.WorkspaceID] = map[string]string{}
	}
	r.members[member.WorkspaceID][member.PrincipalName] = member.Role
	copy := *member
	return &copy, nil
}
func (r *fakeWorkspaceRepo) DeleteMember(_ context.Context, workspaceID string, principalName string) error {
	delete(r.members[workspaceID], principalName)
	return nil
}
func (r *fakeWorkspaceRepo) ListMembers(_ context.Context, workspaceID string) ([]domain.WorkspaceMember, error) {
	out := make([]domain.WorkspaceMember, 0, len(r.members[workspaceID]))
	for principal, role := range r.members[workspaceID] {
		out = append(out, domain.WorkspaceMember{WorkspaceID: workspaceID, PrincipalName: principal, Role: role})
	}
	return out, nil
}
func (r *fakeWorkspaceRepo) GetMemberRole(_ context.Context, workspaceID string, principalName string) (string, error) {
	return r.members[workspaceID][principalName], nil
}

type fakeProjectRepo struct {
	items map[string]*domain.Project
}

func newFakeProjectRepo() *fakeProjectRepo {
	return &fakeProjectRepo{items: map[string]*domain.Project{}}
}

func (r *fakeProjectRepo) Create(_ context.Context, p *domain.Project) (*domain.Project, error) {
	item := *p
	item.ID = fmt.Sprintf("prj-%d", len(r.items)+1)
	r.items[item.ID] = &item
	return &item, nil
}
func (r *fakeProjectRepo) GetByID(_ context.Context, id string) (*domain.Project, error) {
	item, ok := r.items[id]
	if !ok {
		return nil, domain.ErrNotFound("project %q not found", id)
	}
	copy := *item
	return &copy, nil
}
func (r *fakeProjectRepo) GetByName(_ context.Context, name string) (*domain.Project, error) {
	for _, item := range r.items {
		if item.Name == name {
			copy := *item
			return &copy, nil
		}
	}
	return nil, domain.ErrNotFound("project %q not found", name)
}
func (r *fakeProjectRepo) List(_ context.Context, _ domain.PageRequest) ([]domain.Project, int64, error) {
	out := make([]domain.Project, 0, len(r.items))
	for _, item := range r.items {
		out = append(out, *item)
	}
	return out, int64(len(out)), nil
}
func (r *fakeProjectRepo) ListByWorkspace(_ context.Context, workspaceID string, _ domain.PageRequest) ([]domain.Project, int64, error) {
	out := make([]domain.Project, 0)
	for _, item := range r.items {
		if item.WorkspaceID == workspaceID {
			out = append(out, *item)
		}
	}
	return out, int64(len(out)), nil
}
func (r *fakeProjectRepo) ListByProduct(_ context.Context, productID string, _ domain.PageRequest) ([]domain.Project, int64, error) {
	out := make([]domain.Project, 0)
	for _, item := range r.items {
		if item.ProductID != nil && *item.ProductID == productID {
			out = append(out, *item)
		}
	}
	return out, int64(len(out)), nil
}
func (r *fakeProjectRepo) Update(_ context.Context, id string, _ domain.UpdateProjectRequest) (*domain.Project, error) {
	return r.GetByID(context.Background(), id)
}
func (r *fakeProjectRepo) Delete(_ context.Context, id string) error { delete(r.items, id); return nil }

type fakeEnvironmentRepo struct {
	items map[string]*domain.Environment
}

func newFakeEnvironmentRepo() *fakeEnvironmentRepo {
	return &fakeEnvironmentRepo{items: map[string]*domain.Environment{}}
}

func (r *fakeEnvironmentRepo) Create(_ context.Context, e *domain.Environment) (*domain.Environment, error) {
	item := *e
	item.ID = fmt.Sprintf("env-%d", len(r.items)+1)
	r.items[item.ID] = &item
	return &item, nil
}
func (r *fakeEnvironmentRepo) GetByID(_ context.Context, id string) (*domain.Environment, error) {
	item, ok := r.items[id]
	if !ok {
		return nil, domain.ErrNotFound("environment %q not found", id)
	}
	copy := *item
	return &copy, nil
}
func (r *fakeEnvironmentRepo) GetByName(_ context.Context, projectID, name string) (*domain.Environment, error) {
	for _, item := range r.items {
		if item.ProjectID == projectID && item.Name == name {
			copy := *item
			return &copy, nil
		}
	}
	return nil, domain.ErrNotFound("environment %q not found", name)
}
func (r *fakeEnvironmentRepo) ListByProject(_ context.Context, projectID string, _ domain.PageRequest) ([]domain.Environment, int64, error) {
	out := make([]domain.Environment, 0)
	for _, item := range r.items {
		if item.ProjectID == projectID {
			out = append(out, *item)
		}
	}
	return out, int64(len(out)), nil
}
func (r *fakeEnvironmentRepo) Update(_ context.Context, id string, req domain.UpdateEnvironmentRequest) (*domain.Environment, error) {
	item, ok := r.items[id]
	if !ok {
		return nil, domain.ErrNotFound("environment %q not found", id)
	}
	if req.Description != nil {
		item.Description = *req.Description
	}
	if req.TargetCatalog != nil {
		item.TargetCatalog = *req.TargetCatalog
	}
	if req.TargetSchema != nil {
		item.TargetSchema = *req.TargetSchema
	}
	if req.ComputeEndpoint != nil {
		if *req.ComputeEndpoint == "" {
			item.ComputeEndpoint = nil
		} else {
			value := *req.ComputeEndpoint
			item.ComputeEndpoint = &value
		}
	}
	if req.DeferToEnvironment != nil {
		if *req.DeferToEnvironment == "" {
			item.DeferToEnvironment = nil
		} else {
			value := *req.DeferToEnvironment
			item.DeferToEnvironment = &value
		}
	}
	if req.Variables != nil {
		item.Variables = make(map[string]string, len(*req.Variables))
		for key, value := range *req.Variables {
			item.Variables[key] = value
		}
	}
	if req.SourceOverrides != nil {
		item.SourceOverrides = make(map[string]string, len(*req.SourceOverrides))
		for key, value := range *req.SourceOverrides {
			item.SourceOverrides[key] = value
		}
	}
	copy := *item
	return &copy, nil
}
func (r *fakeEnvironmentRepo) Delete(_ context.Context, id string) error {
	delete(r.items, id)
	return nil
}

type fakeBuildRepo struct {
	items map[string]*domain.Build
}

func newFakeBuildRepo() *fakeBuildRepo {
	return &fakeBuildRepo{items: map[string]*domain.Build{}}
}

func (r *fakeBuildRepo) Create(_ context.Context, b *domain.Build) (*domain.Build, error) {
	item := *b
	item.ID = fmt.Sprintf("build-%d", len(r.items)+1)
	r.items[item.ID] = &item
	return &item, nil
}
func (r *fakeBuildRepo) GetByID(_ context.Context, id string) (*domain.Build, error) {
	item, ok := r.items[id]
	if !ok {
		return nil, domain.ErrNotFound("build %q not found", id)
	}
	copy := *item
	return &copy, nil
}
func (r *fakeBuildRepo) ListByProject(_ context.Context, projectID string, _ domain.PageRequest) ([]domain.Build, int64, error) {
	out := make([]domain.Build, 0)
	for _, item := range r.items {
		if item.ProjectID == projectID {
			out = append(out, *item)
		}
	}
	return out, int64(len(out)), nil
}
func (r *fakeBuildRepo) UpdateState(_ context.Context, _ string, _ string) error { return nil }

type fakeModelRepo struct {
	items map[string]*domain.Model
}

func newFakeModelRepo() *fakeModelRepo { return &fakeModelRepo{items: map[string]*domain.Model{}} }

func (r *fakeModelRepo) Create(_ context.Context, m *domain.Model) (*domain.Model, error) {
	item := *m
	item.ID = fmt.Sprintf("model-%d", len(r.items)+1)
	r.items[item.ID] = &item
	return &item, nil
}
func (r *fakeModelRepo) CreateWithNotebookLink(ctx context.Context, m *domain.Model, _, _ string) (*domain.Model, error) {
	return r.Create(ctx, m)
}
func (r *fakeModelRepo) GetByID(_ context.Context, id string) (*domain.Model, error) {
	item, ok := r.items[id]
	if !ok {
		return nil, domain.ErrNotFound("model %q not found", id)
	}
	copy := *item
	return &copy, nil
}
func (r *fakeModelRepo) GetByName(_ context.Context, projectName, name string) (*domain.Model, error) {
	for _, item := range r.items {
		if item.ProjectName == projectName && item.Name == name {
			copy := *item
			return &copy, nil
		}
	}
	return nil, domain.ErrNotFound("model %q not found", name)
}
func (r *fakeModelRepo) List(_ context.Context, projectName *string, _ domain.PageRequest) ([]domain.Model, int64, error) {
	out := make([]domain.Model, 0)
	for _, item := range r.items {
		if projectName == nil || item.ProjectName == *projectName {
			out = append(out, *item)
		}
	}
	return out, int64(len(out)), nil
}
func (r *fakeModelRepo) Update(_ context.Context, id string, _ domain.UpdateModelRequest) (*domain.Model, error) {
	return r.GetByID(context.Background(), id)
}
func (r *fakeModelRepo) Delete(_ context.Context, id string) error { delete(r.items, id); return nil }
func (r *fakeModelRepo) ListAll(_ context.Context) ([]domain.Model, error) {
	out := make([]domain.Model, 0, len(r.items))
	for _, item := range r.items {
		out = append(out, *item)
	}
	return out, nil
}
func (r *fakeModelRepo) UpdateDependencies(_ context.Context, _ string, _ []string) error { return nil }

type fakeMacroRepo struct {
	items map[string]*domain.Macro
}

func newFakeMacroRepo() *fakeMacroRepo { return &fakeMacroRepo{items: map[string]*domain.Macro{}} }

func (r *fakeMacroRepo) Create(_ context.Context, m *domain.Macro) (*domain.Macro, error) {
	item := *m
	item.ID = fmt.Sprintf("macro-%d", len(r.items)+1)
	r.items[item.Name] = &item
	return &item, nil
}
func (r *fakeMacroRepo) GetByName(_ context.Context, name string) (*domain.Macro, error) {
	item, ok := r.items[name]
	if !ok {
		return nil, domain.ErrNotFound("macro %q not found", name)
	}
	copy := *item
	return &copy, nil
}
func (r *fakeMacroRepo) List(_ context.Context, _ domain.PageRequest) ([]domain.Macro, int64, error) {
	all, _ := r.ListAll(context.Background())
	return all, int64(len(all)), nil
}
func (r *fakeMacroRepo) Update(_ context.Context, name string, _ domain.UpdateMacroRequest) (*domain.Macro, error) {
	return r.GetByName(context.Background(), name)
}
func (r *fakeMacroRepo) Delete(_ context.Context, name string) error {
	delete(r.items, name)
	return nil
}
func (r *fakeMacroRepo) ListAll(_ context.Context) ([]domain.Macro, error) {
	out := make([]domain.Macro, 0, len(r.items))
	for _, item := range r.items {
		out = append(out, *item)
	}
	return out, nil
}
func (r *fakeMacroRepo) ListRevisions(context.Context, string) ([]domain.MacroRevision, error) {
	return nil, nil
}
func (r *fakeMacroRepo) GetRevisionByVersion(context.Context, string, int) (*domain.MacroRevision, error) {
	return nil, nil
}

type fakeSemanticModelRepo struct {
	items map[string]*domain.SemanticModel
}

func newFakeSemanticModelRepo() *fakeSemanticModelRepo {
	return &fakeSemanticModelRepo{items: map[string]*domain.SemanticModel{}}
}

func (r *fakeSemanticModelRepo) Create(_ context.Context, m *domain.SemanticModel) (*domain.SemanticModel, error) {
	item := *m
	item.ID = fmt.Sprintf("semantic-%d", len(r.items)+1)
	r.items[item.ID] = &item
	return &item, nil
}
func (r *fakeSemanticModelRepo) GetByID(_ context.Context, id string) (*domain.SemanticModel, error) {
	item, ok := r.items[id]
	if !ok {
		return nil, domain.ErrNotFound("semantic model %q not found", id)
	}
	copy := *item
	return &copy, nil
}
func (r *fakeSemanticModelRepo) GetByName(_ context.Context, name string) (*domain.SemanticModel, error) {
	for _, item := range r.items {
		if item.Name == name {
			copy := *item
			return &copy, nil
		}
	}
	return nil, domain.ErrNotFound("semantic model %q not found", name)
}
func (r *fakeSemanticModelRepo) List(_ context.Context, _ domain.PageRequest) ([]domain.SemanticModel, int64, error) {
	all, _ := r.ListAll(context.Background())
	return all, int64(len(all)), nil
}
func (r *fakeSemanticModelRepo) Update(_ context.Context, id string, _ domain.UpdateSemanticModelRequest) (*domain.SemanticModel, error) {
	return r.GetByID(context.Background(), id)
}
func (r *fakeSemanticModelRepo) Delete(_ context.Context, id string) error {
	delete(r.items, id)
	return nil
}
func (r *fakeSemanticModelRepo) ListAll(_ context.Context) ([]domain.SemanticModel, error) {
	out := make([]domain.SemanticModel, 0, len(r.items))
	for _, item := range r.items {
		out = append(out, *item)
	}
	return out, nil
}

func TestProjectsRoutes_RenderListAndDetail(t *testing.T) {
	t.Parallel()

	fixture := setupProjectsHandler(t)

	router := chi.NewRouter()
	MountRoutes(router, fixture.Handler)

	listReq := httptest.NewRequest(http.MethodGet, "/projects", nil)
	listReq = listReq.WithContext(domain.WithPrincipal(listReq.Context(), domain.ContextPrincipal{Name: "alice"}))
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)
	require.Equal(t, http.StatusOK, listRec.Code)
	assert.Contains(t, listRec.Body.String(), "Projects")
	assert.Contains(t, listRec.Body.String(), "analytics")

	modelsReq := httptest.NewRequest(http.MethodGet, "/projects/"+fixture.ProjectID, nil)
	modelsReq = modelsReq.WithContext(domain.WithPrincipal(modelsReq.Context(), domain.ContextPrincipal{Name: "alice"}))
	modelsRec := httptest.NewRecorder()
	router.ServeHTTP(modelsRec, modelsReq)
	require.Equal(t, http.StatusOK, modelsRec.Code)
	assert.Contains(t, modelsRec.Body.String(), "Models")
	assert.Contains(t, modelsRec.Body.String(), "orders")

	macrosReq := httptest.NewRequest(http.MethodGet, "/projects/"+fixture.ProjectID+"?tab=macros", nil)
	macrosReq = macrosReq.WithContext(domain.WithPrincipal(macrosReq.Context(), domain.ContextPrincipal{Name: "alice"}))
	macrosRec := httptest.NewRecorder()
	router.ServeHTTP(macrosRec, macrosReq)
	require.Equal(t, http.StatusOK, macrosRec.Code)
	assert.Contains(t, macrosRec.Body.String(), "safe_sum")

	buildsReq := httptest.NewRequest(http.MethodGet, "/projects/"+fixture.ProjectID+"?tab=builds", nil)
	buildsReq = buildsReq.WithContext(domain.WithPrincipal(buildsReq.Context(), domain.ContextPrincipal{Name: "alice"}))
	buildsRec := httptest.NewRecorder()
	router.ServeHTTP(buildsRec, buildsReq)
	require.Equal(t, http.StatusOK, buildsRec.Code)
	assert.Contains(t, buildsRec.Body.String(), "Build ID")
	assert.Contains(t, buildsRec.Body.String(), "build-1")
	assert.Contains(t, buildsRec.Body.String(), `href="/ui/projects/`+fixture.ProjectID+`/builds/`+fixture.BuildID+`"`)
	assert.Contains(t, buildsRec.Body.String(), "display-plum-scale-0")

	environmentsReq := httptest.NewRequest(http.MethodGet, "/projects/"+fixture.ProjectID+"?tab=environments", nil)
	environmentsReq = environmentsReq.WithContext(domain.WithPrincipal(environmentsReq.Context(), domain.ContextPrincipal{Name: "alice"}))
	environmentsRec := httptest.NewRecorder()
	router.ServeHTTP(environmentsRec, environmentsReq)
	require.Equal(t, http.StatusOK, environmentsRec.Code)
	assert.Contains(t, environmentsRec.Body.String(), `href="/ui/projects/`+fixture.ProjectID+`/environments/`+fixture.EnvironmentID+`"`)
	assert.Contains(t, environmentsRec.Body.String(), "display-teal-scale-0")
	assert.Contains(t, environmentsRec.Body.String(), "New environment")
}

func TestProjectsRoutes_RenderEnvironmentAndBuildDetails(t *testing.T) {
	t.Parallel()

	fixture := setupProjectsHandler(t)

	router := chi.NewRouter()
	MountRoutes(router, fixture.Handler)

	environmentReq := httptest.NewRequest(http.MethodGet, "/projects/"+fixture.ProjectID+"/environments/"+fixture.EnvironmentID, nil)
	environmentReq = environmentReq.WithContext(domain.WithPrincipal(environmentReq.Context(), domain.ContextPrincipal{Name: "alice"}))
	environmentRec := httptest.NewRecorder()
	router.ServeHTTP(environmentRec, environmentReq)
	require.Equal(t, http.StatusOK, environmentRec.Code)
	assert.Contains(t, environmentRec.Body.String(), "Project environment")
	assert.Contains(t, environmentRec.Body.String(), "warehouse-dev")
	assert.Contains(t, environmentRec.Body.String(), "main.analytics")
	assert.Contains(t, environmentRec.Body.String(), "shared-dev")
	assert.Contains(t, environmentRec.Body.String(), "DUCKDB_SCHEMA")
	assert.Contains(t, environmentRec.Body.String(), "raw.orders")
	assert.Contains(t, environmentRec.Body.String(), "Related builds")
	assert.Contains(t, environmentRec.Body.String(), `href="/ui/projects/`+fixture.ProjectID+`/builds/`+fixture.BuildID+`"`)

	buildReq := httptest.NewRequest(http.MethodGet, "/projects/"+fixture.ProjectID+"/builds/"+fixture.BuildID, nil)
	buildReq = buildReq.WithContext(domain.WithPrincipal(buildReq.Context(), domain.ContextPrincipal{Name: "alice"}))
	buildRec := httptest.NewRecorder()
	router.ServeHTTP(buildRec, buildReq)
	require.Equal(t, http.StatusOK, buildRec.Code)
	assert.Contains(t, buildRec.Body.String(), "Project build")
	assert.Contains(t, buildRec.Body.String(), "refs/heads/main")
	assert.Contains(t, buildRec.Body.String(), "abc123")
	assert.Contains(t, buildRec.Body.String(), "tag:nightly")
	assert.Contains(t, buildRec.Body.String(), "analytics mart ready")
	assert.Contains(t, buildRec.Body.String(), `href="/ui/projects/`+fixture.ProjectID+`/environments/`+fixture.EnvironmentID+`"`)
	assert.Contains(t, buildRec.Body.String(), "warning: freshness drift")
}

func TestProjectsRoutes_RejectCrossProjectNestedDetails(t *testing.T) {
	t.Parallel()

	fixture := setupProjectsHandler(t)

	router := chi.NewRouter()
	MountRoutes(router, fixture.Handler)

	environmentReq := httptest.NewRequest(http.MethodGet, "/projects/"+fixture.OtherProjectID+"/environments/"+fixture.EnvironmentID, nil)
	environmentReq = environmentReq.WithContext(domain.WithPrincipal(environmentReq.Context(), domain.ContextPrincipal{Name: "alice"}))
	environmentRec := httptest.NewRecorder()
	router.ServeHTTP(environmentRec, environmentReq)
	require.Equal(t, http.StatusBadRequest, environmentRec.Code)
	assert.Contains(t, environmentRec.Body.String(), "environment does not belong to project")

	buildReq := httptest.NewRequest(http.MethodGet, "/projects/"+fixture.OtherProjectID+"/builds/"+fixture.BuildID, nil)
	buildReq = buildReq.WithContext(domain.WithPrincipal(buildReq.Context(), domain.ContextPrincipal{Name: "alice"}))
	buildRec := httptest.NewRecorder()
	router.ServeHTTP(buildRec, buildReq)
	require.Equal(t, http.StatusBadRequest, buildRec.Code)
	assert.Contains(t, buildRec.Body.String(), "build does not belong to project")
}

func TestProjectsRoutes_EnvironmentCRUD(t *testing.T) {
	t.Parallel()

	fixture := setupProjectsHandler(t)

	router := chi.NewRouter()
	MountRoutes(router, fixture.Handler)

	newReq := httptest.NewRequest(http.MethodGet, "/projects/"+fixture.ProjectID+"/environments/new", nil)
	newReq = newReq.WithContext(domain.WithPrincipal(newReq.Context(), domain.ContextPrincipal{Name: "alice", IsAdmin: true}))
	newRec := httptest.NewRecorder()
	router.ServeHTTP(newRec, newReq)
	require.Equal(t, http.StatusOK, newRec.Code)
	assert.Contains(t, newRec.Body.String(), "New Environment")
	assert.Contains(t, newRec.Body.String(), "Target catalog")
	assert.NotContains(t, newRec.Body.String(), `option value="staging"`)

	createReq := httptest.NewRequest(http.MethodPost, "/projects/"+fixture.ProjectID+"/environments", strings.NewReader("name=sandbox&kind=development&description=Sandbox+env&target_catalog=main&target_schema=sandbox&compute_endpoint=warehouse-sandbox&defer_to_environment=&variables=FOO%3Dbar&source_overrides=orders%3Dsandbox.orders"))
	createReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	createReq = createReq.WithContext(domain.WithPrincipal(createReq.Context(), domain.ContextPrincipal{Name: "alice", IsAdmin: true}))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	require.Equal(t, http.StatusSeeOther, createRec.Code)
	require.Contains(t, createRec.Header().Get("Location"), "/ui/projects/"+fixture.ProjectID+"/environments/")
	require.Len(t, fixture.EnvironmentRepo.items, 2)

	var createdID string
	for id, item := range fixture.EnvironmentRepo.items {
		if item.Name == "sandbox" {
			createdID = id
			assert.Equal(t, "Sandbox env", item.Description)
			require.NotNil(t, item.ComputeEndpoint)
			assert.Equal(t, "warehouse-sandbox", *item.ComputeEndpoint)
			assert.Equal(t, "bar", item.Variables["FOO"])
			assert.Equal(t, "sandbox.orders", item.SourceOverrides["orders"])
		}
	}
	require.NotEmpty(t, createdID)

	editReq := httptest.NewRequest(http.MethodGet, "/projects/"+fixture.ProjectID+"/environments/"+fixture.EnvironmentID+"/edit", nil)
	editReq = editReq.WithContext(domain.WithPrincipal(editReq.Context(), domain.ContextPrincipal{Name: "alice", IsAdmin: true}))
	editRec := httptest.NewRecorder()
	router.ServeHTTP(editRec, editReq)
	require.Equal(t, http.StatusOK, editRec.Code)
	assert.Contains(t, editRec.Body.String(), "Edit Environment")
	assert.Contains(t, editRec.Body.String(), "warehouse-dev")

	updateReq := httptest.NewRequest(http.MethodPost, "/projects/"+fixture.ProjectID+"/environments/"+fixture.EnvironmentID+"/update", strings.NewReader("description=Updated+dev+env&target_catalog=main&target_schema=analytics_dev&compute_endpoint=&defer_to_environment=&variables=&source_overrides="))
	updateReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	updateReq = updateReq.WithContext(domain.WithPrincipal(updateReq.Context(), domain.ContextPrincipal{Name: "alice", IsAdmin: true}))
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)
	require.Equal(t, http.StatusSeeOther, updateRec.Code)
	updated := fixture.EnvironmentRepo.items[fixture.EnvironmentID]
	require.NotNil(t, updated)
	assert.Equal(t, "Updated dev env", updated.Description)
	assert.Equal(t, "analytics_dev", updated.TargetSchema)
	assert.Nil(t, updated.ComputeEndpoint)
	assert.Nil(t, updated.DeferToEnvironment)
	assert.Empty(t, updated.Variables)
	assert.Empty(t, updated.SourceOverrides)

	deleteReq := httptest.NewRequest(http.MethodPost, "/projects/"+fixture.ProjectID+"/environments/"+createdID+"/delete", nil)
	deleteReq = deleteReq.WithContext(domain.WithPrincipal(deleteReq.Context(), domain.ContextPrincipal{Name: "alice", IsAdmin: true}))
	deleteRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRec, deleteReq)
	require.Equal(t, http.StatusSeeOther, deleteRec.Code)
	assert.Equal(t, projectTab(projectDetailURL(fixture.ProjectID), projectTabEnvironments), deleteRec.Header().Get("Location"))
	_, exists := fixture.EnvironmentRepo.items[createdID]
	assert.False(t, exists)
}

func TestProjectsPage_RenderIncludesNavigationState(t *testing.T) {
	t.Parallel()

	page := projectsListPage(domain.ContextPrincipal{Name: "alice"}, []projectListRowData{
		{
			ID:             "prj_123",
			Name:           "analytics",
			Kind:           "Shared",
			WorkspaceName:  "Team workspace",
			DefaultBranch:  "main",
			OwnerSummary:   "alice",
			ProductSummary: "Unlinked",
			CreatedAt:      "2026-04-14 10:00 UTC",
			URL:            "/ui/projects/prj_123",
		},
	}, domain.PageRequest{MaxResults: 10}, 1)

	var buf bytes.Buffer
	require.NoError(t, page.Render(&buf))
	html := buf.String()

	assert.Contains(t, html, `href="/ui/projects"`)
	assert.Contains(t, html, "analytics")
}

type projectsHandlerFixture struct {
	Handler         *Handler
	ProjectID       string
	OtherProjectID  string
	EnvironmentID   string
	BuildID         string
	EnvironmentRepo *fakeEnvironmentRepo
}

func setupProjectsHandler(t *testing.T) projectsHandlerFixture {
	t.Helper()

	workspaceRepo := newFakeWorkspaceRepo()
	projectRepo := newFakeProjectRepo()
	environmentRepo := newFakeEnvironmentRepo()
	buildRepo := newFakeBuildRepo()
	modelRepo := newFakeModelRepo()
	macroRepo := newFakeMacroRepo()
	audit := testAuditRepo{}

	workspaceSvc := workspacesvc.NewService(workspaceRepo, nil, projectRepo, environmentRepo, nil, audit)
	projectSvc := projectsvc.NewService(workspaceRepo, projectRepo, environmentRepo, buildRepo, nil, nil, audit)
	modelService := modelsvc.NewService(modelsvc.ServiceDeps{
		Models:       modelRepo,
		Projects:     projectRepo,
		Environments: environmentRepo,
		Builds:       buildRepo,
		Macros:       macroRepo,
		Audit:        audit,
	})
	macroService := macrosvc.NewService(macroRepo, audit)
	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})
	workspace, err := workspaceSvc.CreateWorkspace(ctx, "alice", true, domain.CreateWorkspaceRequest{
		Name:           "Team workspace",
		Kind:           domain.WorkspaceKindPersonal,
		OwnerPrincipal: strPtr("alice"),
	})
	require.NoError(t, err)

	project, err := projectSvc.CreateProject(ctx, "alice", domain.CreateProjectRequest{
		WorkspaceID:   workspace.ID,
		Name:          "analytics",
		Kind:          domain.ProjectKindPersonal,
		DefaultBranch: "main",
	})
	require.NoError(t, err)

	otherProject, err := projectSvc.CreateProject(ctx, "alice", domain.CreateProjectRequest{
		WorkspaceID:   workspace.ID,
		Name:          "analytics-sandbox",
		Kind:          domain.ProjectKindPersonal,
		DefaultBranch: "main",
	})
	require.NoError(t, err)

	_, err = modelService.CreateModel(ctx, "alice", domain.CreateModelRequest{
		ProjectName:     "analytics",
		Name:            "orders",
		SQL:             "select 1 as id",
		Materialization: domain.MaterializationView,
	})
	require.NoError(t, err)

	_, err = macroService.Create(ctx, "alice", domain.CreateMacroRequest{
		Name:        "safe_sum",
		ProjectName: "analytics",
		MacroType:   domain.MacroTypeScalar,
		Visibility:  domain.MacroVisibilityProject,
		Body:        "SELECT 1",
	})
	require.NoError(t, err)

	computeEndpoint := "warehouse-dev"
	deferToEnvironment := "shared-dev"
	environment, err := projectSvc.CreateEnvironmentForProject(ctx, "alice", true, project.ID, domain.CreateEnvironmentRequest{
		Name:               "dev",
		Kind:               domain.EnvironmentKindDevelopment,
		Description:        "Development environment for analytics authoring.",
		TargetCatalog:      "main",
		TargetSchema:       "analytics",
		ComputeEndpoint:    &computeEndpoint,
		DeferToEnvironment: &deferToEnvironment,
		Variables: map[string]string{
			"DUCKDB_SCHEMA": "analytics",
		},
		SourceOverrides: map[string]string{
			"orders": "raw.orders",
		},
	})
	require.NoError(t, err)

	commitSHA := "abc123"
	compileDiagnostics := "warning: freshness drift"
	build, err := projectSvc.CreateBuildForProject(ctx, "alice", true, project.ID, domain.CreateBuildRequest{
		EnvironmentName:    "dev",
		GitRef:             "refs/heads/main",
		CommitSHA:          &commitSHA,
		Selector:           "tag:nightly",
		TargetCatalog:      "analytics",
		TargetSchema:       "mart",
		CompileManifest:    "analytics mart ready",
		CompileDiagnostics: &compileDiagnostics,
	})
	require.NoError(t, err)

	deps := &core.Dependencies{
		Workspace: workspaceSvc,
		Project:   projectSvc,
		Model:     modelService,
		Macro:     macroService,
	}
	return projectsHandlerFixture{
		Handler:         New(deps),
		ProjectID:       project.ID,
		OtherProjectID:  otherProject.ID,
		EnvironmentID:   environment.ID,
		BuildID:         build.ID,
		EnvironmentRepo: environmentRepo,
	}
}

func strPtr(v string) *string { return &v }
