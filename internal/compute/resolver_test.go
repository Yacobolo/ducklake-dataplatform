package compute

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/duckdb/duckdb-go/v2"

	"duck-demo/internal/domain"
)

// === Mock repositories for resolver tests ===

type mockPrincipalRepo struct {
	getByNameFn func(ctx context.Context, name string) (*domain.Principal, error)
}

func (m *mockPrincipalRepo) Create(_ context.Context, _ *domain.Principal) (*domain.Principal, error) {
	panic("unexpected")
}
func (m *mockPrincipalRepo) GetByID(_ context.Context, _ string) (*domain.Principal, error) {
	panic("unexpected")
}
func (m *mockPrincipalRepo) GetByName(ctx context.Context, name string) (*domain.Principal, error) {
	if m.getByNameFn != nil {
		return m.getByNameFn(ctx, name)
	}
	panic("unexpected")
}
func (m *mockPrincipalRepo) List(_ context.Context, _ domain.PageRequest) ([]domain.Principal, int64, error) {
	panic("unexpected")
}
func (m *mockPrincipalRepo) Delete(_ context.Context, _ string) error { panic("unexpected") }
func (m *mockPrincipalRepo) SetAdmin(_ context.Context, _ string, _ bool) error {
	panic("unexpected")
}
func (m *mockPrincipalRepo) GetByExternalID(_ context.Context, _, _ string) (*domain.Principal, error) {
	panic("unexpected")
}

type mockGroupRepo struct {
	getGroupsForMemberFn func(ctx context.Context, memberType string, memberID string) ([]domain.Group, error)
}

func (m *mockGroupRepo) Create(_ context.Context, _ *domain.Group) (*domain.Group, error) {
	panic("unexpected")
}
func (m *mockGroupRepo) GetByID(_ context.Context, _ string) (*domain.Group, error) {
	panic("unexpected")
}
func (m *mockGroupRepo) GetByName(_ context.Context, _ string) (*domain.Group, error) {
	panic("unexpected")
}
func (m *mockGroupRepo) List(_ context.Context, _ domain.PageRequest) ([]domain.Group, int64, error) {
	panic("unexpected")
}
func (m *mockGroupRepo) Delete(_ context.Context, _ string) error { panic("unexpected") }
func (m *mockGroupRepo) AddMember(_ context.Context, _ *domain.GroupMember) error {
	panic("unexpected")
}
func (m *mockGroupRepo) RemoveMember(_ context.Context, _ *domain.GroupMember) error {
	panic("unexpected")
}
func (m *mockGroupRepo) ListMembers(_ context.Context, _ string, _ domain.PageRequest) ([]domain.GroupMember, int64, error) {
	panic("unexpected")
}
func (m *mockGroupRepo) GetGroupsForMember(ctx context.Context, memberType string, memberID string) ([]domain.Group, error) {
	if m.getGroupsForMemberFn != nil {
		return m.getGroupsForMemberFn(ctx, memberType, memberID)
	}
	panic("unexpected")
}

type mockComputeRepo struct {
	getDefaultForPrincipalFn func(ctx context.Context, principalID string, principalType string) (*domain.ComputeEndpoint, error)
	// Satisfy remaining interface methods:
	createFn                     func(ctx context.Context, ep *domain.ComputeEndpoint) (*domain.ComputeEndpoint, error)
	getByIDFn                    func(ctx context.Context, id string) (*domain.ComputeEndpoint, error)
	getByNameFn                  func(ctx context.Context, name string) (*domain.ComputeEndpoint, error)
	listFn                       func(ctx context.Context, page domain.PageRequest) ([]domain.ComputeEndpoint, int64, error)
	updateFn                     func(ctx context.Context, id string, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error)
	deleteFn                     func(ctx context.Context, id string) error
	updateStatusFn               func(ctx context.Context, id string, status string) error
	assignFn                     func(ctx context.Context, a *domain.ComputeAssignment) (*domain.ComputeAssignment, error)
	unassignFn                   func(ctx context.Context, id string) error
	listAssignmentsFn            func(ctx context.Context, endpointID string, page domain.PageRequest) ([]domain.ComputeAssignment, int64, error)
	getAssignmentsForPrincipalFn func(ctx context.Context, principalID string, principalType string) ([]domain.ComputeEndpoint, error)
}

func (m *mockComputeRepo) Create(ctx context.Context, ep *domain.ComputeEndpoint) (*domain.ComputeEndpoint, error) {
	if m.createFn != nil {
		return m.createFn(ctx, ep)
	}
	panic("unexpected")
}
func (m *mockComputeRepo) GetByID(ctx context.Context, id string) (*domain.ComputeEndpoint, error) {
	if m.getByIDFn != nil {
		return m.getByIDFn(ctx, id)
	}
	panic("unexpected")
}
func (m *mockComputeRepo) GetByName(ctx context.Context, name string) (*domain.ComputeEndpoint, error) {
	if m.getByNameFn != nil {
		return m.getByNameFn(ctx, name)
	}
	panic("unexpected")
}
func (m *mockComputeRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.ComputeEndpoint, int64, error) {
	if m.listFn != nil {
		return m.listFn(ctx, page)
	}
	panic("unexpected")
}
func (m *mockComputeRepo) Update(ctx context.Context, id string, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
	if m.updateFn != nil {
		return m.updateFn(ctx, id, req)
	}
	panic("unexpected")
}
func (m *mockComputeRepo) Delete(ctx context.Context, id string) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, id)
	}
	panic("unexpected")
}
func (m *mockComputeRepo) UpdateStatus(ctx context.Context, id string, status string) error {
	if m.updateStatusFn != nil {
		return m.updateStatusFn(ctx, id, status)
	}
	panic("unexpected")
}
func (m *mockComputeRepo) Assign(ctx context.Context, a *domain.ComputeAssignment) (*domain.ComputeAssignment, error) {
	if m.assignFn != nil {
		return m.assignFn(ctx, a)
	}
	panic("unexpected")
}
func (m *mockComputeRepo) Unassign(ctx context.Context, id string) error {
	if m.unassignFn != nil {
		return m.unassignFn(ctx, id)
	}
	panic("unexpected")
}
func (m *mockComputeRepo) ListAssignments(ctx context.Context, endpointID string, page domain.PageRequest) ([]domain.ComputeAssignment, int64, error) {
	if m.listAssignmentsFn != nil {
		return m.listAssignmentsFn(ctx, endpointID, page)
	}
	panic("unexpected")
}
func (m *mockComputeRepo) GetDefaultForPrincipal(ctx context.Context, principalID string, principalType string) (*domain.ComputeEndpoint, error) {
	if m.getDefaultForPrincipalFn != nil {
		return m.getDefaultForPrincipalFn(ctx, principalID, principalType)
	}
	panic("unexpected")
}
func (m *mockComputeRepo) GetAssignmentsForPrincipal(ctx context.Context, principalID string, principalType string) ([]domain.ComputeEndpoint, error) {
	if m.getAssignmentsForPrincipalFn != nil {
		return m.getAssignmentsForPrincipalFn(ctx, principalID, principalType)
	}
	panic("unexpected")
}

// === Interface checks ===
var _ domain.PrincipalRepository = (*mockPrincipalRepo)(nil)
var _ domain.GroupRepository = (*mockGroupRepo)(nil)
var _ domain.ComputeEndpointRepository = (*mockComputeRepo)(nil)

// === Tests ===

func TestDefaultResolver_NilComputeRepo(t *testing.T) {
	localDB := openTestDuckDB(t)
	localExec := NewLocalExecutor(localDB)

	resolver := NewDefaultResolver(localExec)
	executor, err := resolver.Resolve(context.Background(), "alice")
	require.NoError(t, err)
	assert.Nil(t, executor) // Falls back to local
}

func TestResolver_PrincipalNotFound(t *testing.T) {
	localDB := openTestDuckDB(t)
	localExec := NewLocalExecutor(localDB)

	principalRepo := &mockPrincipalRepo{
		getByNameFn: func(_ context.Context, _ string) (*domain.Principal, error) {
			return nil, domain.ErrNotFound("principal not found")
		},
	}

	resolver := NewResolver(localExec, &mockComputeRepo{}, principalRepo, &mockGroupRepo{}, nil, nil)
	executor, err := resolver.Resolve(context.Background(), "unknown")
	require.NoError(t, err)
	assert.Nil(t, executor) // Falls back to local
}

func TestResolver_DirectUserAssignment_Local(t *testing.T) {
	localDB := openTestDuckDB(t)
	localExec := NewLocalExecutor(localDB)

	principalRepo := &mockPrincipalRepo{
		getByNameFn: func(_ context.Context, _ string) (*domain.Principal, error) {
			return &domain.Principal{ID: "1", Name: "alice"}, nil
		},
	}

	computeRepo := &mockComputeRepo{
		getDefaultForPrincipalFn: func(_ context.Context, principalID string, principalType string) (*domain.ComputeEndpoint, error) {
			if principalID == "1" && principalType == "user" {
				return &domain.ComputeEndpoint{
					ID: "10", Name: "local-ep", Type: "LOCAL", Status: "ACTIVE",
				}, nil
			}
			return nil, domain.ErrNotFound("no assignment")
		},
	}

	resolver := NewResolver(localExec, computeRepo, principalRepo, &mockGroupRepo{
		getGroupsForMemberFn: func(_ context.Context, _ string, _ string) ([]domain.Group, error) {
			return nil, nil
		},
	}, nil, nil)

	executor, err := resolver.Resolve(context.Background(), "alice")
	require.NoError(t, err)
	assert.Same(t, localExec, executor) // LOCAL type returns the local executor
}

func TestResolver_DirectUserAssignment_Remote(t *testing.T) {
	// Set up a test server that responds to health checks
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	localDB := openTestDuckDB(t)
	localExec := NewLocalExecutor(localDB)
	cache := NewRemoteCache(localDB)

	principalRepo := &mockPrincipalRepo{
		getByNameFn: func(_ context.Context, _ string) (*domain.Principal, error) {
			return &domain.Principal{ID: "1", Name: "alice"}, nil
		},
	}

	computeRepo := &mockComputeRepo{
		getDefaultForPrincipalFn: func(_ context.Context, principalID string, principalType string) (*domain.ComputeEndpoint, error) {
			if principalID == "1" && principalType == "user" {
				return &domain.ComputeEndpoint{
					ID: "10", Name: "remote-ep", Type: "REMOTE", Status: "ACTIVE",
					URL: server.URL, AuthToken: "tok",
				}, nil
			}
			return nil, domain.ErrNotFound("no assignment")
		},
	}

	resolver := NewResolver(localExec, computeRepo, principalRepo, &mockGroupRepo{
		getGroupsForMemberFn: func(_ context.Context, _ string, _ string) ([]domain.Group, error) {
			return nil, nil
		},
	}, cache, nil)

	executor, err := resolver.Resolve(context.Background(), "alice")
	require.NoError(t, err)
	require.NotNil(t, executor)

	// Verify it's a RemoteExecutor
	_, isRemote := executor.(*RemoteExecutor)
	assert.True(t, isRemote)
}

func TestResolver_GroupAssignment(t *testing.T) {
	// Set up a healthy agent
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}
	}))
	defer server.Close()

	localDB := openTestDuckDB(t)
	localExec := NewLocalExecutor(localDB)
	cache := NewRemoteCache(localDB)

	principalRepo := &mockPrincipalRepo{
		getByNameFn: func(_ context.Context, _ string) (*domain.Principal, error) {
			return &domain.Principal{ID: "1", Name: "alice"}, nil
		},
	}

	groupRepo := &mockGroupRepo{
		getGroupsForMemberFn: func(_ context.Context, _ string, _ string) ([]domain.Group, error) {
			return []domain.Group{{ID: "100", Name: "analysts"}}, nil
		},
	}

	computeRepo := &mockComputeRepo{
		getDefaultForPrincipalFn: func(_ context.Context, principalID string, principalType string) (*domain.ComputeEndpoint, error) {
			if principalType == "user" {
				return nil, domain.ErrNotFound("no user assignment")
			}
			if principalType == "group" && principalID == "100" {
				return &domain.ComputeEndpoint{
					ID: "20", Name: "group-ep", Type: "REMOTE", Status: "ACTIVE",
					URL: server.URL, AuthToken: "tok",
				}, nil
			}
			return nil, domain.ErrNotFound("no assignment")
		},
	}

	resolver := NewResolver(localExec, computeRepo, principalRepo, groupRepo, cache, nil)

	executor, err := resolver.Resolve(context.Background(), "alice")
	require.NoError(t, err)
	require.NotNil(t, executor)
	_, isRemote := executor.(*RemoteExecutor)
	assert.True(t, isRemote)
}

func TestResolver_NoAssignment(t *testing.T) {
	localDB := openTestDuckDB(t)
	localExec := NewLocalExecutor(localDB)

	principalRepo := &mockPrincipalRepo{
		getByNameFn: func(_ context.Context, _ string) (*domain.Principal, error) {
			return &domain.Principal{ID: "1", Name: "alice"}, nil
		},
	}

	groupRepo := &mockGroupRepo{
		getGroupsForMemberFn: func(_ context.Context, _ string, _ string) ([]domain.Group, error) {
			return nil, nil // No groups
		},
	}

	computeRepo := &mockComputeRepo{
		getDefaultForPrincipalFn: func(_ context.Context, _ string, _ string) (*domain.ComputeEndpoint, error) {
			return nil, domain.ErrNotFound("no assignment")
		},
	}

	resolver := NewResolver(localExec, computeRepo, principalRepo, groupRepo, nil, nil)

	executor, err := resolver.Resolve(context.Background(), "alice")
	require.NoError(t, err)
	assert.Nil(t, executor) // No assignment → local fallback
}

func TestResolver_RemoteUnhealthy(t *testing.T) {
	// Set up an unhealthy agent
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	localDB := openTestDuckDB(t)
	localExec := NewLocalExecutor(localDB)
	cache := NewRemoteCache(localDB)

	principalRepo := &mockPrincipalRepo{
		getByNameFn: func(_ context.Context, _ string) (*domain.Principal, error) {
			return &domain.Principal{ID: "1", Name: "alice"}, nil
		},
	}

	computeRepo := &mockComputeRepo{
		getDefaultForPrincipalFn: func(_ context.Context, _ string, principalType string) (*domain.ComputeEndpoint, error) {
			if principalType == "user" {
				return &domain.ComputeEndpoint{
					ID: "10", Name: "unhealthy-ep", Type: "REMOTE", Status: "ACTIVE",
					URL: server.URL, AuthToken: "tok",
				}, nil
			}
			return nil, domain.ErrNotFound("no assignment")
		},
	}

	resolver := NewResolver(localExec, computeRepo, principalRepo, &mockGroupRepo{
		getGroupsForMemberFn: func(_ context.Context, _ string, _ string) ([]domain.Group, error) {
			return nil, nil
		},
	}, cache, nil)

	_, err := resolver.Resolve(context.Background(), "alice")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unhealthy")
}
