package compute

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === Compute Endpoint Repository Mock ===

type mockComputeEndpointRepo struct {
	createFn                     func(ctx context.Context, ep *domain.ComputeEndpoint) (*domain.ComputeEndpoint, error)
	getByIDFn                    func(ctx context.Context, id int64) (*domain.ComputeEndpoint, error)
	getByNameFn                  func(ctx context.Context, name string) (*domain.ComputeEndpoint, error)
	listFn                       func(ctx context.Context, page domain.PageRequest) ([]domain.ComputeEndpoint, int64, error)
	updateFn                     func(ctx context.Context, id int64, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error)
	deleteFn                     func(ctx context.Context, id int64) error
	updateStatusFn               func(ctx context.Context, id int64, status string) error
	assignFn                     func(ctx context.Context, a *domain.ComputeAssignment) (*domain.ComputeAssignment, error)
	unassignFn                   func(ctx context.Context, id int64) error
	listAssignmentsFn            func(ctx context.Context, endpointID int64, page domain.PageRequest) ([]domain.ComputeAssignment, int64, error)
	getDefaultForPrincipalFn     func(ctx context.Context, principalID int64, principalType string) (*domain.ComputeEndpoint, error)
	getAssignmentsForPrincipalFn func(ctx context.Context, principalID int64, principalType string) ([]domain.ComputeEndpoint, error)
}

func (m *mockComputeEndpointRepo) Create(ctx context.Context, ep *domain.ComputeEndpoint) (*domain.ComputeEndpoint, error) {
	if m.createFn != nil {
		return m.createFn(ctx, ep)
	}
	panic("unexpected call to mockComputeEndpointRepo.Create")
}

func (m *mockComputeEndpointRepo) GetByID(ctx context.Context, id int64) (*domain.ComputeEndpoint, error) {
	if m.getByIDFn != nil {
		return m.getByIDFn(ctx, id)
	}
	panic("unexpected call to mockComputeEndpointRepo.GetByID")
}

func (m *mockComputeEndpointRepo) GetByName(ctx context.Context, name string) (*domain.ComputeEndpoint, error) {
	if m.getByNameFn != nil {
		return m.getByNameFn(ctx, name)
	}
	panic("unexpected call to mockComputeEndpointRepo.GetByName")
}

func (m *mockComputeEndpointRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.ComputeEndpoint, int64, error) {
	if m.listFn != nil {
		return m.listFn(ctx, page)
	}
	panic("unexpected call to mockComputeEndpointRepo.List")
}

func (m *mockComputeEndpointRepo) Update(ctx context.Context, id int64, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
	if m.updateFn != nil {
		return m.updateFn(ctx, id, req)
	}
	panic("unexpected call to mockComputeEndpointRepo.Update")
}

func (m *mockComputeEndpointRepo) Delete(ctx context.Context, id int64) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, id)
	}
	panic("unexpected call to mockComputeEndpointRepo.Delete")
}

func (m *mockComputeEndpointRepo) UpdateStatus(ctx context.Context, id int64, status string) error {
	if m.updateStatusFn != nil {
		return m.updateStatusFn(ctx, id, status)
	}
	panic("unexpected call to mockComputeEndpointRepo.UpdateStatus")
}

func (m *mockComputeEndpointRepo) Assign(ctx context.Context, a *domain.ComputeAssignment) (*domain.ComputeAssignment, error) {
	if m.assignFn != nil {
		return m.assignFn(ctx, a)
	}
	panic("unexpected call to mockComputeEndpointRepo.Assign")
}

func (m *mockComputeEndpointRepo) Unassign(ctx context.Context, id int64) error {
	if m.unassignFn != nil {
		return m.unassignFn(ctx, id)
	}
	panic("unexpected call to mockComputeEndpointRepo.Unassign")
}

func (m *mockComputeEndpointRepo) ListAssignments(ctx context.Context, endpointID int64, page domain.PageRequest) ([]domain.ComputeAssignment, int64, error) {
	if m.listAssignmentsFn != nil {
		return m.listAssignmentsFn(ctx, endpointID, page)
	}
	panic("unexpected call to mockComputeEndpointRepo.ListAssignments")
}

func (m *mockComputeEndpointRepo) GetDefaultForPrincipal(ctx context.Context, principalID int64, principalType string) (*domain.ComputeEndpoint, error) {
	if m.getDefaultForPrincipalFn != nil {
		return m.getDefaultForPrincipalFn(ctx, principalID, principalType)
	}
	panic("unexpected call to mockComputeEndpointRepo.GetDefaultForPrincipal")
}

func (m *mockComputeEndpointRepo) GetAssignmentsForPrincipal(ctx context.Context, principalID int64, principalType string) ([]domain.ComputeEndpoint, error) {
	if m.getAssignmentsForPrincipalFn != nil {
		return m.getAssignmentsForPrincipalFn(ctx, principalID, principalType)
	}
	panic("unexpected call to mockComputeEndpointRepo.GetAssignmentsForPrincipal")
}

var _ domain.ComputeEndpointRepository = (*mockComputeEndpointRepo)(nil)

// === Test Helpers ===

func newTestComputeEndpointService(repo *mockComputeEndpointRepo, auth *mockAuthService, audit *mockAuditRepo) *ComputeEndpointService {
	return NewComputeEndpointService(repo, auth, audit)
}

func allowManageCompute() *mockAuthService {
	return &mockAuthService{
		checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
			return true, nil
		},
	}
}

func denyManageCompute() *mockAuthService {
	return &mockAuthService{
		checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
			return false, nil
		},
	}
}

// === Create ===

func TestComputeEndpointService_Create(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			createFn: func(_ context.Context, ep *domain.ComputeEndpoint) (*domain.ComputeEndpoint, error) {
				ep.ID = 1
				ep.ExternalID = "uuid-123"
				ep.Status = "INACTIVE"
				return ep, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), audit)

		result, err := svc.Create(context.Background(), "admin", domain.CreateComputeEndpointRequest{
			Name: "test-ep", URL: "https://example.com", Type: "REMOTE", AuthToken: "secret",
		})
		require.NoError(t, err)
		assert.Equal(t, "test-ep", result.Name)
		assert.Equal(t, "admin", result.Owner)
		assert.True(t, audit.hasAction("CREATE_COMPUTE_ENDPOINT"))
	})

	t.Run("access_denied", func(t *testing.T) {
		svc := newTestComputeEndpointService(&mockComputeEndpointRepo{}, denyManageCompute(), &mockAuditRepo{})

		_, err := svc.Create(context.Background(), "user1", domain.CreateComputeEndpointRequest{
			Name: "test", URL: "https://example.com", Type: "REMOTE",
		})
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})

	t.Run("validation_error", func(t *testing.T) {
		svc := newTestComputeEndpointService(&mockComputeEndpointRepo{}, allowManageCompute(), &mockAuditRepo{})

		_, err := svc.Create(context.Background(), "admin", domain.CreateComputeEndpointRequest{
			Name: "", URL: "https://example.com", Type: "REMOTE",
		})
		require.Error(t, err)
		var valErr *domain.ValidationError
		assert.ErrorAs(t, err, &valErr)
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			createFn: func(_ context.Context, _ *domain.ComputeEndpoint) (*domain.ComputeEndpoint, error) {
				return nil, errTest
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		_, err := svc.Create(context.Background(), "admin", domain.CreateComputeEndpointRequest{
			Name: "test", URL: "https://example.com", Type: "REMOTE",
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})
}

// === GetByName ===

func TestComputeEndpointService_GetByName(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			getByNameFn: func(_ context.Context, name string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: 1, Name: name, Type: "REMOTE"}, nil
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		result, err := svc.GetByName(context.Background(), "test-ep")
		require.NoError(t, err)
		assert.Equal(t, "test-ep", result.Name)
	})

	t.Run("not_found", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return nil, domain.ErrNotFound("not found")
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		_, err := svc.GetByName(context.Background(), "nonexistent")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

// === List ===

func TestComputeEndpointService_List(t *testing.T) {
	repo := &mockComputeEndpointRepo{
		listFn: func(_ context.Context, _ domain.PageRequest) ([]domain.ComputeEndpoint, int64, error) {
			return []domain.ComputeEndpoint{
				{ID: 1, Name: "ep1"},
				{ID: 2, Name: "ep2"},
			}, 2, nil
		},
	}
	svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

	eps, total, err := svc.List(context.Background(), domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, eps, 2)
}

// === Update ===

func TestComputeEndpointService_Update(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		newURL := "https://new.example.com"
		repo := &mockComputeEndpointRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: 1, Name: "ep1"}, nil
			},
			updateFn: func(_ context.Context, id int64, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: id, Name: "ep1", URL: *req.URL}, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), audit)

		result, err := svc.Update(context.Background(), "admin", "ep1", domain.UpdateComputeEndpointRequest{URL: &newURL})
		require.NoError(t, err)
		assert.Equal(t, "https://new.example.com", result.URL)
		assert.True(t, audit.hasAction("UPDATE_COMPUTE_ENDPOINT"))
	})

	t.Run("access_denied", func(t *testing.T) {
		svc := newTestComputeEndpointService(&mockComputeEndpointRepo{}, denyManageCompute(), &mockAuditRepo{})
		url := "https://x.com"
		_, err := svc.Update(context.Background(), "user1", "ep1", domain.UpdateComputeEndpointRequest{URL: &url})
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})

	t.Run("not_found", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return nil, domain.ErrNotFound("not found")
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})
		url := "https://x.com"
		_, err := svc.Update(context.Background(), "admin", "nonexistent", domain.UpdateComputeEndpointRequest{URL: &url})
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

// === Delete ===

func TestComputeEndpointService_Delete(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: 1, Name: "ep1"}, nil
			},
			deleteFn: func(_ context.Context, _ int64) error {
				return nil
			},
		}
		audit := &mockAuditRepo{}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), audit)

		err := svc.Delete(context.Background(), "admin", "ep1")
		require.NoError(t, err)
		assert.True(t, audit.hasAction("DELETE_COMPUTE_ENDPOINT"))
	})

	t.Run("access_denied", func(t *testing.T) {
		svc := newTestComputeEndpointService(&mockComputeEndpointRepo{}, denyManageCompute(), &mockAuditRepo{})
		err := svc.Delete(context.Background(), "user1", "ep1")
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})
}

// === UpdateStatus ===

func TestComputeEndpointService_UpdateStatus(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: 1, Name: "ep1", Status: "INACTIVE"}, nil
			},
			updateStatusFn: func(_ context.Context, _ int64, _ string) error {
				return nil
			},
		}
		audit := &mockAuditRepo{}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), audit)

		err := svc.UpdateStatus(context.Background(), "admin", "ep1", "ACTIVE")
		require.NoError(t, err)
		assert.True(t, audit.hasAction("UPDATE_COMPUTE_ENDPOINT_STATUS"))
	})

	t.Run("invalid_status", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: 1, Name: "ep1"}, nil
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		err := svc.UpdateStatus(context.Background(), "admin", "ep1", "BOGUS")
		require.Error(t, err)
		var valErr *domain.ValidationError
		assert.ErrorAs(t, err, &valErr)
	})

	t.Run("all_valid_statuses", func(t *testing.T) {
		for _, status := range []string{"ACTIVE", "INACTIVE", "STARTING", "STOPPING", "ERROR"} {
			t.Run(status, func(t *testing.T) {
				repo := &mockComputeEndpointRepo{
					getByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
						return &domain.ComputeEndpoint{ID: 1, Name: "ep1"}, nil
					},
					updateStatusFn: func(_ context.Context, _ int64, _ string) error {
						return nil
					},
				}
				svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})
				err := svc.UpdateStatus(context.Background(), "admin", "ep1", status)
				require.NoError(t, err)
			})
		}
	})
}

// === Assign ===

func TestComputeEndpointService_Assign(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: 1, Name: "ep1"}, nil
			},
			assignFn: func(_ context.Context, a *domain.ComputeAssignment) (*domain.ComputeAssignment, error) {
				a.ID = 10
				return a, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), audit)

		result, err := svc.Assign(context.Background(), "admin", "ep1", domain.CreateComputeAssignmentRequest{
			PrincipalID: 5, PrincipalType: "user", IsDefault: true, FallbackLocal: true,
		})
		require.NoError(t, err)
		assert.Equal(t, int64(10), result.ID)
		assert.Equal(t, int64(1), result.EndpointID)
		assert.True(t, result.IsDefault)
		assert.True(t, result.FallbackLocal)
		assert.True(t, audit.hasAction("ASSIGN_COMPUTE_ENDPOINT"))
	})

	t.Run("validation_error", func(t *testing.T) {
		svc := newTestComputeEndpointService(&mockComputeEndpointRepo{}, allowManageCompute(), &mockAuditRepo{})

		_, err := svc.Assign(context.Background(), "admin", "ep1", domain.CreateComputeAssignmentRequest{
			PrincipalID: 0, PrincipalType: "user",
		})
		require.Error(t, err)
		var valErr *domain.ValidationError
		assert.ErrorAs(t, err, &valErr)
	})

	t.Run("access_denied", func(t *testing.T) {
		svc := newTestComputeEndpointService(&mockComputeEndpointRepo{}, denyManageCompute(), &mockAuditRepo{})

		_, err := svc.Assign(context.Background(), "user1", "ep1", domain.CreateComputeAssignmentRequest{
			PrincipalID: 5, PrincipalType: "user",
		})
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})
}

// === Unassign ===

func TestComputeEndpointService_Unassign(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			unassignFn: func(_ context.Context, _ int64) error {
				return nil
			},
		}
		audit := &mockAuditRepo{}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), audit)

		err := svc.Unassign(context.Background(), "admin", 10)
		require.NoError(t, err)
		assert.True(t, audit.hasAction("UNASSIGN_COMPUTE_ENDPOINT"))
	})

	t.Run("access_denied", func(t *testing.T) {
		svc := newTestComputeEndpointService(&mockComputeEndpointRepo{}, denyManageCompute(), &mockAuditRepo{})
		err := svc.Unassign(context.Background(), "user1", 10)
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})
}

// === ListAssignments ===

func TestComputeEndpointService_ListAssignments(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: 1, Name: "ep1"}, nil
			},
			listAssignmentsFn: func(_ context.Context, _ int64, _ domain.PageRequest) ([]domain.ComputeAssignment, int64, error) {
				return []domain.ComputeAssignment{
					{ID: 1, PrincipalID: 5, PrincipalType: "user", EndpointID: 1},
				}, 1, nil
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		assignments, total, err := svc.ListAssignments(context.Background(), "ep1", domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Len(t, assignments, 1)
	})

	t.Run("endpoint_not_found", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return nil, domain.ErrNotFound("not found")
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		_, _, err := svc.ListAssignments(context.Background(), "nonexistent", domain.PageRequest{})
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

// === HealthCheck ===

func TestComputeEndpointService_HealthCheck(t *testing.T) {
	t.Run("local_endpoint_always_ok", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: 1, Name: "local-ep", Type: "LOCAL"}, nil
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		result, err := svc.HealthCheck(context.Background(), "admin", "local-ep")
		require.NoError(t, err)
		require.NotNil(t, result.Status)
		assert.Equal(t, "ok", *result.Status)
	})

	t.Run("access_denied", func(t *testing.T) {
		svc := newTestComputeEndpointService(&mockComputeEndpointRepo{}, denyManageCompute(), &mockAuditRepo{})

		_, err := svc.HealthCheck(context.Background(), "user1", "ep1")
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})

	t.Run("not_found", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return nil, domain.ErrNotFound("not found")
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		_, err := svc.HealthCheck(context.Background(), "admin", "nonexistent")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("remote_endpoint_unreachable", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{
					ID: 1, Name: "remote-ep", Type: "REMOTE",
					URL: "https://127.0.0.1:1", AuthToken: "tok",
				}, nil
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		_, err := svc.HealthCheck(context.Background(), "admin", "remote-ep")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "health check failed")
	})
}

func init() {
	// Validate that our helper functions produce valid mock instances
	_ = allowManageCompute()
	_ = denyManageCompute()
}
