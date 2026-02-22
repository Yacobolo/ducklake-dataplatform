package compute

import (
	"context"
	"database/sql"
	"log/slog"
	"net"
	"testing"
	"time"

	"duck-demo/internal/agent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"duck-demo/internal/domain"

	_ "github.com/duckdb/duckdb-go/v2"
)

// === Test Helpers ===

func newTestComputeEndpointService(repo *mockComputeEndpointRepo, auth *mockAuthService, audit *mockAuditRepo) *ComputeEndpointService {
	return NewComputeEndpointService(repo, auth, audit)
}

func allowManageCompute() *mockAuthService {
	return &mockAuthService{
		CheckPrivilegeFn: func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
			return true, nil
		},
	}
}

func denyManageCompute() *mockAuthService {
	return &mockAuthService{
		CheckPrivilegeFn: func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
			return false, nil
		},
	}
}

// === Create ===

func TestComputeEndpointService_Create(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			CreateFn: func(_ context.Context, ep *domain.ComputeEndpoint) (*domain.ComputeEndpoint, error) {
				ep.ID = "1"
				ep.ExternalID = "uuid-123"
				ep.Status = "INACTIVE"
				return ep, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), audit)

		result, err := svc.Create(context.Background(), "admin", domain.CreateComputeEndpointRequest{
			Name: "test-ep", URL: "grpc://example.com:9444", Type: "REMOTE", AuthToken: "secret",
		})
		require.NoError(t, err)
		assert.Equal(t, "test-ep", result.Name)
		assert.Equal(t, "admin", result.Owner)
		assert.True(t, audit.HasAction("CREATE_COMPUTE_ENDPOINT"))
	})

	t.Run("access_denied", func(t *testing.T) {
		svc := newTestComputeEndpointService(&mockComputeEndpointRepo{}, denyManageCompute(), &mockAuditRepo{})

		_, err := svc.Create(context.Background(), "user1", domain.CreateComputeEndpointRequest{
			Name: "test", URL: "grpc://example.com:9444", Type: "REMOTE",
		})
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})

	t.Run("validation_error", func(t *testing.T) {
		svc := newTestComputeEndpointService(&mockComputeEndpointRepo{}, allowManageCompute(), &mockAuditRepo{})

		_, err := svc.Create(context.Background(), "admin", domain.CreateComputeEndpointRequest{
			Name: "", URL: "grpc://example.com:9444", Type: "REMOTE",
		})
		require.Error(t, err)
		var valErr *domain.ValidationError
		assert.ErrorAs(t, err, &valErr)
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			CreateFn: func(_ context.Context, _ *domain.ComputeEndpoint) (*domain.ComputeEndpoint, error) {
				return nil, errTest
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		_, err := svc.Create(context.Background(), "admin", domain.CreateComputeEndpointRequest{
			Name: "test", URL: "grpc://example.com:9444", Type: "REMOTE",
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})
}

// === GetByName ===

func TestComputeEndpointService_GetByName(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			GetByNameFn: func(_ context.Context, name string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: "1", Name: name, Type: "REMOTE"}, nil
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		result, err := svc.GetByName(context.Background(), "admin", "test-ep")
		require.NoError(t, err)
		assert.Equal(t, "test-ep", result.Name)
	})

	t.Run("not_found", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return nil, domain.ErrNotFound("not found")
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		_, err := svc.GetByName(context.Background(), "admin", "nonexistent")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

// === List ===

func TestComputeEndpointService_List(t *testing.T) {
	repo := &mockComputeEndpointRepo{
		ListFn: func(_ context.Context, _ domain.PageRequest) ([]domain.ComputeEndpoint, int64, error) {
			return []domain.ComputeEndpoint{
				{ID: "1", Name: "ep1"},
				{ID: "2", Name: "ep2"},
			}, 2, nil
		},
	}
	svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

	eps, total, err := svc.List(context.Background(), "admin", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, eps, 2)
}

// === Update ===

func TestComputeEndpointService_Update(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		newURL := "grpc://new.example.com:9444"
		repo := &mockComputeEndpointRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: "1", Name: "ep1"}, nil
			},
			UpdateFn: func(_ context.Context, id string, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: id, Name: "ep1", URL: *req.URL}, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), audit)

		result, err := svc.Update(context.Background(), "admin", "ep1", domain.UpdateComputeEndpointRequest{URL: &newURL})
		require.NoError(t, err)
		assert.Equal(t, "grpc://new.example.com:9444", result.URL)
		assert.True(t, audit.HasAction("UPDATE_COMPUTE_ENDPOINT"))
	})

	t.Run("access_denied", func(t *testing.T) {
		svc := newTestComputeEndpointService(&mockComputeEndpointRepo{}, denyManageCompute(), &mockAuditRepo{})
		url := "grpc://x.com:9444"
		_, err := svc.Update(context.Background(), "user1", "ep1", domain.UpdateComputeEndpointRequest{URL: &url})
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})

	t.Run("not_found", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return nil, domain.ErrNotFound("not found")
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})
		url := "grpc://x.com:9444"
		_, err := svc.Update(context.Background(), "admin", "nonexistent", domain.UpdateComputeEndpointRequest{URL: &url})
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("invalid_remote_url_scheme", func(t *testing.T) {
		httpURL := "https://x.com"
		repo := &mockComputeEndpointRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: "1", Name: "ep1", Type: "REMOTE"}, nil
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})
		_, err := svc.Update(context.Background(), "admin", "ep1", domain.UpdateComputeEndpointRequest{URL: &httpURL})
		require.Error(t, err)
		var valErr *domain.ValidationError
		assert.ErrorAs(t, err, &valErr)
		assert.Contains(t, err.Error(), "grpc:// or grpcs://")
	})
}

// === Delete ===

func TestComputeEndpointService_Delete(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: "1", Name: "ep1"}, nil
			},
			DeleteFn: func(_ context.Context, _ string) error {
				return nil
			},
		}
		audit := &mockAuditRepo{}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), audit)

		err := svc.Delete(context.Background(), "admin", "ep1")
		require.NoError(t, err)
		assert.True(t, audit.HasAction("DELETE_COMPUTE_ENDPOINT"))
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
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: "1", Name: "ep1", Status: "INACTIVE"}, nil
			},
			UpdateStatusFn: func(_ context.Context, _ string, _ string) error {
				return nil
			},
		}
		audit := &mockAuditRepo{}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), audit)

		err := svc.UpdateStatus(context.Background(), "admin", "ep1", "ACTIVE")
		require.NoError(t, err)
		assert.True(t, audit.HasAction("UPDATE_COMPUTE_ENDPOINT_STATUS"))
	})

	t.Run("invalid_status", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: "1", Name: "ep1"}, nil
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
					GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
						return &domain.ComputeEndpoint{ID: "1", Name: "ep1"}, nil
					},
					UpdateStatusFn: func(_ context.Context, _ string, _ string) error {
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
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: "1", Name: "ep1"}, nil
			},
			AssignFn: func(_ context.Context, a *domain.ComputeAssignment) (*domain.ComputeAssignment, error) {
				a.ID = "10"
				return a, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), audit)

		result, err := svc.Assign(context.Background(), "admin", "ep1", domain.CreateComputeAssignmentRequest{
			PrincipalID: "5", PrincipalType: "user", IsDefault: true, FallbackLocal: true,
		})
		require.NoError(t, err)
		assert.Equal(t, "10", result.ID)
		assert.Equal(t, "1", result.EndpointID)
		assert.True(t, result.IsDefault)
		assert.True(t, result.FallbackLocal)
		assert.True(t, audit.HasAction("ASSIGN_COMPUTE_ENDPOINT"))
	})

	t.Run("validation_error", func(t *testing.T) {
		svc := newTestComputeEndpointService(&mockComputeEndpointRepo{}, allowManageCompute(), &mockAuditRepo{})

		_, err := svc.Assign(context.Background(), "admin", "ep1", domain.CreateComputeAssignmentRequest{
			PrincipalID: "", PrincipalType: "user",
		})
		require.Error(t, err)
		var valErr *domain.ValidationError
		assert.ErrorAs(t, err, &valErr)
	})

	t.Run("access_denied", func(t *testing.T) {
		svc := newTestComputeEndpointService(&mockComputeEndpointRepo{}, denyManageCompute(), &mockAuditRepo{})

		_, err := svc.Assign(context.Background(), "user1", "ep1", domain.CreateComputeAssignmentRequest{
			PrincipalID: "5", PrincipalType: "user",
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
			UnassignFn: func(_ context.Context, _ string) error {
				return nil
			},
		}
		audit := &mockAuditRepo{}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), audit)

		err := svc.Unassign(context.Background(), "admin", "10")
		require.NoError(t, err)
		assert.True(t, audit.HasAction("UNASSIGN_COMPUTE_ENDPOINT"))
	})

	t.Run("access_denied", func(t *testing.T) {
		svc := newTestComputeEndpointService(&mockComputeEndpointRepo{}, denyManageCompute(), &mockAuditRepo{})
		err := svc.Unassign(context.Background(), "user1", "10")
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})
}

// === ListAssignments ===

func TestComputeEndpointService_ListAssignments(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: "1", Name: "ep1"}, nil
			},
			ListAssignmentsFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.ComputeAssignment, int64, error) {
				return []domain.ComputeAssignment{
					{ID: "1", PrincipalID: "5", PrincipalType: "user", EndpointID: "1"},
				}, 1, nil
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		assignments, total, err := svc.ListAssignments(context.Background(), "admin", "ep1", domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Len(t, assignments, 1)
	})

	t.Run("endpoint_not_found", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return nil, domain.ErrNotFound("not found")
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		_, _, err := svc.ListAssignments(context.Background(), "admin", "nonexistent", domain.PageRequest{})
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

// === HealthCheck ===

func TestComputeEndpointService_HealthCheck(t *testing.T) {
	t.Run("local_endpoint_always_ok", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{ID: "1", Name: "local-ep", Type: "LOCAL"}, nil
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
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
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
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{
					ID: "1", Name: "remote-ep", Type: "REMOTE",
					URL: "https://127.0.0.1:1", AuthToken: "tok",
				}, nil
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		_, err := svc.HealthCheck(context.Background(), "admin", "remote-ep")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "health check failed")
	})

	t.Run("remote_endpoint_grpc_unreachable", func(t *testing.T) {
		repo := &mockComputeEndpointRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{
					ID: "1", Name: "remote-grpc-ep", Type: "REMOTE",
					URL: "grpc://127.0.0.1:1", AuthToken: "tok",
				}, nil
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		_, err := svc.HealthCheck(context.Background(), "admin", "remote-grpc-ep")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "grpc health check failed")
	})

	t.Run("remote_endpoint_grpc_healthy", func(t *testing.T) {
		addr := startTestComputeGRPCServer(t, "tok")
		repo := &mockComputeEndpointRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				return &domain.ComputeEndpoint{
					ID: "1", Name: "remote-grpc-ep", Type: "REMOTE",
					URL: "grpc://" + addr, AuthToken: "tok",
				}, nil
			},
		}
		svc := newTestComputeEndpointService(repo, allowManageCompute(), &mockAuditRepo{})

		result, err := svc.HealthCheck(context.Background(), "admin", "remote-grpc-ep")
		require.NoError(t, err)
		require.NotNil(t, result.Status)
		assert.Equal(t, "ok", *result.Status)
	})
}

func startTestComputeGRPCServer(t *testing.T, token string) string {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	server := agent.NewComputeGRPCServer(agent.HandlerConfig{
		DB:         db,
		AgentToken: token,
		StartTime:  time.Now(),
		CursorMode: true,
		Logger:     slog.Default(),
	})

	grpcServer := grpc.NewServer()
	agent.RegisterComputeWorkerGRPCServer(grpcServer, server)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() {
		grpcServer.GracefulStop()
		_ = ln.Close()
	})

	go func() {
		_ = grpcServer.Serve(ln)
	}()

	return ln.Addr().String()
}

func init() {
	// Validate that our helper functions produce valid mock instances
	_ = allowManageCompute()
	_ = denyManageCompute()
}
