package catalog

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === Minimal mocks for CatalogRegistrationRepository and CatalogAttacher ===

// mockRegistrationRepo implements domain.CatalogRegistrationRepository.
// Only GetByName and Create are wired; all others panic if called unexpectedly.
type mockRegistrationRepo struct {
	GetByNameFn    func(ctx context.Context, name string) (*domain.CatalogRegistration, error)
	CreateFn       func(ctx context.Context, reg *domain.CatalogRegistration) (*domain.CatalogRegistration, error)
	GetByIDFn      func(ctx context.Context, id string) (*domain.CatalogRegistration, error)
	ListFn         func(ctx context.Context, page domain.PageRequest) ([]domain.CatalogRegistration, int64, error)
	UpdateFn       func(ctx context.Context, id string, req domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error)
	DeleteFn       func(ctx context.Context, id string) error
	UpdateStatusFn func(ctx context.Context, id string, status domain.CatalogStatus, message string) error
	GetDefaultFn   func(ctx context.Context) (*domain.CatalogRegistration, error)
	SetDefaultFn   func(ctx context.Context, id string) error
}

func (m *mockRegistrationRepo) GetByName(ctx context.Context, name string) (*domain.CatalogRegistration, error) {
	if m.GetByNameFn != nil {
		return m.GetByNameFn(ctx, name)
	}
	// Default: not found (used for "no duplicate" check)
	return nil, domain.ErrNotFound("catalog %q not found", name)
}

func (m *mockRegistrationRepo) Create(ctx context.Context, reg *domain.CatalogRegistration) (*domain.CatalogRegistration, error) {
	if m.CreateFn != nil {
		return m.CreateFn(ctx, reg)
	}
	// Default: return input with an ID assigned
	reg.ID = "generated-id"
	return reg, nil
}

func (m *mockRegistrationRepo) GetByID(ctx context.Context, id string) (*domain.CatalogRegistration, error) {
	if m.GetByIDFn != nil {
		return m.GetByIDFn(ctx, id)
	}
	panic("unexpected call to mockRegistrationRepo.GetByID")
}

func (m *mockRegistrationRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.CatalogRegistration, int64, error) {
	if m.ListFn != nil {
		return m.ListFn(ctx, page)
	}
	panic("unexpected call to mockRegistrationRepo.List")
}

func (m *mockRegistrationRepo) Update(ctx context.Context, id string, req domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error) {
	if m.UpdateFn != nil {
		return m.UpdateFn(ctx, id, req)
	}
	panic("unexpected call to mockRegistrationRepo.Update")
}

func (m *mockRegistrationRepo) Delete(ctx context.Context, id string) error {
	if m.DeleteFn != nil {
		return m.DeleteFn(ctx, id)
	}
	panic("unexpected call to mockRegistrationRepo.Delete")
}

func (m *mockRegistrationRepo) UpdateStatus(ctx context.Context, id string, status domain.CatalogStatus, message string) error {
	if m.UpdateStatusFn != nil {
		return m.UpdateStatusFn(ctx, id, status, message)
	}
	return nil // best-effort, like the real service
}

func (m *mockRegistrationRepo) GetDefault(ctx context.Context) (*domain.CatalogRegistration, error) {
	if m.GetDefaultFn != nil {
		return m.GetDefaultFn(ctx)
	}
	panic("unexpected call to mockRegistrationRepo.GetDefault")
}

func (m *mockRegistrationRepo) SetDefault(ctx context.Context, id string) error {
	if m.SetDefaultFn != nil {
		return m.SetDefaultFn(ctx, id)
	}
	panic("unexpected call to mockRegistrationRepo.SetDefault")
}

var _ domain.CatalogRegistrationRepository = (*mockRegistrationRepo)(nil)

// noopAttacher implements domain.CatalogAttacher as a no-op.
type noopAttacher struct{}

func (noopAttacher) Attach(_ context.Context, _ domain.CatalogRegistration) error { return nil }
func (noopAttacher) Detach(_ context.Context, _ string) error                     { return nil }
func (noopAttacher) SetDefaultCatalog(_ context.Context, _ string) error          { return nil }

var _ domain.CatalogAttacher = noopAttacher{}

// === Tests ===

func TestCatalogRegistrationService_Register_RejectsReservedNames(t *testing.T) {
	repo := &mockRegistrationRepo{}
	attacher := noopAttacher{}

	svc := NewCatalogRegistrationService(RegistrationServiceDeps{
		Repo:               repo,
		Attacher:           attacher,
		ControlPlaneDBPath: "/tmp/ctrl.db",
		Logger:             slog.Default(),
		MetastoreFactory:   nil,
		IntrospectionClose: nil,
		CatalogRepoEvict:   nil,
	})

	reserved := []string{"main", "memory", "system", "temp"}
	for _, name := range reserved {
		t.Run(name, func(t *testing.T) {
			_, err := svc.Register(context.Background(), domain.CreateCatalogRequest{
				Name:          name,
				MetastoreType: "sqlite",
				DSN:           "/tmp/" + name + ".db",
				DataPath:      "/tmp/" + name + "-data",
			})
			require.Error(t, err, "reserved name %q should be rejected", name)
			var validationErr *domain.ValidationError
			assert.ErrorAs(t, err, &validationErr)
		})
	}
}

func TestCatalogRegistrationService_SystemManagedCatalogGuards(t *testing.T) {
	t.Parallel()

	repo := &mockRegistrationRepo{
		GetByNameFn: func(_ context.Context, name string) (*domain.CatalogRegistration, error) {
			return &domain.CatalogRegistration{
				ID:     "sample-id",
				Name:   name,
				Status: domain.CatalogStatusActive,
			}, nil
		},
	}

	svc := NewCatalogRegistrationService(RegistrationServiceDeps{
		Repo:               repo,
		Attacher:           noopAttacher{},
		ControlPlaneDBPath: "/tmp/ctrl.db",
		Logger:             slog.Default(),
	})

	_, err := svc.Register(context.Background(), domain.CreateCatalogRequest{
		Name:          domain.SampleDataCatalogName,
		MetastoreType: "sqlite",
		DSN:           "/tmp/sample.sqlite",
		DataPath:      "/tmp/sample-data",
	})
	require.Error(t, err)

	_, err = svc.Update(context.Background(), domain.SampleDataCatalogName, domain.UpdateCatalogRegistrationRequest{})
	require.Error(t, err)

	err = svc.Delete(context.Background(), domain.SampleDataCatalogName)
	require.Error(t, err)

	_, err = svc.SetDefault(context.Background(), domain.SampleDataCatalogName)
	require.Error(t, err)

	var validationErr *domain.ValidationError
	assert.ErrorAs(t, err, &validationErr)
}

func TestCatalogRegistrationService_AdminAccess(t *testing.T) {
	newService := func(repo *mockRegistrationRepo) *CatalogRegistrationService {
		return NewCatalogRegistrationService(RegistrationServiceDeps{
			Repo:               repo,
			Attacher:           noopAttacher{},
			ControlPlaneDBPath: "/tmp/ctrl.db",
			Logger:             slog.Default(),
		})
	}

	t.Run("list requires admin when principal present", func(t *testing.T) {
		svc := newService(&mockRegistrationRepo{})

		_, _, err := svc.List(domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"}), domain.PageRequest{})

		require.Error(t, err)
		var denied *domain.AccessDeniedError
		require.ErrorAs(t, err, &denied)
	})

	t.Run("get requires admin when principal present", func(t *testing.T) {
		svc := newService(&mockRegistrationRepo{})

		_, err := svc.Get(domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"}), "cat")

		require.Error(t, err)
		var denied *domain.AccessDeniedError
		require.ErrorAs(t, err, &denied)
	})

	t.Run("update requires admin when principal present", func(t *testing.T) {
		svc := newService(&mockRegistrationRepo{})

		_, err := svc.Update(domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"}), "cat", domain.UpdateCatalogRegistrationRequest{})

		require.Error(t, err)
		var denied *domain.AccessDeniedError
		require.ErrorAs(t, err, &denied)
	})

	t.Run("delete requires admin when principal present", func(t *testing.T) {
		svc := newService(&mockRegistrationRepo{})

		err := svc.Delete(domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"}), "cat")

		require.Error(t, err)
		var denied *domain.AccessDeniedError
		require.ErrorAs(t, err, &denied)
	})

	t.Run("set_default requires admin when principal present", func(t *testing.T) {
		svc := newService(&mockRegistrationRepo{})

		_, err := svc.SetDefault(domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"}), "cat")

		require.Error(t, err)
		var denied *domain.AccessDeniedError
		require.ErrorAs(t, err, &denied)
	})

	t.Run("background list remains allowed for startup flows", func(t *testing.T) {
		repo := &mockRegistrationRepo{
			ListFn: func(_ context.Context, _ domain.PageRequest) ([]domain.CatalogRegistration, int64, error) {
				return []domain.CatalogRegistration{{ID: "1", Name: "cat"}}, 1, nil
			},
		}
		svc := newService(repo)

		regs, total, err := svc.List(context.Background(), domain.PageRequest{MaxResults: 10})

		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		require.Len(t, regs, 1)
		assert.Equal(t, "cat", regs[0].Name)
	})

	t.Run("admin get succeeds", func(t *testing.T) {
		repo := &mockRegistrationRepo{
			GetByNameFn: func(_ context.Context, name string) (*domain.CatalogRegistration, error) {
				return &domain.CatalogRegistration{ID: "1", Name: name}, nil
			},
		}
		svc := newService(repo)

		reg, err := svc.Get(domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin_user", IsAdmin: true}), "cat")

		require.NoError(t, err)
		assert.Equal(t, "cat", reg.Name)
	})
}

func TestCatalogRegistrationService_AttachAll_SelectsStartupDefaultWhenMissing(t *testing.T) {
	t.Parallel()

	var setDefaultID string
	var defaultCatalogName string
	repo := &mockRegistrationRepo{
		ListFn: func(_ context.Context, _ domain.PageRequest) ([]domain.CatalogRegistration, int64, error) {
			cats := []domain.CatalogRegistration{
				{ID: "sample", Name: domain.SampleDataCatalogName, Status: domain.CatalogStatusActive},
				{ID: "manual", Name: "manual_lake", Status: domain.CatalogStatusActive},
			}
			return cats, int64(len(cats)), nil
		},
		GetDefaultFn: func(_ context.Context) (*domain.CatalogRegistration, error) {
			return nil, domain.ErrNotFound("default catalog not found")
		},
		SetDefaultFn: func(_ context.Context, id string) error {
			setDefaultID = id
			return nil
		},
	}
	attacher := &recordingAttacher{
		setDefaultFn: func(_ context.Context, name string) error {
			defaultCatalogName = name
			return nil
		},
	}

	svc := NewCatalogRegistrationService(RegistrationServiceDeps{
		Repo:               repo,
		Attacher:           attacher,
		ControlPlaneDBPath: "/tmp/ctrl.db",
		Logger:             slog.Default(),
	})

	err := svc.AttachAll(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "manual", setDefaultID)
	assert.Equal(t, "manual_lake", defaultCatalogName)
}

type recordingAttacher struct {
	setDefaultFn func(context.Context, string) error
}

func (r *recordingAttacher) Attach(_ context.Context, _ domain.CatalogRegistration) error { return nil }
func (r *recordingAttacher) Detach(_ context.Context, _ string) error                     { return nil }
func (r *recordingAttacher) SetDefaultCatalog(ctx context.Context, name string) error {
	if r.setDefaultFn != nil {
		return r.setDefaultFn(ctx, name)
	}
	return nil
}
