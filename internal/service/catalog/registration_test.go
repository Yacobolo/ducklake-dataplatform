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

var _ domain.CatalogAttacher = noopAttacher{}

// === Tests ===

func TestCatalogRegistrationService_Register_RejectsReservedNames(t *testing.T) {
	repo := &mockRegistrationRepo{}
	attacher := noopAttacher{}

	svc := NewCatalogRegistrationService(RegistrationServiceDeps{
		Repo:               repo,
		Attacher:           attacher,
		ControlPlaneDBPath: "/tmp/ctrl.db",
		DuckDB:             nil,
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
