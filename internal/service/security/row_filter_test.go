package security

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"
)

type mockRowFilterRepo struct {
	CreateFn                  func(ctx context.Context, f *domain.RowFilter) (*domain.RowFilter, error)
	GetForTableFn             func(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.RowFilter, int64, error)
	DeleteFn                  func(ctx context.Context, id string) error
	BindFn                    func(ctx context.Context, b *domain.RowFilterBinding) error
	UnbindFn                  func(ctx context.Context, b *domain.RowFilterBinding) error
	ListBindingsFn            func(ctx context.Context, filterID string) ([]domain.RowFilterBinding, error)
	GetForTableAndPrincipalFn func(ctx context.Context, tableID, principalID string, principalType string) ([]domain.RowFilter, error)
}

func (m *mockRowFilterRepo) Create(ctx context.Context, f *domain.RowFilter) (*domain.RowFilter, error) {
	return m.CreateFn(ctx, f)
}

func (m *mockRowFilterRepo) GetForTable(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.RowFilter, int64, error) {
	return m.GetForTableFn(ctx, tableID, page)
}

func (m *mockRowFilterRepo) Delete(ctx context.Context, id string) error {
	return m.DeleteFn(ctx, id)
}

func (m *mockRowFilterRepo) Bind(ctx context.Context, b *domain.RowFilterBinding) error {
	return m.BindFn(ctx, b)
}

func (m *mockRowFilterRepo) Unbind(ctx context.Context, b *domain.RowFilterBinding) error {
	return m.UnbindFn(ctx, b)
}

func (m *mockRowFilterRepo) ListBindings(ctx context.Context, filterID string) ([]domain.RowFilterBinding, error) {
	return m.ListBindingsFn(ctx, filterID)
}

func (m *mockRowFilterRepo) GetForTableAndPrincipal(ctx context.Context, tableID, principalID string, principalType string) ([]domain.RowFilter, error) {
	return m.GetForTableAndPrincipalFn(ctx, tableID, principalID, principalType)
}

var _ domain.RowFilterRepository = (*mockRowFilterRepo)(nil)

func TestRowFilterService_Create_AdminAllowed(t *testing.T) {
	repo := &mockRowFilterRepo{
		CreateFn: func(_ context.Context, f *domain.RowFilter) (*domain.RowFilter, error) {
			f.ID = "rf-1"
			return f, nil
		},
	}
	audit := &testutil.MockAuditRepo{}
	svc := NewRowFilterService(repo, audit)

	result, err := svc.Create(adminCtx(), domain.CreateRowFilterRequest{
		TableID:   "t-1",
		FilterSQL: `"Pclass" = 1`,
	})
	require.NoError(t, err)
	assert.Equal(t, "rf-1", result.ID)
	assert.True(t, audit.HasAction("CREATE_ROW_FILTER"))
}

func TestRowFilterService_Create_NonAdminDenied(t *testing.T) {
	svc := NewRowFilterService(&mockRowFilterRepo{}, &testutil.MockAuditRepo{})

	_, err := svc.Create(nonAdminCtx(), domain.CreateRowFilterRequest{
		TableID:   "t-1",
		FilterSQL: `"Pclass" = 1`,
	})
	require.Error(t, err)
	var denied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &denied)
}

func TestRowFilterService_Create_ValidationError(t *testing.T) {
	svc := NewRowFilterService(&mockRowFilterRepo{}, &testutil.MockAuditRepo{})

	_, err := svc.Create(adminCtx(), domain.CreateRowFilterRequest{
		TableID: "t-1",
		// Missing FilterSQL.
	})
	require.Error(t, err)
	var validation *domain.ValidationError
	assert.ErrorAs(t, err, &validation)
}

func TestRowFilterService_Delete_AdminAllowed(t *testing.T) {
	called := false
	repo := &mockRowFilterRepo{
		DeleteFn: func(_ context.Context, id string) error {
			called = true
			assert.Equal(t, "rf-1", id)
			return nil
		},
	}
	svc := NewRowFilterService(repo, &testutil.MockAuditRepo{})

	err := svc.Delete(adminCtx(), "rf-1")
	require.NoError(t, err)
	assert.True(t, called)
}

func TestRowFilterService_Delete_NonAdminDenied(t *testing.T) {
	svc := NewRowFilterService(&mockRowFilterRepo{}, &testutil.MockAuditRepo{})

	err := svc.Delete(nonAdminCtx(), "rf-1")
	require.Error(t, err)
	var denied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &denied)
}

func TestRowFilterService_Bind_AdminAllowed(t *testing.T) {
	called := false
	repo := &mockRowFilterRepo{
		BindFn: func(_ context.Context, b *domain.RowFilterBinding) error {
			called = true
			return nil
		},
	}
	svc := NewRowFilterService(repo, &testutil.MockAuditRepo{})

	err := svc.Bind(adminCtx(), domain.BindRowFilterRequest{
		RowFilterID:   "rf-1",
		PrincipalID:   "p-1",
		PrincipalType: "user",
	})
	require.NoError(t, err)
	assert.True(t, called)
}

func TestRowFilterService_Bind_NonAdminDenied(t *testing.T) {
	svc := NewRowFilterService(&mockRowFilterRepo{}, &testutil.MockAuditRepo{})

	err := svc.Bind(nonAdminCtx(), domain.BindRowFilterRequest{
		RowFilterID:   "rf-1",
		PrincipalID:   "p-1",
		PrincipalType: "user",
	})
	require.Error(t, err)
	var denied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &denied)
}

func TestRowFilterService_Unbind_NonAdminDenied(t *testing.T) {
	svc := NewRowFilterService(&mockRowFilterRepo{}, &testutil.MockAuditRepo{})

	err := svc.Unbind(nonAdminCtx(), domain.BindRowFilterRequest{
		RowFilterID:   "rf-1",
		PrincipalID:   "p-1",
		PrincipalType: "user",
	})
	require.Error(t, err)
	var denied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &denied)
}

func TestRowFilterService_GetForTable(t *testing.T) {
	repo := &mockRowFilterRepo{
		GetForTableFn: func(_ context.Context, tableID string, _ domain.PageRequest) ([]domain.RowFilter, int64, error) {
			return []domain.RowFilter{{ID: "rf-1", TableID: tableID}}, 1, nil
		},
	}
	svc := NewRowFilterService(repo, &testutil.MockAuditRepo{})

	// No admin check required for reads.
	filters, total, err := svc.GetForTable(nonAdminCtx(), "t-1", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, filters, 1)
}

func TestRowFilterService_ListBindings(t *testing.T) {
	repo := &mockRowFilterRepo{
		ListBindingsFn: func(_ context.Context, filterID string) ([]domain.RowFilterBinding, error) {
			return []domain.RowFilterBinding{{RowFilterID: filterID}}, nil
		},
	}
	svc := NewRowFilterService(repo, &testutil.MockAuditRepo{})

	bindings, err := svc.ListBindings(nonAdminCtx(), "rf-1")
	require.NoError(t, err)
	assert.Len(t, bindings, 1)
}
