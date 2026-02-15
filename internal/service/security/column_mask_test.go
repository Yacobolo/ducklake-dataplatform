package security

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"
)

type mockColumnMaskRepo struct {
	CreateFn                  func(ctx context.Context, m *domain.ColumnMask) (*domain.ColumnMask, error)
	GetForTableFn             func(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.ColumnMask, int64, error)
	DeleteFn                  func(ctx context.Context, id string) error
	BindFn                    func(ctx context.Context, b *domain.ColumnMaskBinding) error
	UnbindFn                  func(ctx context.Context, b *domain.ColumnMaskBinding) error
	ListBindingsFn            func(ctx context.Context, maskID string) ([]domain.ColumnMaskBinding, error)
	GetForTableAndPrincipalFn func(ctx context.Context, tableID, principalID string, principalType string) ([]domain.ColumnMaskWithBinding, error)
}

func (m *mockColumnMaskRepo) Create(ctx context.Context, mask *domain.ColumnMask) (*domain.ColumnMask, error) {
	return m.CreateFn(ctx, mask)
}

func (m *mockColumnMaskRepo) GetForTable(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.ColumnMask, int64, error) {
	return m.GetForTableFn(ctx, tableID, page)
}

func (m *mockColumnMaskRepo) Delete(ctx context.Context, id string) error {
	return m.DeleteFn(ctx, id)
}

func (m *mockColumnMaskRepo) Bind(ctx context.Context, b *domain.ColumnMaskBinding) error {
	return m.BindFn(ctx, b)
}

func (m *mockColumnMaskRepo) Unbind(ctx context.Context, b *domain.ColumnMaskBinding) error {
	return m.UnbindFn(ctx, b)
}

func (m *mockColumnMaskRepo) ListBindings(ctx context.Context, maskID string) ([]domain.ColumnMaskBinding, error) {
	return m.ListBindingsFn(ctx, maskID)
}

func (m *mockColumnMaskRepo) GetForTableAndPrincipal(ctx context.Context, tableID, principalID string, principalType string) ([]domain.ColumnMaskWithBinding, error) {
	return m.GetForTableAndPrincipalFn(ctx, tableID, principalID, principalType)
}

var _ domain.ColumnMaskRepository = (*mockColumnMaskRepo)(nil)

func TestColumnMaskService_Create_AdminAllowed(t *testing.T) {
	repo := &mockColumnMaskRepo{
		CreateFn: func(_ context.Context, m *domain.ColumnMask) (*domain.ColumnMask, error) {
			m.ID = "cm-1"
			return m, nil
		},
	}
	audit := &testutil.MockAuditRepo{}
	svc := NewColumnMaskService(repo, audit)

	result, err := svc.Create(adminCtx(), domain.CreateColumnMaskRequest{
		TableID:        "t-1",
		ColumnName:     "Name",
		MaskExpression: "'***'",
	})
	require.NoError(t, err)
	assert.Equal(t, "cm-1", result.ID)
	assert.True(t, audit.HasAction("CREATE_COLUMN_MASK"))
}

func TestColumnMaskService_Create_NonAdminDenied(t *testing.T) {
	svc := NewColumnMaskService(&mockColumnMaskRepo{}, &testutil.MockAuditRepo{})

	_, err := svc.Create(nonAdminCtx(), domain.CreateColumnMaskRequest{
		TableID:        "t-1",
		ColumnName:     "Name",
		MaskExpression: "'***'",
	})
	require.Error(t, err)
	var denied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &denied)
}

func TestColumnMaskService_Create_EmptyColumnName(t *testing.T) {
	svc := NewColumnMaskService(&mockColumnMaskRepo{}, &testutil.MockAuditRepo{})

	_, err := svc.Create(adminCtx(), domain.CreateColumnMaskRequest{
		TableID:        "t-1",
		MaskExpression: "'***'",
		// Missing ColumnName.
	})
	require.Error(t, err)
	var validation *domain.ValidationError
	assert.ErrorAs(t, err, &validation)
}

func TestColumnMaskService_Create_EmptyMaskExpression(t *testing.T) {
	svc := NewColumnMaskService(&mockColumnMaskRepo{}, &testutil.MockAuditRepo{})

	_, err := svc.Create(adminCtx(), domain.CreateColumnMaskRequest{
		TableID:    "t-1",
		ColumnName: "Name",
		// Missing MaskExpression.
	})
	require.Error(t, err)
	var validation *domain.ValidationError
	assert.ErrorAs(t, err, &validation)
}

func TestColumnMaskService_Delete_AdminAllowed(t *testing.T) {
	called := false
	repo := &mockColumnMaskRepo{
		DeleteFn: func(_ context.Context, id string) error {
			called = true
			assert.Equal(t, "cm-1", id)
			return nil
		},
	}
	svc := NewColumnMaskService(repo, &testutil.MockAuditRepo{})

	err := svc.Delete(adminCtx(), "cm-1")
	require.NoError(t, err)
	assert.True(t, called)
}

func TestColumnMaskService_Delete_NonAdminDenied(t *testing.T) {
	svc := NewColumnMaskService(&mockColumnMaskRepo{}, &testutil.MockAuditRepo{})

	err := svc.Delete(nonAdminCtx(), "cm-1")
	require.Error(t, err)
	var denied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &denied)
}

func TestColumnMaskService_Bind_AdminAllowed(t *testing.T) {
	called := false
	repo := &mockColumnMaskRepo{
		BindFn: func(_ context.Context, b *domain.ColumnMaskBinding) error {
			called = true
			assert.True(t, b.SeeOriginal)
			return nil
		},
	}
	svc := NewColumnMaskService(repo, &testutil.MockAuditRepo{})

	err := svc.Bind(adminCtx(), domain.BindColumnMaskRequest{
		ColumnMaskID:  "cm-1",
		PrincipalID:   "p-1",
		PrincipalType: "user",
		SeeOriginal:   true,
	})
	require.NoError(t, err)
	assert.True(t, called)
}

func TestColumnMaskService_Bind_NonAdminDenied(t *testing.T) {
	svc := NewColumnMaskService(&mockColumnMaskRepo{}, &testutil.MockAuditRepo{})

	err := svc.Bind(nonAdminCtx(), domain.BindColumnMaskRequest{
		ColumnMaskID:  "cm-1",
		PrincipalID:   "p-1",
		PrincipalType: "user",
	})
	require.Error(t, err)
	var denied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &denied)
}

func TestColumnMaskService_Unbind_NonAdminDenied(t *testing.T) {
	svc := NewColumnMaskService(&mockColumnMaskRepo{}, &testutil.MockAuditRepo{})

	err := svc.Unbind(nonAdminCtx(), domain.BindColumnMaskRequest{
		ColumnMaskID:  "cm-1",
		PrincipalID:   "p-1",
		PrincipalType: "user",
	})
	require.Error(t, err)
	var denied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &denied)
}

func TestColumnMaskService_GetForTable_AdminAllowed(t *testing.T) {
	repo := &mockColumnMaskRepo{
		GetForTableFn: func(_ context.Context, tableID string, _ domain.PageRequest) ([]domain.ColumnMask, int64, error) {
			return []domain.ColumnMask{{ID: "cm-1", TableID: tableID}}, 1, nil
		},
	}
	svc := NewColumnMaskService(repo, &testutil.MockAuditRepo{})

	masks, total, err := svc.GetForTable(adminCtx(), "t-1", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, masks, 1)
}

func TestColumnMaskService_GetForTable_NonAdminDenied(t *testing.T) {
	svc := NewColumnMaskService(&mockColumnMaskRepo{}, &testutil.MockAuditRepo{})

	_, _, err := svc.GetForTable(nonAdminCtx(), "t-1", domain.PageRequest{})
	require.Error(t, err)
	var denied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &denied)
}

func TestColumnMaskService_ListBindings_RequiresAdmin(t *testing.T) {
	repo := &mockColumnMaskRepo{
		ListBindingsFn: func(_ context.Context, _ string) ([]domain.ColumnMaskBinding, error) {
			return []domain.ColumnMaskBinding{}, nil
		},
	}
	svc := NewColumnMaskService(repo, &testutil.MockAuditRepo{})

	// Non-admin should NOT be able to list column mask bindings.
	_, err := svc.ListBindings(nonAdminCtx(), "cm-1")
	require.Error(t, err, "non-admin should not be able to list column mask bindings")
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}
