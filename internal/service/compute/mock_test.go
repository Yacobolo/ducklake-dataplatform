package compute

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
)

// errTest is a sentinel error for test scenarios.
var errTest = fmt.Errorf("test error")

// === Authorization Service Mock ===

type mockAuthService struct {
	checkPrivilegeFn func(ctx context.Context, principalName, securableType string, securableID int64, privilege string) (bool, error)
}

func (m *mockAuthService) LookupTableID(_ context.Context, _ string) (int64, int64, bool, error) {
	panic("unexpected call to mockAuthService.LookupTableID")
}

func (m *mockAuthService) CheckPrivilege(ctx context.Context, principalName, securableType string, securableID int64, privilege string) (bool, error) {
	if m.checkPrivilegeFn != nil {
		return m.checkPrivilegeFn(ctx, principalName, securableType, securableID, privilege)
	}
	panic("unexpected call to mockAuthService.CheckPrivilege")
}

func (m *mockAuthService) GetEffectiveRowFilters(_ context.Context, _ string, _ int64) ([]string, error) {
	panic("unexpected call to mockAuthService.GetEffectiveRowFilters")
}

func (m *mockAuthService) GetEffectiveColumnMasks(_ context.Context, _ string, _ int64) (map[string]string, error) {
	panic("unexpected call to mockAuthService.GetEffectiveColumnMasks")
}

func (m *mockAuthService) GetTableColumnNames(_ context.Context, _ int64) ([]string, error) {
	panic("unexpected call to mockAuthService.GetTableColumnNames")
}

// === Audit Repository Mock ===

type mockAuditRepo struct {
	entries   []*domain.AuditEntry
	insertErr error
}

func (m *mockAuditRepo) Insert(_ context.Context, e *domain.AuditEntry) error {
	if m.insertErr != nil {
		return m.insertErr
	}
	m.entries = append(m.entries, e)
	return nil
}

func (m *mockAuditRepo) List(_ context.Context, _ domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	panic("unexpected call to mockAuditRepo.List")
}

func (m *mockAuditRepo) hasAction(action string) bool {
	for _, e := range m.entries {
		if e.Action == action {
			return true
		}
	}
	return false
}

// === Compile-time interface checks ===

var _ domain.AuthorizationService = (*mockAuthService)(nil)
var _ domain.AuditRepository = (*mockAuditRepo)(nil)
