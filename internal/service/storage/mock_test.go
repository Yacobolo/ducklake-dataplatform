package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/engine"
	"duck-demo/internal/middleware"
)

// errTest is a sentinel error for test scenarios.
var errTest = fmt.Errorf("test error")

func ctxWithPrincipal(name string) context.Context {
	return middleware.WithPrincipal(context.Background(), name)
}

// testSecretManager returns a DuckDBSecretManager wrapping the given DuckDB connection.
func testSecretManager(db *sql.DB) *engine.DuckDBSecretManager {
	return engine.NewDuckDBSecretManager(db)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// testDuckDB opens a fresh in-memory DuckDB with extensions installed.
func testDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, engine.InstallExtensions(context.Background(), db))
	return db
}

// === Storage Credential Repository Mock ===

type mockStorageCredentialRepo struct {
	createFn    func(ctx context.Context, cred *domain.StorageCredential) (*domain.StorageCredential, error)
	getByIDFn   func(ctx context.Context, id int64) (*domain.StorageCredential, error)
	getByNameFn func(ctx context.Context, name string) (*domain.StorageCredential, error)
	listFn      func(ctx context.Context, page domain.PageRequest) ([]domain.StorageCredential, int64, error)
	updateFn    func(ctx context.Context, id int64, req domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error)
	deleteFn    func(ctx context.Context, id int64) error
}

func (m *mockStorageCredentialRepo) Create(ctx context.Context, cred *domain.StorageCredential) (*domain.StorageCredential, error) {
	if m.createFn != nil {
		return m.createFn(ctx, cred)
	}
	panic("unexpected call to mockStorageCredentialRepo.Create")
}

func (m *mockStorageCredentialRepo) GetByID(ctx context.Context, id int64) (*domain.StorageCredential, error) {
	if m.getByIDFn != nil {
		return m.getByIDFn(ctx, id)
	}
	panic("unexpected call to mockStorageCredentialRepo.GetByID")
}

func (m *mockStorageCredentialRepo) GetByName(ctx context.Context, name string) (*domain.StorageCredential, error) {
	if m.getByNameFn != nil {
		return m.getByNameFn(ctx, name)
	}
	panic("unexpected call to mockStorageCredentialRepo.GetByName")
}

func (m *mockStorageCredentialRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.StorageCredential, int64, error) {
	if m.listFn != nil {
		return m.listFn(ctx, page)
	}
	panic("unexpected call to mockStorageCredentialRepo.List")
}

func (m *mockStorageCredentialRepo) Update(ctx context.Context, id int64, req domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error) {
	if m.updateFn != nil {
		return m.updateFn(ctx, id, req)
	}
	panic("unexpected call to mockStorageCredentialRepo.Update")
}

func (m *mockStorageCredentialRepo) Delete(ctx context.Context, id int64) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, id)
	}
	panic("unexpected call to mockStorageCredentialRepo.Delete")
}

// === External Location Repository Mock ===

type mockExternalLocationRepo struct {
	createFn    func(ctx context.Context, loc *domain.ExternalLocation) (*domain.ExternalLocation, error)
	getByIDFn   func(ctx context.Context, id int64) (*domain.ExternalLocation, error)
	getByNameFn func(ctx context.Context, name string) (*domain.ExternalLocation, error)
	listFn      func(ctx context.Context, page domain.PageRequest) ([]domain.ExternalLocation, int64, error)
	updateFn    func(ctx context.Context, id int64, req domain.UpdateExternalLocationRequest) (*domain.ExternalLocation, error)
	deleteFn    func(ctx context.Context, id int64) error
}

func (m *mockExternalLocationRepo) Create(ctx context.Context, loc *domain.ExternalLocation) (*domain.ExternalLocation, error) {
	if m.createFn != nil {
		return m.createFn(ctx, loc)
	}
	panic("unexpected call to mockExternalLocationRepo.Create")
}

func (m *mockExternalLocationRepo) GetByID(ctx context.Context, id int64) (*domain.ExternalLocation, error) {
	if m.getByIDFn != nil {
		return m.getByIDFn(ctx, id)
	}
	panic("unexpected call to mockExternalLocationRepo.GetByID")
}

func (m *mockExternalLocationRepo) GetByName(ctx context.Context, name string) (*domain.ExternalLocation, error) {
	if m.getByNameFn != nil {
		return m.getByNameFn(ctx, name)
	}
	panic("unexpected call to mockExternalLocationRepo.GetByName")
}

func (m *mockExternalLocationRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.ExternalLocation, int64, error) {
	if m.listFn != nil {
		return m.listFn(ctx, page)
	}
	panic("unexpected call to mockExternalLocationRepo.List")
}

func (m *mockExternalLocationRepo) Update(ctx context.Context, id int64, req domain.UpdateExternalLocationRequest) (*domain.ExternalLocation, error) {
	if m.updateFn != nil {
		return m.updateFn(ctx, id, req)
	}
	panic("unexpected call to mockExternalLocationRepo.Update")
}

func (m *mockExternalLocationRepo) Delete(ctx context.Context, id int64) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, id)
	}
	panic("unexpected call to mockExternalLocationRepo.Delete")
}

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

var _ domain.StorageCredentialRepository = (*mockStorageCredentialRepo)(nil)
var _ domain.ExternalLocationRepository = (*mockExternalLocationRepo)(nil)
var _ domain.AuthorizationService = (*mockAuthService)(nil)
var _ domain.AuditRepository = (*mockAuditRepo)(nil)
