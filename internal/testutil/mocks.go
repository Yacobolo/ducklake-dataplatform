// Package testutil provides shared mock implementations of domain interfaces
// for use in tests across the codebase. This follows the Go convention of a
// shared test utility package (like net/http/httptest).
package testutil

import (
	"context"
	"time"

	"duck-demo/internal/domain"
)

// === Audit Repository Mock ===

// MockAuditRepo implements domain.AuditRepository for testing.
type MockAuditRepo struct {
	InsertFn func(ctx context.Context, e *domain.AuditEntry) error
	ListFn   func(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditEntry, int64, error)
	Entries  []*domain.AuditEntry // collected entries for assertions
}

// Insert implements the interface method for testing.
func (m *MockAuditRepo) Insert(ctx context.Context, e *domain.AuditEntry) error {
	if m.InsertFn != nil {
		err := m.InsertFn(ctx, e)
		if err != nil {
			return err
		}
		m.Entries = append(m.Entries, e)
		return nil
	}
	m.Entries = append(m.Entries, e)
	return nil
}

// List implements the interface method for testing.
func (m *MockAuditRepo) List(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	if m.ListFn != nil {
		return m.ListFn(ctx, filter)
	}
	panic("unexpected call to MockAuditRepo.List")
}

// LastEntry returns the last collected audit entry, or nil if none.
func (m *MockAuditRepo) LastEntry() *domain.AuditEntry {
	if len(m.Entries) == 0 {
		return nil
	}
	return m.Entries[len(m.Entries)-1]
}

// HasAction returns true if any collected entry has the given action.
func (m *MockAuditRepo) HasAction(action string) bool {
	for _, e := range m.Entries {
		if e.Action == action {
			return true
		}
	}
	return false
}

var _ domain.AuditRepository = (*MockAuditRepo)(nil)

// === Authorization Service Mock ===

// MockAuthService implements domain.AuthorizationService for testing.
type MockAuthService struct {
	LookupTableIDFn           func(ctx context.Context, tableName string) (string, string, bool, error)
	CheckPrivilegeFn          func(ctx context.Context, principalName, securableType string, securableID string, privilege string) (bool, error)
	GetEffectiveRowFiltersFn  func(ctx context.Context, principalName string, tableID string) ([]string, error)
	GetEffectiveColumnMasksFn func(ctx context.Context, principalName string, tableID string) (map[string]string, error)
	GetTableColumnNamesFn     func(ctx context.Context, tableID string) ([]string, error)
}

// LookupTableID implements the interface method for testing.
func (m *MockAuthService) LookupTableID(ctx context.Context, tableName string) (string, string, bool, error) {
	if m.LookupTableIDFn != nil {
		return m.LookupTableIDFn(ctx, tableName)
	}
	panic("unexpected call to MockAuthService.LookupTableID")
}

// CheckPrivilege implements the interface method for testing.
func (m *MockAuthService) CheckPrivilege(ctx context.Context, principalName, securableType string, securableID string, privilege string) (bool, error) {
	if m.CheckPrivilegeFn != nil {
		return m.CheckPrivilegeFn(ctx, principalName, securableType, securableID, privilege)
	}
	panic("unexpected call to MockAuthService.CheckPrivilege")
}

// GetEffectiveRowFilters implements the interface method for testing.
func (m *MockAuthService) GetEffectiveRowFilters(ctx context.Context, principalName string, tableID string) ([]string, error) {
	if m.GetEffectiveRowFiltersFn != nil {
		return m.GetEffectiveRowFiltersFn(ctx, principalName, tableID)
	}
	panic("unexpected call to MockAuthService.GetEffectiveRowFilters")
}

// GetEffectiveColumnMasks implements the interface method for testing.
func (m *MockAuthService) GetEffectiveColumnMasks(ctx context.Context, principalName string, tableID string) (map[string]string, error) {
	if m.GetEffectiveColumnMasksFn != nil {
		return m.GetEffectiveColumnMasksFn(ctx, principalName, tableID)
	}
	panic("unexpected call to MockAuthService.GetEffectiveColumnMasks")
}

// GetTableColumnNames implements the interface method for testing.
func (m *MockAuthService) GetTableColumnNames(ctx context.Context, tableID string) ([]string, error) {
	if m.GetTableColumnNamesFn != nil {
		return m.GetTableColumnNamesFn(ctx, tableID)
	}
	panic("unexpected call to MockAuthService.GetTableColumnNames")
}

var _ domain.AuthorizationService = (*MockAuthService)(nil)

// === Tag Repository Mock ===

// MockTagRepo implements domain.TagRepository for testing.
type MockTagRepo struct {
	CreateTagFn             func(ctx context.Context, tag *domain.Tag) (*domain.Tag, error)
	GetTagFn                func(ctx context.Context, id string) (*domain.Tag, error)
	ListTagsFn              func(ctx context.Context, page domain.PageRequest) ([]domain.Tag, int64, error)
	DeleteTagFn             func(ctx context.Context, id string) error
	AssignTagFn             func(ctx context.Context, assignment *domain.TagAssignment) (*domain.TagAssignment, error)
	UnassignTagFn           func(ctx context.Context, id string) error
	ListTagsForSecurableFn  func(ctx context.Context, securableType string, securableID string, columnName *string) ([]domain.Tag, error)
	ListAssignmentsForTagFn func(ctx context.Context, tagID string) ([]domain.TagAssignment, error)
}

// CreateTag implements the interface method for testing.
func (m *MockTagRepo) CreateTag(ctx context.Context, tag *domain.Tag) (*domain.Tag, error) {
	if m.CreateTagFn != nil {
		return m.CreateTagFn(ctx, tag)
	}
	panic("unexpected call to MockTagRepo.CreateTag")
}

// GetTag implements the interface method for testing.
func (m *MockTagRepo) GetTag(ctx context.Context, id string) (*domain.Tag, error) {
	if m.GetTagFn != nil {
		return m.GetTagFn(ctx, id)
	}
	panic("unexpected call to MockTagRepo.GetTag")
}

// ListTags implements the interface method for testing.
func (m *MockTagRepo) ListTags(ctx context.Context, page domain.PageRequest) ([]domain.Tag, int64, error) {
	if m.ListTagsFn != nil {
		return m.ListTagsFn(ctx, page)
	}
	panic("unexpected call to MockTagRepo.ListTags")
}

// DeleteTag implements the interface method for testing.
func (m *MockTagRepo) DeleteTag(ctx context.Context, id string) error {
	if m.DeleteTagFn != nil {
		return m.DeleteTagFn(ctx, id)
	}
	panic("unexpected call to MockTagRepo.DeleteTag")
}

// AssignTag implements the interface method for testing.
func (m *MockTagRepo) AssignTag(ctx context.Context, assignment *domain.TagAssignment) (*domain.TagAssignment, error) {
	if m.AssignTagFn != nil {
		return m.AssignTagFn(ctx, assignment)
	}
	panic("unexpected call to MockTagRepo.AssignTag")
}

// UnassignTag implements the interface method for testing.
func (m *MockTagRepo) UnassignTag(ctx context.Context, id string) error {
	if m.UnassignTagFn != nil {
		return m.UnassignTagFn(ctx, id)
	}
	panic("unexpected call to MockTagRepo.UnassignTag")
}

// ListTagsForSecurable implements the interface method for testing.
func (m *MockTagRepo) ListTagsForSecurable(ctx context.Context, securableType string, securableID string, columnName *string) ([]domain.Tag, error) {
	if m.ListTagsForSecurableFn != nil {
		return m.ListTagsForSecurableFn(ctx, securableType, securableID, columnName)
	}
	panic("unexpected call to MockTagRepo.ListTagsForSecurable")
}

// ListAssignmentsForTag implements the interface method for testing.
func (m *MockTagRepo) ListAssignmentsForTag(ctx context.Context, tagID string) ([]domain.TagAssignment, error) {
	if m.ListAssignmentsForTagFn != nil {
		return m.ListAssignmentsForTagFn(ctx, tagID)
	}
	panic("unexpected call to MockTagRepo.ListAssignmentsForTag")
}

var _ domain.TagRepository = (*MockTagRepo)(nil)

// === Lineage Repository Mock ===

// MockLineageRepo implements domain.LineageRepository for testing.
type MockLineageRepo struct {
	InsertEdgeFn     func(ctx context.Context, edge *domain.LineageEdge) error
	GetUpstreamFn    func(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error)
	GetDownstreamFn  func(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error)
	DeleteEdgeFn     func(ctx context.Context, id string) error
	PurgeOlderThanFn func(ctx context.Context, before time.Time) (int64, error)
}

// InsertEdge implements the interface method for testing.
func (m *MockLineageRepo) InsertEdge(ctx context.Context, edge *domain.LineageEdge) error {
	if m.InsertEdgeFn != nil {
		return m.InsertEdgeFn(ctx, edge)
	}
	panic("unexpected call to MockLineageRepo.InsertEdge")
}

// GetUpstream implements the interface method for testing.
func (m *MockLineageRepo) GetUpstream(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error) {
	if m.GetUpstreamFn != nil {
		return m.GetUpstreamFn(ctx, tableName, page)
	}
	panic("unexpected call to MockLineageRepo.GetUpstream")
}

// GetDownstream implements the interface method for testing.
func (m *MockLineageRepo) GetDownstream(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error) {
	if m.GetDownstreamFn != nil {
		return m.GetDownstreamFn(ctx, tableName, page)
	}
	panic("unexpected call to MockLineageRepo.GetDownstream")
}

// DeleteEdge implements the interface method for testing.
func (m *MockLineageRepo) DeleteEdge(ctx context.Context, id string) error {
	if m.DeleteEdgeFn != nil {
		return m.DeleteEdgeFn(ctx, id)
	}
	panic("unexpected call to MockLineageRepo.DeleteEdge")
}

// PurgeOlderThan implements the interface method for testing.
func (m *MockLineageRepo) PurgeOlderThan(ctx context.Context, before time.Time) (int64, error) {
	if m.PurgeOlderThanFn != nil {
		return m.PurgeOlderThanFn(ctx, before)
	}
	panic("unexpected call to MockLineageRepo.PurgeOlderThan")
}

var _ domain.LineageRepository = (*MockLineageRepo)(nil)

// === Query History Repository Mock ===

// MockQueryHistoryRepo implements domain.QueryHistoryRepository for testing.
type MockQueryHistoryRepo struct {
	ListFn func(ctx context.Context, filter domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error)
}

// List implements the interface method for testing.
func (m *MockQueryHistoryRepo) List(ctx context.Context, filter domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error) {
	if m.ListFn != nil {
		return m.ListFn(ctx, filter)
	}
	panic("unexpected call to MockQueryHistoryRepo.List")
}

var _ domain.QueryHistoryRepository = (*MockQueryHistoryRepo)(nil)

// === View Repository Mock ===

// MockViewRepo implements domain.ViewRepository for testing.
type MockViewRepo struct {
	CreateFn    func(ctx context.Context, view *domain.ViewDetail) (*domain.ViewDetail, error)
	GetByNameFn func(ctx context.Context, schemaID string, viewName string) (*domain.ViewDetail, error)
	ListFn      func(ctx context.Context, schemaID string, page domain.PageRequest) ([]domain.ViewDetail, int64, error)
	DeleteFn    func(ctx context.Context, schemaID string, viewName string) error
	UpdateFn    func(ctx context.Context, schemaID string, viewName string, comment *string, props map[string]string, viewDef *string) (*domain.ViewDetail, error)
}

// Create implements the interface method for testing.
func (m *MockViewRepo) Create(ctx context.Context, view *domain.ViewDetail) (*domain.ViewDetail, error) {
	if m.CreateFn != nil {
		return m.CreateFn(ctx, view)
	}
	panic("unexpected call to MockViewRepo.Create")
}

// GetByName implements the interface method for testing.
func (m *MockViewRepo) GetByName(ctx context.Context, schemaID string, viewName string) (*domain.ViewDetail, error) {
	if m.GetByNameFn != nil {
		return m.GetByNameFn(ctx, schemaID, viewName)
	}
	panic("unexpected call to MockViewRepo.GetByName")
}

// List implements the interface method for testing.
func (m *MockViewRepo) List(ctx context.Context, schemaID string, page domain.PageRequest) ([]domain.ViewDetail, int64, error) {
	if m.ListFn != nil {
		return m.ListFn(ctx, schemaID, page)
	}
	panic("unexpected call to MockViewRepo.List")
}

// Delete implements the interface method for testing.
func (m *MockViewRepo) Delete(ctx context.Context, schemaID string, viewName string) error {
	if m.DeleteFn != nil {
		return m.DeleteFn(ctx, schemaID, viewName)
	}
	panic("unexpected call to MockViewRepo.Delete")
}

// Update implements the interface method for testing.
func (m *MockViewRepo) Update(ctx context.Context, schemaID string, viewName string, comment *string, props map[string]string, viewDef *string) (*domain.ViewDetail, error) {
	if m.UpdateFn != nil {
		return m.UpdateFn(ctx, schemaID, viewName, comment, props, viewDef)
	}
	panic("unexpected call to MockViewRepo.Update")
}

var _ domain.ViewRepository = (*MockViewRepo)(nil)

// === Search Repository Mock ===

// MockSearchRepo implements domain.SearchRepository for testing.
type MockSearchRepo struct {
	SearchFn func(ctx context.Context, query string, objectType *string, maxResults int, offset int) ([]domain.SearchResult, int64, error)
}

// Search implements the interface method for testing.
func (m *MockSearchRepo) Search(ctx context.Context, query string, objectType *string, maxResults int, offset int) ([]domain.SearchResult, int64, error) {
	if m.SearchFn != nil {
		return m.SearchFn(ctx, query, objectType, maxResults, offset)
	}
	panic("unexpected call to MockSearchRepo.Search")
}

var _ domain.SearchRepository = (*MockSearchRepo)(nil)

// === Catalog Repository Mock ===

// MockCatalogRepo implements domain.CatalogRepository for testing.
// Uses function fields so tests only need to set the methods they care about.
type MockCatalogRepo struct {
	GetCatalogInfoFn       func(ctx context.Context) (*domain.CatalogInfo, error)
	GetMetastoreSummaryFn  func(ctx context.Context) (*domain.MetastoreSummary, error)
	CreateSchemaFn         func(ctx context.Context, name, comment, owner string) (*domain.SchemaDetail, error)
	GetSchemaFn            func(ctx context.Context, name string) (*domain.SchemaDetail, error)
	ListSchemasFn          func(ctx context.Context, page domain.PageRequest) ([]domain.SchemaDetail, int64, error)
	UpdateSchemaFn         func(ctx context.Context, name string, comment *string, props map[string]string) (*domain.SchemaDetail, error)
	DeleteSchemaFn         func(ctx context.Context, name string, force bool) error
	CreateTableFn          func(ctx context.Context, schemaName string, req domain.CreateTableRequest, owner string) (*domain.TableDetail, error)
	CreateExternalTableFn  func(ctx context.Context, schemaName string, req domain.CreateTableRequest, owner string) (*domain.TableDetail, error)
	GetTableFn             func(ctx context.Context, schemaName, tableName string) (*domain.TableDetail, error)
	ListTablesFn           func(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.TableDetail, int64, error)
	DeleteTableFn          func(ctx context.Context, schemaName, tableName string) error
	UpdateTableFn          func(ctx context.Context, schemaName, tableName string, comment *string, props map[string]string, owner *string) (*domain.TableDetail, error)
	UpdateCatalogFn        func(ctx context.Context, comment *string) (*domain.CatalogInfo, error)
	UpdateColumnFn         func(ctx context.Context, schemaName, tableName, columnName string, comment *string, props map[string]string) (*domain.ColumnDetail, error)
	ListColumnsFn          func(ctx context.Context, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, int64, error)
	SetSchemaStoragePathFn func(ctx context.Context, schemaID string, path string) error
}

// GetCatalogInfo implements the interface method for testing.
func (m *MockCatalogRepo) GetCatalogInfo(ctx context.Context) (*domain.CatalogInfo, error) {
	if m.GetCatalogInfoFn != nil {
		return m.GetCatalogInfoFn(ctx)
	}
	panic("unexpected call to MockCatalogRepo.GetCatalogInfo")
}

// GetMetastoreSummary implements the interface method for testing.
func (m *MockCatalogRepo) GetMetastoreSummary(ctx context.Context) (*domain.MetastoreSummary, error) {
	if m.GetMetastoreSummaryFn != nil {
		return m.GetMetastoreSummaryFn(ctx)
	}
	panic("unexpected call to MockCatalogRepo.GetMetastoreSummary")
}

// CreateSchema implements the interface method for testing.
func (m *MockCatalogRepo) CreateSchema(ctx context.Context, name, comment, owner string) (*domain.SchemaDetail, error) {
	if m.CreateSchemaFn != nil {
		return m.CreateSchemaFn(ctx, name, comment, owner)
	}
	panic("unexpected call to MockCatalogRepo.CreateSchema")
}

// GetSchema implements the interface method for testing.
func (m *MockCatalogRepo) GetSchema(ctx context.Context, name string) (*domain.SchemaDetail, error) {
	if m.GetSchemaFn != nil {
		return m.GetSchemaFn(ctx, name)
	}
	panic("unexpected call to MockCatalogRepo.GetSchema")
}

// ListSchemas implements the interface method for testing.
func (m *MockCatalogRepo) ListSchemas(ctx context.Context, page domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
	if m.ListSchemasFn != nil {
		return m.ListSchemasFn(ctx, page)
	}
	panic("unexpected call to MockCatalogRepo.ListSchemas")
}

// UpdateSchema implements the interface method for testing.
func (m *MockCatalogRepo) UpdateSchema(ctx context.Context, name string, comment *string, props map[string]string) (*domain.SchemaDetail, error) {
	if m.UpdateSchemaFn != nil {
		return m.UpdateSchemaFn(ctx, name, comment, props)
	}
	panic("unexpected call to MockCatalogRepo.UpdateSchema")
}

// DeleteSchema implements the interface method for testing.
func (m *MockCatalogRepo) DeleteSchema(ctx context.Context, name string, force bool) error {
	if m.DeleteSchemaFn != nil {
		return m.DeleteSchemaFn(ctx, name, force)
	}
	panic("unexpected call to MockCatalogRepo.DeleteSchema")
}

// CreateTable implements the interface method for testing.
func (m *MockCatalogRepo) CreateTable(ctx context.Context, schemaName string, req domain.CreateTableRequest, owner string) (*domain.TableDetail, error) {
	if m.CreateTableFn != nil {
		return m.CreateTableFn(ctx, schemaName, req, owner)
	}
	panic("unexpected call to MockCatalogRepo.CreateTable")
}

// CreateExternalTable implements the interface method for testing.
func (m *MockCatalogRepo) CreateExternalTable(ctx context.Context, schemaName string, req domain.CreateTableRequest, owner string) (*domain.TableDetail, error) {
	if m.CreateExternalTableFn != nil {
		return m.CreateExternalTableFn(ctx, schemaName, req, owner)
	}
	panic("unexpected call to MockCatalogRepo.CreateExternalTable")
}

// GetTable implements the interface method for testing.
func (m *MockCatalogRepo) GetTable(ctx context.Context, schemaName, tableName string) (*domain.TableDetail, error) {
	if m.GetTableFn != nil {
		return m.GetTableFn(ctx, schemaName, tableName)
	}
	panic("unexpected call to MockCatalogRepo.GetTable")
}

// ListTables implements the interface method for testing.
func (m *MockCatalogRepo) ListTables(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.TableDetail, int64, error) {
	if m.ListTablesFn != nil {
		return m.ListTablesFn(ctx, schemaName, page)
	}
	panic("unexpected call to MockCatalogRepo.ListTables")
}

// DeleteTable implements the interface method for testing.
func (m *MockCatalogRepo) DeleteTable(ctx context.Context, schemaName, tableName string) error {
	if m.DeleteTableFn != nil {
		return m.DeleteTableFn(ctx, schemaName, tableName)
	}
	panic("unexpected call to MockCatalogRepo.DeleteTable")
}

// UpdateTable implements the interface method for testing.
func (m *MockCatalogRepo) UpdateTable(ctx context.Context, schemaName, tableName string, comment *string, props map[string]string, owner *string) (*domain.TableDetail, error) {
	if m.UpdateTableFn != nil {
		return m.UpdateTableFn(ctx, schemaName, tableName, comment, props, owner)
	}
	panic("unexpected call to MockCatalogRepo.UpdateTable")
}

// UpdateCatalog implements the interface method for testing.
func (m *MockCatalogRepo) UpdateCatalog(ctx context.Context, comment *string) (*domain.CatalogInfo, error) {
	if m.UpdateCatalogFn != nil {
		return m.UpdateCatalogFn(ctx, comment)
	}
	panic("unexpected call to MockCatalogRepo.UpdateCatalog")
}

// UpdateColumn implements the interface method for testing.
func (m *MockCatalogRepo) UpdateColumn(ctx context.Context, schemaName, tableName, columnName string, comment *string, props map[string]string) (*domain.ColumnDetail, error) {
	if m.UpdateColumnFn != nil {
		return m.UpdateColumnFn(ctx, schemaName, tableName, columnName, comment, props)
	}
	panic("unexpected call to MockCatalogRepo.UpdateColumn")
}

// ListColumns implements the interface method for testing.
func (m *MockCatalogRepo) ListColumns(ctx context.Context, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, int64, error) {
	if m.ListColumnsFn != nil {
		return m.ListColumnsFn(ctx, schemaName, tableName, page)
	}
	panic("unexpected call to MockCatalogRepo.ListColumns")
}

// SetSchemaStoragePath implements the interface method for testing.
func (m *MockCatalogRepo) SetSchemaStoragePath(ctx context.Context, schemaID string, path string) error {
	if m.SetSchemaStoragePathFn != nil {
		return m.SetSchemaStoragePathFn(ctx, schemaID, path)
	}
	panic("unexpected call to MockCatalogRepo.SetSchemaStoragePath")
}

var _ domain.CatalogRepository = (*MockCatalogRepo)(nil)

// === Storage Credential Repository Mock ===

// MockStorageCredentialRepo implements domain.StorageCredentialRepository for testing.
type MockStorageCredentialRepo struct {
	CreateFn    func(ctx context.Context, cred *domain.StorageCredential) (*domain.StorageCredential, error)
	GetByIDFn   func(ctx context.Context, id string) (*domain.StorageCredential, error)
	GetByNameFn func(ctx context.Context, name string) (*domain.StorageCredential, error)
	ListFn      func(ctx context.Context, page domain.PageRequest) ([]domain.StorageCredential, int64, error)
	UpdateFn    func(ctx context.Context, id string, req domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error)
	DeleteFn    func(ctx context.Context, id string) error
}

// Create implements the interface method for testing.
func (m *MockStorageCredentialRepo) Create(ctx context.Context, cred *domain.StorageCredential) (*domain.StorageCredential, error) {
	if m.CreateFn != nil {
		return m.CreateFn(ctx, cred)
	}
	panic("unexpected call to MockStorageCredentialRepo.Create")
}

// GetByID implements the interface method for testing.
func (m *MockStorageCredentialRepo) GetByID(ctx context.Context, id string) (*domain.StorageCredential, error) {
	if m.GetByIDFn != nil {
		return m.GetByIDFn(ctx, id)
	}
	panic("unexpected call to MockStorageCredentialRepo.GetByID")
}

// GetByName implements the interface method for testing.
func (m *MockStorageCredentialRepo) GetByName(ctx context.Context, name string) (*domain.StorageCredential, error) {
	if m.GetByNameFn != nil {
		return m.GetByNameFn(ctx, name)
	}
	panic("unexpected call to MockStorageCredentialRepo.GetByName")
}

// List implements the interface method for testing.
func (m *MockStorageCredentialRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.StorageCredential, int64, error) {
	if m.ListFn != nil {
		return m.ListFn(ctx, page)
	}
	panic("unexpected call to MockStorageCredentialRepo.List")
}

// Update implements the interface method for testing.
func (m *MockStorageCredentialRepo) Update(ctx context.Context, id string, req domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error) {
	if m.UpdateFn != nil {
		return m.UpdateFn(ctx, id, req)
	}
	panic("unexpected call to MockStorageCredentialRepo.Update")
}

// Delete implements the interface method for testing.
func (m *MockStorageCredentialRepo) Delete(ctx context.Context, id string) error {
	if m.DeleteFn != nil {
		return m.DeleteFn(ctx, id)
	}
	panic("unexpected call to MockStorageCredentialRepo.Delete")
}

var _ domain.StorageCredentialRepository = (*MockStorageCredentialRepo)(nil)

// === External Location Repository Mock ===

// MockExternalLocationRepo implements domain.ExternalLocationRepository for testing.
type MockExternalLocationRepo struct {
	CreateFn    func(ctx context.Context, loc *domain.ExternalLocation) (*domain.ExternalLocation, error)
	GetByIDFn   func(ctx context.Context, id string) (*domain.ExternalLocation, error)
	GetByNameFn func(ctx context.Context, name string) (*domain.ExternalLocation, error)
	ListFn      func(ctx context.Context, page domain.PageRequest) ([]domain.ExternalLocation, int64, error)
	UpdateFn    func(ctx context.Context, id string, req domain.UpdateExternalLocationRequest) (*domain.ExternalLocation, error)
	DeleteFn    func(ctx context.Context, id string) error
}

// Create implements the interface method for testing.
func (m *MockExternalLocationRepo) Create(ctx context.Context, loc *domain.ExternalLocation) (*domain.ExternalLocation, error) {
	if m.CreateFn != nil {
		return m.CreateFn(ctx, loc)
	}
	panic("unexpected call to MockExternalLocationRepo.Create")
}

// GetByID implements the interface method for testing.
func (m *MockExternalLocationRepo) GetByID(ctx context.Context, id string) (*domain.ExternalLocation, error) {
	if m.GetByIDFn != nil {
		return m.GetByIDFn(ctx, id)
	}
	panic("unexpected call to MockExternalLocationRepo.GetByID")
}

// GetByName implements the interface method for testing.
func (m *MockExternalLocationRepo) GetByName(ctx context.Context, name string) (*domain.ExternalLocation, error) {
	if m.GetByNameFn != nil {
		return m.GetByNameFn(ctx, name)
	}
	panic("unexpected call to MockExternalLocationRepo.GetByName")
}

// List implements the interface method for testing.
func (m *MockExternalLocationRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.ExternalLocation, int64, error) {
	if m.ListFn != nil {
		return m.ListFn(ctx, page)
	}
	panic("unexpected call to MockExternalLocationRepo.List")
}

// Update implements the interface method for testing.
func (m *MockExternalLocationRepo) Update(ctx context.Context, id string, req domain.UpdateExternalLocationRequest) (*domain.ExternalLocation, error) {
	if m.UpdateFn != nil {
		return m.UpdateFn(ctx, id, req)
	}
	panic("unexpected call to MockExternalLocationRepo.Update")
}

// Delete implements the interface method for testing.
func (m *MockExternalLocationRepo) Delete(ctx context.Context, id string) error {
	if m.DeleteFn != nil {
		return m.DeleteFn(ctx, id)
	}
	panic("unexpected call to MockExternalLocationRepo.Delete")
}

var _ domain.ExternalLocationRepository = (*MockExternalLocationRepo)(nil)

// === Compute Endpoint Repository Mock ===

// MockComputeEndpointRepo implements domain.ComputeEndpointRepository for testing.
type MockComputeEndpointRepo struct {
	CreateFn                     func(ctx context.Context, ep *domain.ComputeEndpoint) (*domain.ComputeEndpoint, error)
	GetByIDFn                    func(ctx context.Context, id string) (*domain.ComputeEndpoint, error)
	GetByNameFn                  func(ctx context.Context, name string) (*domain.ComputeEndpoint, error)
	ListFn                       func(ctx context.Context, page domain.PageRequest) ([]domain.ComputeEndpoint, int64, error)
	UpdateFn                     func(ctx context.Context, id string, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error)
	DeleteFn                     func(ctx context.Context, id string) error
	UpdateStatusFn               func(ctx context.Context, id string, status string) error
	AssignFn                     func(ctx context.Context, a *domain.ComputeAssignment) (*domain.ComputeAssignment, error)
	UnassignFn                   func(ctx context.Context, id string) error
	ListAssignmentsFn            func(ctx context.Context, endpointID string, page domain.PageRequest) ([]domain.ComputeAssignment, int64, error)
	GetDefaultForPrincipalFn     func(ctx context.Context, principalID string, principalType string) (*domain.ComputeEndpoint, error)
	GetAssignmentsForPrincipalFn func(ctx context.Context, principalID string, principalType string) ([]domain.ComputeEndpoint, error)
}

// Create implements the interface method for testing.
func (m *MockComputeEndpointRepo) Create(ctx context.Context, ep *domain.ComputeEndpoint) (*domain.ComputeEndpoint, error) {
	if m.CreateFn != nil {
		return m.CreateFn(ctx, ep)
	}
	panic("unexpected call to MockComputeEndpointRepo.Create")
}

// GetByID implements the interface method for testing.
func (m *MockComputeEndpointRepo) GetByID(ctx context.Context, id string) (*domain.ComputeEndpoint, error) {
	if m.GetByIDFn != nil {
		return m.GetByIDFn(ctx, id)
	}
	panic("unexpected call to MockComputeEndpointRepo.GetByID")
}

// GetByName implements the interface method for testing.
func (m *MockComputeEndpointRepo) GetByName(ctx context.Context, name string) (*domain.ComputeEndpoint, error) {
	if m.GetByNameFn != nil {
		return m.GetByNameFn(ctx, name)
	}
	panic("unexpected call to MockComputeEndpointRepo.GetByName")
}

// List implements the interface method for testing.
func (m *MockComputeEndpointRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.ComputeEndpoint, int64, error) {
	if m.ListFn != nil {
		return m.ListFn(ctx, page)
	}
	panic("unexpected call to MockComputeEndpointRepo.List")
}

// Update implements the interface method for testing.
func (m *MockComputeEndpointRepo) Update(ctx context.Context, id string, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
	if m.UpdateFn != nil {
		return m.UpdateFn(ctx, id, req)
	}
	panic("unexpected call to MockComputeEndpointRepo.Update")
}

// Delete implements the interface method for testing.
func (m *MockComputeEndpointRepo) Delete(ctx context.Context, id string) error {
	if m.DeleteFn != nil {
		return m.DeleteFn(ctx, id)
	}
	panic("unexpected call to MockComputeEndpointRepo.Delete")
}

// UpdateStatus implements the interface method for testing.
func (m *MockComputeEndpointRepo) UpdateStatus(ctx context.Context, id string, status string) error {
	if m.UpdateStatusFn != nil {
		return m.UpdateStatusFn(ctx, id, status)
	}
	panic("unexpected call to MockComputeEndpointRepo.UpdateStatus")
}

// Assign implements the interface method for testing.
func (m *MockComputeEndpointRepo) Assign(ctx context.Context, a *domain.ComputeAssignment) (*domain.ComputeAssignment, error) {
	if m.AssignFn != nil {
		return m.AssignFn(ctx, a)
	}
	panic("unexpected call to MockComputeEndpointRepo.Assign")
}

// Unassign implements the interface method for testing.
func (m *MockComputeEndpointRepo) Unassign(ctx context.Context, id string) error {
	if m.UnassignFn != nil {
		return m.UnassignFn(ctx, id)
	}
	panic("unexpected call to MockComputeEndpointRepo.Unassign")
}

// ListAssignments implements the interface method for testing.
func (m *MockComputeEndpointRepo) ListAssignments(ctx context.Context, endpointID string, page domain.PageRequest) ([]domain.ComputeAssignment, int64, error) {
	if m.ListAssignmentsFn != nil {
		return m.ListAssignmentsFn(ctx, endpointID, page)
	}
	panic("unexpected call to MockComputeEndpointRepo.ListAssignments")
}

// GetDefaultForPrincipal implements the interface method for testing.
func (m *MockComputeEndpointRepo) GetDefaultForPrincipal(ctx context.Context, principalID string, principalType string) (*domain.ComputeEndpoint, error) {
	if m.GetDefaultForPrincipalFn != nil {
		return m.GetDefaultForPrincipalFn(ctx, principalID, principalType)
	}
	panic("unexpected call to MockComputeEndpointRepo.GetDefaultForPrincipal")
}

// GetAssignmentsForPrincipal implements the interface method for testing.
func (m *MockComputeEndpointRepo) GetAssignmentsForPrincipal(ctx context.Context, principalID string, principalType string) ([]domain.ComputeEndpoint, error) {
	if m.GetAssignmentsForPrincipalFn != nil {
		return m.GetAssignmentsForPrincipalFn(ctx, principalID, principalType)
	}
	panic("unexpected call to MockComputeEndpointRepo.GetAssignmentsForPrincipal")
}

var _ domain.ComputeEndpointRepository = (*MockComputeEndpointRepo)(nil)

// === DuckDB Executor ===

// MockDuckDBExecutor is a test mock for domain.DuckDBExecutor.
type MockDuckDBExecutor struct {
	ExecContextFn func(ctx context.Context, query string) error
	Queries       []string // records all executed queries
}

// ExecContext implements domain.DuckDBExecutor.
func (m *MockDuckDBExecutor) ExecContext(ctx context.Context, query string) error {
	m.Queries = append(m.Queries, query)
	if m.ExecContextFn != nil {
		return m.ExecContextFn(ctx, query)
	}
	return nil
}

var _ domain.DuckDBExecutor = (*MockDuckDBExecutor)(nil)

// === Volume Repository Mock ===

// MockVolumeRepo implements domain.VolumeRepository for testing.
type MockVolumeRepo struct {
	CreateFn    func(ctx context.Context, vol *domain.Volume) (*domain.Volume, error)
	GetByNameFn func(ctx context.Context, schemaName, name string) (*domain.Volume, error)
	ListFn      func(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.Volume, int64, error)
	UpdateFn    func(ctx context.Context, id string, req domain.UpdateVolumeRequest) (*domain.Volume, error)
	DeleteFn    func(ctx context.Context, id string) error
}

// Create implements the interface method for testing.
func (m *MockVolumeRepo) Create(ctx context.Context, vol *domain.Volume) (*domain.Volume, error) {
	if m.CreateFn != nil {
		return m.CreateFn(ctx, vol)
	}
	panic("unexpected call to MockVolumeRepo.Create")
}

// GetByName implements the interface method for testing.
func (m *MockVolumeRepo) GetByName(ctx context.Context, schemaName, name string) (*domain.Volume, error) {
	if m.GetByNameFn != nil {
		return m.GetByNameFn(ctx, schemaName, name)
	}
	panic("unexpected call to MockVolumeRepo.GetByName")
}

// List implements the interface method for testing.
func (m *MockVolumeRepo) List(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.Volume, int64, error) {
	if m.ListFn != nil {
		return m.ListFn(ctx, schemaName, page)
	}
	panic("unexpected call to MockVolumeRepo.List")
}

// Update implements the interface method for testing.
func (m *MockVolumeRepo) Update(ctx context.Context, id string, req domain.UpdateVolumeRequest) (*domain.Volume, error) {
	if m.UpdateFn != nil {
		return m.UpdateFn(ctx, id, req)
	}
	panic("unexpected call to MockVolumeRepo.Update")
}

// Delete implements the interface method for testing.
func (m *MockVolumeRepo) Delete(ctx context.Context, id string) error {
	if m.DeleteFn != nil {
		return m.DeleteFn(ctx, id)
	}
	panic("unexpected call to MockVolumeRepo.Delete")
}

var _ domain.VolumeRepository = (*MockVolumeRepo)(nil)

// === Introspection Repository Mock ===

// MockIntrospectionRepo implements domain.IntrospectionRepository for testing.
type MockIntrospectionRepo struct {
	ListSchemasFn     func(ctx context.Context, page domain.PageRequest) ([]domain.Schema, int64, error)
	ListTablesFn      func(ctx context.Context, schemaID string, page domain.PageRequest) ([]domain.Table, int64, error)
	GetTableFn        func(ctx context.Context, tableID string) (*domain.Table, error)
	ListColumnsFn     func(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.Column, int64, error)
	GetTableByNameFn  func(ctx context.Context, tableName string) (*domain.Table, error)
	GetSchemaByNameFn func(ctx context.Context, schemaName string) (*domain.Schema, error)
}

// ListSchemas implements the interface method for testing.
func (m *MockIntrospectionRepo) ListSchemas(ctx context.Context, page domain.PageRequest) ([]domain.Schema, int64, error) {
	if m.ListSchemasFn != nil {
		return m.ListSchemasFn(ctx, page)
	}
	panic("unexpected call to MockIntrospectionRepo.ListSchemas")
}

// ListTables implements the interface method for testing.
func (m *MockIntrospectionRepo) ListTables(ctx context.Context, schemaID string, page domain.PageRequest) ([]domain.Table, int64, error) {
	if m.ListTablesFn != nil {
		return m.ListTablesFn(ctx, schemaID, page)
	}
	panic("unexpected call to MockIntrospectionRepo.ListTables")
}

// GetTable implements the interface method for testing.
func (m *MockIntrospectionRepo) GetTable(ctx context.Context, tableID string) (*domain.Table, error) {
	if m.GetTableFn != nil {
		return m.GetTableFn(ctx, tableID)
	}
	panic("unexpected call to MockIntrospectionRepo.GetTable")
}

// ListColumns implements the interface method for testing.
func (m *MockIntrospectionRepo) ListColumns(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.Column, int64, error) {
	if m.ListColumnsFn != nil {
		return m.ListColumnsFn(ctx, tableID, page)
	}
	panic("unexpected call to MockIntrospectionRepo.ListColumns")
}

// GetTableByName implements the interface method for testing.
func (m *MockIntrospectionRepo) GetTableByName(ctx context.Context, tableName string) (*domain.Table, error) {
	if m.GetTableByNameFn != nil {
		return m.GetTableByNameFn(ctx, tableName)
	}
	panic("unexpected call to MockIntrospectionRepo.GetTableByName")
}

// GetSchemaByName implements the interface method for testing.
func (m *MockIntrospectionRepo) GetSchemaByName(ctx context.Context, schemaName string) (*domain.Schema, error) {
	if m.GetSchemaByNameFn != nil {
		return m.GetSchemaByNameFn(ctx, schemaName)
	}
	panic("unexpected call to MockIntrospectionRepo.GetSchemaByName")
}

var _ domain.IntrospectionRepository = (*MockIntrospectionRepo)(nil)

// === Notebook Repository Mock ===

// MockNotebookRepo implements domain.NotebookRepository for testing.
type MockNotebookRepo struct {
	CreateNotebookFn   func(ctx context.Context, nb *domain.Notebook) (*domain.Notebook, error)
	GetNotebookFn      func(ctx context.Context, id string) (*domain.Notebook, error)
	ListNotebooksFn    func(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Notebook, int64, error)
	UpdateNotebookFn   func(ctx context.Context, id string, req domain.UpdateNotebookRequest) (*domain.Notebook, error)
	DeleteNotebookFn   func(ctx context.Context, id string) error
	CreateCellFn       func(ctx context.Context, cell *domain.Cell) (*domain.Cell, error)
	GetCellFn          func(ctx context.Context, id string) (*domain.Cell, error)
	ListCellsFn        func(ctx context.Context, notebookID string) ([]domain.Cell, error)
	UpdateCellFn       func(ctx context.Context, id string, req domain.UpdateCellRequest) (*domain.Cell, error)
	DeleteCellFn       func(ctx context.Context, id string) error
	UpdateCellResultFn func(ctx context.Context, cellID string, result *string) error
	ReorderCellsFn     func(ctx context.Context, notebookID string, cellIDs []string) error
	GetMaxPositionFn   func(ctx context.Context, notebookID string) (int, error)
}

func (m *MockNotebookRepo) CreateNotebook(ctx context.Context, nb *domain.Notebook) (*domain.Notebook, error) {
	if m.CreateNotebookFn != nil {
		return m.CreateNotebookFn(ctx, nb)
	}
	panic("unexpected call to MockNotebookRepo.CreateNotebook")
}

func (m *MockNotebookRepo) GetNotebook(ctx context.Context, id string) (*domain.Notebook, error) {
	if m.GetNotebookFn != nil {
		return m.GetNotebookFn(ctx, id)
	}
	panic("unexpected call to MockNotebookRepo.GetNotebook")
}

func (m *MockNotebookRepo) ListNotebooks(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Notebook, int64, error) {
	if m.ListNotebooksFn != nil {
		return m.ListNotebooksFn(ctx, owner, page)
	}
	panic("unexpected call to MockNotebookRepo.ListNotebooks")
}

func (m *MockNotebookRepo) UpdateNotebook(ctx context.Context, id string, req domain.UpdateNotebookRequest) (*domain.Notebook, error) {
	if m.UpdateNotebookFn != nil {
		return m.UpdateNotebookFn(ctx, id, req)
	}
	panic("unexpected call to MockNotebookRepo.UpdateNotebook")
}

func (m *MockNotebookRepo) DeleteNotebook(ctx context.Context, id string) error {
	if m.DeleteNotebookFn != nil {
		return m.DeleteNotebookFn(ctx, id)
	}
	panic("unexpected call to MockNotebookRepo.DeleteNotebook")
}

func (m *MockNotebookRepo) CreateCell(ctx context.Context, cell *domain.Cell) (*domain.Cell, error) {
	if m.CreateCellFn != nil {
		return m.CreateCellFn(ctx, cell)
	}
	panic("unexpected call to MockNotebookRepo.CreateCell")
}

func (m *MockNotebookRepo) GetCell(ctx context.Context, id string) (*domain.Cell, error) {
	if m.GetCellFn != nil {
		return m.GetCellFn(ctx, id)
	}
	panic("unexpected call to MockNotebookRepo.GetCell")
}

func (m *MockNotebookRepo) ListCells(ctx context.Context, notebookID string) ([]domain.Cell, error) {
	if m.ListCellsFn != nil {
		return m.ListCellsFn(ctx, notebookID)
	}
	panic("unexpected call to MockNotebookRepo.ListCells")
}

func (m *MockNotebookRepo) UpdateCell(ctx context.Context, id string, req domain.UpdateCellRequest) (*domain.Cell, error) {
	if m.UpdateCellFn != nil {
		return m.UpdateCellFn(ctx, id, req)
	}
	panic("unexpected call to MockNotebookRepo.UpdateCell")
}

func (m *MockNotebookRepo) DeleteCell(ctx context.Context, id string) error {
	if m.DeleteCellFn != nil {
		return m.DeleteCellFn(ctx, id)
	}
	panic("unexpected call to MockNotebookRepo.DeleteCell")
}

func (m *MockNotebookRepo) UpdateCellResult(ctx context.Context, cellID string, result *string) error {
	if m.UpdateCellResultFn != nil {
		return m.UpdateCellResultFn(ctx, cellID, result)
	}
	return nil // default no-op like audit
}

func (m *MockNotebookRepo) ReorderCells(ctx context.Context, notebookID string, cellIDs []string) error {
	if m.ReorderCellsFn != nil {
		return m.ReorderCellsFn(ctx, notebookID, cellIDs)
	}
	panic("unexpected call to MockNotebookRepo.ReorderCells")
}

func (m *MockNotebookRepo) GetMaxPosition(ctx context.Context, notebookID string) (int, error) {
	if m.GetMaxPositionFn != nil {
		return m.GetMaxPositionFn(ctx, notebookID)
	}
	panic("unexpected call to MockNotebookRepo.GetMaxPosition")
}

var _ domain.NotebookRepository = (*MockNotebookRepo)(nil)

// === Notebook Job Repository Mock ===

// MockNotebookJobRepo implements domain.NotebookJobRepository for testing.
type MockNotebookJobRepo struct {
	CreateJobFn      func(ctx context.Context, job *domain.NotebookJob) (*domain.NotebookJob, error)
	GetJobFn         func(ctx context.Context, id string) (*domain.NotebookJob, error)
	ListJobsFn       func(ctx context.Context, notebookID string, page domain.PageRequest) ([]domain.NotebookJob, int64, error)
	UpdateJobStateFn func(ctx context.Context, id string, state domain.JobState, result *string, errMsg *string) error
}

func (m *MockNotebookJobRepo) CreateJob(ctx context.Context, job *domain.NotebookJob) (*domain.NotebookJob, error) {
	if m.CreateJobFn != nil {
		return m.CreateJobFn(ctx, job)
	}
	panic("unexpected call to MockNotebookJobRepo.CreateJob")
}

func (m *MockNotebookJobRepo) GetJob(ctx context.Context, id string) (*domain.NotebookJob, error) {
	if m.GetJobFn != nil {
		return m.GetJobFn(ctx, id)
	}
	panic("unexpected call to MockNotebookJobRepo.GetJob")
}

func (m *MockNotebookJobRepo) ListJobs(ctx context.Context, notebookID string, page domain.PageRequest) ([]domain.NotebookJob, int64, error) {
	if m.ListJobsFn != nil {
		return m.ListJobsFn(ctx, notebookID, page)
	}
	panic("unexpected call to MockNotebookJobRepo.ListJobs")
}

func (m *MockNotebookJobRepo) UpdateJobState(ctx context.Context, id string, state domain.JobState, result *string, errMsg *string) error {
	if m.UpdateJobStateFn != nil {
		return m.UpdateJobStateFn(ctx, id, state, result, errMsg)
	}
	return nil // default no-op
}

var _ domain.NotebookJobRepository = (*MockNotebookJobRepo)(nil)

// === Git Repo Repository Mock ===

// MockGitRepoRepo implements domain.GitRepoRepository for testing.
type MockGitRepoRepo struct {
	CreateFn           func(ctx context.Context, repo *domain.GitRepo) (*domain.GitRepo, error)
	GetByIDFn          func(ctx context.Context, id string) (*domain.GitRepo, error)
	ListFn             func(ctx context.Context, page domain.PageRequest) ([]domain.GitRepo, int64, error)
	DeleteFn           func(ctx context.Context, id string) error
	UpdateSyncStatusFn func(ctx context.Context, id string, commitSHA string, syncedAt time.Time) error
}

func (m *MockGitRepoRepo) Create(ctx context.Context, repo *domain.GitRepo) (*domain.GitRepo, error) {
	if m.CreateFn != nil {
		return m.CreateFn(ctx, repo)
	}
	panic("unexpected call to MockGitRepoRepo.Create")
}

func (m *MockGitRepoRepo) GetByID(ctx context.Context, id string) (*domain.GitRepo, error) {
	if m.GetByIDFn != nil {
		return m.GetByIDFn(ctx, id)
	}
	panic("unexpected call to MockGitRepoRepo.GetByID")
}

func (m *MockGitRepoRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.GitRepo, int64, error) {
	if m.ListFn != nil {
		return m.ListFn(ctx, page)
	}
	panic("unexpected call to MockGitRepoRepo.List")
}

func (m *MockGitRepoRepo) Delete(ctx context.Context, id string) error {
	if m.DeleteFn != nil {
		return m.DeleteFn(ctx, id)
	}
	panic("unexpected call to MockGitRepoRepo.Delete")
}

func (m *MockGitRepoRepo) UpdateSyncStatus(ctx context.Context, id string, commitSHA string, syncedAt time.Time) error {
	if m.UpdateSyncStatusFn != nil {
		return m.UpdateSyncStatusFn(ctx, id, commitSHA, syncedAt)
	}
	panic("unexpected call to MockGitRepoRepo.UpdateSyncStatus")
}

var _ domain.GitRepoRepository = (*MockGitRepoRepo)(nil)
