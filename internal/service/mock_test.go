package service

import (
	"context"
	"fmt"
	"time"

	"duck-demo/internal/domain"
)

// === View Repository Mock ===

type mockViewRepo struct {
	createFn    func(ctx context.Context, view *domain.ViewDetail) (*domain.ViewDetail, error)
	getByNameFn func(ctx context.Context, schemaID int64, viewName string) (*domain.ViewDetail, error)
	listFn      func(ctx context.Context, schemaID int64, page domain.PageRequest) ([]domain.ViewDetail, int64, error)
	deleteFn    func(ctx context.Context, schemaID int64, viewName string) error
}

func (m *mockViewRepo) Create(ctx context.Context, view *domain.ViewDetail) (*domain.ViewDetail, error) {
	if m.createFn != nil {
		return m.createFn(ctx, view)
	}
	panic("unexpected call to mockViewRepo.Create")
}

func (m *mockViewRepo) GetByName(ctx context.Context, schemaID int64, viewName string) (*domain.ViewDetail, error) {
	if m.getByNameFn != nil {
		return m.getByNameFn(ctx, schemaID, viewName)
	}
	panic("unexpected call to mockViewRepo.GetByName")
}

func (m *mockViewRepo) List(ctx context.Context, schemaID int64, page domain.PageRequest) ([]domain.ViewDetail, int64, error) {
	if m.listFn != nil {
		return m.listFn(ctx, schemaID, page)
	}
	panic("unexpected call to mockViewRepo.List")
}

func (m *mockViewRepo) Delete(ctx context.Context, schemaID int64, viewName string) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, schemaID, viewName)
	}
	panic("unexpected call to mockViewRepo.Delete")
}

func (m *mockViewRepo) Update(_ context.Context, _ int64, _ string, _ *string, _ map[string]string, _ *string) (*domain.ViewDetail, error) {
	panic("unexpected call to mockViewRepo.Update")
}

// === Tag Repository Mock ===

type mockTagRepo struct {
	createTagFn             func(ctx context.Context, tag *domain.Tag) (*domain.Tag, error)
	getTagFn                func(ctx context.Context, id int64) (*domain.Tag, error)
	listTagsFn              func(ctx context.Context, page domain.PageRequest) ([]domain.Tag, int64, error)
	deleteTagFn             func(ctx context.Context, id int64) error
	assignTagFn             func(ctx context.Context, assignment *domain.TagAssignment) (*domain.TagAssignment, error)
	unassignTagFn           func(ctx context.Context, id int64) error
	listTagsForSecurableFn  func(ctx context.Context, securableType string, securableID int64, columnName *string) ([]domain.Tag, error)
	listAssignmentsForTagFn func(ctx context.Context, tagID int64) ([]domain.TagAssignment, error)
}

func (m *mockTagRepo) CreateTag(ctx context.Context, tag *domain.Tag) (*domain.Tag, error) {
	if m.createTagFn != nil {
		return m.createTagFn(ctx, tag)
	}
	panic("unexpected call to mockTagRepo.CreateTag")
}

func (m *mockTagRepo) GetTag(ctx context.Context, id int64) (*domain.Tag, error) {
	if m.getTagFn != nil {
		return m.getTagFn(ctx, id)
	}
	panic("unexpected call to mockTagRepo.GetTag")
}

func (m *mockTagRepo) ListTags(ctx context.Context, page domain.PageRequest) ([]domain.Tag, int64, error) {
	if m.listTagsFn != nil {
		return m.listTagsFn(ctx, page)
	}
	panic("unexpected call to mockTagRepo.ListTags")
}

func (m *mockTagRepo) DeleteTag(ctx context.Context, id int64) error {
	if m.deleteTagFn != nil {
		return m.deleteTagFn(ctx, id)
	}
	panic("unexpected call to mockTagRepo.DeleteTag")
}

func (m *mockTagRepo) AssignTag(ctx context.Context, assignment *domain.TagAssignment) (*domain.TagAssignment, error) {
	if m.assignTagFn != nil {
		return m.assignTagFn(ctx, assignment)
	}
	panic("unexpected call to mockTagRepo.AssignTag")
}

func (m *mockTagRepo) UnassignTag(ctx context.Context, id int64) error {
	if m.unassignTagFn != nil {
		return m.unassignTagFn(ctx, id)
	}
	panic("unexpected call to mockTagRepo.UnassignTag")
}

func (m *mockTagRepo) ListTagsForSecurable(ctx context.Context, securableType string, securableID int64, columnName *string) ([]domain.Tag, error) {
	if m.listTagsForSecurableFn != nil {
		return m.listTagsForSecurableFn(ctx, securableType, securableID, columnName)
	}
	panic("unexpected call to mockTagRepo.ListTagsForSecurable")
}

func (m *mockTagRepo) ListAssignmentsForTag(ctx context.Context, tagID int64) ([]domain.TagAssignment, error) {
	if m.listAssignmentsForTagFn != nil {
		return m.listAssignmentsForTagFn(ctx, tagID)
	}
	panic("unexpected call to mockTagRepo.ListAssignmentsForTag")
}

// === Lineage Repository Mock ===

type mockLineageRepo struct {
	insertEdgeFn    func(ctx context.Context, edge *domain.LineageEdge) error
	getUpstreamFn   func(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error)
	getDownstreamFn func(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error)
}

func (m *mockLineageRepo) InsertEdge(ctx context.Context, edge *domain.LineageEdge) error {
	if m.insertEdgeFn != nil {
		return m.insertEdgeFn(ctx, edge)
	}
	panic("unexpected call to mockLineageRepo.InsertEdge")
}

func (m *mockLineageRepo) GetUpstream(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error) {
	if m.getUpstreamFn != nil {
		return m.getUpstreamFn(ctx, tableName, page)
	}
	panic("unexpected call to mockLineageRepo.GetUpstream")
}

func (m *mockLineageRepo) GetDownstream(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error) {
	if m.getDownstreamFn != nil {
		return m.getDownstreamFn(ctx, tableName, page)
	}
	panic("unexpected call to mockLineageRepo.GetDownstream")
}

func (m *mockLineageRepo) DeleteEdge(_ context.Context, _ int64) error {
	panic("unexpected call to mockLineageRepo.DeleteEdge")
}

func (m *mockLineageRepo) PurgeOlderThan(_ context.Context, _ time.Time) (int64, error) {
	panic("unexpected call to mockLineageRepo.PurgeOlderThan")
}

// === Query History Repository Mock ===

type mockQueryHistoryRepo struct {
	listFn func(ctx context.Context, filter domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error)
}

func (m *mockQueryHistoryRepo) List(ctx context.Context, filter domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error) {
	if m.listFn != nil {
		return m.listFn(ctx, filter)
	}
	panic("unexpected call to mockQueryHistoryRepo.List")
}

// === Search Repository Mock ===

type mockSearchRepo struct {
	searchFn func(ctx context.Context, query string, objectType *string, maxResults int, offset int) ([]domain.SearchResult, int64, error)
}

func (m *mockSearchRepo) Search(ctx context.Context, query string, objectType *string, maxResults int, offset int) ([]domain.SearchResult, int64, error) {
	if m.searchFn != nil {
		return m.searchFn(ctx, query, objectType, maxResults, offset)
	}
	panic("unexpected call to mockSearchRepo.Search")
}

// === Catalog Repository Mock (subset for ViewService) ===

type mockCatalogRepo struct {
	getSchemaFn   func(ctx context.Context, name string) (*domain.SchemaDetail, error)
	listSchemasFn func(ctx context.Context, page domain.PageRequest) ([]domain.SchemaDetail, int64, error)
	listTablesFn  func(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.TableDetail, int64, error)
	listColumnsFn func(ctx context.Context, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, int64, error)
}

func (m *mockCatalogRepo) GetCatalogInfo(_ context.Context) (*domain.CatalogInfo, error) {
	panic("unexpected call to mockCatalogRepo.GetCatalogInfo")
}

func (m *mockCatalogRepo) GetMetastoreSummary(_ context.Context) (*domain.MetastoreSummary, error) {
	panic("unexpected call to mockCatalogRepo.GetMetastoreSummary")
}

func (m *mockCatalogRepo) CreateSchema(_ context.Context, _, _, _ string) (*domain.SchemaDetail, error) {
	panic("unexpected call to mockCatalogRepo.CreateSchema")
}

func (m *mockCatalogRepo) GetSchema(ctx context.Context, name string) (*domain.SchemaDetail, error) {
	if m.getSchemaFn != nil {
		return m.getSchemaFn(ctx, name)
	}
	panic("unexpected call to mockCatalogRepo.GetSchema")
}

func (m *mockCatalogRepo) ListSchemas(ctx context.Context, page domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
	if m.listSchemasFn != nil {
		return m.listSchemasFn(ctx, page)
	}
	panic("unexpected call to mockCatalogRepo.ListSchemas")
}

func (m *mockCatalogRepo) UpdateSchema(_ context.Context, _ string, _ *string, _ map[string]string) (*domain.SchemaDetail, error) {
	panic("unexpected call to mockCatalogRepo.UpdateSchema")
}

func (m *mockCatalogRepo) DeleteSchema(_ context.Context, _ string, _ bool) error {
	panic("unexpected call to mockCatalogRepo.DeleteSchema")
}

func (m *mockCatalogRepo) CreateTable(_ context.Context, _ string, _ domain.CreateTableRequest, _ string) (*domain.TableDetail, error) {
	panic("unexpected call to mockCatalogRepo.CreateTable")
}

func (m *mockCatalogRepo) GetTable(_ context.Context, _, _ string) (*domain.TableDetail, error) {
	panic("unexpected call to mockCatalogRepo.GetTable")
}

func (m *mockCatalogRepo) ListTables(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.TableDetail, int64, error) {
	if m.listTablesFn != nil {
		return m.listTablesFn(ctx, schemaName, page)
	}
	panic("unexpected call to mockCatalogRepo.ListTables")
}

func (m *mockCatalogRepo) DeleteTable(_ context.Context, _, _ string) error {
	panic("unexpected call to mockCatalogRepo.DeleteTable")
}

func (m *mockCatalogRepo) ListColumns(ctx context.Context, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, int64, error) {
	if m.listColumnsFn != nil {
		return m.listColumnsFn(ctx, schemaName, tableName, page)
	}
	panic("unexpected call to mockCatalogRepo.ListColumns")
}

func (m *mockCatalogRepo) UpdateTable(_ context.Context, _, _ string, _ *string, _ map[string]string, _ *string) (*domain.TableDetail, error) {
	panic("unexpected call to mockCatalogRepo.UpdateTable")
}

func (m *mockCatalogRepo) UpdateCatalog(_ context.Context, _ *string) (*domain.CatalogInfo, error) {
	panic("unexpected call to mockCatalogRepo.UpdateCatalog")
}

func (m *mockCatalogRepo) UpdateColumn(_ context.Context, _, _, _ string, _ *string, _ map[string]string) (*domain.ColumnDetail, error) {
	panic("unexpected call to mockCatalogRepo.UpdateColumn")
}

// === Authorization Service Mock ===

type mockAuthService struct {
	checkPrivilegeFn func(ctx context.Context, principalName, securableType string, securableID int64, privilege string) (bool, error)
}

func (m *mockAuthService) LookupTableID(_ context.Context, _ string) (int64, int64, error) {
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

func (m *mockAuditRepo) lastEntry() *domain.AuditEntry {
	if len(m.entries) == 0 {
		return nil
	}
	return m.entries[len(m.entries)-1]
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

var _ domain.ViewRepository = (*mockViewRepo)(nil)
var _ domain.TagRepository = (*mockTagRepo)(nil)
var _ domain.LineageRepository = (*mockLineageRepo)(nil)
var _ domain.QueryHistoryRepository = (*mockQueryHistoryRepo)(nil)
var _ domain.SearchRepository = (*mockSearchRepo)(nil)
var _ domain.CatalogRepository = (*mockCatalogRepo)(nil)
var _ domain.AuthorizationService = (*mockAuthService)(nil)
var _ domain.AuditRepository = (*mockAuditRepo)(nil)

// === Test Helpers ===

// errTest is a sentinel error for test scenarios.
var errTest = fmt.Errorf("test error")
