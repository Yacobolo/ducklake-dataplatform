package governance

import (
	"context"
	"fmt"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"
)

// errTest is a sentinel error for test scenarios.
var errTest = fmt.Errorf("test error")

func ctxWithPrincipal(name string) context.Context {
	return middleware.WithPrincipal(context.Background(), name)
}

func strPtr(s string) *string { return &s }

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
	insertEdgeFn     func(ctx context.Context, edge *domain.LineageEdge) error
	getUpstreamFn    func(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error)
	getDownstreamFn  func(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error)
	deleteEdgeFn     func(ctx context.Context, id int64) error
	purgeOlderThanFn func(ctx context.Context, before time.Time) (int64, error)
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

func (m *mockLineageRepo) DeleteEdge(ctx context.Context, id int64) error {
	if m.deleteEdgeFn != nil {
		return m.deleteEdgeFn(ctx, id)
	}
	panic("unexpected call to mockLineageRepo.DeleteEdge")
}

func (m *mockLineageRepo) PurgeOlderThan(ctx context.Context, before time.Time) (int64, error) {
	if m.purgeOlderThanFn != nil {
		return m.purgeOlderThanFn(ctx, before)
	}
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

var _ domain.TagRepository = (*mockTagRepo)(nil)
var _ domain.LineageRepository = (*mockLineageRepo)(nil)
var _ domain.QueryHistoryRepository = (*mockQueryHistoryRepo)(nil)
var _ domain.AuditRepository = (*mockAuditRepo)(nil)
