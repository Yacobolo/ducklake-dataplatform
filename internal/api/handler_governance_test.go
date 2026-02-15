package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === Mocks ===

type mockAuditService struct {
	listFn func(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditEntry, int64, error)
}

func (m *mockAuditService) List(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	if m.listFn == nil {
		panic("mockAuditService.List called but not configured")
	}
	return m.listFn(ctx, filter)
}

type mockQueryHistoryService struct {
	listFn func(ctx context.Context, filter domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error)
}

func (m *mockQueryHistoryService) List(ctx context.Context, filter domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error) {
	if m.listFn == nil {
		panic("mockQueryHistoryService.List called but not configured")
	}
	return m.listFn(ctx, filter)
}

type mockSearchService struct {
	searchFn func(ctx context.Context, query string, objectType *string, catalogName *string, page domain.PageRequest) ([]domain.SearchResult, int64, error)
}

func (m *mockSearchService) Search(ctx context.Context, query string, objectType *string, catalogName *string, page domain.PageRequest) ([]domain.SearchResult, int64, error) {
	if m.searchFn == nil {
		panic("mockSearchService.Search called but not configured")
	}
	return m.searchFn(ctx, query, objectType, catalogName, page)
}

type mockLineageService struct {
	getFullLineageFn func(ctx context.Context, tableName string, page domain.PageRequest) (*domain.LineageNode, error)
	getUpstreamFn    func(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error)
	getDownstreamFn  func(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error)
	deleteEdgeFn     func(ctx context.Context, id string) error
	purgeOlderThanFn func(ctx context.Context, olderThanDays int) (int64, error)
}

func (m *mockLineageService) GetFullLineage(ctx context.Context, tableName string, page domain.PageRequest) (*domain.LineageNode, error) {
	if m.getFullLineageFn == nil {
		panic("mockLineageService.GetFullLineage called but not configured")
	}
	return m.getFullLineageFn(ctx, tableName, page)
}

func (m *mockLineageService) GetUpstream(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error) {
	if m.getUpstreamFn == nil {
		panic("mockLineageService.GetUpstream called but not configured")
	}
	return m.getUpstreamFn(ctx, tableName, page)
}

func (m *mockLineageService) GetDownstream(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error) {
	if m.getDownstreamFn == nil {
		panic("mockLineageService.GetDownstream called but not configured")
	}
	return m.getDownstreamFn(ctx, tableName, page)
}

func (m *mockLineageService) DeleteEdge(ctx context.Context, id string) error {
	if m.deleteEdgeFn == nil {
		panic("mockLineageService.DeleteEdge called but not configured")
	}
	return m.deleteEdgeFn(ctx, id)
}

func (m *mockLineageService) PurgeOlderThan(ctx context.Context, olderThanDays int) (int64, error) {
	if m.purgeOlderThanFn == nil {
		panic("mockLineageService.PurgeOlderThan called but not configured")
	}
	return m.purgeOlderThanFn(ctx, olderThanDays)
}

type mockTagService struct {
	listTagsFn    func(ctx context.Context, page domain.PageRequest) ([]domain.Tag, int64, error)
	createTagFn   func(ctx context.Context, principal string, req domain.CreateTagRequest) (*domain.Tag, error)
	deleteTagFn   func(ctx context.Context, principal string, id string) error
	assignTagFn   func(ctx context.Context, principal string, req domain.AssignTagRequest) (*domain.TagAssignment, error)
	unassignTagFn func(ctx context.Context, principal string, id string) error
}

func (m *mockTagService) ListTags(ctx context.Context, page domain.PageRequest) ([]domain.Tag, int64, error) {
	if m.listTagsFn == nil {
		panic("mockTagService.ListTags called but not configured")
	}
	return m.listTagsFn(ctx, page)
}

func (m *mockTagService) CreateTag(ctx context.Context, principal string, req domain.CreateTagRequest) (*domain.Tag, error) {
	if m.createTagFn == nil {
		panic("mockTagService.CreateTag called but not configured")
	}
	return m.createTagFn(ctx, principal, req)
}

func (m *mockTagService) DeleteTag(ctx context.Context, principal string, id string) error {
	if m.deleteTagFn == nil {
		panic("mockTagService.DeleteTag called but not configured")
	}
	return m.deleteTagFn(ctx, principal, id)
}

func (m *mockTagService) AssignTag(ctx context.Context, principal string, req domain.AssignTagRequest) (*domain.TagAssignment, error) {
	if m.assignTagFn == nil {
		panic("mockTagService.AssignTag called but not configured")
	}
	return m.assignTagFn(ctx, principal, req)
}

func (m *mockTagService) UnassignTag(ctx context.Context, principal string, id string) error {
	if m.unassignTagFn == nil {
		panic("mockTagService.UnassignTag called but not configured")
	}
	return m.unassignTagFn(ctx, principal, id)
}

// === Helpers ===

func govTestCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		Name:    "test-user",
		IsAdmin: true,
	})
}

func govNonAdminCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		Name:    "test-user",
		IsAdmin: false,
	})
}

var govFixedTime = time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

func govStrPtr(s string) *string { return &s }

func sampleAuditEntry() domain.AuditEntry {
	return domain.AuditEntry{
		ID:             "audit-1",
		PrincipalName:  "test-user",
		Action:         "QUERY",
		TablesAccessed: []string{"my_table"},
		Status:         "ALLOWED",
		CreatedAt:      govFixedTime,
	}
}

func sampleQueryHistoryEntry() domain.QueryHistoryEntry {
	return domain.QueryHistoryEntry{
		ID:             "qh-1",
		PrincipalName:  "test-user",
		TablesAccessed: []string{"my_table"},
		Status:         "SUCCESS",
		CreatedAt:      govFixedTime,
	}
}

func sampleSearchResult() domain.SearchResult {
	return domain.SearchResult{
		Type:       "table",
		Name:       "my_table",
		MatchField: "name",
	}
}

func sampleLineageEdge() domain.LineageEdge {
	return domain.LineageEdge{
		ID:            "edge-1",
		SourceTable:   "source_table",
		TargetTable:   govStrPtr("target_table"),
		EdgeType:      "READ",
		PrincipalName: "test-user",
		CreatedAt:     govFixedTime,
	}
}

func sampleTag() domain.Tag {
	return domain.Tag{
		ID:        "tag-1",
		Key:       "classification",
		Value:     govStrPtr("pii"),
		CreatedBy: "test-user",
		CreatedAt: govFixedTime,
	}
}

func sampleTagAssignment() domain.TagAssignment {
	return domain.TagAssignment{
		ID:            "ta-1",
		TagID:         "tag-1",
		SecurableType: "table",
		SecurableID:   "table-1",
		AssignedBy:    "test-user",
		AssignedAt:    govFixedTime,
	}
}

// === Tests ===

func TestHandler_ListAuditLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		params   ListAuditLogsParams
		svcFn    func(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditEntry, int64, error)
		assertFn func(t *testing.T, resp ListAuditLogsResponseObject, err error)
	}{
		{
			name:   "happy path returns 200 with results",
			params: ListAuditLogsParams{},
			svcFn: func(_ context.Context, _ domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
				return []domain.AuditEntry{sampleAuditEntry()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListAuditLogsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListAuditLogs200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "audit-1", *(*ok200.Body.Data)[0].Id)
			},
		},
		{
			name:   "service error propagates",
			params: ListAuditLogsParams{},
			svcFn: func(_ context.Context, _ domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp ListAuditLogsResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockAuditService{listFn: tt.svcFn}
			handler := &APIHandler{audit: svc}
			resp, err := handler.ListAuditLogs(govTestCtx(), ListAuditLogsRequestObject{Params: tt.params})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_ListQueryHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		params   ListQueryHistoryParams
		svcFn    func(ctx context.Context, filter domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error)
		assertFn func(t *testing.T, resp ListQueryHistoryResponseObject, err error)
	}{
		{
			name:   "happy path returns 200 with results",
			params: ListQueryHistoryParams{},
			svcFn: func(_ context.Context, _ domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error) {
				return []domain.QueryHistoryEntry{sampleQueryHistoryEntry()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListQueryHistoryResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListQueryHistory200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "qh-1", *(*ok200.Body.Data)[0].Id)
			},
		},
		{
			name:   "service error propagates",
			params: ListQueryHistoryParams{},
			svcFn: func(_ context.Context, _ domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp ListQueryHistoryResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockQueryHistoryService{listFn: tt.svcFn}
			handler := &APIHandler{queryHistory: svc}
			resp, err := handler.ListQueryHistory(govTestCtx(), ListQueryHistoryRequestObject{Params: tt.params})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_SearchCatalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		params   SearchCatalogParams
		svcFn    func(ctx context.Context, query string, objectType *string, catalogName *string, page domain.PageRequest) ([]domain.SearchResult, int64, error)
		assertFn func(t *testing.T, resp SearchCatalogResponseObject, err error)
	}{
		{
			name:   "happy path returns 200 with results",
			params: SearchCatalogParams{Query: "my_table"},
			svcFn: func(_ context.Context, _ string, _ *string, _ *string, _ domain.PageRequest) ([]domain.SearchResult, int64, error) {
				return []domain.SearchResult{sampleSearchResult()}, 1, nil
			},
			assertFn: func(t *testing.T, resp SearchCatalogResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(SearchCatalog200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "my_table", *(*ok200.Body.Data)[0].Name)
			},
		},
		{
			name:   "service error returns 500",
			params: SearchCatalogParams{Query: "fail"},
			svcFn: func(_ context.Context, _ string, _ *string, _ *string, _ domain.PageRequest) ([]domain.SearchResult, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp SearchCatalogResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				serverErr, ok := resp.(SearchCatalog500JSONResponse)
				require.True(t, ok, "expected 500 response, got %T", resp)
				assert.Equal(t, int32(500), serverErr.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockSearchService{searchFn: tt.svcFn}
			handler := &APIHandler{search: svc}
			resp, err := handler.SearchCatalog(govTestCtx(), SearchCatalogRequestObject{Params: tt.params})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_GetTableLineage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, tableName string, page domain.PageRequest) (*domain.LineageNode, error)
		assertFn func(t *testing.T, resp GetTableLineageResponseObject, err error)
	}{
		{
			name: "happy path returns 200",
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) (*domain.LineageNode, error) {
				return &domain.LineageNode{
					TableName:  "test-schema.my_table",
					Upstream:   []domain.LineageEdge{sampleLineageEdge()},
					Downstream: []domain.LineageEdge{},
				}, nil
			},
			assertFn: func(t *testing.T, resp GetTableLineageResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GetTableLineage200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "test-schema.my_table", *ok200.Body.TableName)
				require.NotNil(t, ok200.Body.Upstream)
				require.Len(t, *ok200.Body.Upstream, 1)
			},
		},
		{
			name: "service error propagates",
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) (*domain.LineageNode, error) {
				return nil, assert.AnError
			},
			assertFn: func(t *testing.T, resp GetTableLineageResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockLineageService{getFullLineageFn: tt.svcFn}
			handler := &APIHandler{lineage: svc}
			resp, err := handler.GetTableLineage(govTestCtx(), GetTableLineageRequestObject{
				SchemaName: "test-schema",
				TableName:  "my_table",
				Params:     GetTableLineageParams{},
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_GetUpstreamLineage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error)
		assertFn func(t *testing.T, resp GetUpstreamLineageResponseObject, err error)
	}{
		{
			name: "happy path returns 200",
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return []domain.LineageEdge{sampleLineageEdge()}, 1, nil
			},
			assertFn: func(t *testing.T, resp GetUpstreamLineageResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GetUpstreamLineage200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "edge-1", *(*ok200.Body.Data)[0].Id)
			},
		},
		{
			name: "service error propagates",
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp GetUpstreamLineageResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockLineageService{getUpstreamFn: tt.svcFn}
			handler := &APIHandler{lineage: svc}
			resp, err := handler.GetUpstreamLineage(govTestCtx(), GetUpstreamLineageRequestObject{
				SchemaName: "test-schema",
				TableName:  "my_table",
				Params:     GetUpstreamLineageParams{},
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_GetDownstreamLineage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error)
		assertFn func(t *testing.T, resp GetDownstreamLineageResponseObject, err error)
	}{
		{
			name: "happy path returns 200",
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return []domain.LineageEdge{sampleLineageEdge()}, 1, nil
			},
			assertFn: func(t *testing.T, resp GetDownstreamLineageResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GetDownstreamLineage200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
			},
		},
		{
			name: "service error propagates",
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp GetDownstreamLineageResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockLineageService{getDownstreamFn: tt.svcFn}
			handler := &APIHandler{lineage: svc}
			resp, err := handler.GetDownstreamLineage(govTestCtx(), GetDownstreamLineageRequestObject{
				SchemaName: "test-schema",
				TableName:  "my_table",
				Params:     GetDownstreamLineageParams{},
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeleteLineageEdge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		edgeID   string
		svcFn    func(ctx context.Context, id string) error
		assertFn func(t *testing.T, resp DeleteLineageEdgeResponseObject, err error)
	}{
		{
			name:   "happy path returns 204",
			edgeID: "edge-1",
			svcFn: func(_ context.Context, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeleteLineageEdgeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeleteLineageEdge204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name:   "not found returns 404",
			edgeID: "nonexistent",
			svcFn: func(_ context.Context, id string) error {
				return domain.ErrNotFound("edge %s not found", id)
			},
			assertFn: func(t *testing.T, resp DeleteLineageEdgeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(DeleteLineageEdge404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockLineageService{deleteEdgeFn: tt.svcFn}
			handler := &APIHandler{lineage: svc}
			resp, err := handler.DeleteLineageEdge(govTestCtx(), DeleteLineageEdgeRequestObject{EdgeId: tt.edgeID})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_PurgeLineage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ctx      context.Context
		body     PurgeLineageJSONRequestBody
		svcFn    func(ctx context.Context, olderThanDays int) (int64, error)
		assertFn func(t *testing.T, resp PurgeLineageResponseObject, err error)
	}{
		{
			name: "happy path returns 200",
			ctx:  govTestCtx(),
			body: PurgeLineageJSONRequestBody{OlderThanDays: 30},
			svcFn: func(_ context.Context, _ int) (int64, error) {
				return 42, nil
			},
			assertFn: func(t *testing.T, resp PurgeLineageResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(PurgeLineage200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, int64(42), *ok200.Body.DeletedCount)
			},
		},
		{
			name: "non-admin returns 403",
			ctx:  govNonAdminCtx(),
			body: PurgeLineageJSONRequestBody{OlderThanDays: 30},
			svcFn: func(_ context.Context, _ int) (int64, error) {
				panic("should not be called for non-admin")
			},
			assertFn: func(t *testing.T, resp PurgeLineageResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(PurgeLineage403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
				assert.Contains(t, forbidden.Body.Message, "admin")
			},
		},
		{
			name: "service error returns 500",
			ctx:  govTestCtx(),
			body: PurgeLineageJSONRequestBody{OlderThanDays: 30},
			svcFn: func(_ context.Context, _ int) (int64, error) {
				return 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp PurgeLineageResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				serverErr, ok := resp.(PurgeLineage500JSONResponse)
				require.True(t, ok, "expected 500 response, got %T", resp)
				assert.Equal(t, int32(500), serverErr.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockLineageService{purgeOlderThanFn: tt.svcFn}
			handler := &APIHandler{lineage: svc}
			body := tt.body
			resp, err := handler.PurgeLineage(tt.ctx, PurgeLineageRequestObject{Body: &body})
			tt.assertFn(t, resp, err)
		})
	}
}

// === Tag Tests ===

func TestHandler_ListTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		params   ListTagsParams
		svcFn    func(ctx context.Context, page domain.PageRequest) ([]domain.Tag, int64, error)
		assertFn func(t *testing.T, resp ListTagsResponseObject, err error)
	}{
		{
			name:   "happy path returns 200 with results",
			params: ListTagsParams{},
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.Tag, int64, error) {
				return []domain.Tag{sampleTag()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListTagsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListTags200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "tag-1", *(*ok200.Body.Data)[0].Id)
			},
		},
		{
			name:   "service error propagates",
			params: ListTagsParams{},
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.Tag, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp ListTagsResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockTagService{listTagsFn: tt.svcFn}
			handler := &APIHandler{tags: svc}
			resp, err := handler.ListTags(govTestCtx(), ListTagsRequestObject{Params: tt.params})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_CreateTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ctx      context.Context
		body     CreateTagJSONRequestBody
		svcFn    func(ctx context.Context, principal string, req domain.CreateTagRequest) (*domain.Tag, error)
		assertFn func(t *testing.T, resp CreateTagResponseObject, err error)
	}{
		{
			name: "happy path returns 201",
			ctx:  govTestCtx(),
			body: CreateTagJSONRequestBody{Key: "classification", Value: govStrPtr("pii")},
			svcFn: func(_ context.Context, _ string, _ domain.CreateTagRequest) (*domain.Tag, error) {
				tg := sampleTag()
				return &tg, nil
			},
			assertFn: func(t *testing.T, resp CreateTagResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				created, ok := resp.(CreateTag201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
				assert.Equal(t, "tag-1", *created.Body.Id)
				assert.Equal(t, "classification", *created.Body.Key)
			},
		},
		{
			name: "non-admin returns 403",
			ctx:  govNonAdminCtx(),
			body: CreateTagJSONRequestBody{Key: "classification", Value: govStrPtr("pii")},
			svcFn: func(_ context.Context, _ string, _ domain.CreateTagRequest) (*domain.Tag, error) {
				panic("should not be called for non-admin")
			},
			assertFn: func(t *testing.T, resp CreateTagResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(CreateTag403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
				assert.Contains(t, forbidden.Body.Message, "admin")
			},
		},
		{
			name: "validation error returns 400",
			ctx:  govTestCtx(),
			body: CreateTagJSONRequestBody{Key: ""},
			svcFn: func(_ context.Context, _ string, _ domain.CreateTagRequest) (*domain.Tag, error) {
				return nil, domain.ErrValidation("tag key is required")
			},
			assertFn: func(t *testing.T, resp CreateTagResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateTag400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
		{
			name: "conflict error returns 409",
			ctx:  govTestCtx(),
			body: CreateTagJSONRequestBody{Key: "classification", Value: govStrPtr("pii")},
			svcFn: func(_ context.Context, _ string, _ domain.CreateTagRequest) (*domain.Tag, error) {
				return nil, domain.ErrConflict("tag already exists")
			},
			assertFn: func(t *testing.T, resp CreateTagResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				conflict, ok := resp.(CreateTag409JSONResponse)
				require.True(t, ok, "expected 409 response, got %T", resp)
				assert.Equal(t, int32(409), conflict.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockTagService{createTagFn: tt.svcFn}
			handler := &APIHandler{tags: svc}
			body := tt.body
			resp, err := handler.CreateTag(tt.ctx, CreateTagRequestObject{Body: &body})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeleteTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ctx      context.Context
		tagID    string
		svcFn    func(ctx context.Context, principal string, id string) error
		assertFn func(t *testing.T, resp DeleteTagResponseObject, err error)
	}{
		{
			name:  "happy path returns 204",
			ctx:   govTestCtx(),
			tagID: "tag-1",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeleteTagResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeleteTag204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name:  "non-admin returns 403",
			ctx:   govNonAdminCtx(),
			tagID: "tag-1",
			svcFn: func(_ context.Context, _ string, _ string) error {
				panic("should not be called for non-admin")
			},
			assertFn: func(t *testing.T, resp DeleteTagResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(DeleteTag403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
				assert.Contains(t, forbidden.Body.Message, "admin")
			},
		},
		{
			name:  "not found returns 404",
			ctx:   govTestCtx(),
			tagID: "nonexistent",
			svcFn: func(_ context.Context, _ string, id string) error {
				return domain.ErrNotFound("tag %s not found", id)
			},
			assertFn: func(t *testing.T, resp DeleteTagResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(DeleteTag404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockTagService{deleteTagFn: tt.svcFn}
			handler := &APIHandler{tags: svc}
			resp, err := handler.DeleteTag(tt.ctx, DeleteTagRequestObject{TagId: tt.tagID})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_CreateTagAssignment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tagID    string
		body     CreateTagAssignmentJSONRequestBody
		svcFn    func(ctx context.Context, principal string, req domain.AssignTagRequest) (*domain.TagAssignment, error)
		assertFn func(t *testing.T, resp CreateTagAssignmentResponseObject, err error)
	}{
		{
			name:  "happy path returns 201",
			tagID: "tag-1",
			body:  CreateTagAssignmentJSONRequestBody{SecurableType: "table", SecurableId: "table-1"},
			svcFn: func(_ context.Context, _ string, _ domain.AssignTagRequest) (*domain.TagAssignment, error) {
				a := sampleTagAssignment()
				return &a, nil
			},
			assertFn: func(t *testing.T, resp CreateTagAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				created, ok := resp.(CreateTagAssignment201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
				assert.Equal(t, "ta-1", *created.Body.Id)
			},
		},
		{
			name:  "conflict error returns 409",
			tagID: "tag-1",
			body:  CreateTagAssignmentJSONRequestBody{SecurableType: "table", SecurableId: "table-1"},
			svcFn: func(_ context.Context, _ string, _ domain.AssignTagRequest) (*domain.TagAssignment, error) {
				return nil, domain.ErrConflict("assignment already exists")
			},
			assertFn: func(t *testing.T, resp CreateTagAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				conflict, ok := resp.(CreateTagAssignment409JSONResponse)
				require.True(t, ok, "expected 409 response, got %T", resp)
				assert.Equal(t, int32(409), conflict.Body.Code)
			},
		},
		{
			name:  "unknown error propagates",
			tagID: "tag-1",
			body:  CreateTagAssignmentJSONRequestBody{SecurableType: "table", SecurableId: "table-1"},
			svcFn: func(_ context.Context, _ string, _ domain.AssignTagRequest) (*domain.TagAssignment, error) {
				return nil, assert.AnError
			},
			assertFn: func(t *testing.T, resp CreateTagAssignmentResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockTagService{assignTagFn: tt.svcFn}
			handler := &APIHandler{tags: svc}
			body := tt.body
			resp, err := handler.CreateTagAssignment(govTestCtx(), CreateTagAssignmentRequestObject{
				TagId: tt.tagID,
				Body:  &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeleteTagAssignment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		assignmentID string
		svcFn        func(ctx context.Context, principal string, id string) error
		assertFn     func(t *testing.T, resp DeleteTagAssignmentResponseObject, err error)
	}{
		{
			name:         "happy path returns 204",
			assignmentID: "ta-1",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeleteTagAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeleteTagAssignment204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name:         "not found returns 404",
			assignmentID: "nonexistent",
			svcFn: func(_ context.Context, _ string, id string) error {
				return domain.ErrNotFound("assignment %s not found", id)
			},
			assertFn: func(t *testing.T, resp DeleteTagAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(DeleteTagAssignment404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
		{
			name:         "access denied returns 403",
			assignmentID: "ta-1",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp DeleteTagAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(DeleteTagAssignment403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name:         "validation error returns 400",
			assignmentID: "ta-1",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return domain.ErrValidation("invalid assignment")
			},
			assertFn: func(t *testing.T, resp DeleteTagAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(DeleteTagAssignment400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
		{
			name:         "unknown error returns 500",
			assignmentID: "ta-1",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return assert.AnError
			},
			assertFn: func(t *testing.T, resp DeleteTagAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				serverErr, ok := resp.(DeleteTagAssignment500JSONResponse)
				require.True(t, ok, "expected 500 response, got %T", resp)
				assert.Equal(t, int32(500), serverErr.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockTagService{unassignTagFn: tt.svcFn}
			handler := &APIHandler{tags: svc}
			resp, err := handler.DeleteTagAssignment(govTestCtx(), DeleteTagAssignmentRequestObject{AssignmentId: tt.assignmentID})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_ListClassifications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, page domain.PageRequest) ([]domain.Tag, int64, error)
		assertFn func(t *testing.T, resp ListClassificationsResponseObject, err error)
	}{
		{
			name: "happy path returns only classification and sensitivity tags",
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.Tag, int64, error) {
				return []domain.Tag{
					{ID: "t-1", Key: domain.ClassificationPrefix, Value: govStrPtr("pii"), CreatedBy: "admin", CreatedAt: govFixedTime},
					{ID: "t-2", Key: domain.SensitivityPrefix, Value: govStrPtr("high"), CreatedBy: "admin", CreatedAt: govFixedTime},
					{ID: "t-3", Key: "custom", Value: govStrPtr("value"), CreatedBy: "admin", CreatedAt: govFixedTime},
				}, 3, nil
			},
			assertFn: func(t *testing.T, resp ListClassificationsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListClassifications200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				assert.Len(t, *ok200.Body.Data, 2, "should filter to classification and sensitivity only")
			},
		},
		{
			name: "no matching tags returns empty list",
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.Tag, int64, error) {
				return []domain.Tag{
					{ID: "t-1", Key: "custom", Value: govStrPtr("val"), CreatedBy: "admin", CreatedAt: govFixedTime},
				}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListClassificationsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListClassifications200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				assert.Empty(t, *ok200.Body.Data)
			},
		},
		{
			name: "service error propagates",
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.Tag, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp ListClassificationsResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockTagService{listTagsFn: tt.svcFn}
			handler := &APIHandler{tags: svc}
			resp, err := handler.ListClassifications(govTestCtx(), ListClassificationsRequestObject{})
			tt.assertFn(t, resp, err)
		})
	}
}
