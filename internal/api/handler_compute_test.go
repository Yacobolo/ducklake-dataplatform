package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === Mock ===

type mockComputeEndpointService struct {
	listFn            func(ctx context.Context, page domain.PageRequest) ([]domain.ComputeEndpoint, int64, error)
	createFn          func(ctx context.Context, principal string, req domain.CreateComputeEndpointRequest) (*domain.ComputeEndpoint, error)
	getFn             func(ctx context.Context, name string) (*domain.ComputeEndpoint, error)
	updateFn          func(ctx context.Context, principal string, name string, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error)
	deleteFn          func(ctx context.Context, principal string, name string) error
	listAssignmentsFn func(ctx context.Context, endpointName string, page domain.PageRequest) ([]domain.ComputeAssignment, int64, error)
	assignFn          func(ctx context.Context, principal string, endpointName string, req domain.CreateComputeAssignmentRequest) (*domain.ComputeAssignment, error)
	unassignFn        func(ctx context.Context, principal string, assignmentID string) error
	healthCheckFn     func(ctx context.Context, principal string, endpointName string) (*domain.ComputeEndpointHealthResult, error)
}

func (m *mockComputeEndpointService) List(ctx context.Context, page domain.PageRequest) ([]domain.ComputeEndpoint, int64, error) {
	if m.listFn == nil {
		panic("mockComputeEndpointService.List called but not configured")
	}
	return m.listFn(ctx, page)
}

func (m *mockComputeEndpointService) Create(ctx context.Context, principal string, req domain.CreateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
	if m.createFn == nil {
		panic("mockComputeEndpointService.Create called but not configured")
	}
	return m.createFn(ctx, principal, req)
}

func (m *mockComputeEndpointService) GetByName(ctx context.Context, name string) (*domain.ComputeEndpoint, error) {
	if m.getFn == nil {
		panic("mockComputeEndpointService.GetByName called but not configured")
	}
	return m.getFn(ctx, name)
}

func (m *mockComputeEndpointService) Update(ctx context.Context, principal string, name string, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
	if m.updateFn == nil {
		panic("mockComputeEndpointService.Update called but not configured")
	}
	return m.updateFn(ctx, principal, name, req)
}

func (m *mockComputeEndpointService) Delete(ctx context.Context, principal string, name string) error {
	if m.deleteFn == nil {
		panic("mockComputeEndpointService.Delete called but not configured")
	}
	return m.deleteFn(ctx, principal, name)
}

func (m *mockComputeEndpointService) ListAssignments(ctx context.Context, endpointName string, page domain.PageRequest) ([]domain.ComputeAssignment, int64, error) {
	if m.listAssignmentsFn == nil {
		panic("mockComputeEndpointService.ListAssignments called but not configured")
	}
	return m.listAssignmentsFn(ctx, endpointName, page)
}

func (m *mockComputeEndpointService) Assign(ctx context.Context, principal string, endpointName string, req domain.CreateComputeAssignmentRequest) (*domain.ComputeAssignment, error) {
	if m.assignFn == nil {
		panic("mockComputeEndpointService.Assign called but not configured")
	}
	return m.assignFn(ctx, principal, endpointName, req)
}

func (m *mockComputeEndpointService) Unassign(ctx context.Context, principal string, assignmentID string) error {
	if m.unassignFn == nil {
		panic("mockComputeEndpointService.Unassign called but not configured")
	}
	return m.unassignFn(ctx, principal, assignmentID)
}

func (m *mockComputeEndpointService) HealthCheck(ctx context.Context, principal string, endpointName string) (*domain.ComputeEndpointHealthResult, error) {
	if m.healthCheckFn == nil {
		panic("mockComputeEndpointService.HealthCheck called but not configured")
	}
	return m.healthCheckFn(ctx, principal, endpointName)
}

// === Helpers ===

func computeTestCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		Name:    "test-user",
		IsAdmin: true,
	})
}

var computeFixedTime = time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

func computeStrPtr(s string) *string { return &s }

func sampleComputeEndpoint() domain.ComputeEndpoint {
	return domain.ComputeEndpoint{
		ID:         "ep-1",
		ExternalID: "ext-1",
		Name:       "analytics-xl",
		URL:        "https://compute-1.example.com:9443",
		Type:       "LOCAL",
		Status:     "ACTIVE",
		Owner:      "test-user",
		CreatedAt:  computeFixedTime,
		UpdatedAt:  computeFixedTime,
	}
}

func sampleComputeAssignment() domain.ComputeAssignment {
	return domain.ComputeAssignment{
		ID:            "assign-1",
		PrincipalID:   "user-1",
		PrincipalType: "user",
		EndpointID:    "ep-1",
		EndpointName:  "analytics-xl",
		IsDefault:     true,
		FallbackLocal: false,
		CreatedAt:     computeFixedTime,
	}
}

// === Tests ===

func TestHandler_ListComputeEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		params   ListComputeEndpointsParams
		svcFn    func(ctx context.Context, page domain.PageRequest) ([]domain.ComputeEndpoint, int64, error)
		assertFn func(t *testing.T, resp ListComputeEndpointsResponseObject, err error)
	}{
		{
			name:   "happy path returns 200 with results",
			params: ListComputeEndpointsParams{},
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.ComputeEndpoint, int64, error) {
				return []domain.ComputeEndpoint{sampleComputeEndpoint()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListComputeEndpointsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListComputeEndpoints200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "analytics-xl", *(*ok200.Body.Data)[0].Name)
			},
		},
		{
			name:   "empty list returns 200 with empty data",
			params: ListComputeEndpointsParams{},
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.ComputeEndpoint, int64, error) {
				return []domain.ComputeEndpoint{}, 0, nil
			},
			assertFn: func(t *testing.T, resp ListComputeEndpointsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListComputeEndpoints200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				assert.Empty(t, *ok200.Body.Data)
			},
		},
		{
			name:   "service error propagates",
			params: ListComputeEndpointsParams{},
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.ComputeEndpoint, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp ListComputeEndpointsResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockComputeEndpointService{listFn: tt.svcFn}
			handler := &APIHandler{computeEndpoints: svc}
			resp, err := handler.ListComputeEndpoints(computeTestCtx(), ListComputeEndpointsRequestObject{Params: tt.params})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_CreateComputeEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     CreateComputeEndpointJSONRequestBody
		svcFn    func(ctx context.Context, principal string, req domain.CreateComputeEndpointRequest) (*domain.ComputeEndpoint, error)
		assertFn func(t *testing.T, resp CreateComputeEndpointResponseObject, err error)
	}{
		{
			name: "happy path returns 201",
			body: CreateComputeEndpointJSONRequestBody{Name: "analytics-xl", Url: "https://compute.example.com", Type: CreateComputeEndpointRequestTypeLOCAL},
			svcFn: func(_ context.Context, _ string, _ domain.CreateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
				ep := sampleComputeEndpoint()
				return &ep, nil
			},
			assertFn: func(t *testing.T, resp CreateComputeEndpointResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				created, ok := resp.(CreateComputeEndpoint201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
				assert.Equal(t, "analytics-xl", *created.Body.Name)
				assert.Equal(t, "ep-1", *created.Body.Id)
			},
		},
		{
			name: "validation error returns 400",
			body: CreateComputeEndpointJSONRequestBody{Name: "", Url: "", Type: CreateComputeEndpointRequestTypeLOCAL},
			svcFn: func(_ context.Context, _ string, _ domain.CreateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
				return nil, domain.ErrValidation("name is required")
			},
			assertFn: func(t *testing.T, resp CreateComputeEndpointResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateComputeEndpoint400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
				assert.Contains(t, badReq.Body.Message, "name is required")
			},
		},
		{
			name: "access denied returns 403",
			body: CreateComputeEndpointJSONRequestBody{Name: "ep", Url: "http://x", Type: CreateComputeEndpointRequestTypeLOCAL},
			svcFn: func(_ context.Context, _ string, _ domain.CreateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp CreateComputeEndpointResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(CreateComputeEndpoint403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "conflict error returns 409",
			body: CreateComputeEndpointJSONRequestBody{Name: "analytics-xl", Url: "http://x", Type: CreateComputeEndpointRequestTypeLOCAL},
			svcFn: func(_ context.Context, _ string, _ domain.CreateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
				return nil, domain.ErrConflict("endpoint analytics-xl already exists")
			},
			assertFn: func(t *testing.T, resp CreateComputeEndpointResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				conflict, ok := resp.(CreateComputeEndpoint409JSONResponse)
				require.True(t, ok, "expected 409 response, got %T", resp)
				assert.Equal(t, int32(409), conflict.Body.Code)
				assert.Contains(t, conflict.Body.Message, "already exists")
			},
		},
		{
			name: "unknown error falls through to 400",
			body: CreateComputeEndpointJSONRequestBody{Name: "fail", Url: "http://x", Type: CreateComputeEndpointRequestTypeLOCAL},
			svcFn: func(_ context.Context, _ string, _ domain.CreateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
				return nil, assert.AnError
			},
			assertFn: func(t *testing.T, resp CreateComputeEndpointResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateComputeEndpoint400JSONResponse)
				require.True(t, ok, "expected 400 response for unknown error, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockComputeEndpointService{createFn: tt.svcFn}
			handler := &APIHandler{computeEndpoints: svc}
			body := tt.body
			resp, err := handler.CreateComputeEndpoint(computeTestCtx(), CreateComputeEndpointRequestObject{Body: &body})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_GetComputeEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		endpointName string
		svcFn        func(ctx context.Context, name string) (*domain.ComputeEndpoint, error)
		assertFn     func(t *testing.T, resp GetComputeEndpointResponseObject, err error)
	}{
		{
			name:         "happy path returns 200",
			endpointName: "analytics-xl",
			svcFn: func(_ context.Context, _ string) (*domain.ComputeEndpoint, error) {
				ep := sampleComputeEndpoint()
				return &ep, nil
			},
			assertFn: func(t *testing.T, resp GetComputeEndpointResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GetComputeEndpoint200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "analytics-xl", *ok200.Body.Name)
			},
		},
		{
			name:         "not found returns 404",
			endpointName: "nonexistent",
			svcFn: func(_ context.Context, name string) (*domain.ComputeEndpoint, error) {
				return nil, domain.ErrNotFound("endpoint %s not found", name)
			},
			assertFn: func(t *testing.T, resp GetComputeEndpointResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(GetComputeEndpoint404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
				assert.Contains(t, notFound.Body.Message, "nonexistent")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockComputeEndpointService{getFn: tt.svcFn}
			handler := &APIHandler{computeEndpoints: svc}
			resp, err := handler.GetComputeEndpoint(computeTestCtx(), GetComputeEndpointRequestObject{EndpointName: tt.endpointName})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_UpdateComputeEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		endpointName string
		body         UpdateComputeEndpointJSONRequestBody
		svcFn        func(ctx context.Context, principal string, name string, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error)
		assertFn     func(t *testing.T, resp UpdateComputeEndpointResponseObject, err error)
	}{
		{
			name:         "happy path returns 200",
			endpointName: "analytics-xl",
			body:         UpdateComputeEndpointJSONRequestBody{Url: computeStrPtr("https://new-url.com")},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
				ep := sampleComputeEndpoint()
				ep.URL = "https://new-url.com"
				return &ep, nil
			},
			assertFn: func(t *testing.T, resp UpdateComputeEndpointResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(UpdateComputeEndpoint200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "analytics-xl", *ok200.Body.Name)
			},
		},
		{
			name:         "access denied returns 403",
			endpointName: "analytics-xl",
			body:         UpdateComputeEndpointJSONRequestBody{Url: computeStrPtr("x")},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp UpdateComputeEndpointResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(UpdateComputeEndpoint403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name:         "not found returns 404",
			endpointName: "nonexistent",
			body:         UpdateComputeEndpointJSONRequestBody{Url: computeStrPtr("x")},
			svcFn: func(_ context.Context, _ string, name string, _ domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
				return nil, domain.ErrNotFound("endpoint %s not found", name)
			},
			assertFn: func(t *testing.T, resp UpdateComputeEndpointResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(UpdateComputeEndpoint404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockComputeEndpointService{updateFn: tt.svcFn}
			handler := &APIHandler{computeEndpoints: svc}
			body := tt.body
			resp, err := handler.UpdateComputeEndpoint(computeTestCtx(), UpdateComputeEndpointRequestObject{
				EndpointName: tt.endpointName,
				Body:         &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeleteComputeEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		endpointName string
		svcFn        func(ctx context.Context, principal string, name string) error
		assertFn     func(t *testing.T, resp DeleteComputeEndpointResponseObject, err error)
	}{
		{
			name:         "happy path returns 204",
			endpointName: "analytics-xl",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeleteComputeEndpointResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeleteComputeEndpoint204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name:         "access denied returns 403",
			endpointName: "analytics-xl",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp DeleteComputeEndpointResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(DeleteComputeEndpoint403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name:         "not found returns 404",
			endpointName: "nonexistent",
			svcFn: func(_ context.Context, _ string, name string) error {
				return domain.ErrNotFound("endpoint %s not found", name)
			},
			assertFn: func(t *testing.T, resp DeleteComputeEndpointResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(DeleteComputeEndpoint404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockComputeEndpointService{deleteFn: tt.svcFn}
			handler := &APIHandler{computeEndpoints: svc}
			resp, err := handler.DeleteComputeEndpoint(computeTestCtx(), DeleteComputeEndpointRequestObject{EndpointName: tt.endpointName})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_ListComputeAssignments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		endpointName string
		params       ListComputeAssignmentsParams
		svcFn        func(ctx context.Context, endpointName string, page domain.PageRequest) ([]domain.ComputeAssignment, int64, error)
		assertFn     func(t *testing.T, resp ListComputeAssignmentsResponseObject, err error)
	}{
		{
			name:         "happy path returns 200",
			endpointName: "analytics-xl",
			params:       ListComputeAssignmentsParams{},
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.ComputeAssignment, int64, error) {
				return []domain.ComputeAssignment{sampleComputeAssignment()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListComputeAssignmentsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListComputeAssignments200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "assign-1", *(*ok200.Body.Data)[0].Id)
			},
		},
		{
			name:         "not found returns 404",
			endpointName: "nonexistent",
			params:       ListComputeAssignmentsParams{},
			svcFn: func(_ context.Context, name string, _ domain.PageRequest) ([]domain.ComputeAssignment, int64, error) {
				return nil, 0, domain.ErrNotFound("endpoint %s not found", name)
			},
			assertFn: func(t *testing.T, resp ListComputeAssignmentsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(ListComputeAssignments404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockComputeEndpointService{listAssignmentsFn: tt.svcFn}
			handler := &APIHandler{computeEndpoints: svc}
			resp, err := handler.ListComputeAssignments(computeTestCtx(), ListComputeAssignmentsRequestObject{
				EndpointName: tt.endpointName,
				Params:       tt.params,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_CreateComputeAssignment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		endpointName string
		body         CreateComputeAssignmentJSONRequestBody
		svcFn        func(ctx context.Context, principal string, endpointName string, req domain.CreateComputeAssignmentRequest) (*domain.ComputeAssignment, error)
		assertFn     func(t *testing.T, resp CreateComputeAssignmentResponseObject, err error)
	}{
		{
			name:         "happy path returns 201",
			endpointName: "analytics-xl",
			body:         CreateComputeAssignmentJSONRequestBody{PrincipalId: "user-1", PrincipalType: CreateComputeAssignmentRequestPrincipalTypeUser},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.CreateComputeAssignmentRequest) (*domain.ComputeAssignment, error) {
				a := sampleComputeAssignment()
				return &a, nil
			},
			assertFn: func(t *testing.T, resp CreateComputeAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				created, ok := resp.(CreateComputeAssignment201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
				assert.Equal(t, "assign-1", *created.Body.Id)
			},
		},
		{
			name:         "validation error returns 400",
			endpointName: "analytics-xl",
			body:         CreateComputeAssignmentJSONRequestBody{PrincipalId: "", PrincipalType: CreateComputeAssignmentRequestPrincipalTypeUser},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.CreateComputeAssignmentRequest) (*domain.ComputeAssignment, error) {
				return nil, domain.ErrValidation("principal_id is required")
			},
			assertFn: func(t *testing.T, resp CreateComputeAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateComputeAssignment400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
		{
			name:         "access denied returns 403",
			endpointName: "analytics-xl",
			body:         CreateComputeAssignmentJSONRequestBody{PrincipalId: "u-1", PrincipalType: CreateComputeAssignmentRequestPrincipalTypeUser},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.CreateComputeAssignmentRequest) (*domain.ComputeAssignment, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp CreateComputeAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(CreateComputeAssignment403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name:         "conflict error returns 409",
			endpointName: "analytics-xl",
			body:         CreateComputeAssignmentJSONRequestBody{PrincipalId: "user-1", PrincipalType: CreateComputeAssignmentRequestPrincipalTypeUser},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.CreateComputeAssignmentRequest) (*domain.ComputeAssignment, error) {
				return nil, domain.ErrConflict("assignment already exists")
			},
			assertFn: func(t *testing.T, resp CreateComputeAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				conflict, ok := resp.(CreateComputeAssignment409JSONResponse)
				require.True(t, ok, "expected 409 response, got %T", resp)
				assert.Equal(t, int32(409), conflict.Body.Code)
			},
		},
		{
			name:         "unknown error falls through to 400",
			endpointName: "analytics-xl",
			body:         CreateComputeAssignmentJSONRequestBody{PrincipalId: "u-1", PrincipalType: CreateComputeAssignmentRequestPrincipalTypeUser},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.CreateComputeAssignmentRequest) (*domain.ComputeAssignment, error) {
				return nil, assert.AnError
			},
			assertFn: func(t *testing.T, resp CreateComputeAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateComputeAssignment400JSONResponse)
				require.True(t, ok, "expected 400 response for unknown error, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockComputeEndpointService{assignFn: tt.svcFn}
			handler := &APIHandler{computeEndpoints: svc}
			body := tt.body
			resp, err := handler.CreateComputeAssignment(computeTestCtx(), CreateComputeAssignmentRequestObject{
				EndpointName: tt.endpointName,
				Body:         &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeleteComputeAssignment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		assignmentID string
		svcFn        func(ctx context.Context, principal string, assignmentID string) error
		assertFn     func(t *testing.T, resp DeleteComputeAssignmentResponseObject, err error)
	}{
		{
			name:         "happy path returns 204",
			assignmentID: "assign-1",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeleteComputeAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeleteComputeAssignment204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name:         "access denied returns 403",
			assignmentID: "assign-1",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp DeleteComputeAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(DeleteComputeAssignment403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name:         "not found returns 404",
			assignmentID: "nonexistent",
			svcFn: func(_ context.Context, _ string, id string) error {
				return domain.ErrNotFound("assignment %s not found", id)
			},
			assertFn: func(t *testing.T, resp DeleteComputeAssignmentResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(DeleteComputeAssignment404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockComputeEndpointService{unassignFn: tt.svcFn}
			handler := &APIHandler{computeEndpoints: svc}
			resp, err := handler.DeleteComputeAssignment(computeTestCtx(), DeleteComputeAssignmentRequestObject{AssignmentId: tt.assignmentID})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_GetComputeEndpointHealth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		endpointName string
		svcFn        func(ctx context.Context, principal string, endpointName string) (*domain.ComputeEndpointHealthResult, error)
		assertFn     func(t *testing.T, resp GetComputeEndpointHealthResponseObject, err error)
	}{
		{
			name:         "happy path returns 200",
			endpointName: "analytics-xl",
			svcFn: func(_ context.Context, _ string, _ string) (*domain.ComputeEndpointHealthResult, error) {
				status := "healthy"
				uptime := 3600
				version := "0.10.0"
				return &domain.ComputeEndpointHealthResult{
					Status:        &status,
					UptimeSeconds: &uptime,
					DuckdbVersion: &version,
				}, nil
			},
			assertFn: func(t *testing.T, resp GetComputeEndpointHealthResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GetComputeEndpointHealth200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "healthy", *ok200.Body.Status)
				assert.Equal(t, int32(3600), *ok200.Body.UptimeSeconds)
			},
		},
		{
			name:         "access denied returns 403",
			endpointName: "analytics-xl",
			svcFn: func(_ context.Context, _ string, _ string) (*domain.ComputeEndpointHealthResult, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp GetComputeEndpointHealthResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(GetComputeEndpointHealth403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name:         "not found returns 404",
			endpointName: "nonexistent",
			svcFn: func(_ context.Context, _ string, name string) (*domain.ComputeEndpointHealthResult, error) {
				return nil, domain.ErrNotFound("endpoint %s not found", name)
			},
			assertFn: func(t *testing.T, resp GetComputeEndpointHealthResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(GetComputeEndpointHealth404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
		{
			name:         "unknown error returns 502",
			endpointName: "analytics-xl",
			svcFn: func(_ context.Context, _ string, _ string) (*domain.ComputeEndpointHealthResult, error) {
				return nil, assert.AnError
			},
			assertFn: func(t *testing.T, resp GetComputeEndpointHealthResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badGateway, ok := resp.(GetComputeEndpointHealth502JSONResponse)
				require.True(t, ok, "expected 502 response, got %T", resp)
				assert.Equal(t, int32(502), badGateway.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockComputeEndpointService{healthCheckFn: tt.svcFn}
			handler := &APIHandler{computeEndpoints: svc}
			resp, err := handler.GetComputeEndpointHealth(computeTestCtx(), GetComputeEndpointHealthRequestObject{EndpointName: tt.endpointName})
			tt.assertFn(t, resp, err)
		})
	}
}
