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

type mockPrincipalService struct {
	listFn     func(ctx context.Context, page domain.PageRequest) ([]domain.Principal, int64, error)
	createFn   func(ctx context.Context, req domain.CreatePrincipalRequest) (*domain.Principal, error)
	getByIDFn  func(ctx context.Context, id string) (*domain.Principal, error)
	deleteFn   func(ctx context.Context, id string) error
	setAdminFn func(ctx context.Context, id string, isAdmin bool) error
}

func (m *mockPrincipalService) List(ctx context.Context, page domain.PageRequest) ([]domain.Principal, int64, error) {
	if m.listFn == nil {
		panic("mockPrincipalService.List called but not configured")
	}
	return m.listFn(ctx, page)
}

func (m *mockPrincipalService) Create(ctx context.Context, req domain.CreatePrincipalRequest) (*domain.Principal, error) {
	if m.createFn == nil {
		panic("mockPrincipalService.Create called but not configured")
	}
	return m.createFn(ctx, req)
}

func (m *mockPrincipalService) GetByID(ctx context.Context, id string) (*domain.Principal, error) {
	if m.getByIDFn == nil {
		panic("mockPrincipalService.GetByID called but not configured")
	}
	return m.getByIDFn(ctx, id)
}

func (m *mockPrincipalService) Delete(ctx context.Context, id string) error {
	if m.deleteFn == nil {
		panic("mockPrincipalService.Delete called but not configured")
	}
	return m.deleteFn(ctx, id)
}

func (m *mockPrincipalService) SetAdmin(ctx context.Context, id string, isAdmin bool) error {
	if m.setAdminFn == nil {
		panic("mockPrincipalService.SetAdmin called but not configured")
	}
	return m.setAdminFn(ctx, id, isAdmin)
}

type mockGroupService struct {
	listFn         func(ctx context.Context, page domain.PageRequest) ([]domain.Group, int64, error)
	createFn       func(ctx context.Context, req domain.CreateGroupRequest) (*domain.Group, error)
	getByIDFn      func(ctx context.Context, id string) (*domain.Group, error)
	deleteFn       func(ctx context.Context, id string) error
	listMembersFn  func(ctx context.Context, groupID string, page domain.PageRequest) ([]domain.GroupMember, int64, error)
	addMemberFn    func(ctx context.Context, req domain.AddGroupMemberRequest) error
	removeMemberFn func(ctx context.Context, req domain.RemoveGroupMemberRequest) error
}

func (m *mockGroupService) List(ctx context.Context, page domain.PageRequest) ([]domain.Group, int64, error) {
	if m.listFn == nil {
		panic("mockGroupService.List called but not configured")
	}
	return m.listFn(ctx, page)
}

func (m *mockGroupService) Create(ctx context.Context, req domain.CreateGroupRequest) (*domain.Group, error) {
	if m.createFn == nil {
		panic("mockGroupService.Create called but not configured")
	}
	return m.createFn(ctx, req)
}

func (m *mockGroupService) GetByID(ctx context.Context, id string) (*domain.Group, error) {
	if m.getByIDFn == nil {
		panic("mockGroupService.GetByID called but not configured")
	}
	return m.getByIDFn(ctx, id)
}

func (m *mockGroupService) Delete(ctx context.Context, id string) error {
	if m.deleteFn == nil {
		panic("mockGroupService.Delete called but not configured")
	}
	return m.deleteFn(ctx, id)
}

func (m *mockGroupService) ListMembers(ctx context.Context, groupID string, page domain.PageRequest) ([]domain.GroupMember, int64, error) {
	if m.listMembersFn == nil {
		panic("mockGroupService.ListMembers called but not configured")
	}
	return m.listMembersFn(ctx, groupID, page)
}

func (m *mockGroupService) AddMember(ctx context.Context, req domain.AddGroupMemberRequest) error {
	if m.addMemberFn == nil {
		panic("mockGroupService.AddMember called but not configured")
	}
	return m.addMemberFn(ctx, req)
}

func (m *mockGroupService) RemoveMember(ctx context.Context, req domain.RemoveGroupMemberRequest) error {
	if m.removeMemberFn == nil {
		panic("mockGroupService.RemoveMember called but not configured")
	}
	return m.removeMemberFn(ctx, req)
}

type mockGrantService struct {
	listForPrincipalFn func(ctx context.Context, principalID string, principalType string, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error)
	listForSecurableFn func(ctx context.Context, securableType string, securableID string, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error)
	listAllFn          func(ctx context.Context, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error)
	grantFn            func(ctx context.Context, req domain.CreateGrantRequest) (*domain.PrivilegeGrant, error)
	revokeFn           func(ctx context.Context, principal string, grantID string) error
}

func (m *mockGrantService) ListForPrincipal(ctx context.Context, principalID string, principalType string, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
	if m.listForPrincipalFn == nil {
		panic("mockGrantService.ListForPrincipal called but not configured")
	}
	return m.listForPrincipalFn(ctx, principalID, principalType, page)
}

func (m *mockGrantService) ListForSecurable(ctx context.Context, securableType string, securableID string, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
	if m.listForSecurableFn == nil {
		panic("mockGrantService.ListForSecurable called but not configured")
	}
	return m.listForSecurableFn(ctx, securableType, securableID, page)
}

func (m *mockGrantService) ListAll(ctx context.Context, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
	if m.listAllFn == nil {
		panic("mockGrantService.ListAll called but not configured")
	}
	return m.listAllFn(ctx, page)
}

func (m *mockGrantService) Grant(ctx context.Context, req domain.CreateGrantRequest) (*domain.PrivilegeGrant, error) {
	if m.grantFn == nil {
		panic("mockGrantService.Grant called but not configured")
	}
	return m.grantFn(ctx, req)
}

func (m *mockGrantService) Revoke(ctx context.Context, principal string, grantID string) error {
	if m.revokeFn == nil {
		panic("mockGrantService.Revoke called but not configured")
	}
	return m.revokeFn(ctx, principal, grantID)
}

type mockColumnMaskService struct {
	getForTableFn func(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.ColumnMask, int64, error)
	createFn      func(ctx context.Context, req domain.CreateColumnMaskRequest) (*domain.ColumnMask, error)
	deleteFn      func(ctx context.Context, id string) error
	bindFn        func(ctx context.Context, req domain.BindColumnMaskRequest) error
	unbindFn      func(ctx context.Context, req domain.BindColumnMaskRequest) error
}

func (m *mockColumnMaskService) GetForTable(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.ColumnMask, int64, error) {
	if m.getForTableFn == nil {
		panic("mockColumnMaskService.GetForTable called but not configured")
	}
	return m.getForTableFn(ctx, tableID, page)
}

func (m *mockColumnMaskService) Create(ctx context.Context, req domain.CreateColumnMaskRequest) (*domain.ColumnMask, error) {
	if m.createFn == nil {
		panic("mockColumnMaskService.Create called but not configured")
	}
	return m.createFn(ctx, req)
}

func (m *mockColumnMaskService) Delete(ctx context.Context, id string) error {
	if m.deleteFn == nil {
		panic("mockColumnMaskService.Delete called but not configured")
	}
	return m.deleteFn(ctx, id)
}

func (m *mockColumnMaskService) Bind(ctx context.Context, req domain.BindColumnMaskRequest) error {
	if m.bindFn == nil {
		panic("mockColumnMaskService.Bind called but not configured")
	}
	return m.bindFn(ctx, req)
}

func (m *mockColumnMaskService) Unbind(ctx context.Context, req domain.BindColumnMaskRequest) error {
	if m.unbindFn == nil {
		panic("mockColumnMaskService.Unbind called but not configured")
	}
	return m.unbindFn(ctx, req)
}

// === Helpers ===

func secTestCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		Name:    "test-user",
		IsAdmin: true,
	})
}

var secFixedTime = time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

func secStrPtr(s string) *string { return &s }

func secSamplePrincipal() domain.Principal {
	return domain.Principal{
		ID:        "p-1",
		Name:      "alice",
		Type:      "user",
		IsAdmin:   false,
		CreatedAt: secFixedTime,
	}
}

func secSampleGroup() domain.Group {
	return domain.Group{
		ID:          "g-1",
		Name:        "engineers",
		Description: "Engineering team",
		CreatedAt:   secFixedTime,
	}
}

func secSampleGroupMember() domain.GroupMember {
	return domain.GroupMember{
		GroupID:    "g-1",
		MemberType: "user",
		MemberID:   "p-1",
	}
}

func secSampleGrant() domain.PrivilegeGrant {
	return domain.PrivilegeGrant{
		ID:            "grant-1",
		PrincipalID:   "p-1",
		PrincipalType: "user",
		SecurableType: "table",
		SecurableID:   "t-1",
		Privilege:     "SELECT",
		GrantedBy:     secStrPtr("admin"),
		GrantedAt:     secFixedTime,
	}
}

func secSampleColumnMask() domain.ColumnMask {
	return domain.ColumnMask{
		ID:             "m-1",
		TableID:        "t-1",
		ColumnName:     "ssn",
		MaskExpression: "'***'",
		Description:    "mask SSN",
		CreatedAt:      secFixedTime,
	}
}

// === Tests ===

func TestHandler_GetPrincipal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		principalID string
		svcFn       func(ctx context.Context, id string) (*domain.Principal, error)
		assertFn    func(t *testing.T, resp GetPrincipalResponseObject, err error)
	}{
		{
			name:        "happy path returns 200",
			principalID: "00000000-0000-0000-0000-000000000001",
			svcFn: func(_ context.Context, _ string) (*domain.Principal, error) {
				p := secSamplePrincipal()
				return &p, nil
			},
			assertFn: func(t *testing.T, resp GetPrincipalResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GetPrincipal200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "p-1", *ok200.Body.Id)
				assert.Equal(t, "alice", *ok200.Body.Name)
			},
		},
		{
			name:        "not found returns 404",
			principalID: "00000000-0000-0000-0000-000000000001",
			svcFn: func(_ context.Context, id string) (*domain.Principal, error) {
				return nil, domain.ErrNotFound("principal %s not found", id)
			},
			assertFn: func(t *testing.T, resp GetPrincipalResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(GetPrincipal404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
		{
			name:        "malformed principalId returns 400",
			principalID: "not-a-uuid",
			svcFn: func(_ context.Context, _ string) (*domain.Principal, error) {
				t.Fatalf("service should not be called for malformed principalId")
				return nil, nil
			},
			assertFn: func(t *testing.T, resp GetPrincipalResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(GetPrincipal400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
				assert.Contains(t, badReq.Body.Message, "invalid principalId")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockPrincipalService{getByIDFn: tt.svcFn}
			handler := &APIHandler{principals: svc}
			resp, err := handler.GetPrincipal(secTestCtx(), GetPrincipalRequestObject{PrincipalId: tt.principalID})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_GetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, id string) (*domain.Group, error)
		assertFn func(t *testing.T, resp GetGroupResponseObject, err error)
	}{
		{
			name: "happy path returns 200",
			svcFn: func(_ context.Context, _ string) (*domain.Group, error) {
				g := secSampleGroup()
				return &g, nil
			},
			assertFn: func(t *testing.T, resp GetGroupResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GetGroup200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "g-1", *ok200.Body.Id)
				assert.Equal(t, "engineers", *ok200.Body.Name)
			},
		},
		{
			name: "not found returns 404",
			svcFn: func(_ context.Context, id string) (*domain.Group, error) {
				return nil, domain.ErrNotFound("group %s not found", id)
			},
			assertFn: func(t *testing.T, resp GetGroupResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(GetGroup404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockGroupService{getByIDFn: tt.svcFn}
			handler := &APIHandler{groups: svc}
			resp, err := handler.GetGroup(secTestCtx(), GetGroupRequestObject{GroupId: "g-1"})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_ListGroupMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, groupID string, page domain.PageRequest) ([]domain.GroupMember, int64, error)
		assertFn func(t *testing.T, resp ListGroupMembersResponseObject, err error)
	}{
		{
			name: "happy path returns 200 with members",
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.GroupMember, int64, error) {
				return []domain.GroupMember{secSampleGroupMember()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListGroupMembersResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListGroupMembers200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "p-1", *(*ok200.Body.Data)[0].MemberId)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.GroupMember, int64, error) {
				return nil, 0, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp ListGroupMembersResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(ListGroupMembers403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockGroupService{listMembersFn: tt.svcFn}
			handler := &APIHandler{groups: svc}
			resp, err := handler.ListGroupMembers(secTestCtx(), ListGroupMembersRequestObject{
				GroupId: "g-1",
				Params:  ListGroupMembersParams{},
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_CreateGroupMember(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, req domain.AddGroupMemberRequest) error
		assertFn func(t *testing.T, resp CreateGroupMemberResponseObject, err error)
	}{
		{
			name: "happy path returns 204",
			svcFn: func(_ context.Context, _ domain.AddGroupMemberRequest) error {
				return nil
			},
			assertFn: func(t *testing.T, resp CreateGroupMemberResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(CreateGroupMember204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ domain.AddGroupMemberRequest) error {
				return domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp CreateGroupMemberResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(CreateGroupMember403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockGroupService{addMemberFn: tt.svcFn}
			handler := &APIHandler{groups: svc}
			body := CreateGroupMemberJSONRequestBody{
				MemberId:   "p-1",
				MemberType: "user",
			}
			resp, err := handler.CreateGroupMember(secTestCtx(), CreateGroupMemberRequestObject{
				GroupId: "g-1",
				Body:    &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeleteGroupMember(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, req domain.RemoveGroupMemberRequest) error
		assertFn func(t *testing.T, resp DeleteGroupMemberResponseObject, err error)
	}{
		{
			name: "happy path returns 204",
			svcFn: func(_ context.Context, _ domain.RemoveGroupMemberRequest) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeleteGroupMemberResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeleteGroupMember204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ domain.RemoveGroupMemberRequest) error {
				return domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp DeleteGroupMemberResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(DeleteGroupMember403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockGroupService{removeMemberFn: tt.svcFn}
			handler := &APIHandler{groups: svc}
			resp, err := handler.DeleteGroupMember(secTestCtx(), DeleteGroupMemberRequestObject{
				GroupId: "g-1",
				Params: DeleteGroupMemberParams{
					MemberId:   "p-1",
					MemberType: "user",
				},
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_ListGrants(t *testing.T) {
	t.Parallel()

	principalID := "p-1"
	pt := ListGrantsParamsPrincipalType("user")
	securableType := "table"
	securableID := "t-1"

	tests := []struct {
		name               string
		params             ListGrantsParams
		listForPrincipalFn func(ctx context.Context, principalID string, principalType string, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error)
		listForSecurableFn func(ctx context.Context, securableType string, securableID string, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error)
		listAllFn          func(ctx context.Context, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error)
		assertFn           func(t *testing.T, resp ListGrantsResponseObject, err error)
	}{
		{
			name:   "with principal filter returns 200",
			params: ListGrantsParams{PrincipalId: &principalID, PrincipalType: &pt},
			listForPrincipalFn: func(_ context.Context, _ string, _ string, _ domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
				return []domain.PrivilegeGrant{secSampleGrant()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListGrantsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListGrants200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "grant-1", *(*ok200.Body.Data)[0].Id)
			},
		},
		{
			name:   "with securable filter returns 200",
			params: ListGrantsParams{SecurableType: &securableType, SecurableId: &securableID},
			listForSecurableFn: func(_ context.Context, _ string, _ string, _ domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
				return []domain.PrivilegeGrant{secSampleGrant()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListGrantsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListGrants200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
			},
		},
		{
			name:   "missing params returns 200",
			params: ListGrantsParams{},
			listAllFn: func(_ context.Context, _ domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
				return []domain.PrivilegeGrant{secSampleGrant()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListGrantsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListGrants200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
			},
		},
		{
			name:   "access denied returns 403",
			params: ListGrantsParams{PrincipalId: &principalID, PrincipalType: &pt},
			listForPrincipalFn: func(_ context.Context, _ string, _ string, _ domain.PageRequest) ([]domain.PrivilegeGrant, int64, error) {
				return nil, 0, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp ListGrantsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(ListGrants403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockGrantService{
				listForPrincipalFn: tt.listForPrincipalFn,
				listForSecurableFn: tt.listForSecurableFn,
				listAllFn:          tt.listAllFn,
			}
			handler := &APIHandler{grants: svc}
			resp, err := handler.ListGrants(secTestCtx(), ListGrantsRequestObject{Params: tt.params})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeleteGrant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, principal string, grantID string) error
		assertFn func(t *testing.T, resp DeleteGrantResponseObject, err error)
	}{
		{
			name: "happy path returns 204",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeleteGrantResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeleteGrant204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name: "not found returns 404",
			svcFn: func(_ context.Context, _ string, id string) error {
				return domain.ErrNotFound("grant %s not found", id)
			},
			assertFn: func(t *testing.T, resp DeleteGrantResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(DeleteGrant404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp DeleteGrantResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(DeleteGrant403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockGrantService{revokeFn: tt.svcFn}
			handler := &APIHandler{grants: svc}
			resp, err := handler.DeleteGrant(secTestCtx(), DeleteGrantRequestObject{GrantId: "grant-1"})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_ListColumnMasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.ColumnMask, int64, error)
		assertFn func(t *testing.T, resp ListColumnMasksResponseObject, err error)
	}{
		{
			name: "happy path returns 200 with masks",
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.ColumnMask, int64, error) {
				return []domain.ColumnMask{secSampleColumnMask()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListColumnMasksResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListColumnMasks200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "m-1", *(*ok200.Body.Data)[0].Id)
				assert.Equal(t, "ssn", *(*ok200.Body.Data)[0].ColumnName)
			},
		},
		{
			name: "service error propagates",
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.ColumnMask, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp ListColumnMasksResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockColumnMaskService{getForTableFn: tt.svcFn}
			handler := &APIHandler{columnMasks: svc}
			resp, err := handler.ListColumnMasks(secTestCtx(), ListColumnMasksRequestObject{
				TableId: "t-1",
				Params:  ListColumnMasksParams{},
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_CreateColumnMask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, req domain.CreateColumnMaskRequest) (*domain.ColumnMask, error)
		assertFn func(t *testing.T, resp CreateColumnMaskResponseObject, err error)
	}{
		{
			name: "happy path returns 201",
			svcFn: func(_ context.Context, _ domain.CreateColumnMaskRequest) (*domain.ColumnMask, error) {
				m := secSampleColumnMask()
				return &m, nil
			},
			assertFn: func(t *testing.T, resp CreateColumnMaskResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				created, ok := resp.(CreateColumnMask201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
				assert.Equal(t, "m-1", *created.Body.Id)
				assert.Equal(t, "ssn", *created.Body.ColumnName)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ domain.CreateColumnMaskRequest) (*domain.ColumnMask, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp CreateColumnMaskResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(CreateColumnMask403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "validation error returns 400",
			svcFn: func(_ context.Context, _ domain.CreateColumnMaskRequest) (*domain.ColumnMask, error) {
				return nil, domain.ErrValidation("column_name is required")
			},
			assertFn: func(t *testing.T, resp CreateColumnMaskResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateColumnMask400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockColumnMaskService{createFn: tt.svcFn}
			handler := &APIHandler{columnMasks: svc}
			body := CreateColumnMaskJSONRequestBody{
				ColumnName:     "ssn",
				MaskExpression: "'***'",
			}
			resp, err := handler.CreateColumnMask(secTestCtx(), CreateColumnMaskRequestObject{
				TableId: "t-1",
				Body:    &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeleteColumnMask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, id string) error
		assertFn func(t *testing.T, resp DeleteColumnMaskResponseObject, err error)
	}{
		{
			name: "happy path returns 204",
			svcFn: func(_ context.Context, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeleteColumnMaskResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeleteColumnMask204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ string) error {
				return domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp DeleteColumnMaskResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(DeleteColumnMask403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "not found returns 404",
			svcFn: func(_ context.Context, _ string) error {
				return domain.ErrNotFound("column mask not found")
			},
			assertFn: func(t *testing.T, resp DeleteColumnMaskResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(DeleteColumnMask404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockColumnMaskService{deleteFn: tt.svcFn}
			handler := &APIHandler{columnMasks: svc}
			resp, err := handler.DeleteColumnMask(secTestCtx(), DeleteColumnMaskRequestObject{ColumnMaskId: "m-1"})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_BindColumnMask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, req domain.BindColumnMaskRequest) error
		assertFn func(t *testing.T, resp BindColumnMaskResponseObject, err error)
	}{
		{
			name: "happy path returns 204",
			svcFn: func(_ context.Context, _ domain.BindColumnMaskRequest) error {
				return nil
			},
			assertFn: func(t *testing.T, resp BindColumnMaskResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(BindColumnMask204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ domain.BindColumnMaskRequest) error {
				return domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp BindColumnMaskResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(BindColumnMask403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "validation error returns 400",
			svcFn: func(_ context.Context, _ domain.BindColumnMaskRequest) error {
				return domain.ErrValidation("principal_type must be 'user' or 'group'")
			},
			assertFn: func(t *testing.T, resp BindColumnMaskResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(BindColumnMask400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockColumnMaskService{bindFn: tt.svcFn}
			handler := &APIHandler{columnMasks: svc}
			body := BindColumnMaskJSONRequestBody{
				PrincipalId:   "p-1",
				PrincipalType: "user",
			}
			resp, err := handler.BindColumnMask(secTestCtx(), BindColumnMaskRequestObject{
				ColumnMaskId: "m-1",
				Body:         &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_UnbindColumnMask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, req domain.BindColumnMaskRequest) error
		assertFn func(t *testing.T, resp UnbindColumnMaskResponseObject, err error)
	}{
		{
			name: "happy path returns 204",
			svcFn: func(_ context.Context, _ domain.BindColumnMaskRequest) error {
				return nil
			},
			assertFn: func(t *testing.T, resp UnbindColumnMaskResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(UnbindColumnMask204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name: "access denied returns 403",
			svcFn: func(_ context.Context, _ domain.BindColumnMaskRequest) error {
				return domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp UnbindColumnMaskResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(UnbindColumnMask403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockColumnMaskService{unbindFn: tt.svcFn}
			handler := &APIHandler{columnMasks: svc}
			resp, err := handler.UnbindColumnMask(secTestCtx(), UnbindColumnMaskRequestObject{
				ColumnMaskId: "m-1",
				Params: UnbindColumnMaskParams{
					PrincipalId:   "p-1",
					PrincipalType: "user",
				},
			})
			tt.assertFn(t, resp, err)
		})
	}
}
