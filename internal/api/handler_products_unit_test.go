package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

type mockProductService struct {
	listTeamsFn  func(ctx context.Context, page domain.PageRequest, domainName *string) ([]domain.Team, int64, error)
	getDomainFn  func(ctx context.Context, name string) (*domain.Domain, error)
	getTeamFn    func(ctx context.Context, domainName, teamName string) (*domain.Team, error)
	createTeamFn func(ctx context.Context, req domain.CreateTeamRequest) (*domain.Team, error)
}

func (m *mockProductService) ListDomains(context.Context, domain.PageRequest) ([]domain.Domain, int64, error) {
	panic("not implemented")
}
func (m *mockProductService) GetDomain(ctx context.Context, name string) (*domain.Domain, error) {
	if m.getDomainFn == nil {
		panic("mockProductService.GetDomain called but not configured")
	}
	return m.getDomainFn(ctx, name)
}
func (m *mockProductService) CreateDomain(context.Context, domain.CreateDomainRequest) (*domain.Domain, error) {
	panic("not implemented")
}
func (m *mockProductService) UpdateDomain(context.Context, string, domain.UpdateDomainRequest) (*domain.Domain, error) {
	panic("not implemented")
}
func (m *mockProductService) DeleteDomain(context.Context, string) error { panic("not implemented") }
func (m *mockProductService) ListTeams(ctx context.Context, page domain.PageRequest, domainName *string) ([]domain.Team, int64, error) {
	if m.listTeamsFn == nil {
		panic("mockProductService.ListTeams called but not configured")
	}
	return m.listTeamsFn(ctx, page, domainName)
}
func (m *mockProductService) GetTeam(ctx context.Context, domainName, teamName string) (*domain.Team, error) {
	if m.getTeamFn == nil {
		panic("mockProductService.GetTeam called but not configured")
	}
	return m.getTeamFn(ctx, domainName, teamName)
}
func (m *mockProductService) CreateTeam(ctx context.Context, req domain.CreateTeamRequest) (*domain.Team, error) {
	if m.createTeamFn == nil {
		panic("mockProductService.CreateTeam called but not configured")
	}
	return m.createTeamFn(ctx, req)
}
func (m *mockProductService) UpdateTeam(context.Context, string, string, domain.UpdateTeamRequest) (*domain.Team, error) {
	panic("not implemented")
}
func (m *mockProductService) DeleteTeam(context.Context, string, string) error { panic("not implemented") }
func (m *mockProductService) ListProducts(context.Context, domain.DataProductFilter) ([]domain.DataProductListItem, int64, error) {
	panic("not implemented")
}
func (m *mockProductService) GetProduct(context.Context, string) (*domain.DataProductDetail, error) {
	panic("not implemented")
}
func (m *mockProductService) GetVersion(context.Context, string, int) (*domain.DataProductVersionDetail, error) {
	panic("not implemented")
}
func (m *mockProductService) CreateProduct(context.Context, domain.CreateDataProductRequest) (*domain.DataProductDetail, error) {
	panic("not implemented")
}
func (m *mockProductService) UpdateProduct(context.Context, string, domain.UpdateDataProductRequest) (*domain.DataProductDetail, error) {
	panic("not implemented")
}
func (m *mockProductService) DeleteProduct(context.Context, string) error { panic("not implemented") }
func (m *mockProductService) CreateVersion(context.Context, string, domain.CreateDataProductVersionRequest) (*domain.DataProductDetail, error) {
	panic("not implemented")
}
func (m *mockProductService) PublishVersion(context.Context, string, int) (*domain.DataProductDetail, error) {
	panic("not implemented")
}
func (m *mockProductService) DeprecateVersion(context.Context, string, int, *string) (*domain.DataProductDetail, error) {
	panic("not implemented")
}
func (m *mockProductService) RetireVersion(context.Context, string, int) (*domain.DataProductDetail, error) {
	panic("not implemented")
}
func (m *mockProductService) DeleteVersion(context.Context, string, int) error {
	panic("not implemented")
}
func (m *mockProductService) AddDependency(context.Context, string, string) (*domain.DataProductDetail, error) {
	panic("not implemented")
}
func (m *mockProductService) Subscribe(context.Context, string, string, string, string) (*domain.ProductSubscription, error) {
	panic("not implemented")
}
func (m *mockProductService) ListEvents(context.Context, string, domain.PageRequest) ([]domain.ProductEvent, int64, error) {
	panic("not implemented")
}
func (m *mockProductService) ListScorecards(context.Context, domain.PageRequest) ([]domain.ProductScorecard, int64, error) {
	panic("not implemented")
}
func (m *mockProductService) GetPortfolioReport(context.Context) (*domain.ProductPortfolioReport, error) {
	panic("not implemented")
}

func TestHandler_ListProductTeams_UnexpectedErrorReturns500(t *testing.T) {
	t.Parallel()

	handler := &APIHandler{
		products: &mockProductService{
			listTeamsFn: func(_ context.Context, _ domain.PageRequest, _ *string) ([]domain.Team, int64, error) {
				return nil, 0, assert.AnError
			},
		},
	}

	resp, err := handler.ListProductTeams(context.Background(), GenListProductTeamsRequest{})
	require.NoError(t, err)
	internal, ok := resp.(ListProductTeams500JSONResponse)
	require.True(t, ok, "expected 500 response, got %T", resp)
	assert.Equal(t, int32(500), internal.Body.Code)
}

func TestHandler_CreateProductTeam_MapsDomainErrors(t *testing.T) {
	t.Parallel()

	baseBody := GenCreateProductTeamJSONBody{Name: "analytics"}
	sampleDomain := &domain.Domain{
		ID:        "domain-1",
		Name:      "revenue",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	tests := []struct {
		name     string
		body     *GenCreateProductTeamJSONBody
		service  *mockProductService
		assertFn func(t *testing.T, resp GenCreateProductTeamResponse, err error)
	}{
		{
			name: "missing body returns 400",
			body: nil,
			service: &mockProductService{},
			assertFn: func(t *testing.T, resp GenCreateProductTeamResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateProductTeam400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
		{
			name: "missing domain name returns 400",
			body: &GenCreateProductTeamJSONBody{Name: "analytics"},
			service: &mockProductService{},
			assertFn: func(t *testing.T, resp GenCreateProductTeamResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateProductTeam400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
		{
			name: "missing name returns 400",
			body: &GenCreateProductTeamJSONBody{},
			service: &mockProductService{},
			assertFn: func(t *testing.T, resp GenCreateProductTeamResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateProductTeam400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
		{
			name: "missing domain returns 404",
			body: &baseBody,
			service: &mockProductService{
				getDomainFn: func(_ context.Context, _ string) (*domain.Domain, error) {
					return nil, domain.ErrNotFound("domain missing")
				},
			},
			assertFn: func(t *testing.T, resp GenCreateProductTeamResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(CreateProductTeam404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
		{
			name: "validation error returns 400",
			body: &baseBody,
			service: &mockProductService{
				getDomainFn: func(_ context.Context, _ string) (*domain.Domain, error) {
					return sampleDomain, nil
				},
				createTeamFn: func(_ context.Context, _ domain.CreateTeamRequest) (*domain.Team, error) {
					return nil, domain.ErrValidation("team name is required")
				},
			},
			assertFn: func(t *testing.T, resp GenCreateProductTeamResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateProductTeam400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
		{
			name: "unexpected error returns 500",
			body: &baseBody,
			service: &mockProductService{
				getDomainFn: func(_ context.Context, _ string) (*domain.Domain, error) {
					return sampleDomain, nil
				},
				createTeamFn: func(_ context.Context, _ domain.CreateTeamRequest) (*domain.Team, error) {
					return nil, assert.AnError
				},
			},
			assertFn: func(t *testing.T, resp GenCreateProductTeamResponse, err error) {
				t.Helper()
				require.NoError(t, err)
				internal, ok := resp.(GenCreateProductTeam500JSONResponse)
				require.True(t, ok, "expected 500 response, got %T", resp)
				assert.Equal(t, int32(500), internal.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler := &APIHandler{products: tt.service}
			req := GenCreateProductTeamRequest{Body: tt.body}
			if tt.body != nil {
				req.DomainName = "revenue"
			}
			if tt.name == "missing domain name returns 400" {
				req.DomainName = ""
			}
			resp, err := handler.CreateProductTeam(context.Background(), req)
			tt.assertFn(t, resp, err)
		})
	}
}
