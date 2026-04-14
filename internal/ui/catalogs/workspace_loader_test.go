package catalogs

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/catalog"
	"duck-demo/internal/testutil"
	"duck-demo/internal/ui/core"
)

type testCatalogRepoFactory struct {
	repo domain.CatalogRepository
}

func (f testCatalogRepoFactory) ForCatalog(_ context.Context, _ string) (domain.CatalogRepository, error) {
	return f.repo, nil
}

func TestRenderCatalogWorkspace_ViewColumns(t *testing.T) {
	t.Run("renders discovered view columns on overview", func(t *testing.T) {
		handler := newCatalogWorkspaceTestHandler(t, false)
		req := httptest.NewRequest(http.MethodGet, "/ui/catalogs?schema=analytics&type=view&name=trip_daily_metrics", nil)
		rec := httptest.NewRecorder()

		handler.renderCatalogWorkspace(rec, req, []domain.CatalogRegistration{{
			Name:          "lake",
			Status:        domain.CatalogStatusActive,
			MetastoreType: domain.MetastoreTypeSQLite,
			DataPath:      "/tmp/lake",
		}}, "lake")

		require.Equal(t, http.StatusOK, rec.Code)
		body := rec.Body.String()
		assert.Contains(t, body, "Columns")
		assert.Contains(t, body, "trip_count")
		assert.Contains(t, body, "total_fare")
		assert.Contains(t, body, "Properties")
		assert.Contains(t, body, "Daily metrics")
		assert.NotContains(t, body, "view_property_marker")
		assert.NotContains(t, body, "Columns unavailable for this view.")
	})

	t.Run("renders fallback on overview when view columns are unavailable", func(t *testing.T) {
		handler := newCatalogWorkspaceTestHandler(t, true)
		req := httptest.NewRequest(http.MethodGet, "/ui/catalogs?schema=analytics&type=view&name=trip_daily_metrics", nil)
		rec := httptest.NewRecorder()

		handler.renderCatalogWorkspace(rec, req, []domain.CatalogRegistration{{
			Name:          "lake",
			Status:        domain.CatalogStatusActive,
			MetastoreType: domain.MetastoreTypeSQLite,
			DataPath:      "/tmp/lake",
		}}, "lake")

		require.Equal(t, http.StatusOK, rec.Code)
		body := rec.Body.String()
		assert.Contains(t, body, "Columns unavailable for this view.")
		assert.NotContains(t, body, "trip_count")
	})

	t.Run("does not render view columns on details", func(t *testing.T) {
		handler := newCatalogWorkspaceTestHandler(t, false)
		req := httptest.NewRequest(http.MethodGet, "/ui/catalogs?schema=analytics&type=view&name=trip_daily_metrics&tab=details", nil)
		rec := httptest.NewRecorder()

		handler.renderCatalogWorkspace(rec, req, []domain.CatalogRegistration{{
			Name:          "lake",
			Status:        domain.CatalogStatusActive,
			MetastoreType: domain.MetastoreTypeSQLite,
			DataPath:      "/tmp/lake",
		}}, "lake")

		require.Equal(t, http.StatusOK, rec.Code)
		body := rec.Body.String()
		assert.NotContains(t, body, "trip_count")
		assert.NotContains(t, body, "total_fare")
		assert.NotContains(t, body, "Columns unavailable for this view.")
		assert.Contains(t, body, "view_property_marker")
		assert.Contains(t, body, "SELECT * FROM raw_trips")
	})
}

func TestRenderCatalogWorkspace_TableOverviewMetadata(t *testing.T) {
	handler := newCatalogWorkspaceTestHandler(t, false)

	t.Run("shows table metadata and columns on overview", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ui/catalogs?schema=analytics&type=table&name=fact_trips", nil)
		rec := httptest.NewRecorder()

		handler.renderCatalogWorkspace(rec, req, []domain.CatalogRegistration{{
			Name:          "lake",
			Status:        domain.CatalogStatusActive,
			MetastoreType: domain.MetastoreTypeSQLite,
			DataPath:      "/tmp/lake",
		}}, "lake")

		require.Equal(t, http.StatusOK, rec.Code)
		body := rec.Body.String()
		assert.Contains(t, body, "Fact trips")
		assert.NotContains(t, body, "table_property_marker")
		assert.NotContains(t, body, "table_tag_sensitive")
		assert.Contains(t, body, "fare comment marker")
		assert.Contains(t, body, "money_property_marker")
	})

	t.Run("keeps metadata on details", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ui/catalogs?schema=analytics&type=table&name=fact_trips&tab=details", nil)
		rec := httptest.NewRecorder()

		handler.renderCatalogWorkspace(rec, req, []domain.CatalogRegistration{{
			Name:          "lake",
			Status:        domain.CatalogStatusActive,
			MetastoreType: domain.MetastoreTypeSQLite,
			DataPath:      "/tmp/lake",
		}}, "lake")

		require.Equal(t, http.StatusOK, rec.Code)
		body := rec.Body.String()
		assert.Contains(t, body, "table_property_marker")
		assert.Contains(t, body, "table_tag_sensitive")
		assert.Contains(t, body, "Fact trips")
		assert.NotContains(t, body, "fare comment marker")
		assert.NotContains(t, body, "money_property_marker")
		assert.Contains(t, body, "fact_trips")
	})
}

func TestRenderCatalogWorkspace_UsesLeftExplorerLayout(t *testing.T) {
	handler := newCatalogWorkspaceTestHandler(t, false)
	req := httptest.NewRequest(http.MethodGet, "/ui/catalogs?schema=analytics&type=table&name=fact_trips", nil)
	rec := httptest.NewRecorder()

	handler.renderCatalogWorkspace(rec, req, []domain.CatalogRegistration{{
		Name:          "lake",
		Status:        domain.CatalogStatusActive,
		MetastoreType: domain.MetastoreTypeSQLite,
		DataPath:      "/tmp/lake",
	}}, "lake")

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "md:[grid-template-columns:18rem_minmax(0,1fr)]")
	assert.NotContains(t, body, "workspace-layout-right-aside")
	assert.NotContains(t, body, "workspace-aside-right")
}

func newCatalogWorkspaceTestHandler(t *testing.T, introspectionFails bool) *Handler {
	t.Helper()

	catalogRepo := &testutil.MockCatalogRepo{
		GetMetastoreSummaryFn: func(_ context.Context) (*domain.MetastoreSummary, error) {
			return &domain.MetastoreSummary{
				CatalogName:    "lake",
				MetastoreType:  "DuckLake (SQLite)",
				StorageBackend: "LOCAL",
				SchemaCount:    1,
				TableCount:     0,
			}, nil
		},
		GetCatalogVersionSummaryFn: func(_ context.Context) (*domain.CatalogVersionSummary, error) {
			return &domain.CatalogVersionSummary{CatalogName: "lake", Version: "test"}, nil
		},
		ListSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
			return []domain.SchemaDetail{{
				SchemaID:    "schema-1",
				Name:        "analytics",
				CatalogName: "lake",
				Owner:       "owner",
			}}, 1, nil
		},
		ListTablesFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.TableDetail, int64, error) {
			return []domain.TableDetail{{
				TableID:     "table-1",
				Name:        "fact_trips",
				SchemaName:  "analytics",
				TableType:   domain.TableTypeManaged,
				Owner:       "owner",
				CreatedAt:   viewTime(),
				UpdatedAt:   viewTime(),
				Comment:     "Fact trips",
				Properties:  map[string]string{"marker": "table_property_marker"},
				Tags:        []domain.Tag{{Key: "table_tag_sensitive"}},
				StoragePath: "/tmp/fact_trips",
			}}, 1, nil
		},
		GetTableFn: func(_ context.Context, _, name string) (*domain.TableDetail, error) {
			return &domain.TableDetail{
				TableID:    "table-1",
				Name:       name,
				SchemaName: "analytics",
				TableType:  domain.TableTypeManaged,
				Owner:      "owner",
				CreatedAt:  viewTime(),
				UpdatedAt:  viewTime(),
				Comment:    "Fact trips",
				Properties: map[string]string{
					"marker": "table_property_marker",
				},
				Tags: []domain.Tag{{Key: "table_tag_sensitive"}},
				Columns: []domain.ColumnDetail{
					{Name: "fare_amount", Type: "DOUBLE", Position: 0, Nullable: false, Comment: "fare comment marker", Properties: map[string]string{"semantic": "money_property_marker"}},
				},
			}, nil
		},
		GetSchemaFn: func(_ context.Context, name string) (*domain.SchemaDetail, error) {
			return &domain.SchemaDetail{
				SchemaID:    "schema-1",
				Name:        name,
				CatalogName: "lake",
			}, nil
		},
		DescribeViewColumnsFn: func(_ context.Context, _, _ string) ([]domain.ColumnDetail, error) {
			if introspectionFails {
				return nil, fmt.Errorf("describe failed")
			}
			return []domain.ColumnDetail{
				{Name: "trip_count", Type: "BIGINT", Position: 0, Nullable: false},
				{Name: "total_fare", Type: "DOUBLE", Position: 1, Nullable: true},
			}, nil
		},
	}
	viewRepo := &testutil.MockViewRepo{
		ListFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.ViewDetail, int64, error) {
			return []domain.ViewDetail{{
				ID:           "view-1",
				SchemaID:     "schema-1",
				Name:         "trip_daily_metrics",
				Owner:        "owner",
				CreatedAt:    viewTime(),
				UpdatedAt:    viewTime(),
				Comment:      strPtr("Daily metrics"),
				Properties:   map[string]string{},
				SourceTables: []string{"raw_trips"},
			}}, 1, nil
		},
		GetByNameFn: func(_ context.Context, _ string, _ string) (*domain.ViewDetail, error) {
			return &domain.ViewDetail{
				ID:             "view-1",
				SchemaID:       "schema-1",
				Name:           "trip_daily_metrics",
				ViewDefinition: "SELECT * FROM raw_trips",
				Comment:        strPtr("Daily metrics"),
				Properties:     map[string]string{"marker": "view_property_marker"},
				Owner:          "owner",
				SourceTables:   []string{"raw_trips"},
				CreatedAt:      viewTime(),
				UpdatedAt:      viewTime(),
			}, nil
		},
	}

	catalogSvc := catalog.NewCatalogService(testCatalogRepoFactory{repo: catalogRepo}, nil, nil, nil, nil, nil)
	viewSvc := catalog.NewViewService(viewRepo, testCatalogRepoFactory{repo: catalogRepo}, nil, nil)

	return New(&core.Dependencies{
		Catalog: catalogSvc,
		View:    viewSvc,
	})
}

func viewTime() time.Time {
	return time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
}

func strPtr(s string) *string {
	return &s
}
