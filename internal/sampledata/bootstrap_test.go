package sampledata

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/mattn/go-sqlite3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	"duck-demo/internal/engine"
	"duck-demo/internal/service/security"
)

func TestBootstrap_SeedsQueryableReadOnlySampleData(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	writeDB, readDB := internaldb.OpenTestSQLite(t)
	defer readDB.Close() //nolint:errcheck

	duckDB, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer duckDB.Close() //nolint:errcheck

	if err := engine.InstallExtensions(ctx, duckDB); err != nil {
		t.Skipf("duckdb extensions unavailable: %v", err)
	}

	regRepo := repository.NewCatalogRegistrationRepo(writeDB)
	controlPath := t.TempDir() + "/control.sqlite"
	reg, err := EnsureCatalogRegistration(ctx, regRepo, controlPath)
	require.NoError(t, err)

	attacher := engine.NewDuckDBSecretManager(duckDB)
	require.NoError(t, attacher.Attach(ctx, *reg))
	require.NoError(t, regRepo.UpdateStatus(ctx, reg.ID, domain.CatalogStatusActive, ""))

	require.NoError(t, Bootstrap(ctx, writeDB, duckDB, slog.Default()))

	principalRepo := repository.NewPrincipalRepo(writeDB)
	groupRepo := repository.NewGroupRepo(writeDB)
	grantRepo := repository.NewGrantRepo(writeDB)
	dashboardRepo := repository.NewDashboardRepo(writeDB)
	widgetRepo := repository.NewDashboardWidgetRepo(writeDB)
	rowFilterRepo := repository.NewRowFilterRepo(writeDB)
	columnMaskRepo := repository.NewColumnMaskRepo(writeDB)
	introspectionRepo := repository.NewIntrospectionRepo(readDB)
	extRepo := repository.NewExternalTableRepo(writeDB)
	viewRepo := repository.NewViewRepo(writeDB)
	authSvc := security.NewAuthorizationService(
		principalRepo,
		groupRepo,
		grantRepo,
		rowFilterRepo,
		columnMaskRepo,
		introspectionRepo,
		extRepo,
	)

	catalogFactory := repository.NewCatalogRepoFactory(regRepo, writeDB, duckDB, extRepo, slog.Default())
	defer func() {
		_ = catalogFactory.Close(domain.SampleDataCatalogName)
	}()
	authSvc.SetCatalogSchemaLookup(func(ctx context.Context, catalogName, schemaName string) (*domain.SchemaDetail, error) {
		repo, err := catalogFactory.ForCatalog(ctx, catalogName)
		if err != nil {
			return nil, err
		}
		return repo.GetSchema(ctx, schemaName)
	})
	authSvc.SetCatalogTableLookup(func(ctx context.Context, catalogName, schemaName, tableName string) (*domain.TableDetail, error) {
		repo, err := catalogFactory.ForCatalog(ctx, catalogName)
		if err != nil {
			return nil, err
		}
		return repo.GetTable(ctx, schemaName, tableName)
	})
	authSvc.SetViewRepository(viewRepo)
	authSvc.SetCatalogViewLookup(func(ctx context.Context, catalogName, schemaName, viewName string) (*domain.ViewDetail, error) {
		repo, err := catalogFactory.ForCatalog(ctx, catalogName)
		if err != nil {
			return nil, err
		}
		schema, err := repo.GetSchema(ctx, schemaName)
		if err != nil {
			return nil, err
		}
		return viewRepo.GetByName(ctx, schema.SchemaID, viewName)
	})

	principal, err := principalRepo.Create(ctx, &domain.Principal{
		Name: "analyst@example.com",
		Type: "user",
	})
	require.NoError(t, err)

	secureEngine := engine.NewSecureEngine(duckDB, authSvc, nil, nil, slog.Default())

	rows, err := secureEngine.Query(ctx, principal.Name, `SELECT pickup_date, trip_count, gross_revenue FROM sample_data.nyc_taxi.daily_metrics ORDER BY pickup_date LIMIT 3`)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck

	var gotDates []time.Time
	for rows.Next() {
		var pickupDate time.Time
		var tripCount int64
		var grossRevenue float64
		require.NoError(t, rows.Scan(&pickupDate, &tripCount, &grossRevenue))
		assert.Positive(t, tripCount)
		assert.Positive(t, grossRevenue)
		gotDates = append(gotDates, pickupDate)
	}
	require.NoError(t, rows.Err())
	assert.Len(t, gotDates, 3)

	dashboards, total, err := dashboardRepo.List(ctx, nil, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	require.NoError(t, err)
	assert.Positive(t, total)

	var seededDashboard *domain.Dashboard
	var chartLabDashboard *domain.Dashboard
	for i := range dashboards {
		if dashboards[i].Name == sampleDashboardName {
			seededDashboard = &dashboards[i]
		}
		if dashboards[i].Name == sampleChartLabDashboardName {
			chartLabDashboard = &dashboards[i]
		}
	}
	require.NotNil(t, seededDashboard)
	require.NotNil(t, chartLabDashboard)
	assert.Equal(t, sampleDashboardOwner, seededDashboard.Owner)
	assert.Equal(t, sampleDashboardSemanticProj, seededDashboard.SemanticProjectName)
	assert.Equal(t, sampleDashboardSemanticModel, seededDashboard.SemanticModelName)
	assert.Equal(t, sampleDashboardOwner, chartLabDashboard.Owner)
	assert.Equal(t, sampleDashboardSemanticProj, chartLabDashboard.SemanticProjectName)
	assert.Equal(t, sampleDashboardSemanticModel, chartLabDashboard.SemanticModelName)

	widgets, err := widgetRepo.ListByDashboard(ctx, seededDashboard.ID)
	require.NoError(t, err)
	assert.Len(t, widgets, 5)
	assert.Equal(t, "Total Revenue", widgets[0].Name)
	assert.Equal(t, "Trips by Day", widgets[1].Name)
	assert.Equal(t, "Zone Revenue Detail", widgets[4].Name)
	assert.Equal(t, domain.DashboardWidgetSourceSemanticQuery, widgets[0].Source.Kind)
	assert.Equal(t, domain.DashboardWidgetSourceSemanticQuery, widgets[1].Source.Kind)

	chartLabWidgets, err := widgetRepo.ListByDashboard(ctx, chartLabDashboard.ID)
	require.NoError(t, err)
	assert.Len(t, chartLabWidgets, 8)

	chartTypes := make(map[domain.VisualChartType]bool)
	hasTable := false
	for _, widget := range chartLabWidgets {
		require.NotNil(t, widget.VisualSpec)
		switch widget.VisualSpec.Kind {
		case domain.VisualOutputChart:
			require.NotNil(t, widget.VisualSpec.ChartType)
			chartTypes[*widget.VisualSpec.ChartType] = true
		case domain.VisualOutputTable:
			hasTable = true
		}
	}

	assert.True(t, chartTypes[domain.VisualChartLine])
	assert.True(t, chartTypes[domain.VisualChartArea])
	assert.True(t, chartTypes[domain.VisualChartBar])
	assert.True(t, chartTypes[domain.VisualChartStackedBar])
	assert.True(t, chartTypes[domain.VisualChartPie])
	assert.True(t, chartTypes[domain.VisualChartDoughnut])
	assert.True(t, chartTypes[domain.VisualChartScatter])
	assert.True(t, hasTable)

	blockedRows, err := secureEngine.Query(ctx, principal.Name, `INSERT INTO sample_data.nyc_taxi.zones VALUES (999, 'Nowhere', 'Blocked', 'Boro')`)
	if blockedRows != nil {
		require.NoError(t, blockedRows.Err())
		defer blockedRows.Close() //nolint:errcheck
	}
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lacks INSERT")
}
