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

	internaldb "github.com/Yacobolo/quackstack/internal/db"
	"github.com/Yacobolo/quackstack/internal/db/repository"
	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/engine"
	"github.com/Yacobolo/quackstack/internal/service/security"
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

	workspaceRepo := repository.NewWorkspaceRepo(writeDB)
	projectRepo := repository.NewProjectRepo(writeDB)
	environmentRepo := repository.NewEnvironmentRepo(writeDB)
	dependencyRepo := repository.NewProjectDependencyRepo(writeDB)
	sourceRepo := repository.NewSourceDefinitionRepo(writeDB)
	seedRepo := repository.NewSeedRepo(writeDB)
	modelRepo := repository.NewModelRepo(writeDB)
	macroRepo := repository.NewMacroRepo(writeDB)

	workspace, err := workspaceRepo.GetPersonalByPrincipal(ctx, sampleDashboardOwner)
	require.NoError(t, err)

	project, err := projectRepo.GetByName(ctx, sampleAuthoringProjectName)
	require.NoError(t, err)
	assert.Equal(t, workspace.ID, project.WorkspaceID)
	assert.Equal(t, domain.ProjectKindPersonal, project.Kind)

	libraryProject, err := projectRepo.GetByName(ctx, sampleAuthoringLibraryProjectName)
	require.NoError(t, err)
	assert.Equal(t, domain.ProjectKindLibrary, libraryProject.Kind)

	require.NotNil(t, workspace.DefaultProjectID)
	assert.Equal(t, project.ID, *workspace.DefaultProjectID)

	devEnvironment, err := environmentRepo.GetByName(ctx, project.ID, sampleAuthoringDevEnvironmentName)
	require.NoError(t, err)
	assert.Equal(t, domain.EnvironmentKindDevelopment, devEnvironment.Kind)
	assert.Equal(t, "memory", devEnvironment.TargetCatalog)
	assert.Equal(t, "analytics", devEnvironment.TargetSchema)
	assert.Equal(t, map[string]string{"window_days": "30"}, devEnvironment.Variables)

	prodEnvironment, err := environmentRepo.GetByName(ctx, project.ID, sampleAuthoringProdEnvironmentName)
	require.NoError(t, err)
	assert.Equal(t, domain.EnvironmentKindProduction, prodEnvironment.Kind)
	require.NotNil(t, prodEnvironment.DeferToEnvironment)
	assert.Equal(t, sampleAuthoringDevEnvironmentName, *prodEnvironment.DeferToEnvironment)
	assert.Equal(t, map[string]string{"window_days": "90"}, prodEnvironment.Variables)

	require.NotNil(t, workspace.DefaultEnvironmentID)
	assert.Equal(t, devEnvironment.ID, *workspace.DefaultEnvironmentID)

	deps, err := dependencyRepo.ListByProject(ctx, project.ID)
	require.NoError(t, err)
	require.Len(t, deps, 1)
	assert.Equal(t, sampleAuthoringLibraryProjectName, deps[0].DependencyProject)

	rawTrips, err := sourceRepo.GetByName(ctx, sampleAuthoringProjectName, "raw", "trips")
	require.NoError(t, err)
	assert.Equal(t, "sample_data.nyc_taxi.trips", rawTrips.RelationRef)
	require.NotNil(t, rawTrips.Freshness)
	assert.EqualValues(t, 86400, rawTrips.Freshness.MaxLagSeconds)

	seed, err := seedRepo.GetByName(ctx, sampleAuthoringProjectName, "zone_priority_overrides")
	require.NoError(t, err)
	assert.Equal(t, "csv", seed.Format)
	assert.Contains(t, seed.InputRef, "zone_priority_overrides.csv")
	assert.Equal(t, map[string]string{
		"zone":          "VARCHAR",
		"priority_tier": "VARCHAR",
	}, seed.ColumnTypes)

	stagingModel, err := modelRepo.GetByName(ctx, sampleAuthoringProjectName, "stg_trips")
	require.NoError(t, err)
	assert.Equal(t, domain.MaterializationView, stagingModel.Materialization)
	assert.Contains(t, stagingModel.SQL, "{{ source('raw', 'trips') }}")
	assert.Contains(t, stagingModel.SQL, sampleRevenueBandMacroName)

	martModel, err := modelRepo.GetByName(ctx, sampleAuthoringProjectName, "fct_zone_revenue")
	require.NoError(t, err)
	assert.Equal(t, domain.MaterializationTable, martModel.Materialization)
	assert.Contains(t, martModel.SQL, "{{ ref('zone_priority_overrides') }}")
	assert.Contains(t, martModel.SQL, sampleSafeDivideMacroName)

	revenueMacro, err := macroRepo.GetByName(ctx, sampleRevenueBandMacroName)
	require.NoError(t, err)
	assert.Equal(t, sampleAuthoringLibraryProjectName, revenueMacro.ProjectName)
	assert.Equal(t, domain.MacroVisibilityProject, revenueMacro.Visibility)

	safeDivideMacro, err := macroRepo.GetByName(ctx, sampleSafeDivideMacroName)
	require.NoError(t, err)
	assert.Equal(t, sampleAuthoringProjectName, safeDivideMacro.ProjectName)
	assert.Equal(t, domain.MacroVisibilityProject, safeDivideMacro.Visibility)

	blockedRows, err := secureEngine.Query(ctx, principal.Name, `INSERT INTO sample_data.nyc_taxi.zones VALUES (999, 'Nowhere', 'Blocked', 'Boro')`)
	if blockedRows != nil {
		require.NoError(t, blockedRows.Err())
		defer blockedRows.Close() //nolint:errcheck
	}
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lacks INSERT")
}
