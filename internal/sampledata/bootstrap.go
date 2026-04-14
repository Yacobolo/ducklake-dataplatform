// Package sampledata bootstraps the built-in read-only sample catalog used in local development.
package sampledata

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	dbstore "github.com/Yacobolo/quackstack/internal/db/dbstore"
	"github.com/Yacobolo/quackstack/internal/db/mapper"
	"github.com/Yacobolo/quackstack/internal/db/repository"
	"github.com/Yacobolo/quackstack/internal/domain"
	dashboardsvc "github.com/Yacobolo/quackstack/internal/service/dashboard"
	semanticsvc "github.com/Yacobolo/quackstack/internal/service/semantic"
)

const (
	nycTaxiSchemaName = "nyc_taxi"
	systemOwner       = "system"
)

//go:embed data/*
var seedFS embed.FS

type catalogPaths struct {
	metastorePath string
	dataPath      string
	assetsPath    string
}

// EnsureCatalogRegistration registers the platform-managed sample catalog when it is missing.
func EnsureCatalogRegistration(ctx context.Context, repo domain.CatalogRegistrationRepository, controlPlaneMetaPath string) (*domain.CatalogRegistration, error) {
	reg, err := repo.GetByName(ctx, domain.SampleDataCatalogName)
	if err == nil {
		return reg, nil
	}

	var notFoundErr *domain.NotFoundError
	if err != nil && !errors.As(err, &notFoundErr) {
		return nil, fmt.Errorf("lookup sample catalog: %w", err)
	}

	paths, err := sampleCatalogPaths(controlPlaneMetaPath)
	if err != nil {
		return nil, err
	}

	return repo.Create(ctx, &domain.CatalogRegistration{
		Name:          domain.SampleDataCatalogName,
		MetastoreType: domain.MetastoreTypeSQLite,
		DSN:           paths.metastorePath,
		DataPath:      paths.dataPath,
		Status:        domain.CatalogStatusDetached,
		Comment:       "Platform-managed sample data for zero-setup exploration.",
	})
}

// Bootstrap ensures the built-in sample catalog contains the curated NYC taxi dataset and views.
func Bootstrap(ctx context.Context, controlDB, duckDB *sql.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}

	regRepo := repository.NewCatalogRegistrationRepo(controlDB)
	reg, err := regRepo.GetByName(ctx, domain.SampleDataCatalogName)
	if err != nil {
		return fmt.Errorf("lookup sample catalog registration: %w", err)
	}
	paths := catalogPaths{
		metastorePath: reg.DSN,
		dataPath:      reg.DataPath,
		assetsPath:    filepath.Join(filepath.Dir(reg.DSN), "assets"),
	}

	if err := os.MkdirAll(paths.assetsPath, 0o750); err != nil {
		return fmt.Errorf("create sample assets dir: %w", err)
	}

	tripsAssetPath, err := ensureAsset(paths.assetsPath, "nyc_taxi_trips.parquet")
	if err != nil {
		return err
	}
	zonesAssetPath, err := ensureAsset(paths.assetsPath, "taxi_zone_lookup.csv")
	if err != nil {
		return err
	}
	if _, err := ensureAsset(paths.assetsPath, "SOURCE.md"); err != nil {
		return err
	}

	extRepo := repository.NewExternalTableRepo(controlDB)
	viewRepo := repository.NewViewRepo(controlDB)
	grantRepo := repository.NewGrantRepo(controlDB)
	controlQ := dbstore.New(controlDB)
	catalogFactory := repository.NewCatalogRepoFactory(regRepo, controlDB, duckDB, extRepo, logger.With("component", "sample-data"))
	defer func() {
		_ = catalogFactory.Close(domain.SampleDataCatalogName)
	}()

	catalogRepo, err := catalogFactory.ForCatalog(ctx, domain.SampleDataCatalogName)
	if err != nil {
		return fmt.Errorf("open sample catalog repo: %w", err)
	}

	if err := ensureCatalogMetadata(ctx, controlQ); err != nil {
		return err
	}

	schema, err := ensureSchema(ctx, catalogRepo)
	if err != nil {
		return err
	}

	if err := ensureTripsTable(ctx, catalogRepo, duckDB, tripsAssetPath); err != nil {
		return err
	}
	if err := ensureZonesTable(ctx, catalogRepo, duckDB, zonesAssetPath); err != nil {
		return err
	}
	if err := ensureDailyMetricsView(ctx, duckDB, viewRepo, schema.SchemaID); err != nil {
		return err
	}
	if err := ensureZoneMetricsView(ctx, duckDB, viewRepo, schema.SchemaID); err != nil {
		return err
	}
	if err := ensureDashboardMetricsView(ctx, duckDB, viewRepo, schema.SchemaID); err != nil {
		return err
	}
	if err := ensureSampleGrants(ctx, grantRepo, schema.SchemaID); err != nil {
		return err
	}
	semanticSvc := semanticsvc.NewService(
		repository.NewSemanticModelRepo(controlDB),
		repository.NewSemanticMetricRepo(controlDB),
		repository.NewSemanticRelationshipRepo(controlDB),
		repository.NewSemanticPreAggregationRepo(controlDB),
		repository.NewModelRepo(controlDB),
	)
	if err := ensureSampleDashboardSemanticModel(ctx, semanticSvc); err != nil {
		return fmt.Errorf("ensure sample dashboard semantic model: %w", err)
	}
	dashboardRepo := repository.NewDashboardRepo(controlDB)
	widgetRepo := repository.NewDashboardWidgetRepo(controlDB)
	dashboardService := dashboardsvc.NewService(dashboardRepo, widgetRepo, nil, sampleAuditRepo{}, nil, semanticSvc)
	if err := ensureSampleDashboards(ctx, dashboardService); err != nil {
		return fmt.Errorf("ensure sample dashboards: %w", err)
	}

	logger.Info("sample data ready", "catalog", domain.SampleDataCatalogName, "schema", nycTaxiSchemaName)
	return nil
}

func sampleCatalogPaths(controlPlaneMetaPath string) (catalogPaths, error) {
	baseDir := filepath.Join(filepath.Dir(controlPlaneMetaPath), "sample_data")
	dataDir := filepath.Join(baseDir, "data")
	assetsDir := filepath.Join(baseDir, "assets")
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return catalogPaths{}, fmt.Errorf("create sample data dir: %w", err)
	}
	if err := os.MkdirAll(assetsDir, 0o750); err != nil {
		return catalogPaths{}, fmt.Errorf("create sample assets dir: %w", err)
	}

	metastorePath, err := filepath.Abs(filepath.Join(baseDir, "quackstack_sample_data.sqlite"))
	if err != nil {
		return catalogPaths{}, fmt.Errorf("resolve sample metastore path: %w", err)
	}
	resolvedDataPath, err := filepath.Abs(dataDir)
	if err != nil {
		return catalogPaths{}, fmt.Errorf("resolve sample data path: %w", err)
	}
	resolvedAssetsPath, err := filepath.Abs(assetsDir)
	if err != nil {
		return catalogPaths{}, fmt.Errorf("resolve sample assets path: %w", err)
	}

	return catalogPaths{
		metastorePath: metastorePath,
		dataPath:      filepath.ToSlash(resolvedDataPath),
		assetsPath:    filepath.ToSlash(resolvedAssetsPath),
	}, nil
}

func ensureCatalogMetadata(ctx context.Context, q *dbstore.Queries) error {
	return q.UpsertCatalogMetadata(ctx, dbstore.UpsertCatalogMetadataParams{
		SecurableType: "catalog",
		SecurableName: domain.SampleDataCatalogName,
		Comment:       mapper.NullStrFromStr("Built-in NYC Taxi sample data for first-query, dashboard, and exploration flows."),
		Owner:         mapper.NullStrFromStr(systemOwner),
	})
}

func ensureSchema(ctx context.Context, repo domain.CatalogRepository) (*domain.SchemaDetail, error) {
	schema, err := repo.GetSchema(ctx, nycTaxiSchemaName)
	if err == nil {
		return schema, nil
	}

	var notFoundErr *domain.NotFoundError
	if !errors.As(err, &notFoundErr) {
		return nil, fmt.Errorf("get sample schema: %w", err)
	}

	schema, err = repo.CreateSchema(ctx, nycTaxiSchemaName, "NYC TLC yellow taxi sample data for dashboards and SQL exploration.", systemOwner)
	if err != nil {
		return nil, fmt.Errorf("create sample schema: %w", err)
	}
	return schema, nil
}

func ensureTripsTable(ctx context.Context, repo domain.CatalogRepository, duckDB *sql.DB, assetPath string) error {
	if err := ensureTable(ctx, repo, domain.CreateTableRequest{
		Name: "trips",
		Columns: []domain.CreateColumnDef{
			{Name: "vendor_id", Type: "INTEGER"},
			{Name: "pickup_at", Type: "TIMESTAMP"},
			{Name: "dropoff_at", Type: "TIMESTAMP"},
			{Name: "passenger_count", Type: "INTEGER"},
			{Name: "trip_distance_mi", Type: "DOUBLE"},
			{Name: "rate_code_id", Type: "INTEGER"},
			{Name: "store_and_fwd_flag", Type: "VARCHAR"},
			{Name: "pickup_location_id", Type: "INTEGER"},
			{Name: "dropoff_location_id", Type: "INTEGER"},
			{Name: "payment_type", Type: "INTEGER"},
			{Name: "fare_amount", Type: "DOUBLE"},
			{Name: "extra", Type: "DOUBLE"},
			{Name: "mta_tax", Type: "DOUBLE"},
			{Name: "tip_amount", Type: "DOUBLE"},
			{Name: "tolls_amount", Type: "DOUBLE"},
			{Name: "improvement_surcharge", Type: "DOUBLE"},
			{Name: "total_amount", Type: "DOUBLE"},
			{Name: "congestion_surcharge", Type: "DOUBLE"},
			{Name: "airport_fee", Type: "DOUBLE"},
		},
		Comment: "Curated January 2024 NYC TLC yellow taxi trips sampled from the official trip record data.",
	}); err != nil {
		return err
	}

	insertSQL := fmt.Sprintf(`
INSERT INTO "sample_data"."%s"."trips"
SELECT
	vendor_id,
	pickup_at,
	dropoff_at,
	passenger_count,
	trip_distance_mi,
	ratecode_id,
	store_and_fwd_flag,
	pickup_location_id,
	dropoff_location_id,
	payment_type,
	fare_amount,
	extra,
	mta_tax,
	tip_amount,
	tolls_amount,
	improvement_surcharge,
	total_amount,
	congestion_surcharge,
	airport_fee
FROM read_parquet('%s')
`, nycTaxiSchemaName, sqlStringLiteral(assetPath))

	return seedTableIfEmpty(
		ctx,
		duckDB,
		fmt.Sprintf(`SELECT COUNT(*) FROM "sample_data"."%s"."trips"`, nycTaxiSchemaName),
		insertSQL,
	)
}

func ensureZonesTable(ctx context.Context, repo domain.CatalogRepository, duckDB *sql.DB, assetPath string) error {
	if err := ensureTable(ctx, repo, domain.CreateTableRequest{
		Name: "zones",
		Columns: []domain.CreateColumnDef{
			{Name: "location_id", Type: "INTEGER"},
			{Name: "borough", Type: "VARCHAR"},
			{Name: "zone", Type: "VARCHAR"},
			{Name: "service_zone", Type: "VARCHAR"},
		},
		Comment: "Official NYC taxi zone lookup table from TLC.",
	}); err != nil {
		return err
	}

	insertSQL := fmt.Sprintf(`
INSERT INTO "sample_data"."%s"."zones"
SELECT
	CAST(LocationID AS INTEGER) AS location_id,
	CAST(Borough AS VARCHAR) AS borough,
	CAST(Zone AS VARCHAR) AS zone,
	CAST(service_zone AS VARCHAR) AS service_zone
FROM read_csv_auto('%s', HEADER=TRUE)
`, nycTaxiSchemaName, sqlStringLiteral(assetPath))

	return seedTableIfEmpty(
		ctx,
		duckDB,
		fmt.Sprintf(`SELECT COUNT(*) FROM "sample_data"."%s"."zones"`, nycTaxiSchemaName),
		insertSQL,
	)
}

func ensureDailyMetricsView(ctx context.Context, duckDB *sql.DB, viewRepo *repository.ViewRepo, schemaID string) error {
	const viewName = "daily_metrics"
	viewDefinition := fmt.Sprintf(`
SELECT
	CAST(pickup_at AS DATE) AS pickup_date,
	COUNT(*) AS trip_count,
	ROUND(SUM(total_amount), 2) AS gross_revenue,
	ROUND(AVG(total_amount), 2) AS avg_total_amount,
	ROUND(AVG(trip_distance_mi), 2) AS avg_trip_distance_mi,
	ROUND(SUM(tip_amount), 2) AS total_tip_amount
FROM "sample_data"."%s"."trips"
GROUP BY 1
ORDER BY 1
`, nycTaxiSchemaName)
	return ensureView(ctx, duckDB, viewRepo, schemaID, viewName, viewDefinition, "Daily trip and revenue rollups for dashboard-friendly exploration.", []string{
		fmt.Sprintf("sample_data.%s.trips", nycTaxiSchemaName),
	})
}

func ensureZoneMetricsView(ctx context.Context, duckDB *sql.DB, viewRepo *repository.ViewRepo, schemaID string) error {
	const viewName = "zone_metrics"
	viewDefinition := fmt.Sprintf(`
SELECT
	z.borough,
	z.zone AS pickup_zone,
	COUNT(*) AS trip_count,
	ROUND(SUM(t.total_amount), 2) AS gross_revenue,
	ROUND(AVG(t.tip_amount), 2) AS avg_tip_amount,
	ROUND(AVG(t.trip_distance_mi), 2) AS avg_trip_distance_mi
FROM "sample_data"."%s"."trips" t
JOIN "sample_data"."%s"."zones" z
	ON z.location_id = t.pickup_location_id
GROUP BY 1, 2
ORDER BY gross_revenue DESC, trip_count DESC, pickup_zone
`, nycTaxiSchemaName, nycTaxiSchemaName)
	return ensureView(ctx, duckDB, viewRepo, schemaID, viewName, viewDefinition, "Pickup-zone aggregates for map, ranking, and dashboard scenarios.", []string{
		fmt.Sprintf("sample_data.%s.trips", nycTaxiSchemaName),
		fmt.Sprintf("sample_data.%s.zones", nycTaxiSchemaName),
	})
}

func ensureDashboardMetricsView(ctx context.Context, duckDB *sql.DB, viewRepo *repository.ViewRepo, schemaID string) error {
	const viewName = "dashboard_metrics"
	viewDefinition := fmt.Sprintf(`
SELECT
	CAST(t.pickup_at AS DATE) AS pickup_date,
	z.borough,
	z.zone AS pickup_zone,
	COUNT(*) AS trip_count,
	ROUND(SUM(t.total_amount), 2) AS gross_revenue
FROM "sample_data"."%s"."trips" t
JOIN "sample_data"."%s"."zones" z
	ON z.location_id = t.pickup_location_id
GROUP BY 1, 2, 3
ORDER BY 1, gross_revenue DESC, pickup_zone
`, nycTaxiSchemaName, nycTaxiSchemaName)
	return ensureView(ctx, duckDB, viewRepo, schemaID, viewName, viewDefinition, "Shared semantic-ready dashboard grain across date, borough, and pickup zone.", []string{
		fmt.Sprintf("sample_data.%s.trips", nycTaxiSchemaName),
		fmt.Sprintf("sample_data.%s.zones", nycTaxiSchemaName),
	})
}

func ensureView(ctx context.Context, duckDB *sql.DB, viewRepo *repository.ViewRepo, schemaID string, viewName, viewDefinition, comment string, sourceTables []string) error {
	createSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW "sample_data"."%s"."%s" AS %s`, nycTaxiSchemaName, viewName, viewDefinition)
	if _, err := duckDB.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("create sample view %q: %w", viewName, err)
	}

	_, err := viewRepo.GetByName(ctx, schemaID, viewName)
	if err == nil {
		return nil
	}

	var notFoundErr *domain.NotFoundError
	if !errors.As(err, &notFoundErr) {
		return fmt.Errorf("lookup sample view metadata %q: %w", viewName, err)
	}

	_, err = viewRepo.Create(ctx, &domain.ViewDetail{
		SchemaID:       schemaID,
		SchemaName:     nycTaxiSchemaName,
		CatalogName:    domain.SampleDataCatalogName,
		Name:           viewName,
		ViewDefinition: viewDefinition,
		Comment:        strPtr(comment),
		Owner:          systemOwner,
		SourceTables:   sourceTables,
	})
	if err != nil {
		return fmt.Errorf("create sample view metadata %q: %w", viewName, err)
	}
	return nil
}

func ensureSampleGrants(ctx context.Context, grantRepo *repository.GrantRepo, schemaID string) error {
	securableSchemaID := domain.SyntheticCatalogSchemaID(domain.SampleDataCatalogName, schemaID)
	for _, privilege := range []string{domain.PrivUseSchema, domain.PrivSelect} {
		hasGrant, err := grantRepo.HasPrivilege(ctx, domain.AllAuthenticatedGroupID, "group", domain.SecurableSchema, securableSchemaID, privilege)
		if err != nil {
			return fmt.Errorf("check sample grant %s: %w", privilege, err)
		}
		if hasGrant {
			continue
		}
		_, err = grantRepo.Grant(ctx, &domain.PrivilegeGrant{
			PrincipalID:   domain.AllAuthenticatedGroupID,
			PrincipalType: "group",
			SecurableType: domain.SecurableSchema,
			SecurableID:   securableSchemaID,
			Privilege:     privilege,
			GrantedBy:     strPtr(systemOwner),
		})
		if err != nil {
			return fmt.Errorf("grant sample schema %s: %w", privilege, err)
		}
	}
	return nil
}

func ensureTable(ctx context.Context, repo domain.CatalogRepository, req domain.CreateTableRequest) error {
	_, err := repo.GetTable(ctx, nycTaxiSchemaName, req.Name)
	if err == nil {
		return nil
	}

	var notFoundErr *domain.NotFoundError
	if !errors.As(err, &notFoundErr) {
		return fmt.Errorf("get sample table %q: %w", req.Name, err)
	}

	if _, err := repo.CreateTable(ctx, nycTaxiSchemaName, req, systemOwner); err != nil {
		return fmt.Errorf("create sample table %q: %w", req.Name, err)
	}
	return nil
}

func ensureAsset(assetsPath, name string) (string, error) {
	targetPath := filepath.Join(assetsPath, name)
	if _, err := os.Stat(targetPath); err == nil {
		return filepath.ToSlash(targetPath), nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("stat sample asset %q: %w", name, err)
	}

	data, err := seedFS.ReadFile(filepath.ToSlash(filepath.Join("data", name)))
	if err != nil {
		return "", fmt.Errorf("read embedded sample asset %q: %w", name, err)
	}
	if err := os.WriteFile(targetPath, data, 0o600); err != nil {
		return "", fmt.Errorf("write sample asset %q: %w", name, err)
	}
	return filepath.ToSlash(targetPath), nil
}

func seedTableIfEmpty(ctx context.Context, duckDB *sql.DB, countSQL, insertSQL string) error {
	var count int64
	if err := duckDB.QueryRowContext(ctx, countSQL).Scan(&count); err != nil {
		return fmt.Errorf("count sample rows: %w", err)
	}
	if count > 0 {
		return nil
	}

	if _, err := duckDB.ExecContext(ctx, insertSQL); err != nil {
		return fmt.Errorf("seed sample rows: %w", err)
	}
	return nil
}

func sqlStringLiteral(v string) string {
	return strings.ReplaceAll(v, `'`, `''`)
}

func strPtr(v string) *string {
	return &v
}

func intPtr(v int) *int {
	return &v
}
