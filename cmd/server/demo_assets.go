package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"duck-demo/internal/config"
	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
)

func runSeedAssetsDemo(_ []string) error {
	if err := config.LoadDotEnv(".env"); err != nil {
		fmt.Fprintf(os.Stderr, "warn: could not load .env: %v\n", err)
	}
	cfg, err := config.LoadFromEnv()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	db, err := sql.Open("sqlite3", cfg.MetaDBPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return fmt.Errorf("open metastore: %w", err)
	}
	defer db.Close() //nolint:errcheck

	if err := internaldb.RunMigrations(db); err != nil {
		return fmt.Errorf("migration: %w", err)
	}

	ctx := context.Background()
	assetRepo := repository.NewDataAssetRepo(db)
	depRepo := repository.NewAssetDependencyRepo(db)
	partitionRepo := repository.NewAssetPartitionRepo(db)
	runRepo := repository.NewAssetRunRepo(db)
	checkRepo := repository.NewAssetCheckRepo(db)
	backfillRepo := repository.NewBackfillRepo(db)

	_, total, err := assetRepo.List(ctx, domain.AssetFilter{Page: domain.PageRequest{MaxResults: 1}})
	if err != nil {
		return fmt.Errorf("list assets: %w", err)
	}
	if total > 0 {
		return fmt.Errorf("refusing to seed demo assets into non-empty metastore %q", cfg.MetaDBPath)
	}

	base := time.Now().UTC()

	ordersRaw, err := createDemoAsset(ctx, assetRepo, db, base.Add(-7*time.Hour), demoAssetSeed{
		Key:         "raw.orders",
		Type:        domain.AssetTypeTable,
		Owner:       "platform",
		Description: "Landing-zone order events with one partition per business day.",
		Tags:        []string{"bronze", "ingest"},
		Partition:   &domain.PartitionDefinition{Type: domain.PartitionTypeDaily, Timezone: "UTC"},
		Freshness:   &domain.AssetFreshnessPolicy{MaxLagSeconds: 1800},
		Active:      true,
		CreatedBy:   "demo-seed",
	})
	if err != nil {
		return err
	}

	customersDim, err := createDemoAsset(ctx, assetRepo, db, base.Add(-6*time.Hour), demoAssetSeed{
		Key:         "core.customers",
		Type:        domain.AssetTypeTable,
		Owner:       "growth",
		Description: "Canonical customer dimension enriched from CRM and support signals.",
		Tags:        []string{"silver", "dimension"},
		Freshness:   &domain.AssetFreshnessPolicy{MaxLagSeconds: 7200},
		Active:      true,
		CreatedBy:   "demo-seed",
	})
	if err != nil {
		return err
	}

	ordersModel, err := createDemoAsset(ctx, assetRepo, db, base.Add(-5*time.Hour), demoAssetSeed{
		Key:             "models.orders_enriched",
		Type:            domain.AssetTypeModel,
		Owner:           "analytics",
		Description:     "Order model that joins commerce events, customer segments, and fulfillment facts.",
		Tags:            []string{"gold", "finance"},
		Partition:       &domain.PartitionDefinition{Type: domain.PartitionTypeDaily, Timezone: "UTC"},
		Freshness:       &domain.AssetFreshnessPolicy{MaxLagSeconds: 900},
		Materialize:     &domain.AssetMaterializationPolicy{Mode: "INCREMENTAL", AllowConcurrent: true},
		AutoMaterialize: &domain.AssetAutoMaterializePolicy{Mode: "EAGER", MinIntervalSeconds: 300, RequireAllUpstreams: true, OnUpstreamMaterialized: true},
		IOProfile:       "warehouse/gold",
		Active:          true,
		CreatedBy:       "demo-seed",
	})
	if err != nil {
		return err
	}

	revenueMart, err := createDemoAsset(ctx, assetRepo, db, base.Add(-4*time.Hour), demoAssetSeed{
		Key:             "mart.daily_revenue",
		Type:            domain.AssetTypeOutput,
		Owner:           "finance",
		Description:     "Executive revenue mart powering dashboards, alerts, and partner exports.",
		Tags:            []string{"gold", "executive"},
		Partition:       &domain.PartitionDefinition{Type: domain.PartitionTypeDaily, Timezone: "UTC"},
		Freshness:       &domain.AssetFreshnessPolicy{MaxLagSeconds: 600},
		Materialize:     &domain.AssetMaterializationPolicy{Mode: "FULL_REFRESH", AllowConcurrent: false},
		AutoMaterialize: &domain.AssetAutoMaterializePolicy{Mode: "EAGER", MinIntervalSeconds: 600, RequireAllUpstreams: true, OnFreshnessBreach: true},
		IOProfile:       "warehouse/executive",
		Active:          true,
		CreatedBy:       "demo-seed",
	})
	if err != nil {
		return err
	}

	execSummary, err := createDemoAsset(ctx, assetRepo, db, base.Add(-3*time.Hour), demoAssetSeed{
		Key:         "analytics.exec_summary",
		Type:        domain.AssetTypeView,
		Owner:       "finance",
		Description: "Thin serving view for board-level daily KPIs.",
		Tags:        []string{"serve", "dashboard"},
		Active:      true,
		CreatedBy:   "demo-seed",
	})
	if err != nil {
		return err
	}

	churnFeatures, err := createDemoAsset(ctx, assetRepo, db, base.Add(-2*time.Hour), demoAssetSeed{
		Key:             "ml.churn_features",
		Type:            domain.AssetTypeModel,
		Owner:           "ml",
		Description:     "Feature table consumed by churn scoring notebooks and scheduled retraining.",
		Tags:            []string{"ml", "features"},
		Partition:       &domain.PartitionDefinition{Type: domain.PartitionTypeDaily, Timezone: "UTC"},
		Freshness:       &domain.AssetFreshnessPolicy{MaxLagSeconds: 14400},
		AutoMaterialize: &domain.AssetAutoMaterializePolicy{Mode: "LAZY", MinIntervalSeconds: 1800, RequireAllUpstreams: false, OnUpstreamMaterialized: true},
		IOProfile:       "warehouse/ml",
		Active:          true,
		CreatedBy:       "demo-seed",
	})
	if err != nil {
		return err
	}

	playbook, err := createDemoAsset(ctx, assetRepo, db, base.Add(-90*time.Minute), demoAssetSeed{
		Key:         "ops.incident_playbook",
		Type:        domain.AssetTypeNotebook,
		Owner:       "platform",
		Description: "Notebook operators use for ad hoc investigation and rollback checks.",
		Tags:        []string{"ops", "notebook"},
		Active:      false,
		CreatedBy:   "demo-seed",
	})
	if err != nil {
		return err
	}

	financePack, err := createDemoAsset(ctx, assetRepo, db, base.Add(-45*time.Minute), demoAssetSeed{
		Key:         "exports.finance_pack",
		Type:        domain.AssetTypeOutput,
		Owner:       "finance",
		Description: "Curated external delivery for FP&A and planning workflows.",
		Tags:        []string{"export", "partner"},
		Partition:   &domain.PartitionDefinition{Type: domain.PartitionTypeStatic, StaticKeys: []string{"current", "prior"}},
		Materialize: &domain.AssetMaterializationPolicy{Mode: "SNAPSHOT", AllowConcurrent: false},
		Active:      true,
		CreatedBy:   "demo-seed",
	})
	if err != nil {
		return err
	}

	for _, pair := range [][2]string{{ordersModel.ID, ordersRaw.ID}, {ordersModel.ID, customersDim.ID}, {revenueMart.ID, ordersModel.ID}, {execSummary.ID, revenueMart.ID}, {churnFeatures.ID, customersDim.ID}, {churnFeatures.ID, ordersModel.ID}, {financePack.ID, revenueMart.ID}, {playbook.ID, revenueMart.ID}} {
		if _, err := depRepo.Create(ctx, &domain.AssetDependency{AssetID: pair[0], UpstreamAssetID: pair[1], DependencyType: domain.DependencyTypeHard}); err != nil {
			return fmt.Errorf("create dependency: %w", err)
		}
	}

	for i := 0; i < 7; i++ {
		day := base.Add(time.Duration(-i) * 24 * time.Hour)
		key := day.Format("2006-01-02")
		status := "MATERIALIZED"
		if i == 1 {
			status = "FAILED"
		}
		if i == 3 {
			status = "MISSING"
		}
		if _, err := partitionRepo.Upsert(ctx, &domain.AssetPartition{AssetID: ordersModel.ID, PartitionKey: key, PartitionTime: &day, Status: status, LastMaterializedAt: &day, MetadataJSON: map[string]any{"records": 200000 - i*1700}}); err != nil {
			return fmt.Errorf("upsert model partition: %w", err)
		}
		martStatus := "MATERIALIZED"
		if i == 0 {
			martStatus = "STALE"
		}
		if _, err := partitionRepo.Upsert(ctx, &domain.AssetPartition{AssetID: revenueMart.ID, PartitionKey: key, PartitionTime: &day, Status: martStatus, LastMaterializedAt: &day, MetadataJSON: map[string]any{"sla": "10m"}}); err != nil {
			return fmt.Errorf("upsert mart partition: %w", err)
		}
	}

	modelRunSuccess, err := runRepo.CreateRun(ctx, &domain.AssetRun{AssetID: ordersModel.ID, Status: domain.AssetRunStatusQueued, TriggerType: domain.AssetTriggerTypeUpstreamUpdate, TriggeredBy: "reconciler", AttemptCount: 1, MaxAttempts: 2})
	if err != nil {
		return fmt.Errorf("create model success run: %w", err)
	}
	if err := runRepo.UpdateRunStarted(ctx, modelRunSuccess.ID); err != nil {
		return fmt.Errorf("start model success run: %w", err)
	}
	if err := runRepo.UpdateRunFinished(ctx, modelRunSuccess.ID, domain.AssetRunStatusSuccess, nil); err != nil {
		return fmt.Errorf("finish model success run: %w", err)
	}
	if err := setRunTimestamps(ctx, db, modelRunSuccess.ID, base.Add(-95*time.Minute), base.Add(-92*time.Minute), base.Add(-90*time.Minute)); err != nil {
		return err
	}

	modelPartitionKey := base.Format("2006-01-02")
	modelMatRows := int64(241390)
	modelSchema := "orders-v3"
	if _, err := runRepo.CreateMaterialization(ctx, &domain.AssetMaterialization{AssetID: ordersModel.ID, RunID: &modelRunSuccess.ID, PartitionKey: &modelPartitionKey, RowCount: &modelMatRows, SchemaHash: &modelSchema, MaterializedAt: base.Add(-90 * time.Minute), MetadataJSON: map[string]any{"drift": "none", "warehouse": "gold"}}); err != nil {
		return fmt.Errorf("create model materialization: %w", err)
	}

	martRunFailed, err := runRepo.CreateRun(ctx, &domain.AssetRun{AssetID: revenueMart.ID, Status: domain.AssetRunStatusQueued, TriggerType: domain.AssetTriggerTypeFreshnessBreach, TriggeredBy: "reconciler", AttemptCount: 1, MaxAttempts: 3})
	if err != nil {
		return fmt.Errorf("create mart failed run: %w", err)
	}
	if err := runRepo.UpdateRunStarted(ctx, martRunFailed.ID); err != nil {
		return fmt.Errorf("start mart failed run: %w", err)
	}
	failure := "warehouse timeout while compacting revenue partitions"
	if err := runRepo.UpdateRunFinished(ctx, martRunFailed.ID, domain.AssetRunStatusFailed, &failure); err != nil {
		return fmt.Errorf("finish mart failed run: %w", err)
	}
	if err := setRunTimestamps(ctx, db, martRunFailed.ID, base.Add(-35*time.Minute), base.Add(-34*time.Minute), base.Add(-31*time.Minute)); err != nil {
		return err
	}

	martRunSuccess, err := runRepo.CreateRun(ctx, &domain.AssetRun{AssetID: revenueMart.ID, Status: domain.AssetRunStatusQueued, TriggerType: domain.AssetTriggerTypeManual, TriggeredBy: "demo-admin", AttemptCount: 2, MaxAttempts: 3})
	if err != nil {
		return fmt.Errorf("create mart success run: %w", err)
	}
	if err := runRepo.UpdateRunStarted(ctx, martRunSuccess.ID); err != nil {
		return fmt.Errorf("start mart success run: %w", err)
	}
	if err := runRepo.UpdateRunFinished(ctx, martRunSuccess.ID, domain.AssetRunStatusSuccess, nil); err != nil {
		return fmt.Errorf("finish mart success run: %w", err)
	}
	if err := setRunTimestamps(ctx, db, martRunSuccess.ID, base.Add(-14*time.Minute), base.Add(-13*time.Minute), base.Add(-11*time.Minute)); err != nil {
		return err
	}

	martRows := int64(18342)
	martSchema := "revenue-v5"
	martPartitionKey := base.Format("2006-01-02")
	if _, err := runRepo.CreateMaterialization(ctx, &domain.AssetMaterialization{AssetID: revenueMart.ID, RunID: &martRunSuccess.ID, PartitionKey: &martPartitionKey, RowCount: &martRows, SchemaHash: &martSchema, MaterializedAt: base.Add(-11 * time.Minute), MetadataJSON: map[string]any{"export": "enabled", "currency": "USD"}}); err != nil {
		return fmt.Errorf("create mart materialization: %w", err)
	}

	rowCheck, err := checkRepo.CreateCheck(ctx, &domain.AssetCheck{AssetID: revenueMart.ID, Name: "row_count_delta", CheckType: "THRESHOLD", Severity: "ERROR", ConfigJSON: map[string]any{"max_drop_pct": 8}, Enabled: true})
	if err != nil {
		return fmt.Errorf("create row count check: %w", err)
	}
	if err := setRecordTimestamps(ctx, db, "asset_checks", rowCheck.ID, base.Add(-3*time.Hour), base.Add(-20*time.Minute)); err != nil {
		return err
	}
	passMsg := "within expected variance"
	if _, err := checkRepo.CreateCheckResult(ctx, &domain.AssetCheckResult{CheckID: rowCheck.ID, RunID: &martRunSuccess.ID, PartitionKey: &martPartitionKey, Status: "PASS", Message: &passMsg, MetricsJSON: map[string]any{"delta_pct": 1.2}}); err != nil {
		return fmt.Errorf("create check result: %w", err)
	}

	freshnessCheck, err := checkRepo.CreateCheck(ctx, &domain.AssetCheck{AssetID: churnFeatures.ID, Name: "feature_freshness", CheckType: "LAG", Severity: "WARN", ConfigJSON: map[string]any{"max_hours": 6}, Enabled: true})
	if err != nil {
		return fmt.Errorf("create freshness check: %w", err)
	}
	if err := setRecordTimestamps(ctx, db, "asset_checks", freshnessCheck.ID, base.Add(-2*time.Hour), base.Add(-2*time.Hour)); err != nil {
		return err
	}
	lagMsg := "upstream model late by 42 minutes"
	if _, err := checkRepo.CreateCheckResult(ctx, &domain.AssetCheckResult{CheckID: freshnessCheck.ID, Status: "FAIL", Message: &lagMsg, MetricsJSON: map[string]any{"lag_minutes": 42}}); err != nil {
		return fmt.Errorf("create lag result: %w", err)
	}

	backfill, err := backfillRepo.CreateRequest(ctx, &domain.BackfillRequest{AssetID: ordersModel.ID, PartitionFrom: base.Add(-3 * 24 * time.Hour).Format("2006-01-02"), PartitionTo: base.Add(-1 * 24 * time.Hour).Format("2006-01-02"), Status: domain.BackfillStatusRunning, RequestedBy: "demo-admin", MaxParallelism: 2})
	if err != nil {
		return fmt.Errorf("create backfill request: %w", err)
	}
	if err := setBackfillRequestTimestamps(ctx, db, backfill.ID, base.Add(-70*time.Minute), base.Add(-68*time.Minute), nil); err != nil {
		return err
	}
	for i, key := range []string{base.Add(-3 * 24 * time.Hour).Format("2006-01-02"), base.Add(-2 * 24 * time.Hour).Format("2006-01-02"), base.Add(-1 * 24 * time.Hour).Format("2006-01-02")} {
		status := domain.BackfillStatusPending
		var runID *string
		if i == 0 {
			status = domain.BackfillStatusSuccess
			runID = &modelRunSuccess.ID
		}
		if i == 1 {
			status = domain.BackfillStatusRunning
		}
		if _, err := backfillRepo.CreateSlice(ctx, &domain.BackfillSlice{RequestID: backfill.ID, AssetID: ordersModel.ID, PartitionKey: key, Status: status, RunID: runID, AttemptCount: i + 1, MaxAttempts: 2}); err != nil {
			return fmt.Errorf("create backfill slice: %w", err)
		}
	}

	fmt.Printf("seeded 8 demo assets into %s\n", cfg.MetaDBPath)
	return nil
}

type demoAssetSeed struct {
	Key             string
	Type            string
	Owner           string
	Description     string
	Tags            []string
	Partition       *domain.PartitionDefinition
	Freshness       *domain.AssetFreshnessPolicy
	Materialize     *domain.AssetMaterializationPolicy
	AutoMaterialize *domain.AssetAutoMaterializePolicy
	IOProfile       string
	Active          bool
	CreatedBy       string
}

func createDemoAsset(ctx context.Context, repo *repository.DataAssetRepo, db *sql.DB, stamp time.Time, seed demoAssetSeed) (*domain.DataAsset, error) {
	asset, err := repo.Create(ctx, &domain.DataAsset{
		AssetKey:              seed.Key,
		AssetType:             seed.Type,
		Owner:                 seed.Owner,
		Description:           seed.Description,
		Tags:                  seed.Tags,
		PartitionDefinition:   seed.Partition,
		FreshnessPolicy:       seed.Freshness,
		MaterializationPolicy: seed.Materialize,
		AutoMaterializePolicy: seed.AutoMaterialize,
		IOProfile:             seed.IOProfile,
		IsActive:              seed.Active,
		CreatedBy:             seed.CreatedBy,
	})
	if err != nil {
		return nil, fmt.Errorf("create asset %s: %w", seed.Key, err)
	}
	if err := setRecordTimestamps(ctx, db, "data_assets", asset.ID, stamp, stamp); err != nil {
		return nil, err
	}
	return repo.GetByID(ctx, asset.ID)
}

func setRecordTimestamps(ctx context.Context, db *sql.DB, table string, id string, createdAt time.Time, updatedAt time.Time) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET created_at = ?, updated_at = ? WHERE id = ?", table), createdAt, updatedAt, id); err != nil {
		return fmt.Errorf("stamp %s %s: %w", table, id, err)
	}
	return nil
}

func setRunTimestamps(ctx context.Context, db *sql.DB, id string, createdAt time.Time, startedAt time.Time, finishedAt time.Time) error {
	if _, err := db.ExecContext(ctx, `UPDATE asset_runs SET created_at = ?, updated_at = ?, started_at = ?, finished_at = ? WHERE id = ?`, createdAt, finishedAt, startedAt, finishedAt, id); err != nil {
		return fmt.Errorf("stamp asset_run %s: %w", id, err)
	}
	return nil
}

func setBackfillRequestTimestamps(ctx context.Context, db *sql.DB, id string, createdAt time.Time, startedAt time.Time, finishedAt *time.Time) error {
	if _, err := db.ExecContext(ctx, `UPDATE backfill_requests SET created_at = ?, started_at = ?, finished_at = ? WHERE id = ?`, createdAt, startedAt, finishedAt, id); err != nil {
		return fmt.Errorf("stamp backfill_request %s: %w", id, err)
	}
	return nil
}
