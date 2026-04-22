//nolint:revive // repository constructors and methods intentionally exported for wiring.
package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
)

var (
	_ domain.DataAssetRepository       = (*DataAssetRepo)(nil)
	_ domain.AssetDependencyRepository = (*AssetDependencyRepo)(nil)
	_ domain.AssetPartitionRepository  = (*AssetPartitionRepo)(nil)
	_ domain.AssetRunRepository        = (*AssetRunRepo)(nil)
	_ domain.AssetCheckRepository      = (*AssetCheckRepo)(nil)
)

type DataAssetRepo struct {
	db *sql.DB
}

func NewDataAssetRepo(db *sql.DB) *DataAssetRepo {
	return &DataAssetRepo{db: db}
}

func (r *DataAssetRepo) Create(ctx context.Context, a *domain.DataAsset) (*domain.DataAsset, error) {
	if a == nil {
		return nil, domain.ErrValidation("asset is required")
	}
	if strings.TrimSpace(a.AssetKey) == "" {
		return nil, domain.ErrValidation("asset_key is required")
	}
	if strings.TrimSpace(a.AssetType) == "" {
		return nil, domain.ErrValidation("asset_type is required")
	}
	id := a.ID
	if id == "" {
		id = newID()
	}
	tagsJSON, err := json.Marshal(a.Tags)
	if err != nil {
		return nil, fmt.Errorf("marshal tags: %w", err)
	}
	schemaJSON, err := json.Marshal(a.SchemaJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal schema_json: %w", err)
	}
	partitionJSON, err := marshalNullableJSON(a.PartitionDefinition)
	if err != nil {
		return nil, fmt.Errorf("marshal partition_definition_json: %w", err)
	}
	freshnessJSON, err := marshalNullableJSON(a.FreshnessPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshal freshness_policy_json: %w", err)
	}
	materializationJSON, err := marshalNullableJSON(a.MaterializationPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshal materialization_policy_json: %w", err)
	}
	autoJSON, err := marshalNullableJSON(a.AutoMaterializePolicy)
	if err != nil {
		return nil, fmt.Errorf("marshal auto_materialize_policy_json: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO data_assets (
			id, asset_key, asset_type, owner, description, tags_json, schema_json,
			partition_definition_json, freshness_policy_json, materialization_policy_json,
			auto_materialize_policy_json, io_profile, is_active, created_by
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		id,
		a.AssetKey,
		a.AssetType,
		a.Owner,
		a.Description,
		string(tagsJSON),
		string(schemaJSON),
		partitionJSON,
		freshnessJSON,
		materializationJSON,
		autoJSON,
		a.IOProfile,
		boolToInt(a.IsActive),
		a.CreatedBy,
	)
	if err != nil {
		return nil, mapDBError(err)
	}

	return r.GetByID(ctx, id)
}

func (r *DataAssetRepo) GetByID(ctx context.Context, id string) (*domain.DataAsset, error) {
	return r.getOne(ctx, `
		SELECT id, asset_key, asset_type, owner, description, tags_json, schema_json,
		       partition_definition_json, freshness_policy_json, materialization_policy_json,
		       auto_materialize_policy_json, io_profile, is_active, created_by, created_at, updated_at
		FROM data_assets
		WHERE id = ?
	`, id)
}

func (r *DataAssetRepo) GetByKey(ctx context.Context, assetKey string) (*domain.DataAsset, error) {
	return r.getOne(ctx, `
		SELECT id, asset_key, asset_type, owner, description, tags_json, schema_json,
		       partition_definition_json, freshness_policy_json, materialization_policy_json,
		       auto_materialize_policy_json, io_profile, is_active, created_by, created_at, updated_at
		FROM data_assets
		WHERE asset_key = ?
	`, assetKey)
}

func (r *DataAssetRepo) List(ctx context.Context, filter domain.AssetFilter) ([]domain.DataAsset, int64, error) {
	var (
		args []any
		w    = "WHERE 1=1"
	)
	if filter.Owner != nil {
		w += " AND owner = ?"
		args = append(args, *filter.Owner)
	}
	if filter.AssetType != nil {
		w += " AND asset_type = ?"
		args = append(args, *filter.AssetType)
	}
	if filter.IsActive != nil {
		w += " AND is_active = ?"
		args = append(args, boolToInt(*filter.IsActive))
	}

	countSQL := "SELECT COUNT(*) FROM data_assets " + w
	var total int64
	if err := r.db.QueryRowContext(ctx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}

	//nolint:gosec // query fragments are assembled from fixed, parameterized clauses only.
	listSQL := `
		SELECT id, asset_key, asset_type, owner, description, tags_json, schema_json,
		       partition_definition_json, freshness_policy_json, materialization_policy_json,
		       auto_materialize_policy_json, io_profile, is_active, created_by, created_at, updated_at
		FROM data_assets ` + w + `
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?`
	args = append(args, filter.Page.Limit(), filter.Page.Offset())

	rows, err := r.db.QueryContext(ctx, listSQL, args...)
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.DataAsset, 0)
	for rows.Next() {
		asset, scanErr := scanDataAsset(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		out = append(out, *asset)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

func (r *DataAssetRepo) Update(ctx context.Context, id string, a *domain.DataAsset) (*domain.DataAsset, error) {
	if a == nil {
		return nil, domain.ErrValidation("asset is required")
	}
	tagsJSON, err := json.Marshal(a.Tags)
	if err != nil {
		return nil, fmt.Errorf("marshal tags: %w", err)
	}
	schemaJSON, err := json.Marshal(a.SchemaJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal schema_json: %w", err)
	}
	partitionJSON, err := marshalNullableJSON(a.PartitionDefinition)
	if err != nil {
		return nil, fmt.Errorf("marshal partition_definition_json: %w", err)
	}
	freshnessJSON, err := marshalNullableJSON(a.FreshnessPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshal freshness_policy_json: %w", err)
	}
	materializationJSON, err := marshalNullableJSON(a.MaterializationPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshal materialization_policy_json: %w", err)
	}
	autoJSON, err := marshalNullableJSON(a.AutoMaterializePolicy)
	if err != nil {
		return nil, fmt.Errorf("marshal auto_materialize_policy_json: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
		UPDATE data_assets
		SET asset_key = ?, asset_type = ?, owner = ?, description = ?, tags_json = ?, schema_json = ?,
		    partition_definition_json = ?, freshness_policy_json = ?, materialization_policy_json = ?,
		    auto_materialize_policy_json = ?, io_profile = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`,
		a.AssetKey,
		a.AssetType,
		a.Owner,
		a.Description,
		string(tagsJSON),
		string(schemaJSON),
		partitionJSON,
		freshnessJSON,
		materializationJSON,
		autoJSON,
		a.IOProfile,
		boolToInt(a.IsActive),
		id,
	)
	if err != nil {
		return nil, mapDBError(err)
	}

	return r.GetByID(ctx, id)
}

func (r *DataAssetRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM data_assets WHERE id = ?`, id)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("asset %q not found", id)
	}
	return nil
}

func (r *DataAssetRepo) getOne(ctx context.Context, query string, args ...any) (*domain.DataAsset, error) {
	row := r.db.QueryRowContext(ctx, query, args...)
	asset, err := scanDataAsset(row)
	if err != nil {
		return nil, mapDBError(err)
	}
	return asset, nil
}

type AssetDependencyRepo struct {
	db *sql.DB
}

func NewAssetDependencyRepo(db *sql.DB) *AssetDependencyRepo {
	return &AssetDependencyRepo{db: db}
}

func (r *AssetDependencyRepo) Create(ctx context.Context, d *domain.AssetDependency) (*domain.AssetDependency, error) {
	if d == nil {
		return nil, domain.ErrValidation("dependency is required")
	}
	id := d.ID
	if id == "" {
		id = newID()
	}
	if d.DependencyType == "" {
		d.DependencyType = domain.DependencyTypeHard
	}
	mappingJSON, err := json.Marshal(d.PartitionMappingJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal partition_mapping_json: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO asset_dependencies (id, asset_id, upstream_asset_id, dependency_type, partition_mapping_json)
		VALUES (?, ?, ?, ?, ?)
	`, id, d.AssetID, d.UpstreamAssetID, d.DependencyType, string(mappingJSON))
	if err != nil {
		return nil, mapDBError(err)
	}

	return &domain.AssetDependency{
		ID:                   id,
		AssetID:              d.AssetID,
		UpstreamAssetID:      d.UpstreamAssetID,
		DependencyType:       d.DependencyType,
		PartitionMappingJSON: d.PartitionMappingJSON,
	}, nil
}

func (r *AssetDependencyRepo) ListUpstream(ctx context.Context, assetID string) ([]domain.AssetDependency, error) {
	return r.listByQuery(ctx, `
		SELECT id, asset_id, upstream_asset_id, dependency_type, partition_mapping_json, created_at
		FROM asset_dependencies
		WHERE asset_id = ?
		ORDER BY created_at ASC
	`, assetID)
}

func (r *AssetDependencyRepo) ListDownstream(ctx context.Context, upstreamAssetID string) ([]domain.AssetDependency, error) {
	return r.listByQuery(ctx, `
		SELECT id, asset_id, upstream_asset_id, dependency_type, partition_mapping_json, created_at
		FROM asset_dependencies
		WHERE upstream_asset_id = ?
		ORDER BY created_at ASC
	`, upstreamAssetID)
}

func (r *AssetDependencyRepo) Delete(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM asset_dependencies WHERE id = ?`, id)
	return mapDBError(err)
}

func (r *AssetDependencyRepo) DeleteByAsset(ctx context.Context, assetID string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM asset_dependencies WHERE asset_id = ?`, assetID)
	return mapDBError(err)
}

func (r *AssetDependencyRepo) listByQuery(ctx context.Context, query string, arg string) ([]domain.AssetDependency, error) {
	rows, err := r.db.QueryContext(ctx, query, arg)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.AssetDependency, 0)
	for rows.Next() {
		var (
			d       domain.AssetDependency
			mapping string
		)
		if err := rows.Scan(&d.ID, &d.AssetID, &d.UpstreamAssetID, &d.DependencyType, &mapping, &d.CreatedAt); err != nil {
			return nil, err
		}
		if mapping != "" {
			if err := json.Unmarshal([]byte(mapping), &d.PartitionMappingJSON); err != nil {
				return nil, fmt.Errorf("unmarshal partition_mapping_json: %w", err)
			}
		}
		if d.PartitionMappingJSON == nil {
			d.PartitionMappingJSON = map[string]any{}
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type AssetPartitionRepo struct {
	db *sql.DB
}

func NewAssetPartitionRepo(db *sql.DB) *AssetPartitionRepo {
	return &AssetPartitionRepo{db: db}
}

func (r *AssetPartitionRepo) Upsert(ctx context.Context, p *domain.AssetPartition) (*domain.AssetPartition, error) {
	if p == nil {
		return nil, domain.ErrValidation("partition is required")
	}
	id := p.ID
	if id == "" {
		id = newID()
	}
	metadataJSON, err := json.Marshal(p.MetadataJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata_json: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO asset_partitions (id, asset_id, partition_key, partition_time, status, last_materialized_at, metadata_json)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(asset_id, partition_key)
		DO UPDATE SET
			partition_time = excluded.partition_time,
			status = excluded.status,
			last_materialized_at = excluded.last_materialized_at,
			metadata_json = excluded.metadata_json,
			updated_at = CURRENT_TIMESTAMP
	`, id, p.AssetID, p.PartitionKey, nullTimeFromPtr(p.PartitionTime), p.Status, nullTimeFromPtr(p.LastMaterializedAt), string(metadataJSON))
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByKey(ctx, p.AssetID, p.PartitionKey)
}

func (r *AssetPartitionRepo) GetByKey(ctx context.Context, assetID, partitionKey string) (*domain.AssetPartition, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, asset_id, partition_key, partition_time, status, last_materialized_at, metadata_json, created_at, updated_at
		FROM asset_partitions
		WHERE asset_id = ? AND partition_key = ?
	`, assetID, partitionKey)
	return scanAssetPartition(row)
}

func (r *AssetPartitionRepo) ListByAsset(ctx context.Context, assetID string, page domain.PageRequest) ([]domain.AssetPartition, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM asset_partitions WHERE asset_id = ?`, assetID).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, asset_id, partition_key, partition_time, status, last_materialized_at, metadata_json, created_at, updated_at
		FROM asset_partitions
		WHERE asset_id = ?
		ORDER BY partition_key DESC
		LIMIT ? OFFSET ?
	`, assetID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.AssetPartition, 0)
	for rows.Next() {
		partition, scanErr := scanAssetPartition(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		out = append(out, *partition)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

func (r *AssetPartitionRepo) UpdateStatus(ctx context.Context, assetID, partitionKey, status string, metadata map[string]any, materializedAt *time.Time) error {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		UPDATE asset_partitions
		SET status = ?, metadata_json = ?, last_materialized_at = ?, updated_at = CURRENT_TIMESTAMP
		WHERE asset_id = ? AND partition_key = ?
	`, status, string(metadataJSON), nullTimeFromPtr(materializedAt), assetID, partitionKey)
	return mapDBError(err)
}

type AssetRunRepo struct {
	db *sql.DB
}

func NewAssetRunRepo(db *sql.DB) *AssetRunRepo {
	return &AssetRunRepo{db: db}
}

func (r *AssetRunRepo) CreateRun(ctx context.Context, run *domain.AssetRun) (*domain.AssetRun, error) {
	if run == nil {
		return nil, domain.ErrValidation("run is required")
	}
	id := run.ID
	if id == "" {
		id = newID()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO asset_runs (id, asset_id, run_group_id, partition_key, partition_from, partition_to, status, trigger_type, triggered_by, attempt_count, max_attempts)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, id, run.AssetID, nullStrFromPtr(run.RunGroupID), nullStrFromPtr(run.PartitionKey), nullStrFromPtr(run.PartitionFrom), nullStrFromPtr(run.PartitionTo), run.Status, run.TriggerType, run.TriggeredBy, run.AttemptCount, run.MaxAttempts)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetRunByID(ctx, id)
}

func (r *AssetRunRepo) GetRunByID(ctx context.Context, id string) (*domain.AssetRun, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, asset_id, run_group_id, partition_key, partition_from, partition_to, status, trigger_type, triggered_by,
		       attempt_count, max_attempts, started_at, finished_at, error_message, created_at, updated_at
		FROM asset_runs
		WHERE id = ?
	`, id)
	return scanAssetRun(row)
}

func (r *AssetRunRepo) ListRuns(ctx context.Context, filter domain.AssetRunFilter) ([]domain.AssetRun, int64, error) {
	var (
		args []any
		w    = "WHERE 1=1"
	)
	if filter.AssetID != nil {
		w += " AND asset_id = ?"
		args = append(args, *filter.AssetID)
	}
	if filter.RunGroupID != nil {
		w += " AND run_group_id = ?"
		args = append(args, *filter.RunGroupID)
	}
	if filter.Status != nil {
		w += " AND status = ?"
		args = append(args, *filter.Status)
	}

	var total int64
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM asset_runs "+w, args...).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}

	//nolint:gosec // query fragments are assembled from fixed, parameterized clauses only.
	q := `
		SELECT id, asset_id, run_group_id, partition_key, partition_from, partition_to, status, trigger_type, triggered_by,
		       attempt_count, max_attempts, started_at, finished_at, error_message, created_at, updated_at
		FROM asset_runs ` + w + `
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?`
	args = append(args, filter.Page.Limit(), filter.Page.Offset())
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.AssetRun, 0)
	for rows.Next() {
		run, scanErr := scanAssetRun(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		out = append(out, *run)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

func (r *AssetRunRepo) UpdateRunStarted(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE asset_runs
		SET status = ?, started_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, domain.AssetRunStatusRunning, id)
	return mapDBError(err)
}

func (r *AssetRunRepo) UpdateRunFinished(ctx context.Context, id string, status string, errMsg *string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE asset_runs
		SET status = ?, error_message = ?, finished_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, status, nullStrFromPtr(errMsg), id)
	return mapDBError(err)
}

func (r *AssetRunRepo) UpdateRunRetrying(ctx context.Context, id string, attempt int, errMsg *string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE asset_runs
		SET status = ?, attempt_count = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, domain.AssetRunStatusRetrying, attempt, nullStrFromPtr(errMsg), id)
	return mapDBError(err)
}

func (r *AssetRunRepo) CreateRunEvent(ctx context.Context, event *domain.AssetRunEvent) (*domain.AssetRunEvent, error) {
	if event == nil {
		return nil, domain.ErrValidation("event is required")
	}
	metadataJSON, err := json.Marshal(event.MetadataJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata_json: %w", err)
	}
	checksJSON, err := json.Marshal(event.CheckResultsJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal check_results_json: %w", err)
	}
	statsJSON, err := json.Marshal(event.StatsJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal stats_json: %w", err)
	}

	res, err := r.db.ExecContext(ctx, `
		INSERT INTO asset_run_events (run_id, event_type, event_at, message, metadata_json, check_results_json, stats_json)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, event.RunID, event.EventType, event.EventAt, nullStrFromPtr(event.Message), string(metadataJSON), string(checksJSON), string(statsJSON))
	if err != nil {
		return nil, mapDBError(err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	event.ID = id
	return event, nil
}

func (r *AssetRunRepo) ListRunEvents(ctx context.Context, runID string, page domain.PageRequest) ([]domain.AssetRunEvent, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM asset_run_events WHERE run_id = ?`, runID).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, run_id, event_type, event_at, message, metadata_json, check_results_json, stats_json, created_at
		FROM asset_run_events
		WHERE run_id = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`, runID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.AssetRunEvent, 0)
	for rows.Next() {
		event, scanErr := scanAssetRunEvent(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		out = append(out, *event)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

func (r *AssetRunRepo) CreateMaterialization(ctx context.Context, m *domain.AssetMaterialization) (*domain.AssetMaterialization, error) {
	if m == nil {
		return nil, domain.ErrValidation("materialization is required")
	}
	id := m.ID
	if id == "" {
		id = newID()
	}
	metadataJSON, err := json.Marshal(m.MetadataJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata_json: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO asset_materializations (id, asset_id, run_id, partition_key, metadata_json, row_count, schema_hash, materialized_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, id, m.AssetID, nullStrFromPtr(m.RunID), nullStrFromPtr(m.PartitionKey), string(metadataJSON), nullInt64Ptr(m.RowCount), nullStrFromPtr(m.SchemaHash), m.MaterializedAt)
	if err != nil {
		return nil, mapDBError(err)
	}
	m.ID = id
	return m, nil
}

func (r *AssetRunRepo) ListMaterializationsByAsset(ctx context.Context, assetID string, page domain.PageRequest) ([]domain.AssetMaterialization, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM asset_materializations WHERE asset_id = ?`, assetID).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, asset_id, run_id, partition_key, metadata_json, row_count, schema_hash, materialized_at, created_at
		FROM asset_materializations
		WHERE asset_id = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`, assetID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.AssetMaterialization, 0)
	for rows.Next() {
		mat, scanErr := scanAssetMaterialization(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		out = append(out, *mat)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

type AssetCheckRepo struct {
	db *sql.DB
}

func NewAssetCheckRepo(db *sql.DB) *AssetCheckRepo {
	return &AssetCheckRepo{db: db}
}

func (r *AssetCheckRepo) CreateCheck(ctx context.Context, c *domain.AssetCheck) (*domain.AssetCheck, error) {
	if c == nil {
		return nil, domain.ErrValidation("check is required")
	}
	id := c.ID
	if id == "" {
		id = newID()
	}
	configJSON, err := json.Marshal(c.ConfigJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal config_json: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO asset_checks (id, asset_id, name, check_type, severity, config_json, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, id, c.AssetID, c.Name, c.CheckType, c.Severity, string(configJSON), boolToInt(c.Enabled))
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetCheckByID(ctx, id)
}

func (r *AssetCheckRepo) GetCheckByID(ctx context.Context, id string) (*domain.AssetCheck, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, asset_id, name, check_type, severity, config_json, enabled, created_at, updated_at
		FROM asset_checks
		WHERE id = ?
	`, id)
	return scanAssetCheck(row)
}

func (r *AssetCheckRepo) ListChecksByAsset(ctx context.Context, assetID string) ([]domain.AssetCheck, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, asset_id, name, check_type, severity, config_json, enabled, created_at, updated_at
		FROM asset_checks
		WHERE asset_id = ?
		ORDER BY name ASC
	`, assetID)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.AssetCheck, 0)
	for rows.Next() {
		check, scanErr := scanAssetCheck(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, *check)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *AssetCheckRepo) UpdateCheck(ctx context.Context, id string, c *domain.AssetCheck) (*domain.AssetCheck, error) {
	if c == nil {
		return nil, domain.ErrValidation("check is required")
	}
	configJSON, err := json.Marshal(c.ConfigJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal config_json: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		UPDATE asset_checks
		SET name = ?, check_type = ?, severity = ?, config_json = ?, enabled = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, c.Name, c.CheckType, c.Severity, string(configJSON), boolToInt(c.Enabled), id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetCheckByID(ctx, id)
}

func (r *AssetCheckRepo) DeleteCheck(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM asset_checks WHERE id = ?`, id)
	return mapDBError(err)
}

func (r *AssetCheckRepo) CreateCheckResult(ctx context.Context, result *domain.AssetCheckResult) (*domain.AssetCheckResult, error) {
	if result == nil {
		return nil, domain.ErrValidation("check result is required")
	}
	id := result.ID
	if id == "" {
		id = newID()
	}
	metricsJSON, err := json.Marshal(result.MetricsJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal metrics_json: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO asset_check_results (id, check_id, run_id, partition_key, status, message, metrics_json)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, id, result.CheckID, nullStrFromPtr(result.RunID), nullStrFromPtr(result.PartitionKey), result.Status, nullStrFromPtr(result.Message), string(metricsJSON))
	if err != nil {
		return nil, mapDBError(err)
	}
	result.ID = id
	return result, nil
}

func (r *AssetCheckRepo) ListCheckResults(ctx context.Context, checkID string, page domain.PageRequest) ([]domain.AssetCheckResult, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM asset_check_results WHERE check_id = ?`, checkID).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, check_id, run_id, partition_key, status, message, metrics_json, created_at
		FROM asset_check_results
		WHERE check_id = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`, checkID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.AssetCheckResult, 0)
	for rows.Next() {
		result, scanErr := scanAssetCheckResult(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		out = append(out, *result)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

func scanDataAsset(scanner interface {
	Scan(dest ...any) error
}) (*domain.DataAsset, error) {
	var (
		asset                                                       domain.DataAsset
		tagsJSON, schemaJSON                                        string
		partitionJSON, freshnessJSON, materializationJSON, autoJSON sql.NullString
		isActive                                                    int64
	)
	err := scanner.Scan(
		&asset.ID,
		&asset.AssetKey,
		&asset.AssetType,
		&asset.Owner,
		&asset.Description,
		&tagsJSON,
		&schemaJSON,
		&partitionJSON,
		&freshnessJSON,
		&materializationJSON,
		&autoJSON,
		&asset.IOProfile,
		&isActive,
		&asset.CreatedBy,
		&asset.CreatedAt,
		&asset.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	asset.IsActive = isActive == 1
	if err := json.Unmarshal([]byte(tagsJSON), &asset.Tags); err != nil {
		return nil, fmt.Errorf("unmarshal tags_json: %w", err)
	}
	if err := json.Unmarshal([]byte(schemaJSON), &asset.SchemaJSON); err != nil {
		return nil, fmt.Errorf("unmarshal schema_json: %w", err)
	}
	if err := unmarshalNullableJSON(partitionJSON, &asset.PartitionDefinition); err != nil {
		return nil, fmt.Errorf("unmarshal partition_definition_json: %w", err)
	}
	if err := unmarshalNullableJSON(freshnessJSON, &asset.FreshnessPolicy); err != nil {
		return nil, fmt.Errorf("unmarshal freshness_policy_json: %w", err)
	}
	if err := unmarshalNullableJSON(materializationJSON, &asset.MaterializationPolicy); err != nil {
		return nil, fmt.Errorf("unmarshal materialization_policy_json: %w", err)
	}
	if err := unmarshalNullableJSON(autoJSON, &asset.AutoMaterializePolicy); err != nil {
		return nil, fmt.Errorf("unmarshal auto_materialize_policy_json: %w", err)
	}
	if asset.Tags == nil {
		asset.Tags = []string{}
	}
	if asset.SchemaJSON == nil {
		asset.SchemaJSON = map[string]any{}
	}
	return &asset, nil
}

func scanAssetPartition(scanner interface {
	Scan(dest ...any) error
}) (*domain.AssetPartition, error) {
	var (
		partition                         domain.AssetPartition
		partitionTime, lastMaterializedAt sql.NullTime
		metadataJSON                      string
	)
	err := scanner.Scan(
		&partition.ID,
		&partition.AssetID,
		&partition.PartitionKey,
		&partitionTime,
		&partition.Status,
		&lastMaterializedAt,
		&metadataJSON,
		&partition.CreatedAt,
		&partition.UpdatedAt,
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	if partitionTime.Valid {
		t := partitionTime.Time
		partition.PartitionTime = &t
	}
	if lastMaterializedAt.Valid {
		t := lastMaterializedAt.Time
		partition.LastMaterializedAt = &t
	}
	if metadataJSON != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &partition.MetadataJSON); err != nil {
			return nil, fmt.Errorf("unmarshal metadata_json: %w", err)
		}
	}
	if partition.MetadataJSON == nil {
		partition.MetadataJSON = map[string]any{}
	}
	return &partition, nil
}

func scanAssetRun(scanner interface {
	Scan(dest ...any) error
}) (*domain.AssetRun, error) {
	var (
		run                                                                domain.AssetRun
		runGroupID, partitionKey, partitionFrom, partitionTo, errorMessage sql.NullString
		startedAt, finishedAt                                              sql.NullTime
	)
	err := scanner.Scan(
		&run.ID,
		&run.AssetID,
		&runGroupID,
		&partitionKey,
		&partitionFrom,
		&partitionTo,
		&run.Status,
		&run.TriggerType,
		&run.TriggeredBy,
		&run.AttemptCount,
		&run.MaxAttempts,
		&startedAt,
		&finishedAt,
		&errorMessage,
		&run.CreatedAt,
		&run.UpdatedAt,
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	if runGroupID.Valid {
		s := runGroupID.String
		run.RunGroupID = &s
	}
	if partitionKey.Valid {
		s := partitionKey.String
		run.PartitionKey = &s
	}
	if partitionFrom.Valid {
		s := partitionFrom.String
		run.PartitionFrom = &s
	}
	if partitionTo.Valid {
		s := partitionTo.String
		run.PartitionTo = &s
	}
	if startedAt.Valid {
		t := startedAt.Time
		run.StartedAt = &t
	}
	if finishedAt.Valid {
		t := finishedAt.Time
		run.FinishedAt = &t
	}
	if errorMessage.Valid {
		s := errorMessage.String
		run.ErrorMessage = &s
	}
	return &run, nil
}

func scanAssetRunEvent(scanner interface {
	Scan(dest ...any) error
}) (*domain.AssetRunEvent, error) {
	var (
		event                               domain.AssetRunEvent
		message                             sql.NullString
		metadataJSON, checksJSON, statsJSON string
	)
	err := scanner.Scan(
		&event.ID,
		&event.RunID,
		&event.EventType,
		&event.EventAt,
		&message,
		&metadataJSON,
		&checksJSON,
		&statsJSON,
		&event.CreatedAt,
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	if message.Valid {
		s := message.String
		event.Message = &s
	}
	if metadataJSON != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &event.MetadataJSON); err != nil {
			return nil, fmt.Errorf("unmarshal metadata_json: %w", err)
		}
	}
	if checksJSON != "" {
		if err := json.Unmarshal([]byte(checksJSON), &event.CheckResultsJSON); err != nil {
			return nil, fmt.Errorf("unmarshal check_results_json: %w", err)
		}
	}
	if statsJSON != "" {
		if err := json.Unmarshal([]byte(statsJSON), &event.StatsJSON); err != nil {
			return nil, fmt.Errorf("unmarshal stats_json: %w", err)
		}
	}
	if event.MetadataJSON == nil {
		event.MetadataJSON = map[string]any{}
	}
	if event.CheckResultsJSON == nil {
		event.CheckResultsJSON = map[string]any{}
	}
	if event.StatsJSON == nil {
		event.StatsJSON = map[string]any{}
	}
	return &event, nil
}

func scanAssetMaterialization(scanner interface {
	Scan(dest ...any) error
}) (*domain.AssetMaterialization, error) {
	var (
		mat                             domain.AssetMaterialization
		runID, partitionKey, schemaHash sql.NullString
		rowCount                        sql.NullInt64
		metadataJSON                    string
	)
	err := scanner.Scan(
		&mat.ID,
		&mat.AssetID,
		&runID,
		&partitionKey,
		&metadataJSON,
		&rowCount,
		&schemaHash,
		&mat.MaterializedAt,
		&mat.CreatedAt,
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	if runID.Valid {
		s := runID.String
		mat.RunID = &s
	}
	if partitionKey.Valid {
		s := partitionKey.String
		mat.PartitionKey = &s
	}
	if schemaHash.Valid {
		s := schemaHash.String
		mat.SchemaHash = &s
	}
	if rowCount.Valid {
		v := rowCount.Int64
		mat.RowCount = &v
	}
	if metadataJSON != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &mat.MetadataJSON); err != nil {
			return nil, fmt.Errorf("unmarshal metadata_json: %w", err)
		}
	}
	if mat.MetadataJSON == nil {
		mat.MetadataJSON = map[string]any{}
	}
	return &mat, nil
}

func scanAssetCheck(scanner interface {
	Scan(dest ...any) error
}) (*domain.AssetCheck, error) {
	var (
		check      domain.AssetCheck
		enabled    int64
		configJSON string
	)
	err := scanner.Scan(
		&check.ID,
		&check.AssetID,
		&check.Name,
		&check.CheckType,
		&check.Severity,
		&configJSON,
		&enabled,
		&check.CreatedAt,
		&check.UpdatedAt,
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	check.Enabled = enabled == 1
	if configJSON != "" {
		if err := json.Unmarshal([]byte(configJSON), &check.ConfigJSON); err != nil {
			return nil, fmt.Errorf("unmarshal config_json: %w", err)
		}
	}
	if check.ConfigJSON == nil {
		check.ConfigJSON = map[string]any{}
	}
	return &check, nil
}

func scanAssetCheckResult(scanner interface {
	Scan(dest ...any) error
}) (*domain.AssetCheckResult, error) {
	var (
		result                       domain.AssetCheckResult
		runID, partitionKey, message sql.NullString
		metricsJSON                  string
	)
	err := scanner.Scan(
		&result.ID,
		&result.CheckID,
		&runID,
		&partitionKey,
		&result.Status,
		&message,
		&metricsJSON,
		&result.CreatedAt,
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	if runID.Valid {
		s := runID.String
		result.RunID = &s
	}
	if partitionKey.Valid {
		s := partitionKey.String
		result.PartitionKey = &s
	}
	if message.Valid {
		s := message.String
		result.Message = &s
	}
	if metricsJSON != "" {
		if err := json.Unmarshal([]byte(metricsJSON), &result.MetricsJSON); err != nil {
			return nil, fmt.Errorf("unmarshal metrics_json: %w", err)
		}
	}
	if result.MetricsJSON == nil {
		result.MetricsJSON = map[string]any{}
	}
	return &result, nil
}

func marshalNullableJSON(v any) (sql.NullString, error) {
	if v == nil {
		return sql.NullString{}, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return sql.NullString{}, err
	}
	return sql.NullString{String: string(b), Valid: true}, nil
}

func unmarshalNullableJSON(raw sql.NullString, target any) error {
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return nil
	}
	return json.Unmarshal([]byte(raw.String), target)
}

func nullTimeFromPtr(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: t.UTC(), Valid: true}
}
