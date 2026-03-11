using TypeSpec.Http;

namespace Duck;

enum AssetRunStatus {
  QUEUED,
  PLANNING,
  RUNNING,
  RETRYING,
  SUCCESS,
  FAILED,
  CANCELLED,
  SKIPPED,
  STALE,
}

enum AssetTriggerType {
  MANUAL,
  SCHEDULED,
  UPSTREAM_UPDATE,
  FRESHNESS_BREACH,
  API_EVENT,
  BACKFILL,
  RECONCILER,
  PIPELINE,
}

enum AssetCheckSeverity {
  ERROR,
  WARN,
}

model AssetFreshnessPolicy {
  max_lag_seconds?: int64;
  cron_schedule?: string;
}

model AssetMaterializationPolicy {
  mode?: string;
  allow_concurrent?: boolean;
}

model AssetAutoMaterializePolicy {
  mode?: string;
  min_interval_seconds?: int64;
  require_all_upstreams?: boolean;
  on_freshness_breach?: boolean;
  on_upstream_materialized?: boolean;
  respect_downtime_windows?: boolean;
  downtime_windows_cron_expr?: string[];
}

model Asset {
  id?: string;
  asset_key?: string;
  asset_type?: string;
  owner?: string;
  description?: string;
  tags?: string[];
  freshness_policy?: AssetFreshnessPolicy;
  materialization_policy?: AssetMaterializationPolicy;
  auto_materialize_policy?: AssetAutoMaterializePolicy;
  io_profile?: string;
  is_active?: boolean;
  created_by?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model AssetGraph {
  asset_key?: string;
  upstream_asset_keys?: string[];
  downstream_asset_keys?: string[];
}

model AssetPartition {
  id?: string;
  asset_id?: string;
  partition_key?: string;
  status?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model AssetRun {
  id?: string;
  asset_id?: string;
  run_group_id?: string;
  partition_key?: string;
  partition_from?: string;
  partition_to?: string;
  status?: AssetRunStatus;
  trigger_type?: AssetTriggerType;
  triggered_by?: string;
  attempt_count?: int32;
  max_attempts?: int32;
  started_at?: utcDateTime;
  finished_at?: utcDateTime;
  error_message?: string;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model AssetMaterialization {
  id?: string;
  asset_id?: string;
  run_id?: string;
  partition_key?: string;
  row_count?: int64;
  schema_hash?: string;
  materialized_at?: utcDateTime;
  created_at?: utcDateTime;
}

model AssetFreshnessStatus {
  asset_id?: string;
  asset_key?: string;
  asset_type?: string;
  freshness_status?: string;
  effective_max_lag_seconds?: int64;
  last_materialized_at?: utcDateTime;
  stale_since?: utcDateTime;
  reason?: string;
  basis?: string[];
}

model AssetFreshnessEdge {
  from_asset_key?: string;
  to_asset_key?: string;
  dependency_type?: string;
}

model AssetFreshnessExplanation {
  asset?: AssetFreshnessStatus;
  nodes?: AssetFreshnessStatus[];
  edges?: AssetFreshnessEdge[];
}

model AssetFreshnessReconcileTarget {
  asset_id?: string;
  asset_key?: string;
  asset_type?: string;
  freshness_status?: string;
  event_id?: string;
}

model AssetFreshnessReconcileResponse {
  asset?: AssetFreshnessStatus;
  targets?: AssetFreshnessReconcileTarget[];
}

model AssetCheck {
  id?: string;
  asset_id?: string;
  name?: string;
  check_type?: string;
  severity?: AssetCheckSeverity;
  enabled?: boolean;
  created_at?: utcDateTime;
  updated_at?: utcDateTime;
}

model AssetCheckResult {
  id?: string;
  check_id?: string;
  run_id?: string;
  partition_key?: string;
  status?: string;
  message?: string;
  metrics_json?: JsonObject;
  created_at?: utcDateTime;
}

model AssetCheckInput {
  name: string;
  check_type: string;
  severity?: AssetCheckSeverity;
  enabled?: boolean;
  config_json?: JsonObject;
}

model CreateAssetRequest {
  asset_key: string;
  asset_type: string;
  owner: string;
  description?: string;
  tags?: string[];
  freshness_policy?: AssetFreshnessPolicy;
  materialization_policy?: AssetMaterializationPolicy;
  auto_materialize_policy?: AssetAutoMaterializePolicy;
  io_profile?: string;
  is_active?: boolean;
  upstream_asset_keys?: string[];
  checks?: AssetCheckInput[];
}

model UpdateAssetRequest {
  asset_type: string;
  owner: string;
  description?: string;
  tags?: string[];
  freshness_policy?: AssetFreshnessPolicy;
  materialization_policy?: AssetMaterializationPolicy;
  auto_materialize_policy?: AssetAutoMaterializePolicy;
  io_profile?: string;
  is_active?: boolean;
  upstream_asset_keys?: string[];
  checks?: AssetCheckInput[];
}

model BackfillRequest {
  id?: string;
  asset_id?: string;
  partition_from?: string;
  partition_to?: string;
  status?: string;
  requested_by?: string;
  max_parallelism?: int32;
  created_at?: utcDateTime;
  started_at?: utcDateTime;
  finished_at?: utcDateTime;
  error_message?: string;
}

model BackfillSlice {
  id?: string;
  request_id?: string;
  asset_id?: string;
  partition_key?: string;
  status?: string;
  run_id?: string;
  created_at?: utcDateTime;
  started_at?: utcDateTime;
  finished_at?: utcDateTime;
  error_message?: string;
  attempt_count?: int32;
  max_attempts?: int32;
}

model TriggerAssetMaterializationRequest {
  partition_key?: string;
  idempotency_key?: string;
  payload?: JsonObject;
}

model AssetTriggerResponse {
  event_id?: string;
  status?: string;
}

model CreateAssetBackfillRequest {
  partition_from: string;
  partition_to: string;
  max_parallelism?: int32;
}

model CreateAssetBackfillResponse {
  request?: BackfillRequest;
  slices?: BackfillSlice[];
}

model AssetBackfillDetails {
  request?: BackfillRequest;
  slices?: BackfillSlice[];
}

model PaginatedAssets {
  data: Asset[];
  next_page_token?: string;
}

model PaginatedAssetPartitions {
  data: AssetPartition[];
  next_page_token?: string;
}

model PaginatedAssetRuns {
  data: AssetRun[];
  next_page_token?: string;
}

model PaginatedAssetMaterializations {
  data: AssetMaterialization[];
  next_page_token?: string;
}

model AssetCheckList {
  data: AssetCheck[];
}

model PaginatedAssetCheckResults {
  data: AssetCheckResult[];
  next_page_token?: string;
}

model PaginatedBackfillRequests {
  data: BackfillRequest[];
  next_page_token?: string;
}
