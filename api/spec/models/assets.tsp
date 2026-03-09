using TypeSpec.Http;

namespace Duck;

model Asset {
  id?: string;
  asset_key?: string;
  asset_type?: string;
  owner?: string;
  description?: string;
  tags?: string[];
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
  status?: string;
  trigger_type?: string;
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

model AssetCheck {
  id?: string;
  asset_id?: string;
  name?: string;
  check_type?: string;
  severity?: string;
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
  metrics_json?: Record<unknown>;
  created_at?: utcDateTime;
}

model AssetCheckInput {
  name: string;
  check_type: string;
  severity?: string;
  enabled?: boolean;
  config_json?: Record<unknown>;
}

model CreateAssetRequest {
  asset_key: string;
  asset_type: string;
  owner: string;
  description?: string;
  tags?: string[];
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
  payload?: Record<unknown>;
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
