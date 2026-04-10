using TypeSpec.Http;

namespace Duck;

@route("/assets")
@get
@tag("Assets")
@summary("List assets")
op listAssets(@query max_results?: int32, @query page_token?: string): Ok<PaginatedAssets> | StandardErrorResponses;

@route("/assets")
@post
@tag("Assets")
@summary("Create asset")
@doc("Creates a managed asset definition together with its ownership, checks, tags, and upstream lineage metadata.")
op createAsset(@body body: CreateAssetRequest): Created<Asset> | MutatingErrorResponses | ConflictResponse;

@route("/assets/{asset_key}")
@get
@tag("Assets")
@summary("Get asset")
op getAsset(asset_key: string): Ok<Asset> | ResourceErrorResponses;

@route("/assets/{asset_key}")
@patch(#{ implicitOptionality: true })
@tag("Assets")
@summary("Update asset")
op updateAsset(asset_key: string, @body body: UpdateAssetRequest): Ok<Asset> | ResourceErrorResponses;

@route("/assets/{asset_key}")
@delete
@tag("Assets")
@summary("Delete asset")
op deleteAsset(asset_key: string): NoContent | ResourceErrorResponses;

@route("/assets/{asset_key}/graph")
@get
@tag("Assets")
@summary("Get asset graph")
op getAssetGraph(asset_key: string): Ok<AssetGraph> | ResourceErrorResponses;

@route("/assets/{asset_key}/freshness")
@get
@tag("Assets")
@summary("Get asset freshness")
op getAssetFreshness(asset_key: string): Ok<AssetFreshnessStatus> | ResourceErrorResponses;

@route("/assets/{asset_key}/freshness/explanation")
@get
@tag("Assets")
@summary("Explain asset freshness")
op explainAssetFreshness(asset_key: string): Ok<AssetFreshnessExplanation> | ResourceErrorResponses;

@route("/assets/{asset_key}/freshness/requirements")
@get
@tag("Assets")
@summary("List asset freshness requirements")
op listAssetFreshnessRequirements(asset_key: string): Ok<AssetFreshnessRequirementsResponse> | ResourceErrorResponses;

@route("/assets/{asset_key}/freshness/blockers")
@get
@tag("Assets")
@summary("List asset freshness blockers")
op listAssetFreshnessBlockers(asset_key: string): Ok<AssetFreshnessBlockersResponse> | ResourceErrorResponses;

@route("/assets/{asset_key}/freshness-reconciliations")
@post
@tag("Assets")
@summary("Reconcile asset freshness")
op reconcileAssetFreshness(asset_key: string): Accepted<AssetFreshnessReconcileResponse> | ResourceErrorResponses;

@route("/assets/{asset_key}/partitions")
@get
@tag("Assets")
@summary("List asset partitions")
op listAssetPartitions(asset_key: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAssetPartitions> | ResourceErrorResponses;

@route("/assets/{asset_key}/runs")
@get
@tag("Assets")
@summary("List asset runs")
op listAssetRuns(asset_key: string, @query status?: AssetRunStatus, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAssetRuns> | ResourceErrorResponses;

@route("/assets/{asset_key}/materializations")
@post
@tag("Assets")
@summary("Trigger asset materialization")
@doc("Starts a materialization run for the specified asset and returns the queued execution metadata.")
op triggerAssetMaterialization(asset_key: string, @body body?: TriggerAssetMaterializationRequest): Accepted<AssetTriggerResponse> | ResourceErrorResponses;

@route("/assets/{asset_key}/materializations")
@get
@tag("Assets")
@summary("List asset materializations")
op listAssetMaterializations(asset_key: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAssetMaterializations> | ResourceErrorResponses;

@route("/assets/{asset_key}/checks")
@get
@tag("Assets")
@summary("List asset checks")
op listAssetChecks(asset_key: string): Ok<AssetCheckList> | ResourceErrorResponses;

@route("/assets/{asset_key}/checks/results")
@get
@tag("Assets")
@summary("List asset check results")
op listAssetCheckResults(asset_key: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAssetCheckResults> | ResourceErrorResponses;

@route("/assets/{asset_key}/backfills")
@get
@tag("Assets")
@summary("List asset backfills")
op listAssetBackfills(asset_key: string, @query status?: AssetRunStatus, @query max_results?: int32, @query page_token?: string): Ok<PaginatedBackfillRequests> | ResourceErrorResponses;

@route("/assets/{asset_key}/backfills")
@post
@tag("Assets")
@summary("Create asset backfill")
op createAssetBackfill(asset_key: string, @body body: CreateAssetBackfillRequest): Created<CreateAssetBackfillResponse> | ResourceErrorResponses;

@route("/assets/{asset_key}/backfills/{backfill_id}")
@get
@tag("Assets")
@summary("Get asset backfill")
op getAssetBackfill(asset_key: string, backfill_id: string): Ok<AssetBackfillDetails> | ResourceErrorResponses;
