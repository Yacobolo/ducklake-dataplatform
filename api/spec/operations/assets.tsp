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

@route("/assets/{assetKey}")
@get
@tag("Assets")
@summary("Get asset")
op getAsset(assetKey: string): Ok<Asset> | ResourceErrorResponses;

@route("/assets/{assetKey}")
@put
@tag("Assets")
@summary("Update asset")
op updateAsset(assetKey: string, @body body: UpdateAssetRequest): Ok<Asset> | ResourceErrorResponses;

@route("/assets/{assetKey}")
@delete
@tag("Assets")
@summary("Delete asset")
op deleteAsset(assetKey: string): NoContent | ResourceErrorResponses;

@route("/assets/{assetKey}/graph")
@get
@tag("Assets")
@summary("Get asset graph")
op getAssetGraph(assetKey: string): Ok<AssetGraph> | ResourceErrorResponses;

@route("/assets/{assetKey}/freshness")
@get
@tag("Assets")
@summary("Get asset freshness")
op getAssetFreshness(assetKey: string): Ok<AssetFreshnessStatus> | ResourceErrorResponses;

@route("/assets/{assetKey}/freshness/explain")
@get
@tag("Assets")
@summary("Explain asset freshness")
op explainAssetFreshness(assetKey: string): Ok<AssetFreshnessExplanation> | ResourceErrorResponses;

@route("/assets/{assetKey}/partitions")
@get
@tag("Assets")
@summary("List asset partitions")
op listAssetPartitions(assetKey: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAssetPartitions> | ResourceErrorResponses;

@route("/assets/{assetKey}/runs")
@get
@tag("Assets")
@summary("List asset runs")
op listAssetRuns(assetKey: string, @query status?: AssetRunStatus, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAssetRuns> | ResourceErrorResponses;

@route("/assets/{assetKey}/materialize")
@post
@tag("Assets")
@summary("Trigger asset materialization")
@doc("Starts a materialization run for the specified asset and returns the queued execution metadata.")
op triggerAssetMaterialization(assetKey: string, @body body?: TriggerAssetMaterializationRequest): Accepted<AssetTriggerResponse> | ResourceErrorResponses;

@route("/assets/{assetKey}/materializations")
@get
@tag("Assets")
@summary("List asset materializations")
op listAssetMaterializations(assetKey: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAssetMaterializations> | ResourceErrorResponses;

@route("/assets/{assetKey}/checks")
@get
@tag("Assets")
@summary("List asset checks")
op listAssetChecks(assetKey: string): Ok<AssetCheckList> | ResourceErrorResponses;

@route("/assets/{assetKey}/check-results")
@get
@tag("Assets")
@summary("List asset check results")
op listAssetCheckResults(assetKey: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAssetCheckResults> | ResourceErrorResponses;

@route("/assets/{assetKey}/backfills")
@get
@tag("Assets")
@summary("List asset backfills")
op listAssetBackfills(assetKey: string, @query status?: AssetRunStatus, @query max_results?: int32, @query page_token?: string): Ok<PaginatedBackfillRequests> | ResourceErrorResponses;

@route("/assets/{assetKey}/backfills")
@post
@tag("Assets")
@summary("Create asset backfill")
op createAssetBackfill(assetKey: string, @body body: CreateAssetBackfillRequest): Created<CreateAssetBackfillResponse> | ResourceErrorResponses;

@route("/assets/{assetKey}/backfills/{backfillId}")
@get
@tag("Assets")
@summary("Get asset backfill")
op getAssetBackfill(assetKey: string, backfillId: string): Ok<AssetBackfillDetails> | ResourceErrorResponses;
