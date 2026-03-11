using TypeSpec.Http;

namespace Duck;

@route("/assets")
@get
op listAssets(@query max_results?: int32, @query page_token?: string): Ok<PaginatedAssets> | StandardErrorResponses;

@route("/assets")
@post
op createAsset(@body body: CreateAssetRequest): Created<Asset> | MutatingErrorResponses | ConflictResponse;

@route("/assets/{assetKey}")
@get
op getAsset(assetKey: string): Ok<Asset> | ResourceErrorResponses;

@route("/assets/{assetKey}")
@put
op updateAsset(assetKey: string, @body body: UpdateAssetRequest): Ok<Asset> | ResourceErrorResponses;

@route("/assets/{assetKey}")
@delete
op deleteAsset(assetKey: string): NoContent | ResourceErrorResponses;

@route("/assets/{assetKey}/graph")
@get
op getAssetGraph(assetKey: string): Ok<AssetGraph> | ResourceErrorResponses;

@route("/assets/{assetKey}/partitions")
@get
op listAssetPartitions(assetKey: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAssetPartitions> | ResourceErrorResponses;

@route("/assets/{assetKey}/runs")
@get
op listAssetRuns(assetKey: string, @query status?: AssetRunStatus, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAssetRuns> | ResourceErrorResponses;

@route("/assets/{assetKey}/materialize")
@post
op triggerAssetMaterialization(assetKey: string, @body body?: TriggerAssetMaterializationRequest): Accepted<AssetTriggerResponse> | ResourceErrorResponses;

@route("/assets/{assetKey}/materializations")
@get
op listAssetMaterializations(assetKey: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAssetMaterializations> | ResourceErrorResponses;

@route("/assets/{assetKey}/checks")
@get
op listAssetChecks(assetKey: string): Ok<AssetCheckList> | ResourceErrorResponses;

@route("/assets/{assetKey}/check-results")
@get
op listAssetCheckResults(assetKey: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedAssetCheckResults> | ResourceErrorResponses;

@route("/assets/{assetKey}/backfills")
@get
op listAssetBackfills(assetKey: string, @query status?: AssetRunStatus, @query max_results?: int32, @query page_token?: string): Ok<PaginatedBackfillRequests> | ResourceErrorResponses;

@route("/assets/{assetKey}/backfills")
@post
op createAssetBackfill(assetKey: string, @body body: CreateAssetBackfillRequest): Created<CreateAssetBackfillResponse> | ResourceErrorResponses;

@route("/assets/{assetKey}/backfills/{backfillId}")
@get
op getAssetBackfill(assetKey: string, backfillId: string): Ok<AssetBackfillDetails> | ResourceErrorResponses;
