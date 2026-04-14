//nolint:revive // strict server interface methods are exported by generated contract.
package api

import (
	"context"
	"math"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
)

type assetService interface {
	ListAssets(ctx context.Context, filter domain.AssetFilter) ([]domain.DataAsset, int64, error)
	CreateAsset(ctx context.Context, req domain.CreateAssetRequest) (*domain.DataAsset, error)
	GetAsset(ctx context.Context, key string) (*domain.DataAsset, error)
	UpdateAsset(ctx context.Context, assetKey string, req domain.UpdateAssetRequest) (*domain.DataAsset, error)
	DeleteAsset(ctx context.Context, assetKey string) error
	CheckFreshness(ctx context.Context, assetKey string) (*domain.AssetFreshnessStatus, error)
	ExplainFreshness(ctx context.Context, assetKey string) (*domain.AssetFreshnessNode, error)
	ReconcileFreshness(ctx context.Context, assetKey string) (*domain.AssetFreshnessReconcileResult, error)
	ResolveAssetKeys(ctx context.Context, assetIDs []string) (map[string]string, error)
	GetGraph(ctx context.Context, assetID string) ([]domain.AssetDependency, []domain.AssetDependency, error)
	ListPartitions(ctx context.Context, assetID string, page domain.PageRequest) ([]domain.AssetPartition, int64, error)
	ListRuns(ctx context.Context, filter domain.AssetRunFilter) ([]domain.AssetRun, int64, error)
	ListMaterializations(ctx context.Context, assetID string, page domain.PageRequest) ([]domain.AssetMaterialization, int64, error)
	ListChecks(ctx context.Context, assetID string) ([]domain.AssetCheck, error)
	ListCheckResults(ctx context.Context, assetID string, page domain.PageRequest) ([]domain.AssetCheckResult, int64, error)
	ListBackfills(ctx context.Context, filter domain.BackfillFilter) ([]domain.BackfillRequest, int64, error)
	GetBackfill(ctx context.Context, assetID, backfillID string) (*domain.BackfillRequest, []domain.BackfillSlice, error)
	TriggerMaterialization(ctx context.Context, assetID string, partitionKey *string, payload map[string]any, idempotencyKey *string) (*domain.OrchestrationEvent, error)
}

type assetBackfillService interface {
	Create(ctx context.Context, assetID, requestedBy, from, to string, maxParallelism int) (*domain.BackfillRequest, []domain.BackfillSlice, error)
}

func (h *APIHandler) CreateAsset(ctx context.Context, req GenCreateAssetRequest) (GenCreateAssetResponse, error) {
	created, err := h.assets.CreateAsset(ctx, domainCreateAssetRequest(req.Body))
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateAssetResponse]("createAsset", err, domainErrorResponder[GenCreateAssetResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateAssetResponse { return CreateAsset400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenCreateAssetResponse { return CreateAsset403JSONResponse{resp} },
			Conflict:   func(resp ConflictJSONResponse) GenCreateAssetResponse { return CreateAsset409JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return CreateAsset201JSONResponse{
		Body:    assetToAPI(*created),
		Headers: CreateAsset201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListAssets(ctx context.Context, req GenListAssetsRequest) (GenListAssetsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	assets, total, err := h.assets.ListAssets(ctx, domain.AssetFilter{Page: page})
	if err != nil {
		return nil, err
	}

	data := make([]Asset, len(assets))
	for i := range assets {
		data[i] = assetToAPI(assets[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListAssets200JSONResponse{
		Body:    PaginatedAssets{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListAssets200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) GetAsset(ctx context.Context, req GenGetAssetRequest) (GenGetAssetResponse, error) {
	asset, err := h.assets.GetAsset(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetAssetResponse]("getAsset", err, domainErrorResponder[GenGetAssetResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetAssetResponse { return GetAsset404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return GetAsset200JSONResponse{
		Body:    assetToAPI(*asset),
		Headers: GetAsset200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) UpdateAsset(ctx context.Context, req GenUpdateAssetRequest) (GenUpdateAssetResponse, error) {
	updated, err := h.assets.UpdateAsset(ctx, req.AssetKey, domainUpdateAssetRequest(req.Body))
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateAssetResponse]("updateAsset", err, domainErrorResponder[GenUpdateAssetResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateAssetResponse { return UpdateAsset400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenUpdateAssetResponse { return UpdateAsset403JSONResponse{resp} },
			NotFound:   func(resp NotFoundJSONResponse) GenUpdateAssetResponse { return UpdateAsset404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return UpdateAsset200JSONResponse{
		Body:    assetToAPI(*updated),
		Headers: UpdateAsset200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) DeleteAsset(ctx context.Context, req GenDeleteAssetRequest) (GenDeleteAssetResponse, error) {
	if err := h.assets.DeleteAsset(ctx, req.AssetKey); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteAssetResponse]("deleteAsset", err, domainErrorResponder[GenDeleteAssetResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeleteAssetResponse { return DeleteAsset400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenDeleteAssetResponse { return DeleteAsset403JSONResponse{resp} },
			NotFound:   func(resp NotFoundJSONResponse) GenDeleteAssetResponse { return DeleteAsset404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return DeleteAsset204Response{}, nil
}

func (h *APIHandler) GetAssetGraph(ctx context.Context, req GenGetAssetGraphRequest) (GenGetAssetGraphResponse, error) {
	asset, err := h.assets.GetAsset(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetAssetGraphResponse]("getAssetGraph", err, domainErrorResponder[GenGetAssetGraphResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetAssetGraphResponse { return GetAssetGraph404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	upstream, downstream, err := h.assets.GetGraph(ctx, asset.ID)
	if err != nil {
		return nil, err
	}
	assetIDs := dependencyAssetIDs(upstream, downstream)
	keyByID, err := h.assets.ResolveAssetKeys(ctx, assetIDs)
	if err != nil {
		return nil, err
	}

	upstreamKeys := make([]string, 0, len(upstream))
	downstreamKeys := make([]string, 0, len(downstream))
	for i := range upstream {
		if key, ok := keyByID[upstream[i].UpstreamAssetID]; ok {
			upstreamKeys = append(upstreamKeys, key)
		}
	}
	for i := range downstream {
		if key, ok := keyByID[downstream[i].AssetID]; ok {
			downstreamKeys = append(downstreamKeys, key)
		}
	}

	graph := AssetGraph{
		AssetKey:            &asset.AssetKey,
		UpstreamAssetKeys:   &upstreamKeys,
		DownstreamAssetKeys: &downstreamKeys,
	}
	return GetAssetGraph200JSONResponse{
		Body:    graph,
		Headers: GetAssetGraph200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) GetAssetFreshness(ctx context.Context, req GenGetAssetFreshnessRequest) (GenGetAssetFreshnessResponse, error) {
	status, err := h.assets.CheckFreshness(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetAssetFreshnessResponse]("getAssetFreshness", err, domainErrorResponder[GenGetAssetFreshnessResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenGetAssetFreshnessResponse {
				return GetAssetFreshness400JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenGetAssetFreshnessResponse {
				return GetAssetFreshness404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	body := assetFreshnessStatusToAPI(*status)
	return GetAssetFreshness200JSONResponse{
		Body:    body,
		Headers: GetAssetFreshness200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ExplainAssetFreshness(ctx context.Context, req GenExplainAssetFreshnessRequest) (GenExplainAssetFreshnessResponse, error) {
	node, err := h.assets.ExplainFreshness(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenExplainAssetFreshnessResponse]("explainAssetFreshness", err, domainErrorResponder[GenExplainAssetFreshnessResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenExplainAssetFreshnessResponse {
				return ExplainAssetFreshness400JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenExplainAssetFreshnessResponse {
				return ExplainAssetFreshness404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	body := assetFreshnessExplanationToAPI(*node)
	return ExplainAssetFreshness200JSONResponse{
		Body:    body,
		Headers: ExplainAssetFreshness200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListAssetFreshnessRequirements(ctx context.Context, req GenListAssetFreshnessRequirementsRequest) (GenListAssetFreshnessRequirementsResponse, error) {
	node, err := h.assets.ExplainFreshness(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListAssetFreshnessRequirementsResponse]("listAssetFreshnessRequirements", err, domainErrorResponder[GenListAssetFreshnessRequirementsResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenListAssetFreshnessRequirementsResponse {
				return ListAssetFreshnessRequirements400JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListAssetFreshnessRequirementsResponse {
				return ListAssetFreshnessRequirements404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	body := assetFreshnessRequirementsToAPI(*node)
	return ListAssetFreshnessRequirements200JSONResponse{
		Body:    body,
		Headers: ListAssetFreshnessRequirements200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListAssetFreshnessBlockers(ctx context.Context, req GenListAssetFreshnessBlockersRequest) (GenListAssetFreshnessBlockersResponse, error) {
	node, err := h.assets.ExplainFreshness(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListAssetFreshnessBlockersResponse]("listAssetFreshnessBlockers", err, domainErrorResponder[GenListAssetFreshnessBlockersResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenListAssetFreshnessBlockersResponse {
				return ListAssetFreshnessBlockers400JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListAssetFreshnessBlockersResponse {
				return ListAssetFreshnessBlockers404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	body := assetFreshnessBlockersToAPI(*node)
	return ListAssetFreshnessBlockers200JSONResponse{
		Body:    body,
		Headers: ListAssetFreshnessBlockers200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ReconcileAssetFreshness(ctx context.Context, req GenReconcileAssetFreshnessRequest) (GenReconcileAssetFreshnessResponse, error) {
	result, err := h.assets.ReconcileFreshness(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenReconcileAssetFreshnessResponse]("reconcileAssetFreshness", err, domainErrorResponder[GenReconcileAssetFreshnessResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenReconcileAssetFreshnessResponse {
				return ReconcileAssetFreshness400JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenReconcileAssetFreshnessResponse {
				return ReconcileAssetFreshness404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	body := assetFreshnessReconcileResultToAPI(*result)
	return ReconcileAssetFreshness202JSONResponse{
		Body:    body,
		Headers: ReconcileAssetFreshness202ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func dependencyAssetIDs(upstream []domain.AssetDependency, downstream []domain.AssetDependency) []string {
	ids := make([]string, 0, len(upstream)+len(downstream))
	for i := range upstream {
		ids = append(ids, upstream[i].UpstreamAssetID)
	}
	for i := range downstream {
		ids = append(ids, downstream[i].AssetID)
	}
	return ids
}

func (h *APIHandler) ListAssetPartitions(ctx context.Context, req GenListAssetPartitionsRequest) (GenListAssetPartitionsResponse, error) {
	asset, err := h.assets.GetAsset(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListAssetPartitionsResponse]("listAssetPartitions", err, domainErrorResponder[GenListAssetPartitionsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListAssetPartitionsResponse {
				return ListAssetPartitions404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	partitions, total, err := h.assets.ListPartitions(ctx, asset.ID, page)
	if err != nil {
		return nil, err
	}

	data := make([]AssetPartition, len(partitions))
	for i := range partitions {
		data[i] = assetPartitionToAPI(partitions[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListAssetPartitions200JSONResponse{
		Body:    PaginatedAssetPartitions{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListAssetPartitions200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListAssetRuns(ctx context.Context, req GenListAssetRunsRequest) (GenListAssetRunsResponse, error) {
	asset, err := h.assets.GetAsset(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListAssetRunsResponse]("listAssetRuns", err, domainErrorResponder[GenListAssetRunsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListAssetRunsResponse { return ListAssetRuns404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	filter := domain.AssetRunFilter{AssetID: &asset.ID, Page: page}
	if req.Params.Status != nil {
		status := string(*req.Params.Status)
		filter.Status = &status
	}
	runs, total, err := h.assets.ListRuns(ctx, filter)
	if err != nil {
		return nil, err
	}

	data := make([]AssetRun, len(runs))
	for i := range runs {
		data[i] = assetRunToAPI(runs[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListAssetRuns200JSONResponse{
		Body:    PaginatedAssetRuns{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListAssetRuns200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) TriggerAssetMaterialization(ctx context.Context, req GenTriggerAssetMaterializationRequest) (GenTriggerAssetMaterializationResponse, error) {
	asset, err := h.assets.GetAsset(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenTriggerAssetMaterializationResponse]("triggerAssetMaterialization", err, domainErrorResponder[GenTriggerAssetMaterializationResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenTriggerAssetMaterializationResponse {
				return TriggerAssetMaterialization404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	var (
		partitionKey   *string
		idempotencyKey *string
		payload        map[string]any
	)
	if req.Body != nil {
		partitionKey = req.Body.PartitionKey
		idempotencyKey = req.Body.IdempotencyKey
		if req.Body.Payload != nil {
			payload = *req.Body.Payload
		}
	}

	event, err := h.assets.TriggerMaterialization(ctx, asset.ID, partitionKey, payload, idempotencyKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenTriggerAssetMaterializationResponse]("triggerAssetMaterialization", err, domainErrorResponder[GenTriggerAssetMaterializationResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenTriggerAssetMaterializationResponse {
				return TriggerAssetMaterialization400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenTriggerAssetMaterializationResponse {
				return TriggerAssetMaterialization403JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return TriggerAssetMaterialization202JSONResponse{
		Body:    AssetTriggerResponse{EventId: &event.ID, Status: &event.Status},
		Headers: TriggerAssetMaterialization202ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListAssetMaterializations(ctx context.Context, req GenListAssetMaterializationsRequest) (GenListAssetMaterializationsResponse, error) {
	asset, err := h.assets.GetAsset(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListAssetMaterializationsResponse]("listAssetMaterializations", err, domainErrorResponder[GenListAssetMaterializationsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListAssetMaterializationsResponse {
				return ListAssetMaterializations404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	materializations, total, err := h.assets.ListMaterializations(ctx, asset.ID, page)
	if err != nil {
		return nil, err
	}

	data := make([]AssetMaterialization, len(materializations))
	for i := range materializations {
		data[i] = assetMaterializationToAPI(materializations[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListAssetMaterializations200JSONResponse{
		Body:    PaginatedAssetMaterializations{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListAssetMaterializations200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListAssetChecks(ctx context.Context, req GenListAssetChecksRequest) (GenListAssetChecksResponse, error) {
	asset, err := h.assets.GetAsset(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListAssetChecksResponse]("listAssetChecks", err, domainErrorResponder[GenListAssetChecksResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListAssetChecksResponse {
				return ListAssetChecks404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	checks, err := h.assets.ListChecks(ctx, asset.ID)
	if err != nil {
		return nil, err
	}

	data := make([]AssetCheck, len(checks))
	for i := range checks {
		data[i] = assetCheckToAPI(checks[i])
	}
	return ListAssetChecks200JSONResponse{
		Body:    AssetCheckList{Data: data},
		Headers: ListAssetChecks200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListAssetCheckResults(ctx context.Context, req GenListAssetCheckResultsRequest) (GenListAssetCheckResultsResponse, error) {
	asset, err := h.assets.GetAsset(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListAssetCheckResultsResponse]("listAssetCheckResults", err, domainErrorResponder[GenListAssetCheckResultsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListAssetCheckResultsResponse {
				return ListAssetCheckResults404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	results, total, err := h.assets.ListCheckResults(ctx, asset.ID, page)
	if err != nil {
		return nil, err
	}

	data := make([]AssetCheckResult, len(results))
	for i := range results {
		data[i] = assetCheckResultToAPI(results[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListAssetCheckResults200JSONResponse{
		Body:    PaginatedAssetCheckResults{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListAssetCheckResults200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) ListAssetBackfills(ctx context.Context, req GenListAssetBackfillsRequest) (GenListAssetBackfillsResponse, error) {
	asset, err := h.assets.GetAsset(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListAssetBackfillsResponse]("listAssetBackfills", err, domainErrorResponder[GenListAssetBackfillsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListAssetBackfillsResponse {
				return ListAssetBackfills404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	filter := domain.BackfillFilter{AssetID: &asset.ID, Page: page}
	if req.Params.Status != nil {
		status := string(*req.Params.Status)
		filter.Status = &status
	}
	backfills, total, err := h.assets.ListBackfills(ctx, filter)
	if err != nil {
		return nil, err
	}

	data := make([]BackfillRequest, len(backfills))
	for i := range backfills {
		data[i] = backfillRequestToAPI(backfills[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListAssetBackfills200JSONResponse{
		Body:    PaginatedBackfillRequests{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListAssetBackfills200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) CreateAssetBackfill(ctx context.Context, req GenCreateAssetBackfillRequest) (GenCreateAssetBackfillResponse, error) {
	principal, _ := domain.PrincipalFromContext(ctx)

	asset, err := h.assets.GetAsset(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateAssetBackfillResponse]("createAssetBackfill", err, domainErrorResponder[GenCreateAssetBackfillResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenCreateAssetBackfillResponse {
				return CreateAssetBackfill404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	maxParallelism := 0
	if req.Body.MaxParallelism != nil {
		maxParallelism = int(*req.Body.MaxParallelism)
	}
	created, slices, err := h.backfills.Create(ctx, asset.ID, principal.Name, req.Body.PartitionFrom, req.Body.PartitionTo, maxParallelism)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateAssetBackfillResponse]("createAssetBackfill", err, domainErrorResponder[GenCreateAssetBackfillResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateAssetBackfillResponse {
				return CreateAssetBackfill400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateAssetBackfillResponse {
				return CreateAssetBackfill403JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	apiSlices := make([]BackfillSlice, len(slices))
	for i := range slices {
		apiSlices[i] = backfillSliceToAPI(slices[i])
	}
	request := backfillRequestToAPI(*created)
	return CreateAssetBackfill201JSONResponse{
		Body:    CreateAssetBackfillResponse{Request: &request, Slices: &apiSlices},
		Headers: CreateAssetBackfill201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) GetAssetBackfill(ctx context.Context, req GenGetAssetBackfillRequest) (GenGetAssetBackfillResponse, error) {
	asset, err := h.assets.GetAsset(ctx, req.AssetKey)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetAssetBackfillResponse]("getAssetBackfill", err, domainErrorResponder[GenGetAssetBackfillResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetAssetBackfillResponse {
				return GetAssetBackfill404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	backfill, slices, err := h.assets.GetBackfill(ctx, asset.ID, req.BackfillId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetAssetBackfillResponse]("getAssetBackfill", err, domainErrorResponder[GenGetAssetBackfillResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenGetAssetBackfillResponse {
				return GetAssetBackfill400JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenGetAssetBackfillResponse {
				return GetAssetBackfill404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	apiSlices := make([]BackfillSlice, len(slices))
	for i := range slices {
		apiSlices[i] = backfillSliceToAPI(slices[i])
	}
	request := backfillRequestToAPI(*backfill)

	return GetAssetBackfill200JSONResponse{
		Body:    AssetBackfillDetails{Request: &request, Slices: &apiSlices},
		Headers: GetAssetBackfill200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func assetToAPI(a domain.DataAsset) Asset {
	return Asset{
		Id:                    &a.ID,
		AssetKey:              &a.AssetKey,
		AssetType:             assetTypePtr(a.AssetType),
		Owner:                 &a.Owner,
		Description:           &a.Description,
		Tags:                  &a.Tags,
		FreshnessPolicy:       assetFreshnessPolicyToAPI(a.FreshnessPolicy),
		MaterializationPolicy: assetMaterializationPolicyToAPI(a.MaterializationPolicy),
		AutoMaterializePolicy: assetAutoMaterializePolicyToAPI(a.AutoMaterializePolicy),
		IoProfile:             &a.IOProfile,
		IsActive:              &a.IsActive,
		CreatedBy:             &a.CreatedBy,
		CreatedAt:             formatTimePtr(&a.CreatedAt),
		UpdatedAt:             formatTimePtr(&a.UpdatedAt),
	}
}

func domainCreateAssetRequest(req *CreateAssetJSONRequestBody) domain.CreateAssetRequest {
	if req == nil {
		return domain.CreateAssetRequest{}
	}
	return domain.CreateAssetRequest{
		AssetKey:              req.AssetKey,
		AssetType:             string(req.AssetType),
		ProductSlug:           req.ProductSlug,
		Owner:                 req.Owner,
		Description:           derefString(req.Description),
		Tags:                  derefStringSlice(req.Tags),
		FreshnessPolicy:       domainAssetFreshnessPolicy(req.FreshnessPolicy),
		MaterializationPolicy: domainAssetMaterializationPolicy(req.MaterializationPolicy),
		AutoMaterializePolicy: domainAssetAutoMaterializePolicy(req.AutoMaterializePolicy),
		IOProfile:             derefString(req.IoProfile),
		IsActive:              derefBoolDefault(req.IsActive, true),
		UpstreamAssetKeys:     derefStringSlice(req.UpstreamAssetKeys),
		Checks:                domainAssetChecks(req.Checks),
	}
}

func domainUpdateAssetRequest(req *UpdateAssetJSONRequestBody) domain.UpdateAssetRequest {
	if req == nil {
		return domain.UpdateAssetRequest{}
	}
	return domain.UpdateAssetRequest{
		AssetType:             derefStringEnum(req.AssetType),
		ProductSlug:           derefString(req.ProductSlug),
		Owner:                 derefString(req.Owner),
		Description:           derefString(req.Description),
		Tags:                  derefStringSlice(req.Tags),
		FreshnessPolicy:       domainAssetFreshnessPolicy(req.FreshnessPolicy),
		MaterializationPolicy: domainAssetMaterializationPolicy(req.MaterializationPolicy),
		AutoMaterializePolicy: domainAssetAutoMaterializePolicy(req.AutoMaterializePolicy),
		IOProfile:             derefString(req.IoProfile),
		IsActive:              derefBoolDefault(req.IsActive, true),
		UpstreamAssetKeys:     derefStringSlice(req.UpstreamAssetKeys),
		Checks:                domainAssetChecks(req.Checks),
	}
}

func domainAssetFreshnessPolicy(policy *AssetFreshnessPolicy) *domain.AssetFreshnessPolicy {
	if policy == nil {
		return nil
	}
	result := &domain.AssetFreshnessPolicy{}
	if policy.MaxLagSeconds != nil {
		result.MaxLagSeconds = int64(*policy.MaxLagSeconds)
	}
	result.CronSchedule = derefString(policy.CronSchedule)
	return result
}

func domainAssetMaterializationPolicy(policy *AssetMaterializationPolicy) *domain.AssetMaterializationPolicy {
	if policy == nil {
		return nil
	}
	return &domain.AssetMaterializationPolicy{
		Mode:            derefString(policy.Mode),
		AllowConcurrent: derefBoolDefault(policy.AllowConcurrent, false),
	}
}

func domainAssetAutoMaterializePolicy(policy *AssetAutoMaterializePolicy) *domain.AssetAutoMaterializePolicy {
	if policy == nil {
		return nil
	}
	result := &domain.AssetAutoMaterializePolicy{
		Mode:                   derefString(policy.Mode),
		RequireAllUpstreams:    derefBoolDefault(policy.RequireAllUpstreams, false),
		OnFreshnessBreach:      derefBoolDefault(policy.OnFreshnessBreach, false),
		OnUpstreamMaterialized: derefBoolDefault(policy.OnUpstreamMaterialized, false),
		RespectDowntimeWindows: derefBoolDefault(policy.RespectDowntimeWindows, false),
		DowntimeWindowsCronExpr: derefStringSlice(
			policy.DowntimeWindowsCronExpr,
		),
	}
	if policy.MinIntervalSeconds != nil {
		result.MinIntervalSeconds = int64(*policy.MinIntervalSeconds)
	}
	return result
}

func assetFreshnessPolicyToAPI(policy *domain.AssetFreshnessPolicy) *AssetFreshnessPolicy {
	if policy == nil {
		return nil
	}
	return &AssetFreshnessPolicy{
		MaxLagSeconds: safeAssetInt32Ptr(policy.MaxLagSeconds),
		CronSchedule:  optStr(policy.CronSchedule),
	}
}

func assetMaterializationPolicyToAPI(policy *domain.AssetMaterializationPolicy) *AssetMaterializationPolicy {
	if policy == nil {
		return nil
	}
	return &AssetMaterializationPolicy{
		Mode:            optStr(policy.Mode),
		AllowConcurrent: &policy.AllowConcurrent,
	}
}

func assetAutoMaterializePolicyToAPI(policy *domain.AssetAutoMaterializePolicy) *AssetAutoMaterializePolicy {
	if policy == nil {
		return nil
	}
	downtime := append([]string(nil), policy.DowntimeWindowsCronExpr...)
	return &AssetAutoMaterializePolicy{
		Mode:                    optStr(policy.Mode),
		MinIntervalSeconds:      safeAssetInt32Ptr(policy.MinIntervalSeconds),
		RequireAllUpstreams:     &policy.RequireAllUpstreams,
		OnFreshnessBreach:       &policy.OnFreshnessBreach,
		OnUpstreamMaterialized:  &policy.OnUpstreamMaterialized,
		RespectDowntimeWindows:  &policy.RespectDowntimeWindows,
		DowntimeWindowsCronExpr: &downtime,
	}
}

func safeAssetInt32Ptr(value int64) *int32 {
	if value < math.MinInt32 || value > math.MaxInt32 {
		return nil
	}
	v := int32(value)
	return &v
}

func domainAssetChecks(checks *[]AssetCheckInput) []domain.AssetCheckInput {
	if checks == nil {
		return []domain.AssetCheckInput{}
	}
	out := make([]domain.AssetCheckInput, 0, len(*checks))
	for i := range *checks {
		out = append(out, domain.AssetCheckInput{
			Name:       (*checks)[i].Name,
			CheckType:  (*checks)[i].CheckType,
			Severity:   derefStringEnum((*checks)[i].Severity),
			Enabled:    derefBoolDefault((*checks)[i].Enabled, true),
			ConfigJSON: derefAnyMap((*checks)[i].ConfigJson),
		})
	}
	return out
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func derefStringEnum[T ~string](value *T) string {
	if value == nil {
		return ""
	}
	return string(*value)
}

func derefStringSlice(value *[]string) []string {
	if value == nil {
		return []string{}
	}
	return append([]string{}, (*value)...)
}

func derefBoolDefault(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func derefAnyMap(value *map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(*value))
	for k, v := range *value {
		out[k] = v
	}
	return out
}

func assetPartitionToAPI(p domain.AssetPartition) AssetPartition {
	return AssetPartition{
		Id:           &p.ID,
		AssetId:      &p.AssetID,
		PartitionKey: &p.PartitionKey,
		Status:       &p.Status,
		CreatedAt:    formatTimePtr(&p.CreatedAt),
		UpdatedAt:    formatTimePtr(&p.UpdatedAt),
	}
}

func assetRunToAPI(r domain.AssetRun) AssetRun {
	return AssetRun{
		Id:            &r.ID,
		AssetId:       &r.AssetID,
		RunGroupId:    r.RunGroupID,
		PartitionKey:  r.PartitionKey,
		PartitionFrom: r.PartitionFrom,
		PartitionTo:   r.PartitionTo,
		Status:        ptrAssetRunStatus(r.Status),
		TriggerType:   ptrAssetTriggerType(r.TriggerType),
		TriggeredBy:   &r.TriggeredBy,
		AttemptCount:  int32Ptr(safeIntToInt32(r.AttemptCount)),
		MaxAttempts:   int32Ptr(safeIntToInt32(r.MaxAttempts)),
		StartedAt:     formatTimePtr(r.StartedAt),
		FinishedAt:    formatTimePtr(r.FinishedAt),
		ErrorMessage:  r.ErrorMessage,
		CreatedAt:     formatTimePtr(&r.CreatedAt),
		UpdatedAt:     formatTimePtr(&r.UpdatedAt),
	}
}

func assetMaterializationToAPI(m domain.AssetMaterialization) AssetMaterialization {
	return AssetMaterialization{
		Id:             &m.ID,
		AssetId:        &m.AssetID,
		RunId:          m.RunID,
		PartitionKey:   m.PartitionKey,
		RowCount:       safeInt64ToInt32Ptr(m.RowCount),
		SchemaHash:     m.SchemaHash,
		MaterializedAt: formatTimePtr(&m.MaterializedAt),
		CreatedAt:      formatTimePtr(&m.CreatedAt),
	}
}

func assetCheckToAPI(c domain.AssetCheck) AssetCheck {
	return AssetCheck{
		Id:        &c.ID,
		AssetId:   &c.AssetID,
		Name:      &c.Name,
		CheckType: &c.CheckType,
		Severity:  ptrAssetCheckSeverity(c.Severity),
		Enabled:   &c.Enabled,
		CreatedAt: formatTimePtr(&c.CreatedAt),
		UpdatedAt: formatTimePtr(&c.UpdatedAt),
	}
}

func assetFreshnessStatusToAPI(status domain.AssetFreshnessStatus) AssetFreshnessStatus {
	return AssetFreshnessStatus{
		AssetId:                optStr(status.AssetID),
		AssetKey:               optStr(status.AssetKey),
		AssetType:              assetTypePtr(status.AssetType),
		FreshnessStatus:        optStr(status.FreshnessStatus),
		EffectiveMaxLagSeconds: safeInt64ToInt32Ptr(&status.EffectiveMaxLagSeconds),
		LastMaterializedAt:     formatTimePtr(status.LastMaterializedAt),
		StaleSince:             formatTimePtr(status.StaleSince),
		Reason:                 optStr(status.Reason),
		Basis:                  slicePtr(status.Basis),
	}
}

func assetFreshnessExplanationToAPI(root domain.AssetFreshnessNode) AssetFreshnessExplanation {
	nodes := make([]AssetFreshnessStatus, 0)
	edges := make([]AssetFreshnessEdge, 0)
	flattenAssetFreshnessTree(root, &nodes, &edges)
	asset := assetFreshnessStatusToAPI(domain.AssetFreshnessStatus{
		AssetID:                root.AssetID,
		AssetKey:               root.AssetKey,
		AssetType:              root.AssetType,
		FreshnessStatus:        root.FreshnessStatus,
		EffectiveMaxLagSeconds: root.EffectiveMaxLagSeconds,
		LastMaterializedAt:     root.LastMaterializedAt,
		StaleSince:             root.StaleSince,
		Reason:                 root.Reason,
		Basis:                  root.Basis,
	})
	return AssetFreshnessExplanation{
		Asset: &asset,
		Nodes: &nodes,
		Edges: &edges,
	}
}

func assetFreshnessRequirementsToAPI(root domain.AssetFreshnessNode) AssetFreshnessRequirementsResponse {
	requirements := make([]AssetFreshnessRequirement, 0)
	collectAssetFreshnessRequirements(root, &requirements)
	asset := assetFreshnessStatusToAPI(domain.AssetFreshnessStatus{
		AssetID:                root.AssetID,
		AssetKey:               root.AssetKey,
		AssetType:              root.AssetType,
		FreshnessStatus:        root.FreshnessStatus,
		EffectiveMaxLagSeconds: root.EffectiveMaxLagSeconds,
		LastMaterializedAt:     root.LastMaterializedAt,
		StaleSince:             root.StaleSince,
		Reason:                 root.Reason,
		Basis:                  root.Basis,
	})
	return AssetFreshnessRequirementsResponse{
		Asset:        &asset,
		Requirements: &requirements,
	}
}

func collectAssetFreshnessRequirements(root domain.AssetFreshnessNode, requirements *[]AssetFreshnessRequirement) {
	if requirements == nil {
		return
	}
	for _, child := range root.Upstream {
		status := assetFreshnessStatusToAPI(domain.AssetFreshnessStatus{
			AssetID:                child.AssetID,
			AssetKey:               child.AssetKey,
			AssetType:              child.AssetType,
			FreshnessStatus:        child.FreshnessStatus,
			EffectiveMaxLagSeconds: child.EffectiveMaxLagSeconds,
			LastMaterializedAt:     child.LastMaterializedAt,
			StaleSince:             child.StaleSince,
			Reason:                 child.Reason,
			Basis:                  child.Basis,
		})
		*requirements = append(*requirements, AssetFreshnessRequirement{
			Asset:          &status,
			DependencyType: optStr(child.UpstreamDependencyType),
		})
		collectAssetFreshnessRequirements(child, requirements)
	}
}

func assetFreshnessBlockersToAPI(root domain.AssetFreshnessNode) AssetFreshnessBlockersResponse {
	blockers := make([]AssetFreshnessBlocker, 0)
	seen := map[string]struct{}{}
	collectAssetFreshnessBlockers(root, seen, &blockers)
	asset := assetFreshnessStatusToAPI(domain.AssetFreshnessStatus{
		AssetID:                root.AssetID,
		AssetKey:               root.AssetKey,
		AssetType:              root.AssetType,
		FreshnessStatus:        root.FreshnessStatus,
		EffectiveMaxLagSeconds: root.EffectiveMaxLagSeconds,
		LastMaterializedAt:     root.LastMaterializedAt,
		StaleSince:             root.StaleSince,
		Reason:                 root.Reason,
		Basis:                  root.Basis,
	})
	return AssetFreshnessBlockersResponse{
		Asset:    &asset,
		Blockers: &blockers,
	}
}

func collectAssetFreshnessBlockers(root domain.AssetFreshnessNode, seen map[string]struct{}, blockers *[]AssetFreshnessBlocker) {
	if blockers == nil {
		return
	}
	for _, child := range root.Upstream {
		if child.FreshnessStatus != domain.AssetFreshnessStatusFresh {
			if _, ok := seen[child.AssetID]; !ok {
				seen[child.AssetID] = struct{}{}
				status := assetFreshnessStatusToAPI(domain.AssetFreshnessStatus{
					AssetID:                child.AssetID,
					AssetKey:               child.AssetKey,
					AssetType:              child.AssetType,
					FreshnessStatus:        child.FreshnessStatus,
					EffectiveMaxLagSeconds: child.EffectiveMaxLagSeconds,
					LastMaterializedAt:     child.LastMaterializedAt,
					StaleSince:             child.StaleSince,
					Reason:                 child.Reason,
					Basis:                  child.Basis,
				})
				*blockers = append(*blockers, AssetFreshnessBlocker{
					Asset:          &status,
					DependencyType: optStr(child.UpstreamDependencyType),
				})
			}
		}
		collectAssetFreshnessBlockers(child, seen, blockers)
	}
}

func flattenAssetFreshnessTree(root domain.AssetFreshnessNode, nodes *[]AssetFreshnessStatus, edges *[]AssetFreshnessEdge) {
	if nodes == nil || edges == nil {
		return
	}
	*nodes = append(*nodes, assetFreshnessStatusToAPI(domain.AssetFreshnessStatus{
		AssetID:                root.AssetID,
		AssetKey:               root.AssetKey,
		AssetType:              root.AssetType,
		FreshnessStatus:        root.FreshnessStatus,
		EffectiveMaxLagSeconds: root.EffectiveMaxLagSeconds,
		LastMaterializedAt:     root.LastMaterializedAt,
		StaleSince:             root.StaleSince,
		Reason:                 root.Reason,
		Basis:                  root.Basis,
	}))
	for _, child := range root.Upstream {
		fromKey := root.AssetKey
		toKey := child.AssetKey
		*edges = append(*edges, AssetFreshnessEdge{
			FromAssetKey:   &fromKey,
			ToAssetKey:     &toKey,
			DependencyType: optStr(child.UpstreamDependencyType),
		})
		flattenAssetFreshnessTree(child, nodes, edges)
	}
}

func assetFreshnessReconcileResultToAPI(result domain.AssetFreshnessReconcileResult) AssetFreshnessReconcileResponse {
	targets := make([]AssetFreshnessReconcileTarget, 0, len(result.Targets))
	for _, target := range result.Targets {
		targets = append(targets, AssetFreshnessReconcileTarget{
			AssetId:         optStr(target.AssetID),
			AssetKey:        optStr(target.AssetKey),
			AssetType:       assetTypePtr(target.AssetType),
			FreshnessStatus: optStr(target.FreshnessStatus),
			EventId:         optStr(target.EventID),
		})
	}
	asset := assetFreshnessStatusToAPI(result.Asset)
	return AssetFreshnessReconcileResponse{
		Asset:   &asset,
		Targets: &targets,
	}
}

func assetTypePtr(value string) *AssetType {
	value = strings.ToUpper(strings.TrimSpace(value))
	if value == "" {
		return nil
	}
	apiValue := AssetType(value)
	return &apiValue
}

func assetCheckResultToAPI(r domain.AssetCheckResult) AssetCheckResult {
	var metrics *map[string]any
	if r.MetricsJSON != nil {
		metrics = &r.MetricsJSON
	}
	return AssetCheckResult{
		Id:           &r.ID,
		CheckId:      &r.CheckID,
		RunId:        r.RunID,
		PartitionKey: r.PartitionKey,
		Status:       &r.Status,
		Message:      r.Message,
		MetricsJson:  metrics,
		CreatedAt:    formatTimePtr(&r.CreatedAt),
	}
}

func backfillRequestToAPI(b domain.BackfillRequest) BackfillRequest {
	return BackfillRequest{
		Id:             &b.ID,
		AssetId:        &b.AssetID,
		PartitionFrom:  &b.PartitionFrom,
		PartitionTo:    &b.PartitionTo,
		Status:         &b.Status,
		RequestedBy:    &b.RequestedBy,
		MaxParallelism: int32Ptr(safeIntToInt32(b.MaxParallelism)),
		CreatedAt:      formatTimePtr(&b.CreatedAt),
		StartedAt:      formatTimePtr(b.StartedAt),
		FinishedAt:     formatTimePtr(b.FinishedAt),
		ErrorMessage:   b.ErrorMessage,
	}
}

func backfillSliceToAPI(s domain.BackfillSlice) BackfillSlice {
	return BackfillSlice{
		Id:           &s.ID,
		RequestId:    &s.RequestID,
		AssetId:      &s.AssetID,
		PartitionKey: &s.PartitionKey,
		Status:       &s.Status,
		RunId:        s.RunID,
		AttemptCount: int32Ptr(safeIntToInt32(s.AttemptCount)),
		MaxAttempts:  int32Ptr(safeIntToInt32(s.MaxAttempts)),
		CreatedAt:    formatTimePtr(&s.CreatedAt),
		StartedAt:    formatTimePtr(s.StartedAt),
		FinishedAt:   formatTimePtr(s.FinishedAt),
		ErrorMessage: s.ErrorMessage,
	}
}

func int32Ptr(v int32) *int32 {
	return &v
}
