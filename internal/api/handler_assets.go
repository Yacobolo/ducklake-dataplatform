//nolint:revive // strict server interface methods are exported by generated contract.
package api

import (
	"context"
	"errors"

	"duck-demo/internal/domain"
)

type assetService interface {
	ListAssets(ctx context.Context, filter domain.AssetFilter) ([]domain.DataAsset, int64, error)
	CreateAsset(ctx context.Context, req domain.CreateAssetRequest) (*domain.DataAsset, error)
	GetAsset(ctx context.Context, key string) (*domain.DataAsset, error)
	UpdateAsset(ctx context.Context, assetKey string, req domain.UpdateAssetRequest) (*domain.DataAsset, error)
	DeleteAsset(ctx context.Context, assetKey string) error
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateAsset403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateAsset400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateAsset409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
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
		if errors.As(err, new(*domain.NotFoundError)) {
			return GetAsset404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateAsset403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return UpdateAsset400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateAsset404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	return UpdateAsset200JSONResponse{
		Body:    assetToAPI(*updated),
		Headers: UpdateAsset200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func (h *APIHandler) DeleteAsset(ctx context.Context, req GenDeleteAssetRequest) (GenDeleteAssetResponse, error) {
	if err := h.assets.DeleteAsset(ctx, req.AssetKey); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteAsset403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return DeleteAsset400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteAsset404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	return DeleteAsset204Response{}, nil
}

func (h *APIHandler) GetAssetGraph(ctx context.Context, req GenGetAssetGraphRequest) (GenGetAssetGraphResponse, error) {
	asset, err := h.assets.GetAsset(ctx, req.AssetKey)
	if err != nil {
		if errors.As(err, new(*domain.NotFoundError)) {
			return GetAssetGraph404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
		if errors.As(err, new(*domain.NotFoundError)) {
			return ListAssetPartitions404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
		if errors.As(err, new(*domain.NotFoundError)) {
			return ListAssetRuns404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
		if errors.As(err, new(*domain.NotFoundError)) {
			return TriggerAssetMaterialization404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
		if errors.As(err, new(*domain.ValidationError)) {
			return TriggerAssetMaterialization400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		if errors.As(err, new(*domain.AccessDeniedError)) {
			return TriggerAssetMaterialization403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
		if errors.As(err, new(*domain.NotFoundError)) {
			return ListAssetMaterializations404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
		if errors.As(err, new(*domain.NotFoundError)) {
			return ListAssetChecks404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
		if errors.As(err, new(*domain.NotFoundError)) {
			return ListAssetCheckResults404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
		if errors.As(err, new(*domain.NotFoundError)) {
			return ListAssetBackfills404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
		if errors.As(err, new(*domain.NotFoundError)) {
			return CreateAssetBackfill404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return nil, err
	}

	maxParallelism := 0
	if req.Body.MaxParallelism != nil {
		maxParallelism = int(*req.Body.MaxParallelism)
	}
	created, slices, err := h.backfills.Create(ctx, asset.ID, principal.Name, req.Body.PartitionFrom, req.Body.PartitionTo, maxParallelism)
	if err != nil {
		if errors.As(err, new(*domain.ValidationError)) {
			return CreateAssetBackfill400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		if errors.As(err, new(*domain.AccessDeniedError)) {
			return CreateAssetBackfill403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
		if errors.As(err, new(*domain.NotFoundError)) {
			return GetAssetBackfill404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return nil, err
	}

	backfill, slices, err := h.assets.GetBackfill(ctx, asset.ID, req.BackfillId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ValidationError)):
			return GetAssetBackfill400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return GetAssetBackfill404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
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
		Id:          &a.ID,
		AssetKey:    &a.AssetKey,
		AssetType:   &a.AssetType,
		Owner:       &a.Owner,
		Description: &a.Description,
		Tags:        &a.Tags,
		IoProfile:   &a.IOProfile,
		IsActive:    &a.IsActive,
		CreatedBy:   &a.CreatedBy,
		CreatedAt:   formatTimePtr(&a.CreatedAt),
		UpdatedAt:   formatTimePtr(&a.UpdatedAt),
	}
}

func domainCreateAssetRequest(req *CreateAssetJSONRequestBody) domain.CreateAssetRequest {
	if req == nil {
		return domain.CreateAssetRequest{}
	}
	return domain.CreateAssetRequest{
		AssetKey:          req.AssetKey,
		AssetType:         req.AssetType,
		Owner:             req.Owner,
		Description:       derefString(req.Description),
		Tags:              derefStringSlice(req.Tags),
		IOProfile:         derefString(req.IoProfile),
		IsActive:          derefBoolDefault(req.IsActive, true),
		UpstreamAssetKeys: derefStringSlice(req.UpstreamAssetKeys),
		Checks:            domainAssetChecks(req.Checks),
	}
}

func domainUpdateAssetRequest(req *UpdateAssetJSONRequestBody) domain.UpdateAssetRequest {
	if req == nil {
		return domain.UpdateAssetRequest{}
	}
	return domain.UpdateAssetRequest{
		AssetType:         req.AssetType,
		Owner:             req.Owner,
		Description:       derefString(req.Description),
		Tags:              derefStringSlice(req.Tags),
		IOProfile:         derefString(req.IoProfile),
		IsActive:          derefBoolDefault(req.IsActive, true),
		UpstreamAssetKeys: derefStringSlice(req.UpstreamAssetKeys),
		Checks:            domainAssetChecks(req.Checks),
	}
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
			ConfigJSON: derefRecordMap((*checks)[i].ConfigJson),
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

func derefRecordMap(value *Record) map[string]any {
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

func assetCheckResultToAPI(r domain.AssetCheckResult) AssetCheckResult {
	var metrics *Record
	if r.MetricsJSON != nil {
		record := Record(r.MetricsJSON)
		metrics = &record
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
