//nolint:revive // strict server interface methods are exported by generated contract.
package api

import (
	"context"
	"errors"

	"duck-demo/internal/domain"
)

type assetService interface {
	ListAssets(ctx context.Context, filter domain.AssetFilter) ([]domain.DataAsset, int64, error)
	GetAsset(ctx context.Context, key string) (*domain.DataAsset, error)
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

func (h *APIHandler) ListAssets(ctx context.Context, req ListAssetsRequestObject) (ListAssetsResponseObject, error) {
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
	return ListAssets200JSONResponse(PaginatedAssets{Data: &data, NextPageToken: optStr(nextToken)}), nil
}

func (h *APIHandler) GetAsset(ctx context.Context, req GetAssetRequestObject) (GetAssetResponseObject, error) {
	asset, err := h.assets.GetAsset(ctx, req.AssetKey)
	if err != nil {
		if errors.As(err, new(*domain.NotFoundError)) {
			return GetAsset404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return nil, err
	}

	return GetAsset200JSONResponse(assetToAPI(*asset)), nil
}

func (h *APIHandler) GetAssetGraph(ctx context.Context, req GetAssetGraphRequestObject) (GetAssetGraphResponseObject, error) {
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
	assets, _, err := h.assets.ListAssets(ctx, domain.AssetFilter{Page: domain.PageRequest{MaxResults: 10000}})
	if err != nil {
		return nil, err
	}
	keyByID := make(map[string]string, len(assets))
	for i := range assets {
		keyByID[assets[i].ID] = assets[i].AssetKey
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
	return GetAssetGraph200JSONResponse(graph), nil
}

func (h *APIHandler) ListAssetPartitions(ctx context.Context, req ListAssetPartitionsRequestObject) (ListAssetPartitionsResponseObject, error) {
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
	return ListAssetPartitions200JSONResponse(PaginatedAssetPartitions{Data: &data, NextPageToken: optStr(nextToken)}), nil
}

func (h *APIHandler) ListAssetRuns(ctx context.Context, req ListAssetRunsRequestObject) (ListAssetRunsResponseObject, error) {
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
	return ListAssetRuns200JSONResponse(PaginatedAssetRuns{Data: &data, NextPageToken: optStr(nextToken)}), nil
}

func (h *APIHandler) TriggerAssetMaterialization(ctx context.Context, req TriggerAssetMaterializationRequestObject) (TriggerAssetMaterializationResponseObject, error) {
	principal, _ := domain.PrincipalFromContext(ctx)
	if !principal.IsAdmin {
		return TriggerAssetMaterialization403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: "asset materialization requires admin privileges"}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

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
		return nil, err
	}

	return TriggerAssetMaterialization202JSONResponse(AssetTriggerResponse{EventId: &event.ID, Status: &event.Status}), nil
}

func (h *APIHandler) ListAssetMaterializations(ctx context.Context, req ListAssetMaterializationsRequestObject) (ListAssetMaterializationsResponseObject, error) {
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
	return ListAssetMaterializations200JSONResponse(PaginatedAssetMaterializations{Data: &data, NextPageToken: optStr(nextToken)}), nil
}

func (h *APIHandler) ListAssetChecks(ctx context.Context, req ListAssetChecksRequestObject) (ListAssetChecksResponseObject, error) {
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
	return ListAssetChecks200JSONResponse(AssetCheckList{Data: &data}), nil
}

func (h *APIHandler) ListAssetCheckResults(ctx context.Context, req ListAssetCheckResultsRequestObject) (ListAssetCheckResultsResponseObject, error) {
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
	return ListAssetCheckResults200JSONResponse(PaginatedAssetCheckResults{Data: &data, NextPageToken: optStr(nextToken)}), nil
}

func (h *APIHandler) ListAssetBackfills(ctx context.Context, req ListAssetBackfillsRequestObject) (ListAssetBackfillsResponseObject, error) {
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
	return ListAssetBackfills200JSONResponse(PaginatedBackfillRequests{Data: &data, NextPageToken: optStr(nextToken)}), nil
}

func (h *APIHandler) CreateAssetBackfill(ctx context.Context, req CreateAssetBackfillRequestObject) (CreateAssetBackfillResponseObject, error) {
	principal, _ := domain.PrincipalFromContext(ctx)
	if !principal.IsAdmin {
		return CreateAssetBackfill403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: "asset backfill requires admin privileges"}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

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
		return nil, err
	}

	apiSlices := make([]BackfillSlice, len(slices))
	for i := range slices {
		apiSlices[i] = backfillSliceToAPI(slices[i])
	}
	request := backfillRequestToAPI(*created)
	return CreateAssetBackfill201JSONResponse(CreateAssetBackfillResponse{Request: &request, Slices: &apiSlices}), nil
}

func (h *APIHandler) GetAssetBackfill(ctx context.Context, req GetAssetBackfillRequestObject) (GetAssetBackfillResponseObject, error) {
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

	return GetAssetBackfill200JSONResponse(AssetBackfillDetails{Request: &request, Slices: &apiSlices}), nil
}

func assetToAPI(a domain.DataAsset) Asset {
	ct := a.CreatedAt
	ut := a.UpdatedAt
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
		CreatedAt:   &ct,
		UpdatedAt:   &ut,
	}
}

func assetPartitionToAPI(p domain.AssetPartition) AssetPartition {
	ct := p.CreatedAt
	ut := p.UpdatedAt
	return AssetPartition{
		Id:           &p.ID,
		AssetId:      &p.AssetID,
		PartitionKey: &p.PartitionKey,
		Status:       &p.Status,
		CreatedAt:    &ct,
		UpdatedAt:    &ut,
	}
}

func assetRunToAPI(r domain.AssetRun) AssetRun {
	ct := r.CreatedAt
	ut := r.UpdatedAt
	return AssetRun{
		Id:           &r.ID,
		AssetId:      &r.AssetID,
		RunGroupId:   r.RunGroupID,
		PartitionKey: r.PartitionKey,
		Status:       &r.Status,
		TriggerType:  &r.TriggerType,
		TriggeredBy:  &r.TriggeredBy,
		AttemptCount: int32Ptr(safeIntToInt32(r.AttemptCount)),
		MaxAttempts:  int32Ptr(safeIntToInt32(r.MaxAttempts)),
		StartedAt:    r.StartedAt,
		FinishedAt:   r.FinishedAt,
		ErrorMessage: r.ErrorMessage,
		CreatedAt:    &ct,
		UpdatedAt:    &ut,
	}
}

func assetMaterializationToAPI(m domain.AssetMaterialization) AssetMaterialization {
	ct := m.CreatedAt
	return AssetMaterialization{
		Id:             &m.ID,
		AssetId:        &m.AssetID,
		RunId:          m.RunID,
		PartitionKey:   m.PartitionKey,
		RowCount:       m.RowCount,
		SchemaHash:     m.SchemaHash,
		MaterializedAt: &m.MaterializedAt,
		CreatedAt:      &ct,
	}
}

func assetCheckToAPI(c domain.AssetCheck) AssetCheck {
	ct := c.CreatedAt
	ut := c.UpdatedAt
	return AssetCheck{
		Id:        &c.ID,
		AssetId:   &c.AssetID,
		Name:      &c.Name,
		CheckType: &c.CheckType,
		Severity:  &c.Severity,
		Enabled:   &c.Enabled,
		CreatedAt: &ct,
		UpdatedAt: &ut,
	}
}

func assetCheckResultToAPI(r domain.AssetCheckResult) AssetCheckResult {
	ct := r.CreatedAt
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
		CreatedAt:    &ct,
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
		CreatedAt:      &b.CreatedAt,
		StartedAt:      b.StartedAt,
		FinishedAt:     b.FinishedAt,
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
	}
}

func int32Ptr(v int32) *int32 {
	return &v
}
