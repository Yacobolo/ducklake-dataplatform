package runtimeassets

import (
	"net/http"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"
)

type Handler struct{ deps *core.Dependencies }

func New(deps *core.Dependencies) *Handler { return &Handler{deps: deps} }

func (h *Handler) AssetsList(w http.ResponseWriter, r *http.Request) {
	if h.deps.Asset == nil {
		core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Assets UI is not configured."))
		return
	}

	pageReq := pageFromRequest(r, 30)
	items, total, err := h.deps.Asset.ListAssets(r.Context(), domain.AssetFilter{Page: pageReq})
	if err != nil {
		renderServiceError(w, err)
		return
	}

	rows := make([]assetsListRowData, 0, len(items))
	for i := range items {
		item := items[i]
		partitionType := "Unpartitioned"
		if item.PartitionDefinition != nil {
			partitionType = core.TitleizeWords(item.PartitionDefinition.Type)
		}
		materializationMode := "Manual"
		if item.MaterializationPolicy != nil && strings.TrimSpace(item.MaterializationPolicy.Mode) != "" {
			materializationMode = core.TitleizeWords(item.MaterializationPolicy.Mode)
		}
		if item.AutoMaterializePolicy != nil && strings.TrimSpace(item.AutoMaterializePolicy.Mode) != "" {
			materializationMode = core.TitleizeWords(item.AutoMaterializePolicy.Mode)
		}
		rows = append(rows, assetsListRowData{
			AssetKey:            item.AssetKey,
			URL:                 "/ui/assets/" + item.AssetKey,
			Type:                item.AssetType,
			Owner:               item.Owner,
			Description:         item.Description,
			Active:              item.IsActive,
			Updated:             formatTime(item.UpdatedAt),
			FreshnessTracked:    item.FreshnessPolicy != nil,
			PartitionType:       partitionType,
			AutoMaterialized:    item.AutoMaterializePolicy != nil,
			MaterializationMode: materializationMode,
		})
	}

	canMaterialize, err := h.deps.Asset.CanTriggerMaterialization(r.Context())
	if err != nil {
		renderServiceError(w, err)
		return
	}

	_ = core.TrackResourceVisit(r, h.deps, domain.ResourceRef{
		ResourceType: "workspace",
		ResourceKey:  "assets",
		DisplayName:  "Runtime Assets",
		Section:      "Discover",
	})
	core.RenderHTML(w, http.StatusOK, assetsListPage(core.PrincipalFromContext(r.Context()), rows, pageReq, total, canMaterialize, h.deps.Backfill != nil))
}

func (h *Handler) AssetsDetail(w http.ResponseWriter, r *http.Request) {
	if h.deps.Asset == nil {
		core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Assets UI is not configured."))
		return
	}
	assetKey := chi.URLParam(r, "assetKey")
	asset, err := h.deps.Asset.GetAsset(r.Context(), assetKey)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	upstreamDeps, downstreamDeps, err := h.deps.Asset.GetGraph(r.Context(), asset.ID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	assetIDs := dependencyAssetIDs(upstreamDeps, downstreamDeps)
	keyByID, err := h.deps.Asset.ResolveAssetKeys(r.Context(), assetIDs)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	upstream := make([]string, 0, len(upstreamDeps))
	for i := range upstreamDeps {
		if key, ok := keyByID[upstreamDeps[i].UpstreamAssetID]; ok {
			upstream = append(upstream, key)
		}
	}
	downstream := make([]string, 0, len(downstreamDeps))
	for i := range downstreamDeps {
		if key, ok := keyByID[downstreamDeps[i].AssetID]; ok {
			downstream = append(downstream, key)
		}
	}
	sort.Strings(upstream)
	sort.Strings(downstream)
	edges := make([]assetDependencyEdgeData, 0, len(upstream)+len(downstream))
	for i := range upstream {
		edges = append(edges, assetDependencyEdgeData{FromKey: upstream[i], ToKey: asset.AssetKey})
	}
	for i := range downstream {
		edges = append(edges, assetDependencyEdgeData{FromKey: asset.AssetKey, ToKey: downstream[i]})
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].FromKey == edges[j].FromKey {
			return edges[i].ToKey < edges[j].ToKey
		}
		return edges[i].FromKey < edges[j].FromKey
	})
	runs, _, err := h.deps.Asset.ListRuns(r.Context(), domain.AssetRunFilter{AssetID: &asset.ID, Page: domain.PageRequest{MaxResults: 20}})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	materializations, _, err := h.deps.Asset.ListMaterializations(r.Context(), asset.ID, domain.PageRequest{MaxResults: 20})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	checks, err := h.deps.Asset.ListChecks(r.Context(), asset.ID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	partitions, _, err := h.deps.Asset.ListPartitions(r.Context(), asset.ID, domain.PageRequest{MaxResults: 20})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	productSlug := ""
	productName := ""
	if h.deps.Product != nil {
		product, productErr := h.deps.Product.GetProductForAsset(r.Context(), asset.ID)
		if productErr == nil {
			productSlug = product.Product.Slug
			productName = product.Product.Name
		}
	}
	backfills, _, err := h.deps.Asset.ListBackfills(r.Context(), domain.BackfillFilter{AssetID: &asset.ID, Page: domain.PageRequest{MaxResults: 20}})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	freshnessLabel, freshnessTone := freshnessStatus(asset, materializations)
	partitionStatus := summarizePartitions(partitions)
	retryTimeline := shapeRetryTimeline(runs)
	failureRootCauses := shapeFailureRootCauses(runs)
	partitionCalendar := shapePartitionCalendar(partitions)
	canMaterialize, err := h.deps.Asset.CanTriggerMaterialization(r.Context())
	if err != nil {
		renderServiceError(w, err)
		return
	}
	backfillConfigured := h.deps.Backfill != nil
	canBackfill := false
	if backfillConfigured {
		canBackfill, err = h.deps.Backfill.CanCreate(r.Context(), principalName(r))
		if err != nil {
			renderServiceError(w, err)
			return
		}
	}
	_ = core.TrackResourceVisit(r, h.deps, domain.ResourceRef{
		ResourceType: "runtime-asset",
		ResourceKey:  asset.AssetKey,
		DisplayName:  asset.AssetKey,
		Section:      "Discover",
	})
	core.RenderHTML(w, http.StatusOK, assetDetailPage(assetDetailPageData{
		Principal:           core.PrincipalFromContext(r.Context()),
		ProductSlug:         productSlug,
		ProductName:         productName,
		AssetKey:            asset.AssetKey,
		AssetType:           asset.AssetType,
		Owner:               asset.Owner,
		Description:         asset.Description,
		IOProfile:           asset.IOProfile,
		IsActive:            asset.IsActive,
		FreshnessLabel:      freshnessLabel,
		FreshnessTone:       freshnessTone,
		UpdatedAt:           formatTime(asset.UpdatedAt),
		UpstreamAssetKeys:   upstream,
		DownstreamAssetKeys: downstream,
		DependencyEdges:     edges,
		Runs:                runs,
		Materializations:    materializations,
		Checks:              checks,
		Partitions:          partitions,
		Backfills:           backfills,
		RetryTimeline:       retryTimeline,
		FailureRootCauses:   failureRootCauses,
		PartitionCalendar:   partitionCalendar,
		PartitionStatus:     partitionStatus,
		CanMaterialize:      canMaterialize,
		CanBackfill:         canBackfill,
		BackfillConfigured:  backfillConfigured,
		CSRFFieldFunc:       h.deps.CSRFFieldProvider(r),
	}))
}

func (h *Handler) AssetMaterialize(w http.ResponseWriter, r *http.Request) {
	if h.deps.Asset == nil {
		core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Assets UI is not configured."))
		return
	}

	assetKey := chi.URLParam(r, "assetKey")
	asset, err := h.deps.Asset.GetAsset(r.Context(), assetKey)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	partitionKey := formOptionalString(r.Form, "partition_key")
	if _, err := h.deps.Asset.TriggerMaterialization(r.Context(), asset.ID, partitionKey, nil, nil); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/assets/"+assetKey, http.StatusSeeOther)
}

func (h *Handler) AssetBackfillCreate(w http.ResponseWriter, r *http.Request) {
	if h.deps.Asset == nil || h.deps.Backfill == nil {
		core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Asset backfill UI is not configured."))
		return
	}

	assetKey := chi.URLParam(r, "assetKey")
	asset, err := h.deps.Asset.GetAsset(r.Context(), assetKey)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	maxParallelism := 0
	if value, parseErr := formOptionalInt(r.Form, "max_parallelism"); parseErr != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "max_parallelism must be an integer."))
		return
	} else if value != nil {
		maxParallelism = *value
	}

	_, _, err = h.deps.Backfill.Create(
		r.Context(),
		asset.ID,
		principalName(r),
		formString(r.Form, "partition_from"),
		formString(r.Form, "partition_to"),
		maxParallelism,
	)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, "/ui/assets/"+assetKey, http.StatusSeeOther)
}
