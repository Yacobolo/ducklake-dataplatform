package ui

import (
	"net/http"
	"sort"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
)

func (h *Handler) AssetsList(w http.ResponseWriter, r *http.Request) {
	if h.Asset == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Assets UI is not configured."))
		return
	}

	pageReq := pageFromRequest(r, 30)
	items, total, err := h.Asset.ListAssets(r.Context(), domain.AssetFilter{Page: pageReq})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]assetsListRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, assetsListRowData{
			Filter:   item.AssetKey + " " + item.AssetType + " " + item.Owner,
			AssetKey: item.AssetKey,
			URL:      "/ui/assets/" + item.AssetKey,
			Type:     item.AssetType,
			Owner:    item.Owner,
			Active:   item.IsActive,
			Updated:  formatTime(item.UpdatedAt),
		})
	}

	renderHTML(w, http.StatusOK, assetsListPage(principalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) AssetsDetail(w http.ResponseWriter, r *http.Request) {
	if h.Asset == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Assets UI is not configured."))
		return
	}

	assetKey := chi.URLParam(r, "assetKey")
	asset, err := h.Asset.GetAsset(r.Context(), assetKey)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	upstreamDeps, downstreamDeps, err := h.Asset.GetGraph(r.Context(), asset.ID)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	assets, _, err := h.Asset.ListAssets(r.Context(), domain.AssetFilter{Page: domain.PageRequest{MaxResults: 10000}})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	keyByID := make(map[string]string, len(assets))
	for i := range assets {
		keyByID[assets[i].ID] = assets[i].AssetKey
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

	runs, _, err := h.Asset.ListRuns(r.Context(), domain.AssetRunFilter{AssetID: &asset.ID, Page: domain.PageRequest{MaxResults: 20}})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	materializations, _, err := h.Asset.ListMaterializations(r.Context(), asset.ID, domain.PageRequest{MaxResults: 20})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	checks, err := h.Asset.ListChecks(r.Context(), asset.ID)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	partitions, _, err := h.Asset.ListPartitions(r.Context(), asset.ID, domain.PageRequest{MaxResults: 20})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	backfills, _, err := h.Asset.ListBackfills(r.Context(), domain.BackfillFilter{AssetID: &asset.ID, Page: domain.PageRequest{MaxResults: 20}})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	renderHTML(w, http.StatusOK, assetDetailPage(assetDetailPageData{
		Principal:           principalFromContext(r.Context()),
		AssetKey:            asset.AssetKey,
		AssetType:           asset.AssetType,
		Owner:               asset.Owner,
		Description:         asset.Description,
		IOProfile:           asset.IOProfile,
		IsActive:            asset.IsActive,
		UpdatedAt:           formatTime(asset.UpdatedAt),
		UpstreamAssetKeys:   upstream,
		DownstreamAssetKeys: downstream,
		Runs:                runs,
		Materializations:    materializations,
		Checks:              checks,
		Partitions:          partitions,
		Backfills:           backfills,
		CSRFFieldFunc:       csrfFieldProvider(r),
	}))
}

func (h *Handler) AssetMaterialize(w http.ResponseWriter, r *http.Request) {
	if h.Asset == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Assets UI is not configured."))
		return
	}

	principal := principalFromContext(r.Context())
	if !principal.IsAdmin {
		h.renderServiceError(w, r, domain.ErrAccessDenied("asset materialization requires admin privileges"))
		return
	}
	assetKey := chi.URLParam(r, "assetKey")
	asset, err := h.Asset.GetAsset(r.Context(), assetKey)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	partitionKey := formOptionalString(r.Form, "partition_key")
	if _, err := h.Asset.TriggerMaterialization(r.Context(), asset.ID, partitionKey, nil, nil); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/assets/"+assetKey, http.StatusSeeOther)
}

func (h *Handler) AssetBackfillCreate(w http.ResponseWriter, r *http.Request) {
	if h.Asset == nil || h.Backfill == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Asset backfill UI is not configured."))
		return
	}

	principal := principalFromContext(r.Context())
	if !principal.IsAdmin {
		h.renderServiceError(w, r, domain.ErrAccessDenied("asset backfill requires admin privileges"))
		return
	}
	assetKey := chi.URLParam(r, "assetKey")
	asset, err := h.Asset.GetAsset(r.Context(), assetKey)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	maxParallelism := 0
	if value, parseErr := formOptionalInt(r.Form, "max_parallelism"); parseErr != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "max_parallelism must be an integer."))
		return
	} else if value != nil {
		maxParallelism = *value
	}

	requestedBy, _ := principalLabel(r.Context())
	_, _, err = h.Backfill.Create(
		r.Context(),
		asset.ID,
		requestedBy,
		formString(r.Form, "partition_from"),
		formString(r.Form, "partition_to"),
		maxParallelism,
	)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	http.Redirect(w, r, "/ui/assets/"+assetKey, http.StatusSeeOther)
}
