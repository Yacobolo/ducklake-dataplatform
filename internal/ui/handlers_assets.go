package ui

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

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
		partitionType := "Unpartitioned"
		if item.PartitionDefinition != nil {
			partitionType = strings.Title(strings.ToLower(strings.TrimSpace(item.PartitionDefinition.Type)))
		}
		materializationMode := "Manual"
		if item.MaterializationPolicy != nil && strings.TrimSpace(item.MaterializationPolicy.Mode) != "" {
			materializationMode = strings.Title(strings.ToLower(strings.TrimSpace(item.MaterializationPolicy.Mode)))
		}
		if item.AutoMaterializePolicy != nil && strings.TrimSpace(item.AutoMaterializePolicy.Mode) != "" {
			materializationMode = strings.Title(strings.ToLower(strings.TrimSpace(item.AutoMaterializePolicy.Mode)))
		}
		rows = append(rows, assetsListRowData{
			Filter:              strings.Join(append([]string{item.AssetKey, item.AssetType, item.Owner, item.Description}, item.Tags...), " "),
			AssetKey:            item.AssetKey,
			URL:                 "/ui/assets/" + item.AssetKey,
			Type:                item.AssetType,
			Owner:               item.Owner,
			Description:         item.Description,
			Tags:                append([]string(nil), item.Tags...),
			Active:              item.IsActive,
			Updated:             formatTime(item.UpdatedAt),
			FreshnessTracked:    item.FreshnessPolicy != nil,
			PartitionType:       partitionType,
			AutoMaterialized:    item.AutoMaterializePolicy != nil,
			MaterializationMode: materializationMode,
		})
	}

	canMaterialize, err := h.Asset.CanTriggerMaterialization(r.Context())
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	filterValue := strings.TrimSpace(r.URL.Query().Get("q"))
	backfillConfigured := h.Backfill != nil

	renderHTML(w, http.StatusOK, assetsListPage(principalFromContext(r.Context()), rows, pageReq, total, filterValue, canMaterialize, backfillConfigured))
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
	assetIDs := dependencyAssetIDs(upstreamDeps, downstreamDeps)
	keyByID, err := h.Asset.ResolveAssetKeys(r.Context(), assetIDs)
	if err != nil {
		h.renderServiceError(w, r, err)
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

	freshnessLabel, freshnessTone := freshnessStatus(asset, materializations)
	partitionStatus := summarizePartitions(partitions)
	retryTimeline := shapeRetryTimeline(runs)
	failureRootCauses := shapeFailureRootCauses(runs)
	partitionCalendar := shapePartitionCalendar(partitions)
	canMaterialize, err := h.Asset.CanTriggerMaterialization(r.Context())
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	backfillConfigured := h.Backfill != nil
	canBackfill := false
	if backfillConfigured {
		requestedBy, _ := principalLabel(r.Context())
		canBackfill, err = h.Backfill.CanCreate(r.Context(), requestedBy)
		if err != nil {
			h.renderServiceError(w, r, err)
			return
		}
	}

	renderHTML(w, http.StatusOK, assetDetailPage(assetDetailPageData{
		Principal:           principalFromContext(r.Context()),
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
		CSRFFieldFunc:       csrfFieldProvider(r),
	}))
}

func freshnessStatus(asset *domain.DataAsset, materializations []domain.AssetMaterialization) (string, string) {
	if asset == nil || asset.FreshnessPolicy == nil {
		return "No SLA", "accent"
	}
	if asset.FreshnessPolicy.MaxLagSeconds <= 0 {
		return "Configured", "attention"
	}
	if len(materializations) == 0 {
		return "Never materialized", "severe"
	}
	lag := time.Since(materializations[0].MaterializedAt)
	if lag <= time.Duration(asset.FreshnessPolicy.MaxLagSeconds)*time.Second {
		return "Healthy", "success"
	}
	return "Stale", "severe"
}

func summarizePartitions(partitions []domain.AssetPartition) map[string]int {
	counts := make(map[string]int)
	for i := range partitions {
		counts[partitions[i].Status]++
	}
	return counts
}

func shapeRetryTimeline(runs []domain.AssetRun) []assetRetryTimelineEntry {
	entries := make([]assetRetryTimelineEntry, 0, len(runs))
	for i := range runs {
		r := runs[i]
		windowLabel := "created " + formatTime(r.CreatedAt)
		started := formatTimePtr(r.StartedAt)
		finished := formatTimePtr(r.FinishedAt)
		if started != "-" || finished != "-" {
			windowLabel = started + " -> " + finished
		}

		retryHint := ""
		if r.MaxAttempts > 0 && r.AttemptCount < r.MaxAttempts {
			retryHint = fmt.Sprintf("%d retries remaining", r.MaxAttempts-r.AttemptCount)
		}

		entries = append(entries, assetRetryTimelineEntry{
			RunID:          r.ID,
			Status:         r.Status,
			TriggerType:    r.TriggerType,
			AttemptSummary: formatAttemptSummary(r.AttemptCount, r.MaxAttempts),
			WindowLabel:    windowLabel,
			RetryHint:      retryHint,
			IsRetry:        r.AttemptCount > 1 || r.Status == domain.AssetRunStatusRetrying,
		})
	}
	return entries
}

func shapeFailureRootCauses(runs []domain.AssetRun) []assetFailureRootCauseGroup {
	type groupedFailure struct {
		Signature   string
		Message     string
		Count       int
		LastSeen    time.Time
		Statuses    map[string]struct{}
		RunIDs      []string
		RunIDLookup map[string]struct{}
	}

	groups := make(map[string]*groupedFailure)
	for i := range runs {
		r := runs[i]
		if !isFailureLikeRunStatus(r.Status) {
			continue
		}

		message := strings.TrimSpace(ptrStringValue(r.ErrorMessage))
		if message == "" {
			message = "Status=" + r.Status
		}

		signature := normalizeFailureSignature(message)
		group := groups[signature]
		if group == nil {
			group = &groupedFailure{
				Signature:   signature,
				Message:     compactFailureMessage(message),
				Statuses:    map[string]struct{}{},
				RunIDs:      make([]string, 0, 3),
				RunIDLookup: map[string]struct{}{},
			}
			groups[signature] = group
		}

		group.Count++
		group.Statuses[r.Status] = struct{}{}
		if _, exists := group.RunIDLookup[r.ID]; !exists && len(group.RunIDs) < 3 {
			group.RunIDs = append(group.RunIDs, r.ID)
			group.RunIDLookup[r.ID] = struct{}{}
		}

		seenAt := r.CreatedAt
		if r.FinishedAt != nil && !r.FinishedAt.IsZero() {
			seenAt = *r.FinishedAt
		} else if r.StartedAt != nil && !r.StartedAt.IsZero() {
			seenAt = *r.StartedAt
		}
		if seenAt.After(group.LastSeen) {
			group.LastSeen = seenAt
			group.Message = compactFailureMessage(message)
		}
	}

	items := make([]assetFailureRootCauseGroup, 0, len(groups))
	for _, group := range groups {
		statuses := make([]string, 0, len(group.Statuses))
		for status := range group.Statuses {
			statuses = append(statuses, status)
		}
		sort.Strings(statuses)

		items = append(items, assetFailureRootCauseGroup{
			Signature: group.Signature,
			Message:   group.Message,
			Count:     group.Count,
			LastSeen:  formatTime(group.LastSeen),
			Statuses:  statuses,
			RunIDs:    group.RunIDs,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].LastSeen > items[j].LastSeen
		}
		return items[i].Count > items[j].Count
	})

	if len(items) > 8 {
		items = items[:8]
	}

	return items
}

func shapePartitionCalendar(partitions []domain.AssetPartition) []assetPartitionCalendarMonth {
	type dayInfo struct {
		PartitionKey string
		Status       string
		UpdatedAt    time.Time
	}

	monthDays := make(map[string]map[int]dayInfo)
	for i := range partitions {
		partition := partitions[i]
		day, ok := parsePartitionDay(partition)
		if !ok {
			continue
		}
		monthKey := day.Format("2006-01")
		dayOfMonth := day.Day()

		days := monthDays[monthKey]
		if days == nil {
			days = map[int]dayInfo{}
			monthDays[monthKey] = days
		}

		if existing, exists := days[dayOfMonth]; exists && !partition.UpdatedAt.After(existing.UpdatedAt) {
			continue
		}

		days[dayOfMonth] = dayInfo{PartitionKey: partition.PartitionKey, Status: partition.Status, UpdatedAt: partition.UpdatedAt}
	}

	monthKeys := make([]string, 0, len(monthDays))
	for key := range monthDays {
		monthKeys = append(monthKeys, key)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(monthKeys)))

	result := make([]assetPartitionCalendarMonth, 0, len(monthKeys))
	for i := range monthKeys {
		monthKey := monthKeys[i]
		monthStart, err := time.Parse("2006-01", monthKey)
		if err != nil {
			continue
		}

		cells := make([]assetPartitionCalendarCell, 0, 42)
		for padding := 0; padding < int(monthStart.Weekday()); padding++ {
			cells = append(cells, assetPartitionCalendarCell{IsPadding: true})
		}

		dayCount := daysInMonth(monthStart)
		for day := 1; day <= dayCount; day++ {
			dayLabel := strconv.Itoa(day)
			cell := assetPartitionCalendarCell{DayLabel: dayLabel}
			if info, exists := monthDays[monthKey][day]; exists {
				cell.HasPartition = true
				cell.PartitionKey = info.PartitionKey
				cell.Status = info.Status
				cell.Tone = partitionStatusTone(info.Status)
			}
			cells = append(cells, cell)
		}

		result = append(result, assetPartitionCalendarMonth{Label: monthStart.Format("January 2006"), Cells: cells})
	}

	return result
}

func isFailureLikeRunStatus(status string) bool {
	switch status {
	case domain.AssetRunStatusFailed, domain.AssetRunStatusCancelled, domain.AssetRunStatusRetrying, domain.AssetRunStatusStale:
		return true
	default:
		return false
	}
}

func normalizeFailureSignature(message string) string {
	trimmed := strings.ToLower(strings.TrimSpace(message))
	if trimmed == "" {
		return "unknown"
	}
	normalized := strings.Join(strings.Fields(trimmed), " ")
	if len(normalized) > 72 {
		return normalized[:72]
	}
	return normalized
}

func compactFailureMessage(message string) string {
	trimmed := strings.TrimSpace(strings.Split(message, "\n")[0])
	if trimmed == "" {
		return "-"
	}
	if len(trimmed) > 160 {
		return trimmed[:160] + "..."
	}
	return trimmed
}

func ptrStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func parsePartitionDay(partition domain.AssetPartition) (time.Time, bool) {
	if partition.PartitionTime != nil && !partition.PartitionTime.IsZero() {
		ts := partition.PartitionTime.UTC()
		return time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC), true
	}

	key := strings.TrimSpace(partition.PartitionKey)
	if key == "" {
		return time.Time{}, false
	}

	if len(key) >= len("2006-01-02") {
		prefix := key[:len("2006-01-02")]
		if day, err := time.Parse("2006-01-02", prefix); err == nil {
			return day, true
		}
	}

	layouts := []string{"20060102", "2006/01/02", time.RFC3339, "2006-01-02 15:04:05"}
	for i := range layouts {
		if day, err := time.Parse(layouts[i], key); err == nil {
			return time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC), true
		}
	}

	return time.Time{}, false
}

func daysInMonth(value time.Time) int {
	return time.Date(value.Year(), value.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
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

func (h *Handler) AssetMaterialize(w http.ResponseWriter, r *http.Request) {
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
