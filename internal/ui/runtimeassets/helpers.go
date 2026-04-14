package runtimeassets

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/ui/core"
)

func pageFromRequest(r *http.Request, defaultPageSize int) domain.PageRequest {
	maxResults := defaultPageSize
	if maxResults <= 0 {
		maxResults = 25
	}
	if raw := r.URL.Query().Get("max_results"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			maxResults = parsed
		}
	}
	if maxResults < 1 {
		maxResults = 1
	}
	if maxResults > 200 {
		maxResults = 200
	}
	return domain.PageRequest{
		MaxResults: maxResults,
		PageToken:  r.URL.Query().Get("page_token"),
	}
}

func parseFormOrRenderBadRequest(w http.ResponseWriter, r *http.Request) bool {
	if err := r.ParseForm(); err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Unable to parse form."))
		return false
	}
	return true
}

func renderServiceError(w http.ResponseWriter, err error) {
	status, message := core.ServiceErrorStatus(err)
	title := "Unexpected Error"
	switch status {
	case http.StatusNotFound:
		title = "Not Found"
	case http.StatusForbidden:
		title = "Access Denied"
	case http.StatusBadRequest:
		title = "Invalid Request"
	case http.StatusConflict:
		title = "Conflict"
	}
	core.RenderHTML(w, status, core.ErrorPage(title, message))
}

func formString(values map[string][]string, key string) string {
	if values == nil {
		return ""
	}
	return strings.TrimSpace(first(values[key]))
}

func formOptionalString(values map[string][]string, key string) *string {
	v := formString(values, key)
	if v == "" {
		return nil
	}
	return &v
}

func formOptionalInt(values map[string][]string, key string) (*int, error) {
	v := formString(values, key)
	if v == "" {
		return nil, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func principalName(r *http.Request) string {
	p := core.PrincipalFromContext(r.Context())
	if strings.TrimSpace(p.Name) == "" {
		return "unknown"
	}
	return p.Name
}

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	return ts.UTC().Format("2006-01-02 15:04 UTC")
}

func formatTimePtr(ts *time.Time) string {
	if ts == nil || ts.IsZero() {
		return "-"
	}
	return ts.UTC().Format("2006-01-02 15:04 UTC")
}

func fallbackString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
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

func formatAttemptSummary(attemptCount, maxAttempts int) string {
	if maxAttempts <= 0 {
		return strconv.Itoa(attemptCount) + "/-"
	}
	return strconv.Itoa(attemptCount) + "/" + strconv.Itoa(maxAttempts)
}

func partitionStatusTone(status string) string {
	upper := strings.ToUpper(strings.TrimSpace(status))
	switch upper {
	case "READY", "MATERIALIZED", "SUCCESS", "HEALTHY":
		return "success"
	case "FAILED", "ERROR", "LATE", "MISSING":
		return "severe"
	case "RUNNING", "PENDING", "QUEUED", "STALE":
		return "attention"
	default:
		return "accent"
	}
}
