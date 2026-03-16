package macros

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"
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

func formCSV(values map[string][]string, key string) []string {
	raw := formString(values, key)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil
	}
	return out
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

func csvValues(values []string) string {
	return strings.Join(values, ", ")
}

type macroImpactRowData struct {
	ModelName string
	LastSeen  string
	URL       string
}

func listMacroImpactAsOf(ctx context.Context, h *Handler, macroName string, asOf *time.Time) ([]macroImpactRowData, error) {
	if h.deps.Lineage == nil {
		return []macroImpactRowData{}, nil
	}
	pageReq := domain.PageRequest{MaxResults: domain.MaxMaxResults}
	edges, _, err := h.deps.Lineage.GetDownstream(ctx, "macro."+macroName, pageReq)
	if err != nil {
		return nil, err
	}

	seen := map[string]macroImpactRowData{}
	lastSeenByModel := map[string]time.Time{}
	for i := range edges {
		edge := edges[i]
		if asOf != nil && edge.CreatedAt.After(*asOf) {
			continue
		}
		if edge.TargetTable == nil || strings.TrimSpace(*edge.TargetTable) == "" {
			continue
		}
		targetTable := strings.TrimSpace(*edge.TargetTable)
		label := targetTable
		if strings.TrimSpace(edge.TargetSchema) != "" {
			label = edge.TargetSchema + "." + targetTable
		}
		currentSeen, ok := lastSeenByModel[label]
		if ok && !edge.CreatedAt.After(currentSeen) {
			continue
		}
		lastSeenByModel[label] = edge.CreatedAt
		seen[label] = macroImpactRowData{
			ModelName: label,
			LastSeen:  formatTime(edge.CreatedAt),
			URL:       "/ui/models/" + edge.TargetSchema + "/" + targetTable,
		}
	}

	rows := make([]macroImpactRowData, 0, len(seen))
	for _, row := range seen {
		rows = append(rows, row)
	}
	sortMacroImpactRows(rows)
	return rows, nil
}

func macroImpactDelta(ctx context.Context, h *Handler, macroName string, from, to *time.Time) ([]macroImpactRowData, []macroImpactRowData, []macroImpactRowData, error) {
	fromRows, err := listMacroImpactAsOf(ctx, h, macroName, from)
	if err != nil {
		return nil, nil, nil, err
	}
	toRows, err := listMacroImpactAsOf(ctx, h, macroName, to)
	if err != nil {
		return nil, nil, nil, err
	}

	fromByModel := map[string]macroImpactRowData{}
	toByModel := map[string]macroImpactRowData{}
	for i := range fromRows {
		fromByModel[fromRows[i].ModelName] = fromRows[i]
	}
	for i := range toRows {
		toByModel[toRows[i].ModelName] = toRows[i]
	}

	added := make([]macroImpactRowData, 0)
	removed := make([]macroImpactRowData, 0)
	unchanged := make([]macroImpactRowData, 0)
	for modelName, row := range toByModel {
		if _, ok := fromByModel[modelName]; ok {
			unchanged = append(unchanged, row)
			continue
		}
		added = append(added, row)
	}
	for modelName, row := range fromByModel {
		if _, ok := toByModel[modelName]; ok {
			continue
		}
		removed = append(removed, row)
	}

	sortMacroImpactRows(added)
	sortMacroImpactRows(removed)
	sortMacroImpactRows(unchanged)
	return added, removed, unchanged, nil
}

func sortMacroImpactRows(rows []macroImpactRowData) {
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].ModelName < rows[j].ModelName
	})
}
