package models

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
	modelsvc "github.com/Yacobolo/quackstack/internal/service/model"
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
	return domain.PageRequest{MaxResults: maxResults, PageToken: r.URL.Query().Get("page_token")}
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

func principalName(r *http.Request) string {
	p := core.PrincipalFromContext(r.Context())
	if strings.TrimSpace(p.Name) == "" {
		return "unknown"
	}
	return p.Name
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

func csvValues(values []string) string {
	return strings.Join(values, ", ")
}

func stringsJoin(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ", ")
}

func valueOrDash(v string) string {
	if strings.TrimSpace(v) == "" {
		return "-"
	}
	return v
}

func strOrDash(v *string) string {
	if v == nil || strings.TrimSpace(*v) == "" {
		return "-"
	}
	return *v
}

func mapJSON(m map[string]string) string {
	if len(m) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, ", ")
}

func modelConfigMap(c domain.ModelConfig) map[string]string {
	out := map[string]string{}
	if len(c.UniqueKey) > 0 {
		out["unique_key"] = stringsJoin(c.UniqueKey)
	}
	if c.IncrementalStrategy != "" {
		out["incremental_strategy"] = c.IncrementalStrategy
	}
	if c.OnSchemaChange != "" {
		out["on_schema_change"] = c.OnSchemaChange
	}
	return out
}

func mapDAGTiers(tiers [][]modelsvc.DAGNode) []modelDAGTierData {
	out := make([]modelDAGTierData, 0, len(tiers))
	for i := range tiers {
		nodes := make([]modelDAGNodeData, 0, len(tiers[i]))
		for j := range tiers[i] {
			node := tiers[i][j]
			nodes = append(nodes, modelDAGNodeData{
				Name:         node.Model.QualifiedName(),
				Materialized: node.Model.Materialization,
				DependsOn:    stringsJoin(node.Model.DependsOn),
				URL:          "/ui/models/" + node.Model.ProjectName + "/" + node.Model.Name,
			})
		}
		out = append(out, modelDAGTierData{Label: "Tier " + strconv.Itoa(i), Nodes: nodes})
	}
	return out
}

func mapModelTestResults(results []domain.ModelTestResult) []modelTestResultRowData {
	rows := make([]modelTestResultRowData, 0, len(results))
	for i := range results {
		result := results[i]
		rows = append(rows, modelTestResultRowData{
			TestName: result.TestName,
			Status:   result.Status,
			Message:  strOrDash(result.ErrorMessage),
			Executed: formatTime(result.CreatedAt),
		})
	}
	return rows
}

func int64PtrString(v *int64) string {
	if v == nil {
		return "-"
	}
	return strconv.FormatInt(*v, 10)
}

func defaultString(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func boolLabel(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
