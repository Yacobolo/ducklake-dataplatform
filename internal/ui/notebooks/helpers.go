package notebooks

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"
)

type accessShareRow struct {
	Principal string
	Role      string
	DeleteURL string
}

func parseFormOrRenderBadRequest(w http.ResponseWriter, r *http.Request) bool {
	if err := r.ParseForm(); err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Unable to parse form."))
		return false
	}
	return true
}

func formString(values map[string][]string, key string) string {
	if values == nil {
		return ""
	}
	return strings.TrimSpace(first(values[key]))
}

func formOptionalString(values map[string][]string, key string) *string {
	value := formString(values, key)
	if value == "" {
		return nil
	}
	return &value
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

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

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	return core.FormatTimeDisplay(ts)
}

func formatTimePtr(ts *time.Time) string {
	if ts == nil || ts.IsZero() {
		return "-"
	}
	return core.FormatTimeDisplay(*ts)
}

func strOrDash(v *string) string {
	if v == nil || *v == "" {
		return "-"
	}
	return *v
}

func valueOrDash(v string) string {
	if strings.TrimSpace(v) == "" {
		return "-"
	}
	return v
}

func stringPtr(v *string) string {
	if v == nil || strings.TrimSpace(*v) == "" {
		return "-"
	}
	return *v
}

func notebookExplorerURL(notebookID, catalogName, schemaName string) string {
	q := url.Values{}
	if catalogName != "" {
		q.Set("catalog", catalogName)
	}
	if schemaName != "" {
		q.Set("schema", schemaName)
	}
	base := "/ui/notebooks/" + notebookID
	encoded := q.Encode()
	if encoded == "" {
		return base
	}
	return base + "?" + encoded
}

func notebookShareRows(notebookID string, shares []domain.NotebookShare) []accessShareRow {
	rows := make([]accessShareRow, 0, len(shares))
	for i := range shares {
		share := shares[i]
		rows = append(rows, accessShareRow{
			Principal: share.PrincipalName,
			Role:      share.Role,
			DeleteURL: "/ui/notebooks/" + notebookID + "/shares/" + url.PathEscape(share.PrincipalName) + "/delete",
		})
	}
	return rows
}

func folderShareRows(folderID string, shares []domain.FolderShare) []accessShareRow {
	rows := make([]accessShareRow, 0, len(shares))
	for i := range shares {
		share := shares[i]
		rows = append(rows, accessShareRow{
			Principal: share.PrincipalName,
			Role:      share.Role,
			DeleteURL: "/ui/explore/folders/" + folderID + "/shares/" + url.PathEscape(share.PrincipalName) + "/delete",
		})
	}
	return rows
}

func formOptionalInt(values map[string][]string, key string) (*int, error) {
	value := formString(values, key)
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func formCSV(values map[string][]string, key string) []string {
	raw := formString(values, key)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func formBool(values map[string][]string, key string) bool {
	if values == nil {
		return false
	}
	value := strings.TrimSpace(strings.ToLower(first(values[key])))
	return value == "true" || value == "1" || value == "on" || value == "yes"
}

func normalizeExploreKind(kind string) string {
	switch strings.TrimSpace(kind) {
	case "", domain.ExploreKindAll:
		return domain.ExploreKindAll
	case domain.ExploreKindNotebook, domain.ExploreKindModel, domain.ExploreKindMacro,
		domain.ExploreKindDashboard, domain.ExploreKindPipeline, domain.ExploreKindSemanticModel:
		return strings.TrimSpace(kind)
	default:
		return domain.ExploreKindAll
	}
}

type sqlComputeTarget struct {
	Label              string
	Mode               string
	EndpointName       string
	Status             string
	AvailabilityReason string
	Default            bool
	Selectable         bool
}

const sqlEditorMaxRows = 200

func sqlComputeContext(ctx context.Context, req domain.ComputeExecutionRequest) (context.Context, error) {
	req = req.Normalize()
	if req.WorkloadType == "" {
		req.WorkloadType = domain.ComputeWorkloadInteractive
	}
	if req.Mode == "" {
		req.Mode = domain.ComputeModeByocLocal
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	return domain.WithComputeExecutionRequest(ctx, req), nil
}

func sqlComputeExecutionRequestFromForm(form url.Values) domain.ComputeExecutionRequest {
	if form == nil {
		return domain.ComputeExecutionRequest{}
	}
	return domain.ComputeExecutionRequest{
		Mode:         strings.TrimSpace(form.Get("compute_mode")),
		EndpointName: strings.TrimSpace(form.Get("endpoint_name")),
		WorkloadType: strings.TrimSpace(form.Get("workload_type")),
	}
}

func sqlComputeTargetsFromDomain(targets []domain.ComputeTarget) []sqlComputeTarget {
	items := make([]sqlComputeTarget, 0, len(targets))
	for _, target := range targets {
		endpointName := ""
		if target.EndpointName != nil {
			endpointName = *target.EndpointName
		}
		reason := ""
		if target.AvailabilityReason != nil {
			reason = *target.AvailabilityReason
		}
		items = append(items, sqlComputeTarget{
			Label:              target.DisplayName,
			Mode:               target.Mode,
			EndpointName:       endpointName,
			Status:             target.Status,
			AvailabilityReason: reason,
			Default:            target.IsDefault,
			Selectable:         target.SelectableForInteractive,
		})
	}
	return items
}

func sqlApplyDefaultComputeTarget(req domain.ComputeExecutionRequest, targets []sqlComputeTarget) domain.ComputeExecutionRequest {
	for _, target := range targets {
		if !target.Default {
			continue
		}
		req.Mode = target.Mode
		req.EndpointName = target.EndpointName
		return req
	}
	req.Mode = domain.ComputeModeByocLocal
	return req
}

func sqlCellString(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", value)
}

type notebookCellResultData struct {
	Columns    []string
	RawRows    [][]interface{}
	Rows       [][]string
	RowCount   int
	Error      string
	Duration   time.Duration
	ExecutedAt *time.Time
}

type persistedNotebookCellResult struct {
	Columns    []string        `json:"Columns"`
	Rows       [][]interface{} `json:"Rows"`
	RowCount   int             `json:"RowCount"`
	Error      *string         `json:"Error"`
	Duration   time.Duration   `json:"Duration"`
	ExecutedAt *time.Time      `json:"ExecutedAt"`
}

func parseNotebookCellResult(raw *string) *notebookCellResultData {
	if raw == nil || *raw == "" {
		return nil
	}

	var parsed persistedNotebookCellResult
	if err := json.Unmarshal([]byte(*raw), &parsed); err != nil {
		return &notebookCellResultData{Error: "Unable to parse cached result."}
	}

	rows := make([][]string, 0, len(parsed.Rows))
	for i := range parsed.Rows {
		cells := make([]string, 0, len(parsed.Rows[i]))
		for j := range parsed.Rows[i] {
			cells = append(cells, sqlCellString(parsed.Rows[i][j]))
		}
		rows = append(rows, cells)
	}

	out := &notebookCellResultData{
		Columns:    parsed.Columns,
		RawRows:    parsed.Rows,
		Rows:       rows,
		RowCount:   parsed.RowCount,
		Duration:   parsed.Duration,
		ExecutedAt: parsed.ExecutedAt,
	}
	if parsed.Error != nil {
		out.Error = *parsed.Error
	}
	return out
}

func writeCSV(columns []string, rows [][]string) ([]byte, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write(columns); err != nil {
		return nil, err
	}
	for i := range rows {
		if err := writer.Write(rows[i]); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
