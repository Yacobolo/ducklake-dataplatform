package ui

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

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"

	gomponents "maragu.dev/gomponents"
)

const sqlEditorMaxRows = 200
const sqlEditorCSVMaxRows = 5000

type queryAsyncUI interface {
	SubmitAsync(ctx context.Context, principalName, sqlQuery, requestID string) (*domain.QueryJob, error)
	GetAsyncJob(ctx context.Context, principalName, jobID string) (*domain.QueryJob, error)
	ListAsyncJobs(ctx context.Context, principalName string, page domain.PageRequest) ([]domain.QueryJob, int64, error)
	CancelAsyncJob(ctx context.Context, principalName, jobID string) error
	DeleteAsyncJob(ctx context.Context, principalName, jobID string) error
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

func (h *Handler) SQLEditorPage(w http.ResponseWriter, r *http.Request) {
	state := h.sqlEditorState(r, nil)
	sqlText := strings.TrimSpace(r.URL.Query().Get("sql"))
	if sqlText == "" {
		sqlText = defaultSQLSnippet(r.URL.Query().Get("snippet"), state.SelectedCatalog, state.SelectedSchema)
	}
	h.renderSQLEditor(w, r, sqlText, nil, "", state)
}

func (h *Handler) SQLEditorRun(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	sqlText := strings.TrimSpace(r.Form.Get("sql"))
	state := h.sqlEditorState(r, r.Form)
	principal, _ := principalLabel(r.Context())
	ctx, err := h.sqlComputeContext(r.Context(), state.ComputeRequest)
	if err != nil {
		h.renderSQLEditor(w, r, sqlText, nil, err.Error(), state)
		return
	}
	result, err := h.Query.Execute(ctx, principal, sqlText)
	if err != nil {
		h.renderSQLEditor(w, r, sqlText, nil, err.Error(), state)
		return
	}

	h.renderSQLEditor(w, r, sqlText, result, "", state)
}

func (h *Handler) SQLEditorDownloadCSV(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	sqlText := strings.TrimSpace(r.Form.Get("sql"))
	state := h.sqlEditorState(r, r.Form)
	principal, _ := principalLabel(r.Context())
	ctx, err := h.sqlComputeContext(r.Context(), state.ComputeRequest)
	if err != nil {
		h.renderSQLEditor(w, r, sqlText, nil, err.Error(), state)
		return
	}
	result, err := h.Query.Execute(ctx, principal, sqlText)
	if err != nil {
		h.renderSQLEditor(w, r, sqlText, nil, err.Error(), state)
		return
	}

	rows := result.Rows
	if len(rows) > sqlEditorCSVMaxRows {
		rows = rows[:sqlEditorCSVMaxRows]
	}

	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write(result.Columns); err != nil {
		renderHTML(w, http.StatusInternalServerError, errorPage("Export Failed", "Failed writing CSV header."))
		return
	}
	for i := range rows {
		record := make([]string, 0, len(rows[i]))
		for j := range rows[i] {
			record = append(record, sqlCellString(rows[i][j]))
		}
		if err := writer.Write(record); err != nil {
			renderHTML(w, http.StatusInternalServerError, errorPage("Export Failed", "Failed writing CSV rows."))
			return
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		renderHTML(w, http.StatusInternalServerError, errorPage("Export Failed", "Failed finalizing CSV."))
		return
	}

	filename := "query-results.csv"
	if state.SelectedCatalog != "" && state.SelectedSchema != "" {
		filename = fmt.Sprintf("%s_%s_results.csv", state.SelectedCatalog, state.SelectedSchema)
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	if len(result.Rows) > sqlEditorCSVMaxRows {
		w.Header().Set("X-Duck-Results-Truncated", "true")
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

func (h *Handler) SQLEditorRunAsync(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	asyncSvc, ok := any(h.Query).(queryAsyncUI)
	if !ok {
		renderHTML(w, http.StatusInternalServerError, errorPage("Async Query Unavailable", "Async query service is not configured."))
		return
	}

	sqlText := strings.TrimSpace(r.Form.Get("sql"))
	principal, _ := principalLabel(r.Context())
	state := h.sqlEditorState(r, r.Form)
	if strings.EqualFold(state.ComputeRequest.Mode, domain.ComputeModeByocLocal) {
		h.renderSQLEditor(w, r, sqlText, nil, "BYOC local execution is interactive only. Switch compute mode to Shared Endpoint or Auto for async jobs.", state)
		return
	}
	ctx, err := h.sqlComputeContext(r.Context(), state.ComputeRequest)
	if err != nil {
		h.renderSQLEditor(w, r, sqlText, nil, err.Error(), state)
		return
	}
	job, err := asyncSvc.SubmitAsync(ctx, principal, sqlText, strings.TrimSpace(r.Form.Get("request_id")))
	if err != nil {
		h.renderSQLEditor(w, r, sqlText, nil, err.Error(), state)
		return
	}

	http.Redirect(w, r, "/ui/sql/jobs/"+job.ID, http.StatusSeeOther)
}

func (h *Handler) SQLEditorRuntimeManifest(w http.ResponseWriter, r *http.Request) {
	if h.Manifest == nil {
		writeJSONError(w, http.StatusInternalServerError, "manifest service is not configured")
		return
	}

	catalogName := strings.TrimSpace(r.URL.Query().Get("catalog"))
	schemaName := strings.TrimSpace(r.URL.Query().Get("schema"))
	tableName := strings.TrimSpace(r.URL.Query().Get("table"))
	if schemaName == "" {
		schemaName = "main"
	}
	if tableName == "" {
		writeJSONError(w, http.StatusBadRequest, "table is required")
		return
	}

	principal, _ := principalLabel(r.Context())
	result, err := h.Manifest.GetManifest(r.Context(), principal, catalogName, schemaName, tableName)
	if err != nil {
		status, message := serviceErrorStatus(err)
		writeJSONError(w, status, message)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(result)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func (h *Handler) SQLEditorJobsList(w http.ResponseWriter, r *http.Request) {
	asyncSvc, ok := any(h.Query).(queryAsyncUI)
	if !ok {
		renderHTML(w, http.StatusInternalServerError, errorPage("Async Query Unavailable", "Async query service is not configured."))
		return
	}

	pageReq := pageFromRequest(r, 30)
	principal, _ := principalLabel(r.Context())
	jobs, total, err := asyncSvc.ListAsyncJobs(r.Context(), principal, pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]sqlAsyncJobRowData, 0, len(jobs))
	for i := range jobs {
		job := jobs[i]
		rows = append(rows, sqlAsyncJobRowData{
			JobID:       job.ID,
			URL:         "/ui/sql/jobs/" + job.ID,
			Status:      string(job.Status),
			Compute:     sqlComputeSummary(job.ComputeMode, job.EndpointName, job.ResolvedMode, job.ResolvedEndpointName),
			RequestID:   job.RequestID,
			RowCount:    strconv.Itoa(job.RowCount),
			CreatedAt:   formatTime(job.CreatedAt),
			CompletedAt: formatTimePtr(job.CompletedAt),
		})
	}

	renderHTML(w, http.StatusOK, sqlAsyncJobsListPage(sqlAsyncJobsListPageData{
		Principal: principalFromContext(r.Context()),
		Rows:      rows,
		Page:      pageReq,
		Total:     total,
	}))
}

func (h *Handler) SQLEditorJobDetail(w http.ResponseWriter, r *http.Request) {
	asyncSvc, ok := any(h.Query).(queryAsyncUI)
	if !ok {
		renderHTML(w, http.StatusInternalServerError, errorPage("Async Query Unavailable", "Async query service is not configured."))
		return
	}

	jobID := chi.URLParam(r, "jobID")
	principal, _ := principalLabel(r.Context())
	job, err := asyncSvc.GetAsyncJob(r.Context(), principal, jobID)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	renderHTML(w, http.StatusOK, sqlAsyncJobPage(sqlAsyncJobPageData{
		Principal:         principalFromContext(r.Context()),
		JobID:             job.ID,
		Status:            string(job.Status),
		RequestID:         job.RequestID,
		SQLText:           job.SQLText,
		Columns:           job.Columns,
		Rows:              job.Rows,
		RowCount:          job.RowCount,
		RequestedCompute:  sqlRequestedComputeSummary(job.ComputeMode, job.EndpointName, job.WorkloadType),
		ResolvedCompute:   sqlResolvedComputeSummary(job.ResolvedMode, job.ResolvedEndpointName),
		ErrorText:         strOrDash(job.ErrorMessage),
		AttemptCount:      job.AttemptCount,
		MaxAttempts:       job.MaxAttempts,
		LastHeartbeatText: formatTimePtr(job.LastHeartbeat),
		NextRetryText:     formatTimePtr(job.NextRetryAt),
		CreatedAtText:     formatTime(job.CreatedAt),
		StartedAtText:     formatTimePtr(job.StartedAt),
		CompletedAtText:   formatTimePtr(job.CompletedAt),
		CancelURL:         "/ui/sql/jobs/" + job.ID + "/cancel",
		DeleteURL:         "/ui/sql/jobs/" + job.ID + "/delete",
		EditorURL:         "/ui/sql?sql=" + url.QueryEscape(job.SQLText),
		CSRFFieldProvider: csrfFieldProvider(r),
	}))
}

func (h *Handler) SQLEditorJobCancel(w http.ResponseWriter, r *http.Request) {
	asyncSvc, ok := any(h.Query).(queryAsyncUI)
	if !ok {
		renderHTML(w, http.StatusInternalServerError, errorPage("Async Query Unavailable", "Async query service is not configured."))
		return
	}
	jobID := chi.URLParam(r, "jobID")
	principal, _ := principalLabel(r.Context())
	if err := asyncSvc.CancelAsyncJob(r.Context(), principal, jobID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/sql/jobs/"+jobID, http.StatusSeeOther)
}

func (h *Handler) SQLEditorJobDelete(w http.ResponseWriter, r *http.Request) {
	asyncSvc, ok := any(h.Query).(queryAsyncUI)
	if !ok {
		renderHTML(w, http.StatusInternalServerError, errorPage("Async Query Unavailable", "Async query service is not configured."))
		return
	}
	jobID := chi.URLParam(r, "jobID")
	principal, _ := principalLabel(r.Context())
	if err := asyncSvc.DeleteAsyncJob(r.Context(), principal, jobID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/sql", http.StatusSeeOther)
}

func (h *Handler) renderSQLEditor(w http.ResponseWriter, r *http.Request, sqlText string, result *query.QueryResult, runError string, state sqlEditorContext) {
	renderHTML(w, http.StatusOK, sqlEditorPage(principalFromContext(r.Context()), sqlText, result, runError, state, func() gomponents.Node { return csrfField(r) }))
}

type sqlEditorContext struct {
	SelectedCatalog string
	SelectedSchema  string
	Catalogs        []domain.CatalogRegistration
	Schemas         []domain.SchemaDetail
	BrowserRuntime  query.ManifestBrowserRuntimeSpec
	ComputeTargets  []sqlComputeTarget
	ComputeRequest  domain.ComputeExecutionRequest
}

func (h *Handler) sqlEditorState(r *http.Request, form url.Values) sqlEditorContext {
	selectedCatalog := strings.TrimSpace(r.URL.Query().Get("catalog"))
	selectedSchema := strings.TrimSpace(r.URL.Query().Get("schema"))
	if form != nil {
		if c := strings.TrimSpace(form.Get("catalog")); c != "" {
			selectedCatalog = c
		}
		if s := strings.TrimSpace(form.Get("schema")); s != "" {
			selectedSchema = s
		}
	}

	catalogs, _, err := h.CatalogRegistration.List(r.Context(), domain.PageRequest{MaxResults: 100})
	if err != nil {
		catalogs = nil
	}
	if selectedCatalog == "" && len(catalogs) > 0 {
		selectedCatalog = catalogs[0].Name
	}

	var schemas []domain.SchemaDetail
	if selectedCatalog != "" {
		s, _, err := h.Catalog.ListSchemas(r.Context(), selectedCatalog, domain.PageRequest{MaxResults: 200})
		if err == nil {
			schemas = s
		}
	}
	if selectedSchema == "" && len(schemas) > 0 {
		selectedSchema = schemas[0].Name
	}

	computeReq := sqlComputeExecutionRequestFromForm(form)
	computeTargets := []sqlComputeTarget{}
	if h.ComputeEndpoint != nil {
		principal, _ := principalLabel(r.Context())
		if targets, err := h.ComputeEndpoint.ListAvailableTargets(r.Context(), principal, domain.ComputeWorkloadInteractive); err == nil {
			computeTargets = sqlComputeTargetsFromDomain(targets)
		}
	}
	if computeReq.WorkloadType == "" {
		computeReq.WorkloadType = domain.ComputeWorkloadInteractive
	}
	if computeReq.Mode == "" {
		computeReq = sqlApplyDefaultComputeTarget(computeReq, computeTargets)
	}
	computeReq = computeReq.Normalize()

	return sqlEditorContext{
		SelectedCatalog: selectedCatalog,
		SelectedSchema:  selectedSchema,
		Catalogs:        catalogs,
		Schemas:         schemas,
		BrowserRuntime:  query.DefaultManifestBrowserRuntimeSpec(),
		ComputeTargets:  computeTargets,
		ComputeRequest:  computeReq,
	}
}

func (h *Handler) sqlComputeContext(ctx context.Context, req domain.ComputeExecutionRequest) (context.Context, error) {
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

func sqlRequestedComputeSummary(mode string, endpointName *string, workloadType string) string {
	mode = strings.TrimSpace(mode)
	if mode == "" {
		mode = domain.ComputeModeAuto
	}
	parts := []string{mode}
	if endpointName != nil && strings.TrimSpace(*endpointName) != "" {
		parts = append(parts, *endpointName)
	}
	if strings.TrimSpace(workloadType) != "" {
		parts = append(parts, strings.TrimSpace(workloadType))
	}
	return strings.Join(parts, " · ")
}

func sqlResolvedComputeSummary(mode, endpointName *string) string {
	if mode == nil || strings.TrimSpace(*mode) == "" {
		return "Pending"
	}
	parts := []string{strings.TrimSpace(*mode)}
	if endpointName != nil && strings.TrimSpace(*endpointName) != "" {
		parts = append(parts, strings.TrimSpace(*endpointName))
	}
	return strings.Join(parts, " · ")
}

func sqlComputeSummary(requestedMode string, endpointName, resolvedMode, resolvedEndpointName *string) string {
	if resolvedMode != nil && strings.TrimSpace(*resolvedMode) != "" {
		return sqlResolvedComputeSummary(resolvedMode, resolvedEndpointName)
	}
	return sqlRequestedComputeSummary(requestedMode, endpointName, "")
}

func defaultSQLSnippet(snippetID, catalogName, schemaName string) string {
	qualifiedSchema := schemaName
	if catalogName != "" && schemaName != "" {
		qualifiedSchema = catalogName + "." + schemaName
	}
	schemaFilter := schemaName
	if schemaFilter == "" {
		schemaFilter = "main"
	}

	switch snippetID {
	case "show_tables":
		return fmt.Sprintf("SELECT table_name\nFROM information_schema.tables\nWHERE table_schema = '%s'\nORDER BY table_name;", schemaFilter)
	case "show_views":
		return fmt.Sprintf("SELECT table_name\nFROM information_schema.views\nWHERE table_schema = '%s'\nORDER BY table_name;", schemaFilter)
	case "describe_table":
		if qualifiedSchema != "" {
			return fmt.Sprintf("DESCRIBE SELECT * FROM %s.<table_name>;", qualifiedSchema)
		}
		return "DESCRIBE SELECT * FROM <schema_name>.<table_name>;"
	case "sample_rows":
		if qualifiedSchema != "" {
			return fmt.Sprintf("SELECT *\nFROM %s.<table_name>\nLIMIT 50;", qualifiedSchema)
		}
		return "SELECT *\nFROM <schema_name>.<table_name>\nLIMIT 50;"
	default:
		return ""
	}
}

func sqlCellString(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", value)
}
