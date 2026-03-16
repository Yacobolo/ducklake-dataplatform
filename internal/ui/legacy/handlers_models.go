package legacy

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
	modelsvc "duck-demo/internal/service/model"
)

func (h *Handler) ModelsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	var projectName *string
	if p := r.URL.Query().Get("project"); p != "" {
		projectName = &p
	}

	items, total, err := h.Model.ListModels(r.Context(), projectName, pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]modelsListRowData, 0, len(items))
	for i := range items {
		m := items[i]
		rows = append(rows, modelsListRowData{
			FilterValue:   m.ProjectName + "." + m.Name + " " + m.Materialization,
			DetailURL:     fmt.Sprintf("/ui/models/%s/%s", m.ProjectName, m.Name),
			ModelName:     m.ProjectName + "." + m.Name,
			Materialized:  m.Materialization,
			Dependencies:  strconv.Itoa(len(m.DependsOn)),
			UpdatedAtText: formatTime(m.UpdatedAt),
		})
	}

	renderHTML(w, http.StatusOK, modelsListPage(modelsListPageData{
		Principal: principalFromContext(r.Context()),
		Rows:      rows,
		Page:      pageReq,
		Total:     total,
	}))
}

func (h *Handler) ModelsDetail(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")

	m, err := h.Model.GetModel(r.Context(), projectName, modelName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	tests, err := h.Model.ListTests(r.Context(), projectName, modelName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	freshness, err := h.Model.CheckFreshness(r.Context(), projectName, modelName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	testRows := make([]modelTestRowData, 0, len(tests))
	for i := range tests {
		t := tests[i]
		testRows = append(testRows, modelTestRowData{
			Name:      t.Name,
			TestType:  t.TestType,
			Column:    t.Column,
			DeleteURL: "/ui/models/" + projectName + "/" + modelName + "/tests/" + t.ID + "/delete",
		})
	}

	renderHTML(w, http.StatusOK, modelsDetailPage(modelsDetailPageData{
		Principal:          principalFromContext(r.Context()),
		ProjectName:        projectName,
		ModelName:          modelName,
		QualifiedName:      m.ProjectName + "." + m.Name,
		Materialization:    m.Materialization,
		Owner:              m.Owner,
		DependsOn:          stringsJoin(m.DependsOn),
		ConfigText:         mapJSON(modelConfigMap(m.Config)),
		EditURL:            "/ui/models/" + projectName + "/" + modelName + "/edit",
		DeleteURL:          "/ui/models/" + projectName + "/" + modelName + "/delete",
		NewTestURL:         "/ui/models/" + projectName + "/" + modelName + "/tests/new",
		TriggerRunURL:      "/ui/models/runs/trigger",
		CancelRunURL:       "/ui/models/runs/manual-cancel",
		RunsURL:            "/ui/models/runs",
		DAGURL:             "/ui/models/dag?project=" + projectName,
		FreshnessURL:       "/ui/models/" + projectName + "/" + modelName + "/freshness",
		SourceFreshnessURL: "/ui/models/source-freshness",
		DefaultSelector:    m.ProjectName + "." + m.Name,
		SQL:                m.SQL,
		Tests:              testRows,
		TriggerProject:     projectName,
		TriggerModel:       modelName,
		FreshnessStatus:    freshness,
		CSRFFieldProvider:  csrfFieldProvider(r),
	}))
}

func (h *Handler) ModelsNew(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, modelsNewPage(principalFromContext(r.Context()), csrfFieldProvider(r)))
}

func (h *Handler) ModelsCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	m, err := h.Model.CreateModel(r.Context(), principal, domain.CreateModelRequest{
		ProjectName:     formString(r.Form, "project_name"),
		Name:            formString(r.Form, "name"),
		Materialization: formString(r.Form, "materialization"),
		Description:     formString(r.Form, "description"),
		Tags:            formCSV(r.Form, "tags"),
		SQL:             formString(r.Form, "sql"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+m.ProjectName+"/"+m.Name, http.StatusSeeOther)
}

func (h *Handler) ModelsEdit(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	m, err := h.Model.GetModel(r.Context(), projectName, modelName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, modelsEditPage(principalFromContext(r.Context()), projectName, modelName, m, csrfFieldProvider(r)))
}

func (h *Handler) ModelsUpdate(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	sql := formString(r.Form, "sql")
	materialization := formString(r.Form, "materialization")
	description := formString(r.Form, "description")
	_, err := h.Model.UpdateModel(r.Context(), principal, projectName, modelName, domain.UpdateModelRequest{
		SQL:             &sql,
		Materialization: &materialization,
		Description:     &description,
		Tags:            formCSV(r.Form, "tags"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+projectName+"/"+modelName, http.StatusSeeOther)
}

func (h *Handler) ModelsDelete(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	principal, _ := principalLabel(r.Context())
	if err := h.Model.DeleteModel(r.Context(), principal, projectName, modelName); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models", http.StatusSeeOther)
}

func (h *Handler) ModelTestsNew(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	renderHTML(w, http.StatusOK, modelTestsNewPage(principalFromContext(r.Context()), projectName, modelName, csrfFieldProvider(r)))
}

func (h *Handler) ModelTestsCreate(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.Model.CreateTest(r.Context(), principal, projectName, modelName, domain.CreateModelTestRequest{
		Name:     formString(r.Form, "name"),
		TestType: formString(r.Form, "test_type"),
		Column:   formString(r.Form, "column"),
		Config: domain.ModelTestConfig{
			Values:   formCSV(r.Form, "values"),
			ToModel:  formString(r.Form, "to_model"),
			ToColumn: formString(r.Form, "to_column"),
			SQL:      formString(r.Form, "test_sql"),
		},
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+projectName+"/"+modelName, http.StatusSeeOther)
}

func (h *Handler) ModelTestsDelete(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	testID := chi.URLParam(r, "testID")
	principal, _ := principalLabel(r.Context())
	if err := h.Model.DeleteTest(r.Context(), principal, projectName, modelName, testID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+projectName+"/"+modelName, http.StatusSeeOther)
}

func (h *Handler) ModelRunsTrigger(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.TriggerModelRunRequest{
		TargetCatalog: formString(r.Form, "target_catalog"),
		TargetSchema:  formString(r.Form, "target_schema"),
		Selector:      formString(r.Form, "selector"),
		TriggerType:   domain.ModelTriggerTypeManual,
		FullRefresh:   formBool(r.Form, "full_refresh"),
	}
	if _, err := h.Model.TriggerRun(r.Context(), principal, req); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	projectName := formString(r.Form, "project_name")
	modelName := formString(r.Form, "model_name")
	if projectName != "" && modelName != "" {
		http.Redirect(w, r, "/ui/models/"+projectName+"/"+modelName, http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/ui/models", http.StatusSeeOther)
}

func (h *Handler) ModelRunsCancel(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	principal, _ := principalLabel(r.Context())
	if err := h.Model.CancelRun(r.Context(), principal, runID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models", http.StatusSeeOther)
}

func (h *Handler) ModelsDAG(w http.ResponseWriter, r *http.Request) {
	var projectName *string
	if p := r.URL.Query().Get("project"); p != "" {
		projectName = &p
	}

	tiers, err := h.Model.GetDAG(r.Context(), projectName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	renderHTML(w, http.StatusOK, modelsDAGPage(modelsDAGPageData{
		Principal:   principalFromContext(r.Context()),
		ProjectName: projectName,
		Tiers:       mapDAGTiers(tiers),
	}))
}

func (h *Handler) ModelRunsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	filter := domain.ModelRunFilter{Page: pageReq}
	if status := r.URL.Query().Get("status"); status != "" {
		filter.Status = &status
	}

	runs, total, err := h.Model.ListRuns(r.Context(), filter)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]modelRunRowData, 0, len(runs))
	for i := range runs {
		run := runs[i]
		rows = append(rows, modelRunRowData{
			ID:            run.ID,
			URL:           "/ui/models/runs/" + run.ID,
			Status:        run.Status,
			TriggerType:   run.TriggerType,
			TriggeredBy:   run.TriggeredBy,
			Target:        run.TargetCatalog + "." + run.TargetSchema,
			Selector:      valueOrDash(run.ModelSelector),
			CreatedAtText: formatTime(run.CreatedAt),
		})
	}

	renderHTML(w, http.StatusOK, modelRunsListPage(modelRunsListPageData{
		Principal:      principalFromContext(r.Context()),
		Rows:           rows,
		Page:           pageReq,
		Total:          total,
		SelectedStatus: r.URL.Query().Get("status"),
	}))
}

func (h *Handler) ModelRunsDetail(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")

	run, err := h.Model.GetRun(r.Context(), runID)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	steps, err := h.Model.ListRunSteps(r.Context(), runID)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]modelRunStepRowData, 0, len(steps))
	for i := range steps {
		step := steps[i]
		testResults, err := h.Model.ListTestResults(r.Context(), runID, step.ID)
		if err != nil {
			h.renderServiceError(w, r, err)
			return
		}
		rows = append(rows, modelRunStepRowData{
			ModelName:      step.ModelName,
			Status:         step.Status,
			Tier:           strconv.Itoa(step.Tier),
			RowsAffected:   int64PtrString(step.RowsAffected),
			StartedAtText:  formatTimePtr(step.StartedAt),
			FinishedAtText: formatTimePtr(step.FinishedAt),
			ErrorText:      strOrDash(step.ErrorMessage),
			TestResults:    mapModelTestResults(testResults),
		})
	}

	renderHTML(w, http.StatusOK, modelRunDetailPage(modelRunDetailPageData{
		Principal:         principalFromContext(r.Context()),
		RunID:             run.ID,
		Status:            run.Status,
		TriggerType:       run.TriggerType,
		TriggeredBy:       run.TriggeredBy,
		TargetCatalog:     run.TargetCatalog,
		TargetSchema:      run.TargetSchema,
		Selector:          valueOrDash(run.ModelSelector),
		Variables:         mapJSON(run.Variables),
		CompileManifest:   strOrDash(run.CompileManifest),
		ErrorText:         strOrDash(run.ErrorMessage),
		CreatedAtText:     formatTime(run.CreatedAt),
		StartedAtText:     formatTimePtr(run.StartedAt),
		FinishedAtText:    formatTimePtr(run.FinishedAt),
		CancelURL:         "/ui/models/runs/" + run.ID + "/cancel",
		Steps:             rows,
		CSRFFieldProvider: csrfFieldProvider(r),
	}))
}

func (h *Handler) ModelFreshnessCheck(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	if _, err := h.Model.CheckFreshness(r.Context(), projectName, modelName); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+projectName+"/"+modelName, http.StatusSeeOther)
}

func (h *Handler) ModelSourceFreshnessPage(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, modelSourceFreshnessPage(modelSourceFreshnessPageData{
		Principal:         principalFromContext(r.Context()),
		CSRFFieldProvider: csrfFieldProvider(r),
	}))
}

func (h *Handler) ModelSourceFreshnessCheck(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	maxLagSeconds, err := strconv.ParseInt(formString(r.Form, "max_lag_seconds"), 10, 64)
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "max_lag_seconds must be an integer."))
		return
	}
	status, err := h.Model.CheckSourceFreshness(
		r.Context(),
		principal,
		formString(r.Form, "source_schema"),
		formString(r.Form, "source_table"),
		formString(r.Form, "timestamp_column"),
		maxLagSeconds,
	)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, modelSourceFreshnessPage(modelSourceFreshnessPageData{
		Principal:         principalFromContext(r.Context()),
		Result:            status,
		SourceSchema:      formString(r.Form, "source_schema"),
		SourceTable:       formString(r.Form, "source_table"),
		TimestampColumn:   formString(r.Form, "timestamp_column"),
		MaxLagSecondsText: formString(r.Form, "max_lag_seconds"),
		CSRFFieldProvider: csrfFieldProvider(r),
	}))
}

func (h *Handler) ModelPromoteNotebook(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	modelItem, err := h.Model.PromoteNotebook(r.Context(), principal, domain.PromoteNotebookRequest{
		NotebookID:      formString(r.Form, "notebook_id"),
		OutputCellID:    formString(r.Form, "output_cell_id"),
		ProjectName:     formString(r.Form, "project_name"),
		Name:            formString(r.Form, "name"),
		Materialization: formString(r.Form, "materialization"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+modelItem.ProjectName+"/"+modelItem.Name, http.StatusSeeOther)
}

func (h *Handler) ModelRunsManualCancel(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	runID := formString(r.Form, "run_id")
	if runID == "" {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "run_id is required."))
		return
	}
	if err := h.Model.CancelRun(r.Context(), principal, runID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models", http.StatusSeeOther)
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

func valueOrDash(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

func int64PtrString(v *int64) string {
	if v == nil {
		return "-"
	}
	return strconv.FormatInt(*v, 10)
}
