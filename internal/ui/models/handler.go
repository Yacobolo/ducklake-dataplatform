package models

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/ui/core"
)

type Handler struct{ deps *core.Dependencies }

func New(deps *core.Dependencies) *Handler { return &Handler{deps: deps} }

func (h *Handler) ModelsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	var projectName *string
	if p := r.URL.Query().Get("project"); p != "" {
		projectName = &p
	}

	items, total, err := h.deps.Model.ListModels(r.Context(), projectName, pageReq)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	rows := make([]modelsListRowData, 0, len(items))
	for i := range items {
		m := items[i]
		rows = append(rows, modelsListRowData{
			DetailURL:     fmt.Sprintf("/ui/models/%s/%s", m.ProjectName, m.Name),
			ModelName:     m.ProjectName + "." + m.Name,
			Materialized:  m.Materialization,
			Dependencies:  len(m.DependsOn),
			UpdatedAtText: formatTime(m.UpdatedAt),
		})
	}

	_ = core.TrackResourceVisit(r, h.deps, domain.ResourceRef{
		ResourceType: "workspace",
		ResourceKey:  "models",
		DisplayName:  "Models",
		Section:      "Build",
	})
	core.RenderHTML(w, http.StatusOK, modelsListPage(core.PrincipalFromContext(r.Context()), rows, pageReq, total, projectName))
}

func (h *Handler) ModelsDAG(w http.ResponseWriter, r *http.Request) {
	var projectName *string
	if p := r.URL.Query().Get("project"); p != "" {
		projectName = &p
	}
	tiers, err := h.deps.Model.GetDAG(r.Context(), projectName)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, modelsDAGPage(modelsDAGPageData{
		Principal:   core.PrincipalFromContext(r.Context()),
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
	runs, total, err := h.deps.Model.ListRuns(r.Context(), filter)
	if err != nil {
		renderServiceError(w, err)
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
	core.RenderHTML(w, http.StatusOK, modelRunsListPage(modelRunsListPageData{
		Principal:      core.PrincipalFromContext(r.Context()),
		Rows:           rows,
		Page:           pageReq,
		Total:          total,
		SelectedStatus: r.URL.Query().Get("status"),
	}))
}
func (h *Handler) ModelRunsDetail(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	run, err := h.deps.Model.GetRun(r.Context(), runID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	steps, err := h.deps.Model.ListRunSteps(r.Context(), runID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	rows := make([]modelRunStepRowData, 0, len(steps))
	for i := range steps {
		step := steps[i]
		testResults, err := h.deps.Model.ListTestResults(r.Context(), runID, step.ID)
		if err != nil {
			renderServiceError(w, err)
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
	core.RenderHTML(w, http.StatusOK, modelRunDetailPage(modelRunDetailPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
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
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}
func (h *Handler) ModelSourceFreshnessPage(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, modelSourceFreshnessPage(modelSourceFreshnessPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}
func (h *Handler) ModelSourceFreshnessCheck(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	maxLagSeconds, err := strconv.ParseInt(formString(r.Form, "max_lag_seconds"), 10, 64)
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "max_lag_seconds must be an integer."))
		return
	}
	status, err := h.deps.Model.CheckSourceFreshness(
		r.Context(),
		principalName(r),
		formString(r.Form, "source_schema"),
		formString(r.Form, "source_table"),
		formString(r.Form, "timestamp_column"),
		maxLagSeconds,
	)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, modelSourceFreshnessPage(modelSourceFreshnessPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		Result:            status,
		SourceSchema:      formString(r.Form, "source_schema"),
		SourceTable:       formString(r.Form, "source_table"),
		TimestampColumn:   formString(r.Form, "timestamp_column"),
		MaxLagSecondsText: formString(r.Form, "max_lag_seconds"),
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}
func (h *Handler) ModelPromoteNotebook(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	modelItem, err := h.deps.Model.PromoteNotebook(r.Context(), principalName(r), domain.PromoteNotebookRequest{
		NotebookID:      formString(r.Form, "notebook_id"),
		OutputCellID:    formString(r.Form, "output_cell_id"),
		ProjectName:     formString(r.Form, "project_name"),
		Name:            formString(r.Form, "name"),
		Materialization: formString(r.Form, "materialization"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+modelItem.ProjectName+"/"+modelItem.Name, http.StatusSeeOther)
}

func (h *Handler) ModelsDetail(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")

	m, err := h.deps.Model.GetModel(r.Context(), projectName, modelName)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	tests, err := h.deps.Model.ListTests(r.Context(), projectName, modelName)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	freshness, err := h.deps.Model.CheckFreshness(r.Context(), projectName, modelName)
	if err != nil {
		renderServiceError(w, err)
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

	_ = core.TrackResourceVisit(r, h.deps, domain.ResourceRef{
		ResourceType: "model",
		ResourceKey:  m.ProjectName + "/" + m.Name,
		DisplayName:  m.ProjectName + "." + m.Name,
		Section:      "Build",
	})
	core.RenderHTML(w, http.StatusOK, modelsDetailPage(modelsDetailPageData{
		Principal:          core.PrincipalFromContext(r.Context()),
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
		CSRFFieldProvider:  h.deps.CSRFFieldProvider(r),
	}))
}

func (h *Handler) ModelsNew(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, modelsNewPage(core.PrincipalFromContext(r.Context()), strings.TrimSpace(r.URL.Query().Get("project")), h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) ModelsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	m, err := h.deps.Model.CreateModel(r.Context(), principalName(r), domain.CreateModelRequest{
		ProjectName:     formString(r.Form, "project_name"),
		Name:            formString(r.Form, "name"),
		Materialization: formString(r.Form, "materialization"),
		Description:     formString(r.Form, "description"),
		Tags:            formCSV(r.Form, "tags"),
		SQL:             formString(r.Form, "sql"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+m.ProjectName+"/"+m.Name, http.StatusSeeOther)
}

func (h *Handler) ModelsEdit(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	m, err := h.deps.Model.GetModel(r.Context(), projectName, modelName)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, modelsEditPage(core.PrincipalFromContext(r.Context()), projectName, modelName, m, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) ModelsUpdate(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	sql := formString(r.Form, "sql")
	materialization := formString(r.Form, "materialization")
	description := formString(r.Form, "description")
	_, err := h.deps.Model.UpdateModel(r.Context(), principalName(r), projectName, modelName, domain.UpdateModelRequest{
		SQL:             &sql,
		Materialization: &materialization,
		Description:     &description,
		Tags:            formCSV(r.Form, "tags"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+projectName+"/"+modelName, http.StatusSeeOther)
}

func (h *Handler) ModelsDelete(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	if err := h.deps.Model.DeleteModel(r.Context(), principalName(r), projectName, modelName); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/models", http.StatusSeeOther)
}

func (h *Handler) ModelTestsNew(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	core.RenderHTML(w, http.StatusOK, modelTestsNewPage(core.PrincipalFromContext(r.Context()), projectName, modelName, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) ModelTestsCreate(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.Model.CreateTest(r.Context(), principalName(r), projectName, modelName, domain.CreateModelTestRequest{
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
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+projectName+"/"+modelName, http.StatusSeeOther)
}

func (h *Handler) ModelTestsDelete(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	testID := chi.URLParam(r, "testID")
	if err := h.deps.Model.DeleteTest(r.Context(), principalName(r), projectName, modelName, testID); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+projectName+"/"+modelName, http.StatusSeeOther)
}

func (h *Handler) ModelFreshnessCheck(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	if _, err := h.deps.Model.CheckFreshness(r.Context(), projectName, modelName); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+projectName+"/"+modelName, http.StatusSeeOther)
}
func (h *Handler) ModelRunsTrigger(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.TriggerModelRunRequest{
		TargetCatalog: formString(r.Form, "target_catalog"),
		TargetSchema:  formString(r.Form, "target_schema"),
		Selector:      formString(r.Form, "selector"),
		TriggerType:   domain.ModelTriggerTypeManual,
		FullRefresh:   formString(r.Form, "full_refresh") != "",
	}
	if _, err := h.deps.Model.TriggerRun(r.Context(), principalName(r), req); err != nil {
		renderServiceError(w, err)
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
	if err := h.deps.Model.CancelRun(r.Context(), principalName(r), runID); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/models", http.StatusSeeOther)
}
func (h *Handler) ModelRunsManualCancel(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	runID := formString(r.Form, "run_id")
	if runID == "" {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "run_id is required."))
		return
	}
	if err := h.deps.Model.CancelRun(r.Context(), principalName(r), runID); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/models", http.StatusSeeOther)
}
