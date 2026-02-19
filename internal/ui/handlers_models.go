package ui

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
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
		Principal:         principalFromContext(r.Context()),
		ProjectName:       projectName,
		ModelName:         modelName,
		QualifiedName:     m.ProjectName + "." + m.Name,
		Materialization:   m.Materialization,
		Owner:             m.Owner,
		DependsOn:         stringsJoin(m.DependsOn),
		ConfigText:        mapJSON(modelConfigMap(m.Config)),
		EditURL:           "/ui/models/" + projectName + "/" + modelName + "/edit",
		DeleteURL:         "/ui/models/" + projectName + "/" + modelName + "/delete",
		NewTestURL:        "/ui/models/" + projectName + "/" + modelName + "/tests/new",
		TriggerRunURL:     "/ui/models/runs/trigger",
		CancelRunURL:      "/ui/models/runs/manual-cancel",
		DefaultSelector:   m.ProjectName + "." + m.Name,
		SQL:               m.SQL,
		Tests:             testRows,
		TriggerProject:    projectName,
		TriggerModel:      modelName,
		CSRFFieldProvider: csrfFieldProvider(r),
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
