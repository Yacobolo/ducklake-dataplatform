package semantic

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
	semsvc "duck-demo/internal/service/semantic"
	"duck-demo/internal/ui/core"
)

type Handler struct{ deps *core.Dependencies }

func New(deps *core.Dependencies) *Handler { return &Handler{deps: deps} }

func (h *Handler) SemanticHome(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/ui/semantic/models", http.StatusSeeOther)
}

func (h *Handler) SemanticModelsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	var projectName *string
	if p := r.URL.Query().Get("project"); p != "" {
		projectName = &p
	}
	items, total, err := h.deps.Semantic.ListSemanticModels(r.Context(), projectName, pageReq)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	rows := make([]semanticModelRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, semanticModelRowData{
			Name:      item.ProjectName + "." + item.Name,
			URL:       "/ui/semantic/models/" + item.ProjectName + "/" + item.Name,
			BaseModel: item.BaseModelRef,
			Owner:     item.Owner,
			UpdatedAt: formatTime(item.UpdatedAt),
		})
	}

	core.RenderHTML(w, http.StatusOK, semanticModelsListPage(core.PrincipalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) SemanticModelsNew(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, semanticModelsNewPage(core.PrincipalFromContext(r.Context()), h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) SemanticModelsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	item, err := h.deps.Semantic.CreateSemanticModel(r.Context(), principalName(r), domain.CreateSemanticModelRequest{
		ProjectName:          formString(r.Form, "project_name"),
		Name:                 formString(r.Form, "name"),
		Description:          formString(r.Form, "description"),
		BaseModelRef:         formString(r.Form, "base_model_ref"),
		DefaultTimeDimension: formString(r.Form, "default_time_dimension"),
		Tags:                 formCSV(r.Form, "tags"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+item.ProjectName+"/"+item.Name, http.StatusSeeOther)
}

func (h *Handler) SemanticModelsDetail(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")

	item, err := h.deps.Semantic.GetSemanticModel(r.Context(), projectName, semanticModelName)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	metrics, err := h.deps.Semantic.ListMetrics(r.Context(), projectName, semanticModelName)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	preAggs, err := h.deps.Semantic.ListPreAggregations(r.Context(), projectName, semanticModelName)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	metricRows := make([]semanticMetricRowData, 0, len(metrics))
	for i := range metrics {
		metric := metrics[i]
		metricRows = append(metricRows, semanticMetricRowData{
			Name:       metric.Name,
			Type:       metric.MetricType,
			Expression: metric.Expression,
			Status:     metric.CertificationState,
			EditURL:    "/ui/semantic/models/" + projectName + "/" + semanticModelName + "/metrics/" + metric.Name + "/edit",
			DeleteURL:  "/ui/semantic/models/" + projectName + "/" + semanticModelName + "/metrics/" + metric.Name + "/delete",
		})
	}

	preAggRows := make([]semanticPreAggRowData, 0, len(preAggs))
	for i := range preAggs {
		preAgg := preAggs[i]
		preAggRows = append(preAggRows, semanticPreAggRowData{
			Name:      preAgg.Name,
			Grain:     preAgg.Grain,
			Target:    preAgg.TargetRelation,
			EditURL:   "/ui/semantic/models/" + projectName + "/" + semanticModelName + "/pre-aggregations/" + preAgg.Name + "/edit",
			DeleteURL: "/ui/semantic/models/" + projectName + "/" + semanticModelName + "/pre-aggregations/" + preAgg.Name + "/delete",
		})
	}

	core.RenderHTML(w, http.StatusOK, semanticModelDetailPage(semanticModelDetailPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		ProjectName:       projectName,
		ModelName:         semanticModelName,
		BaseModelRef:      item.BaseModelRef,
		DefaultTimeDim:    valueOrDash(item.DefaultTimeDimension),
		Description:       item.Description,
		EditURL:           "/ui/semantic/models/" + projectName + "/" + semanticModelName + "/edit",
		DeleteURL:         "/ui/semantic/models/" + projectName + "/" + semanticModelName + "/delete",
		MetricsCreateURL:  "/ui/semantic/models/" + projectName + "/" + semanticModelName + "/metrics",
		PreAggCreateURL:   "/ui/semantic/models/" + projectName + "/" + semanticModelName + "/pre-aggregations",
		QueryExplainURL:   "/ui/semantic/query/explain",
		QueryRunURL:       "/ui/semantic/query/run",
		Metrics:           metricRows,
		PreAggregations:   preAggRows,
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}

func (h *Handler) SemanticModelsEdit(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	item, err := h.deps.Semantic.GetSemanticModel(r.Context(), projectName, semanticModelName)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, semanticModelsEditPage(core.PrincipalFromContext(r.Context()), projectName, semanticModelName, item, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) SemanticModelsUpdate(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	description := formString(r.Form, "description")
	baseModelRef := formString(r.Form, "base_model_ref")
	defaultTimeDimension := formString(r.Form, "default_time_dimension")
	_, err := h.deps.Semantic.UpdateSemanticModel(r.Context(), projectName, semanticModelName, domain.UpdateSemanticModelRequest{
		Description:          &description,
		BaseModelRef:         &baseModelRef,
		DefaultTimeDimension: &defaultTimeDimension,
		Tags:                 formCSV(r.Form, "tags"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+projectName+"/"+semanticModelName, http.StatusSeeOther)
}

func (h *Handler) SemanticModelsDelete(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	if err := h.deps.Semantic.DeleteSemanticModel(r.Context(), projectName, semanticModelName); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models", http.StatusSeeOther)
}

func (h *Handler) SemanticMetricsEdit(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	metricName := chi.URLParam(r, "metricName")
	metrics, err := h.deps.Semantic.ListMetrics(r.Context(), projectName, semanticModelName)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	for i := range metrics {
		if metrics[i].Name == metricName {
			core.RenderHTML(w, http.StatusOK, semanticMetricEditPage(core.PrincipalFromContext(r.Context()), projectName, semanticModelName, &metrics[i], h.deps.CSRFFieldProvider(r)))
			return
		}
	}
	core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Semantic metric not found."))
}

func (h *Handler) SemanticMetricsUpdate(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	metricName := chi.URLParam(r, "metricName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	description := formString(r.Form, "description")
	label := formString(r.Form, "label")
	metricType := formString(r.Form, "metric_type")
	expressionMode := formString(r.Form, "expression_mode")
	expression := formString(r.Form, "expression")
	filterSQL := formString(r.Form, "filter_sql")
	defaultTimeGrain := formString(r.Form, "default_time_grain")
	format := formString(r.Form, "format")
	certificationState := formString(r.Form, "certification_state")
	_, err := h.deps.Semantic.UpdateMetric(r.Context(), projectName, semanticModelName, metricName, domain.UpdateSemanticMetricRequest{
		Label:              &label,
		Description:        &description,
		MetricType:         &metricType,
		ExpressionMode:     &expressionMode,
		Expression:         &expression,
		FilterSQL:          &filterSQL,
		DefaultTimeGrain:   &defaultTimeGrain,
		Format:             &format,
		CertificationState: &certificationState,
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+projectName+"/"+semanticModelName, http.StatusSeeOther)
}

func (h *Handler) SemanticMetricsCreate(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.Semantic.CreateMetric(r.Context(), principalName(r), projectName, semanticModelName, domain.CreateSemanticMetricRequest{
		Name:               formString(r.Form, "name"),
		Label:              formString(r.Form, "label"),
		Description:        formString(r.Form, "description"),
		MetricType:         formString(r.Form, "metric_type"),
		ExpressionMode:     formString(r.Form, "expression_mode"),
		Expression:         formString(r.Form, "expression"),
		FilterSQL:          formString(r.Form, "filter_sql"),
		DefaultTimeGrain:   formString(r.Form, "default_time_grain"),
		Format:             formString(r.Form, "format"),
		CertificationState: formString(r.Form, "certification_state"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+projectName+"/"+semanticModelName, http.StatusSeeOther)
}

func (h *Handler) SemanticMetricsDelete(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	metricName := chi.URLParam(r, "metricName")
	if err := h.deps.Semantic.DeleteMetric(r.Context(), projectName, semanticModelName, metricName); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+projectName+"/"+semanticModelName, http.StatusSeeOther)
}

func (h *Handler) SemanticPreAggregationsEdit(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	preAggName := chi.URLParam(r, "preAggName")
	items, err := h.deps.Semantic.ListPreAggregations(r.Context(), projectName, semanticModelName)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	for i := range items {
		if items[i].Name == preAggName {
			core.RenderHTML(w, http.StatusOK, semanticPreAggregationEditPage(core.PrincipalFromContext(r.Context()), projectName, semanticModelName, &items[i], h.deps.CSRFFieldProvider(r)))
			return
		}
	}
	core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Semantic pre-aggregation not found."))
}

func (h *Handler) SemanticPreAggregationsUpdate(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	preAggName := chi.URLParam(r, "preAggName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	grain := formString(r.Form, "grain")
	targetRelation := formString(r.Form, "target_relation")
	refreshPolicy := formString(r.Form, "refresh_policy")
	_, err := h.deps.Semantic.UpdatePreAggregation(r.Context(), projectName, semanticModelName, preAggName, domain.UpdateSemanticPreAggregationRequest{
		MetricSet:      formCSV(r.Form, "metric_set"),
		DimensionSet:   formCSV(r.Form, "dimension_set"),
		Grain:          &grain,
		TargetRelation: &targetRelation,
		RefreshPolicy:  &refreshPolicy,
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+projectName+"/"+semanticModelName, http.StatusSeeOther)
}

func (h *Handler) SemanticPreAggregationsCreate(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.Semantic.CreatePreAggregation(r.Context(), principalName(r), projectName, semanticModelName, domain.CreateSemanticPreAggregationRequest{
		Name:           formString(r.Form, "name"),
		MetricSet:      formCSV(r.Form, "metric_set"),
		DimensionSet:   formCSV(r.Form, "dimension_set"),
		Grain:          formString(r.Form, "grain"),
		TargetRelation: formString(r.Form, "target_relation"),
		RefreshPolicy:  formString(r.Form, "refresh_policy"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+projectName+"/"+semanticModelName, http.StatusSeeOther)
}

func (h *Handler) SemanticPreAggregationsDelete(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	preAggName := chi.URLParam(r, "preAggName")
	if err := h.deps.Semantic.DeletePreAggregation(r.Context(), projectName, semanticModelName, preAggName); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+projectName+"/"+semanticModelName, http.StatusSeeOther)
}

func (h *Handler) SemanticRelationshipsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.deps.Semantic.ListRelationships(r.Context(), pageReq)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	models, _, err := h.deps.Semantic.ListSemanticModels(r.Context(), nil, domain.PageRequest{MaxResults: 1000})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	modelByID := map[string]string{}
	for i := range models {
		modelByID[models[i].ID] = models[i].ProjectName + "." + models[i].Name
	}
	rows := make([]semanticRelationshipRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, semanticRelationshipRowData{
			Name:      item.Name,
			FromModel: valueOrDefault(modelByID[item.FromSemanticID], item.FromSemanticID),
			ToModel:   valueOrDefault(modelByID[item.ToSemanticID], item.ToSemanticID),
			Type:      item.RelationshipType,
			JoinSQL:   item.JoinSQL,
			EditURL:   "/ui/semantic/relationships/" + item.Name + "/edit",
			DeleteURL: "/ui/semantic/relationships/" + item.Name + "/delete",
		})
	}
	modelOptions := make([]semanticOptionData, 0, len(models))
	for i := range models {
		modelOptions = append(modelOptions, semanticOptionData{Value: models[i].ID, Label: models[i].ProjectName + "." + models[i].Name})
	}
	core.RenderHTML(w, http.StatusOK, semanticRelationshipsPage(semanticRelationshipsPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		Rows:              rows,
		ModelOptions:      modelOptions,
		Page:              pageReq,
		Total:             total,
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}
func (h *Handler) SemanticRelationshipsEdit(w http.ResponseWriter, r *http.Request) {
	relationshipName := chi.URLParam(r, "relationshipName")
	items, _, err := h.deps.Semantic.ListRelationships(r.Context(), domain.PageRequest{MaxResults: 200})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	models, _, err := h.deps.Semantic.ListSemanticModels(r.Context(), nil, domain.PageRequest{MaxResults: 1000})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	modelOptions := make([]semanticOptionData, 0, len(models))
	for i := range models {
		modelOptions = append(modelOptions, semanticOptionData{Value: models[i].ID, Label: models[i].ProjectName + "." + models[i].Name})
	}
	for i := range items {
		if items[i].Name == relationshipName {
			core.RenderHTML(w, http.StatusOK, semanticRelationshipEditPage(core.PrincipalFromContext(r.Context()), &items[i], modelOptions, h.deps.CSRFFieldProvider(r)))
			return
		}
	}
	core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Semantic relationship not found."))
}
func (h *Handler) SemanticRelationshipsUpdate(w http.ResponseWriter, r *http.Request) {
	relationshipName := chi.URLParam(r, "relationshipName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	relationshipType := formString(r.Form, "relationship_type")
	joinSQL := formString(r.Form, "join_sql")
	cost, err := strconv.Atoi(defaultString(formString(r.Form, "cost"), "0"))
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "cost must be an integer."))
		return
	}
	maxHops, err := strconv.Atoi(defaultString(formString(r.Form, "max_hops"), "0"))
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "max_hops must be an integer."))
		return
	}
	_, err = h.deps.Semantic.UpdateRelationship(r.Context(), relationshipName, domain.UpdateSemanticRelationshipRequest{
		RelationshipType: &relationshipType,
		JoinSQL:          &joinSQL,
		IsDefault:        boolPtr(formString(r.Form, "is_default") != ""),
		Cost:             intPtr(cost),
		MaxHops:          intPtr(maxHops),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/relationships", http.StatusSeeOther)
}
func (h *Handler) SemanticRelationshipsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	cost, err := strconv.Atoi(defaultString(formString(r.Form, "cost"), "0"))
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "cost must be an integer."))
		return
	}
	maxHops, err := strconv.Atoi(defaultString(formString(r.Form, "max_hops"), "0"))
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "max_hops must be an integer."))
		return
	}
	_, err = h.deps.Semantic.CreateRelationship(r.Context(), principalName(r), domain.CreateSemanticRelationshipRequest{
		Name:             formString(r.Form, "name"),
		FromSemanticID:   formString(r.Form, "from_semantic_id"),
		ToSemanticID:     formString(r.Form, "to_semantic_id"),
		RelationshipType: formString(r.Form, "relationship_type"),
		JoinSQL:          formString(r.Form, "join_sql"),
		IsDefault:        formString(r.Form, "is_default") != "",
		Cost:             cost,
		MaxHops:          maxHops,
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/relationships", http.StatusSeeOther)
}
func (h *Handler) SemanticRelationshipsDelete(w http.ResponseWriter, r *http.Request) {
	relationshipName := chi.URLParam(r, "relationshipName")
	if err := h.deps.Semantic.DeleteRelationship(r.Context(), relationshipName); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/relationships", http.StatusSeeOther)
}
func (h *Handler) SemanticQueryExplain(w http.ResponseWriter, r *http.Request) {
	h.semanticQueryRender(w, r, false)
}
func (h *Handler) SemanticQueryRun(w http.ResponseWriter, r *http.Request) {
	h.semanticQueryRender(w, r, true)
}

func (h *Handler) semanticQueryRender(w http.ResponseWriter, r *http.Request, execute bool) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := semsvc.MetricQueryRequest{
		ProjectName:       formString(r.Form, "project_name"),
		SemanticModelName: formString(r.Form, "semantic_model_name"),
		Metrics:           formCSV(r.Form, "metrics"),
		Dimensions:        formCSV(r.Form, "dimensions"),
		Filters:           formCSV(r.Form, "filters"),
		OrderBy:           formCSV(r.Form, "order_by"),
	}
	if rawTimeGrain := formString(r.Form, "time_grain"); rawTimeGrain != "" {
		req.TimeGrain = &rawTimeGrain
	}
	if rawLimit := formString(r.Form, "limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil {
			core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "limit must be an integer."))
			return
		}
		req.Limit = &parsed
	}

	var plan *semsvc.MetricQueryPlan
	var result *semsvc.MetricQueryResult
	var err error
	if execute {
		result, err = h.deps.Semantic.RunMetricQuery(r.Context(), principalName(r), req)
		if err == nil {
			plan = &result.Plan
		}
	} else {
		plan, err = h.deps.Semantic.ExplainMetricQuery(r.Context(), req)
	}
	if err != nil {
		renderServiceError(w, err)
		return
	}

	core.RenderHTML(w, http.StatusOK, semanticQueryResultPage(semanticQueryResultPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		Request:           req,
		Plan:              plan,
		Result:            result,
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}
