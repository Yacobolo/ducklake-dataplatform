package legacy

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
	semsvc "duck-demo/internal/service/semantic"
)

func (h *Handler) SemanticHome(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, semanticHomePage(principalFromContext(r.Context())))
}

func (h *Handler) SemanticModelsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	var projectName *string
	if p := r.URL.Query().Get("project"); p != "" {
		projectName = &p
	}
	items, total, err := h.Semantic.ListSemanticModels(r.Context(), projectName, pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]semanticModelRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, semanticModelRowData{
			Name:       item.ProjectName + "." + item.Name,
			URL:        "/ui/semantic/models/" + item.ProjectName + "/" + item.Name,
			BaseModel:  item.BaseModelRef,
			Owner:      item.Owner,
			UpdatedAt:  formatTime(item.UpdatedAt),
			FilterText: item.ProjectName + " " + item.Name + " " + item.BaseModelRef,
		})
	}

	renderHTML(w, http.StatusOK, semanticModelsListPage(semanticModelsListPageData{
		Principal: principalFromContext(r.Context()),
		Rows:      rows,
		Page:      pageReq,
		Total:     total,
	}))
}

func (h *Handler) SemanticModelsNew(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, semanticModelsNewPage(principalFromContext(r.Context()), csrfFieldProvider(r)))
}

func (h *Handler) SemanticModelsCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	item, err := h.Semantic.CreateSemanticModel(r.Context(), principal, domain.CreateSemanticModelRequest{
		ProjectName:          formString(r.Form, "project_name"),
		Name:                 formString(r.Form, "name"),
		Description:          formString(r.Form, "description"),
		BaseModelRef:         formString(r.Form, "base_model_ref"),
		DefaultTimeDimension: formString(r.Form, "default_time_dimension"),
		Tags:                 formCSV(r.Form, "tags"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+item.ProjectName+"/"+item.Name, http.StatusSeeOther)
}

func (h *Handler) SemanticModelsDetail(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")

	item, err := h.Semantic.GetSemanticModel(r.Context(), projectName, semanticModelName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	metrics, err := h.Semantic.ListMetrics(r.Context(), projectName, semanticModelName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	preAggs, err := h.Semantic.ListPreAggregations(r.Context(), projectName, semanticModelName)
	if err != nil {
		h.renderServiceError(w, r, err)
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

	renderHTML(w, http.StatusOK, semanticModelDetailPage(semanticModelDetailPageData{
		Principal:         principalFromContext(r.Context()),
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
		CSRFFieldProvider: csrfFieldProvider(r),
	}))
}

func (h *Handler) SemanticModelsDelete(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	if err := h.Semantic.DeleteSemanticModel(r.Context(), projectName, semanticModelName); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models", http.StatusSeeOther)
}

func (h *Handler) SemanticModelsEdit(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	item, err := h.Semantic.GetSemanticModel(r.Context(), projectName, semanticModelName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, semanticModelsEditPage(principalFromContext(r.Context()), projectName, semanticModelName, item, csrfFieldProvider(r)))
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
	_, err := h.Semantic.UpdateSemanticModel(r.Context(), projectName, semanticModelName, domain.UpdateSemanticModelRequest{
		Description:          &description,
		BaseModelRef:         &baseModelRef,
		DefaultTimeDimension: &defaultTimeDimension,
		Tags:                 formCSV(r.Form, "tags"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+projectName+"/"+semanticModelName, http.StatusSeeOther)
}

func (h *Handler) SemanticMetricsCreate(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.Semantic.CreateMetric(r.Context(), principal, projectName, semanticModelName, domain.CreateSemanticMetricRequest{
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
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+projectName+"/"+semanticModelName, http.StatusSeeOther)
}

func (h *Handler) SemanticMetricsDelete(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	metricName := chi.URLParam(r, "metricName")
	if err := h.Semantic.DeleteMetric(r.Context(), projectName, semanticModelName, metricName); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+projectName+"/"+semanticModelName, http.StatusSeeOther)
}

func (h *Handler) SemanticMetricsEdit(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	metricName := chi.URLParam(r, "metricName")
	metrics, err := h.Semantic.ListMetrics(r.Context(), projectName, semanticModelName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	for i := range metrics {
		if metrics[i].Name == metricName {
			renderHTML(w, http.StatusOK, semanticMetricEditPage(principalFromContext(r.Context()), projectName, semanticModelName, &metrics[i], csrfFieldProvider(r)))
			return
		}
	}
	renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Semantic metric not found."))
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
	_, err := h.Semantic.UpdateMetric(r.Context(), projectName, semanticModelName, metricName, domain.UpdateSemanticMetricRequest{
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
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+projectName+"/"+semanticModelName, http.StatusSeeOther)
}

func (h *Handler) SemanticPreAggregationsCreate(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.Semantic.CreatePreAggregation(r.Context(), principal, projectName, semanticModelName, domain.CreateSemanticPreAggregationRequest{
		Name:           formString(r.Form, "name"),
		MetricSet:      formCSV(r.Form, "metric_set"),
		DimensionSet:   formCSV(r.Form, "dimension_set"),
		Grain:          formString(r.Form, "grain"),
		TargetRelation: formString(r.Form, "target_relation"),
		RefreshPolicy:  formString(r.Form, "refresh_policy"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+projectName+"/"+semanticModelName, http.StatusSeeOther)
}

func (h *Handler) SemanticPreAggregationsDelete(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	preAggName := chi.URLParam(r, "preAggName")
	if err := h.Semantic.DeletePreAggregation(r.Context(), projectName, semanticModelName, preAggName); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+projectName+"/"+semanticModelName, http.StatusSeeOther)
}

func (h *Handler) SemanticPreAggregationsEdit(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	semanticModelName := chi.URLParam(r, "semanticModelName")
	preAggName := chi.URLParam(r, "preAggName")
	items, err := h.Semantic.ListPreAggregations(r.Context(), projectName, semanticModelName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	for i := range items {
		if items[i].Name == preAggName {
			renderHTML(w, http.StatusOK, semanticPreAggregationEditPage(principalFromContext(r.Context()), projectName, semanticModelName, &items[i], csrfFieldProvider(r)))
			return
		}
	}
	renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Semantic pre-aggregation not found."))
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
	_, err := h.Semantic.UpdatePreAggregation(r.Context(), projectName, semanticModelName, preAggName, domain.UpdateSemanticPreAggregationRequest{
		MetricSet:      formCSV(r.Form, "metric_set"),
		DimensionSet:   formCSV(r.Form, "dimension_set"),
		Grain:          &grain,
		TargetRelation: &targetRelation,
		RefreshPolicy:  &refreshPolicy,
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/models/"+projectName+"/"+semanticModelName, http.StatusSeeOther)
}

func (h *Handler) SemanticRelationshipsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.Semantic.ListRelationships(r.Context(), pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	models, _, err := h.Semantic.ListSemanticModels(r.Context(), nil, domain.PageRequest{MaxResults: 1000})
	if err != nil {
		h.renderServiceError(w, r, err)
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
		modelOptions = append(modelOptions, semanticOptionData{
			Value: models[i].ID,
			Label: models[i].ProjectName + "." + models[i].Name,
		})
	}

	renderHTML(w, http.StatusOK, semanticRelationshipsPage(semanticRelationshipsPageData{
		Principal:         principalFromContext(r.Context()),
		Rows:              rows,
		ModelOptions:      modelOptions,
		Page:              pageReq,
		Total:             total,
		CSRFFieldProvider: csrfFieldProvider(r),
	}))
}

func (h *Handler) SemanticRelationshipsCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	cost, err := strconv.Atoi(defaultString(formString(r.Form, "cost"), "0"))
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "cost must be an integer."))
		return
	}
	maxHops, err := strconv.Atoi(defaultString(formString(r.Form, "max_hops"), "0"))
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "max_hops must be an integer."))
		return
	}
	_, err = h.Semantic.CreateRelationship(r.Context(), principal, domain.CreateSemanticRelationshipRequest{
		Name:             formString(r.Form, "name"),
		FromSemanticID:   formString(r.Form, "from_semantic_id"),
		ToSemanticID:     formString(r.Form, "to_semantic_id"),
		RelationshipType: formString(r.Form, "relationship_type"),
		JoinSQL:          formString(r.Form, "join_sql"),
		IsDefault:        formBool(r.Form, "is_default"),
		Cost:             cost,
		MaxHops:          maxHops,
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/relationships", http.StatusSeeOther)
}

func (h *Handler) SemanticRelationshipsDelete(w http.ResponseWriter, r *http.Request) {
	relationshipName := chi.URLParam(r, "relationshipName")
	if err := h.Semantic.DeleteRelationship(r.Context(), relationshipName); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/semantic/relationships", http.StatusSeeOther)
}

func (h *Handler) SemanticRelationshipsEdit(w http.ResponseWriter, r *http.Request) {
	relationshipName := chi.URLParam(r, "relationshipName")
	items, _, err := h.Semantic.ListRelationships(r.Context(), domain.PageRequest{MaxResults: 200})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	models, _, err := h.Semantic.ListSemanticModels(r.Context(), nil, domain.PageRequest{MaxResults: 1000})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	modelOptions := make([]semanticOptionData, 0, len(models))
	for i := range models {
		modelOptions = append(modelOptions, semanticOptionData{Value: models[i].ID, Label: models[i].ProjectName + "." + models[i].Name})
	}
	for i := range items {
		if items[i].Name == relationshipName {
			renderHTML(w, http.StatusOK, semanticRelationshipEditPage(principalFromContext(r.Context()), &items[i], modelOptions, csrfFieldProvider(r)))
			return
		}
	}
	renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Semantic relationship not found."))
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
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "cost must be an integer."))
		return
	}
	maxHops, err := strconv.Atoi(defaultString(formString(r.Form, "max_hops"), "0"))
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "max_hops must be an integer."))
		return
	}
	_, err = h.Semantic.UpdateRelationship(r.Context(), relationshipName, domain.UpdateSemanticRelationshipRequest{
		RelationshipType: &relationshipType,
		JoinSQL:          &joinSQL,
		IsDefault:        boolPtr(formBool(r.Form, "is_default")),
		Cost:             intPtr(cost),
		MaxHops:          intPtr(maxHops),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
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
			renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "limit must be an integer."))
			return
		}
		req.Limit = &parsed
	}

	var plan *semsvc.MetricQueryPlan
	var result *semsvc.MetricQueryResult
	var err error
	if execute {
		principal, _ := principalLabel(r.Context())
		result, err = h.Semantic.RunMetricQuery(r.Context(), principal, req)
		if err == nil {
			plan = &result.Plan
		}
	} else {
		plan, err = h.Semantic.ExplainMetricQuery(r.Context(), req)
	}
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	renderHTML(w, http.StatusOK, semanticQueryResultPage(semanticQueryResultPageData{
		Principal:         principalFromContext(r.Context()),
		Request:           req,
		Plan:              plan,
		Result:            result,
		CSRFFieldProvider: csrfFieldProvider(r),
	}))
}

func valueOrDefault(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func boolPtr(v bool) *bool {
	return &v
}

func intPtr(v int) *int {
	return &v
}
