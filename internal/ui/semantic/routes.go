package semantic

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	SemanticHome(http.ResponseWriter, *http.Request)
	SemanticModelsList(http.ResponseWriter, *http.Request)
	SemanticModelsNew(http.ResponseWriter, *http.Request)
	SemanticModelsCreate(http.ResponseWriter, *http.Request)
	SemanticModelsDetail(http.ResponseWriter, *http.Request)
	SemanticModelsEdit(http.ResponseWriter, *http.Request)
	SemanticModelsUpdate(http.ResponseWriter, *http.Request)
	SemanticModelsDelete(http.ResponseWriter, *http.Request)
	SemanticMetricsEdit(http.ResponseWriter, *http.Request)
	SemanticMetricsUpdate(http.ResponseWriter, *http.Request)
	SemanticMetricsCreate(http.ResponseWriter, *http.Request)
	SemanticMetricsDelete(http.ResponseWriter, *http.Request)
	SemanticPreAggregationsEdit(http.ResponseWriter, *http.Request)
	SemanticPreAggregationsUpdate(http.ResponseWriter, *http.Request)
	SemanticPreAggregationsCreate(http.ResponseWriter, *http.Request)
	SemanticPreAggregationsDelete(http.ResponseWriter, *http.Request)
	SemanticModelRelationshipsCreate(http.ResponseWriter, *http.Request)
	SemanticModelRelationshipsUpdate(http.ResponseWriter, *http.Request)
	SemanticModelRelationshipsDelete(http.ResponseWriter, *http.Request)
	SemanticQueryExplain(http.ResponseWriter, *http.Request)
	SemanticQueryRun(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/semantic", h.SemanticHome)
	r.Get("/semantic/models", h.SemanticModelsList)
	r.Get("/semantic/models/new", h.SemanticModelsNew)
	r.Post("/semantic/models", h.SemanticModelsCreate)
	r.Get("/semantic/models/{semanticModelID}", h.SemanticModelsDetail)
	r.Get("/semantic/models/{semanticModelID}/edit", h.SemanticModelsEdit)
	r.Post("/semantic/models/{semanticModelID}/update", h.SemanticModelsUpdate)
	r.Post("/semantic/models/{semanticModelID}/delete", h.SemanticModelsDelete)
	r.Get("/semantic/models/{semanticModelID}/metrics/{metricName}/edit", h.SemanticMetricsEdit)
	r.Post("/semantic/models/{semanticModelID}/metrics/{metricName}/update", h.SemanticMetricsUpdate)
	r.Post("/semantic/models/{semanticModelID}/metrics", h.SemanticMetricsCreate)
	r.Post("/semantic/models/{semanticModelID}/metrics/{metricName}/delete", h.SemanticMetricsDelete)
	r.Post("/semantic/models/{semanticModelID}/relationships", h.SemanticModelRelationshipsCreate)
	r.Post("/semantic/models/{semanticModelID}/relationships/{relationshipName}/update", h.SemanticModelRelationshipsUpdate)
	r.Post("/semantic/models/{semanticModelID}/relationships/{relationshipName}/delete", h.SemanticModelRelationshipsDelete)
	r.Get("/semantic/models/{semanticModelID}/pre-aggregations/{preAggName}/edit", h.SemanticPreAggregationsEdit)
	r.Post("/semantic/models/{semanticModelID}/pre-aggregations/{preAggName}/update", h.SemanticPreAggregationsUpdate)
	r.Post("/semantic/models/{semanticModelID}/pre-aggregations", h.SemanticPreAggregationsCreate)
	r.Post("/semantic/models/{semanticModelID}/pre-aggregations/{preAggName}/delete", h.SemanticPreAggregationsDelete)
	r.Post("/semantic/query/explain", h.SemanticQueryExplain)
	r.Post("/semantic/query/run", h.SemanticQueryRun)
}
