package models

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	ModelsList(http.ResponseWriter, *http.Request)
	ModelsDAG(http.ResponseWriter, *http.Request)
	ModelRunsList(http.ResponseWriter, *http.Request)
	ModelRunsDetail(http.ResponseWriter, *http.Request)
	ModelSourceFreshnessPage(http.ResponseWriter, *http.Request)
	ModelSourceFreshnessCheck(http.ResponseWriter, *http.Request)
	ModelPromoteNotebook(http.ResponseWriter, *http.Request)
	ModelsDetail(http.ResponseWriter, *http.Request)
	ModelsNew(http.ResponseWriter, *http.Request)
	ModelsCreate(http.ResponseWriter, *http.Request)
	ModelsEdit(http.ResponseWriter, *http.Request)
	ModelsUpdate(http.ResponseWriter, *http.Request)
	ModelsDelete(http.ResponseWriter, *http.Request)
	ModelTestsNew(http.ResponseWriter, *http.Request)
	ModelTestsCreate(http.ResponseWriter, *http.Request)
	ModelTestsDelete(http.ResponseWriter, *http.Request)
	ModelFreshnessCheck(http.ResponseWriter, *http.Request)
	ModelRunsTrigger(http.ResponseWriter, *http.Request)
	ModelRunsCancel(http.ResponseWriter, *http.Request)
	ModelRunsManualCancel(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/models", h.ModelsList)
	r.Get("/models/dag", h.ModelsDAG)
	r.Get("/models/runs", h.ModelRunsList)
	r.Get("/models/runs/{runID}", h.ModelRunsDetail)
	r.Get("/models/source-freshness", h.ModelSourceFreshnessPage)
	r.Post("/models/source-freshness", h.ModelSourceFreshnessCheck)
	r.Post("/models/promote", h.ModelPromoteNotebook)
	r.Get("/models/{projectName}/{modelName}", h.ModelsDetail)
	r.Get("/models/new", h.ModelsNew)
	r.Post("/models", h.ModelsCreate)
	r.Get("/models/{projectName}/{modelName}/edit", h.ModelsEdit)
	r.Post("/models/{projectName}/{modelName}/update", h.ModelsUpdate)
	r.Post("/models/{projectName}/{modelName}/delete", h.ModelsDelete)
	r.Get("/models/{projectName}/{modelName}/tests/new", h.ModelTestsNew)
	r.Post("/models/{projectName}/{modelName}/tests", h.ModelTestsCreate)
	r.Post("/models/{projectName}/{modelName}/tests/{testID}/delete", h.ModelTestsDelete)
	r.Post("/models/{projectName}/{modelName}/freshness", h.ModelFreshnessCheck)
	r.Post("/models/runs/trigger", h.ModelRunsTrigger)
	r.Post("/models/runs/{runID}/cancel", h.ModelRunsCancel)
	r.Post("/models/runs/manual-cancel", h.ModelRunsManualCancel)
}
