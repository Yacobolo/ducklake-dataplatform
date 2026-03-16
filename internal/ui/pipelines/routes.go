package pipelines

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	PipelinesList(http.ResponseWriter, *http.Request)
	PipelinesDetail(http.ResponseWriter, *http.Request)
	PipelinesNew(http.ResponseWriter, *http.Request)
	PipelinesCreate(http.ResponseWriter, *http.Request)
	PipelinesEdit(http.ResponseWriter, *http.Request)
	PipelinesUpdate(http.ResponseWriter, *http.Request)
	PipelinesDelete(http.ResponseWriter, *http.Request)
	PipelineJobsNew(http.ResponseWriter, *http.Request)
	PipelineJobsCreate(http.ResponseWriter, *http.Request)
	PipelineJobsDelete(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/pipelines", h.PipelinesList)
	r.Get("/pipelines/{pipelineName}", h.PipelinesDetail)
	r.Get("/pipelines/new", h.PipelinesNew)
	r.Post("/pipelines", h.PipelinesCreate)
	r.Get("/pipelines/{pipelineName}/edit", h.PipelinesEdit)
	r.Post("/pipelines/{pipelineName}/update", h.PipelinesUpdate)
	r.Post("/pipelines/{pipelineName}/delete", h.PipelinesDelete)
	r.Get("/pipelines/{pipelineName}/jobs/new", h.PipelineJobsNew)
	r.Post("/pipelines/{pipelineName}/jobs", h.PipelineJobsCreate)
	r.Post("/pipelines/{pipelineName}/jobs/{jobID}/delete", h.PipelineJobsDelete)
}
