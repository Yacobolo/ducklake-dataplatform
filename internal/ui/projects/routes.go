package projects

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	ProjectsList(http.ResponseWriter, *http.Request)
	ProjectsDetail(http.ResponseWriter, *http.Request)
	ProjectsEnvironmentNew(http.ResponseWriter, *http.Request)
	ProjectsEnvironmentCreate(http.ResponseWriter, *http.Request)
	ProjectsEnvironmentDetail(http.ResponseWriter, *http.Request)
	ProjectsEnvironmentEdit(http.ResponseWriter, *http.Request)
	ProjectsEnvironmentUpdate(http.ResponseWriter, *http.Request)
	ProjectsEnvironmentDelete(http.ResponseWriter, *http.Request)
	ProjectsBuildDetail(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/projects", h.ProjectsList)
	r.Get("/projects/{projectID}", h.ProjectsDetail)
	r.Get("/projects/{projectID}/environments/new", h.ProjectsEnvironmentNew)
	r.Post("/projects/{projectID}/environments", h.ProjectsEnvironmentCreate)
	r.Get("/projects/{projectID}/environments/{environmentID}", h.ProjectsEnvironmentDetail)
	r.Get("/projects/{projectID}/environments/{environmentID}/edit", h.ProjectsEnvironmentEdit)
	r.Post("/projects/{projectID}/environments/{environmentID}/update", h.ProjectsEnvironmentUpdate)
	r.Post("/projects/{projectID}/environments/{environmentID}/delete", h.ProjectsEnvironmentDelete)
	r.Get("/projects/{projectID}/builds/{buildID}", h.ProjectsBuildDetail)
}
