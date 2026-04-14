package projects

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	ProjectsList(http.ResponseWriter, *http.Request)
	ProjectsDetail(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/projects", h.ProjectsList)
	r.Get("/projects/{projectID}", h.ProjectsDetail)
}
