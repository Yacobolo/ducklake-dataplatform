package overview

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	Home(http.ResponseWriter, *http.Request)
	SaveResource(http.ResponseWriter, *http.Request)
	UnsaveResource(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/", h.Home)
	r.Post("/resources/save", h.SaveResource)
	r.Post("/resources/unsave", h.UnsaveResource)
}
