package components

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	ComponentsPage(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/components", h.ComponentsPage)
}
