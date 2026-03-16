package macros

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	MacrosList(http.ResponseWriter, *http.Request)
	MacrosDiff(http.ResponseWriter, *http.Request)
	MacrosImpact(http.ResponseWriter, *http.Request)
	MacrosDetail(http.ResponseWriter, *http.Request)
	MacrosNew(http.ResponseWriter, *http.Request)
	MacrosCreate(http.ResponseWriter, *http.Request)
	MacrosEdit(http.ResponseWriter, *http.Request)
	MacrosUpdate(http.ResponseWriter, *http.Request)
	MacrosDelete(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/macros", h.MacrosList)
	r.Get("/macros/{macroName}/diff", h.MacrosDiff)
	r.Get("/macros/{macroName}/impact", h.MacrosImpact)
	r.Get("/macros/{macroName}", h.MacrosDetail)
	r.Get("/macros/new", h.MacrosNew)
	r.Post("/macros", h.MacrosCreate)
	r.Get("/macros/{macroName}/edit", h.MacrosEdit)
	r.Post("/macros/{macroName}/update", h.MacrosUpdate)
	r.Post("/macros/{macroName}/delete", h.MacrosDelete)
}
