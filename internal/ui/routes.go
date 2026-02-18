package ui

import (
	"io/fs"
	"net/http"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/ui/assets"
)

func MountRoutes(r chi.Router, h *Handler, authMiddleware func(http.Handler) http.Handler) {
	r.Get("/login", h.LoginPage)
	r.Post("/login", h.LoginSubmit)
	r.Post("/logout", h.Logout)

	staticFS, err := fs.Sub(assets.StaticFS(), "static")
	if err == nil {
		r.Handle("/static/*", http.StripPrefix("/ui/static/", http.FileServer(http.FS(staticFS))))
	}

	r.Group(func(r chi.Router) {
		r.Use(h.CookieHeaderBridge)
		r.Use(authMiddleware)
		r.Get("/", h.Home)
		r.Get("/catalogs", h.CatalogsList)
		r.Get("/catalogs/{catalogName}", h.CatalogsDetail)
		r.Get("/pipelines", h.PipelinesList)
		r.Get("/pipelines/{pipelineName}", h.PipelinesDetail)
		r.Get("/notebooks", h.NotebooksList)
		r.Get("/notebooks/{notebookID}", h.NotebooksDetail)
		r.Get("/macros", h.MacrosList)
		r.Get("/macros/{macroName}", h.MacrosDetail)
		r.Get("/models", h.ModelsList)
		r.Get("/models/{projectName}/{modelName}", h.ModelsDetail)
	})
}
