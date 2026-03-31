package ui

import (
	"io/fs"
	"net/http"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/ui/assets"
	"duck-demo/internal/ui/auth"
	"duck-demo/internal/ui/catalogs"
	"duck-demo/internal/ui/components"
	"duck-demo/internal/ui/compute"
	"duck-demo/internal/ui/dashboards"
	"duck-demo/internal/ui/explore"
	"duck-demo/internal/ui/governance"
	"duck-demo/internal/ui/macros"
	"duck-demo/internal/ui/models"
	"duck-demo/internal/ui/notebooks"
	"duck-demo/internal/ui/overview"
	"duck-demo/internal/ui/pipelines"
	"duck-demo/internal/ui/products"
	"duck-demo/internal/ui/runtimeassets"
	"duck-demo/internal/ui/security"
	"duck-demo/internal/ui/semantic"
	"duck-demo/internal/ui/storage"
)

func MountRoutes(r chi.Router, h *Handler) {
	auth.MountRoutes(r, h.Auth)

	staticFS, err := fs.Sub(assets.StaticFS(), "static")
	if err == nil {
		r.Handle("/static/*", http.StripPrefix("/ui/static/", http.FileServer(http.FS(staticFS))))
	}

	r.Group(func(r chi.Router) {
		r.Use(h.RequireWebSession)
		r.Use(h.EnsureCSRFToken)
		r.Use(h.RequireCSRF)

		overview.MountRoutes(r, h.Overview)
		components.MountRoutes(r, h.Components)
		catalogs.MountRoutes(r, h.Catalogs)
		security.MountRoutes(r, h.Security)
		storage.MountRoutes(r, h.Storage)
		compute.MountRoutes(r, h.Compute)
		governance.MountRoutes(r, h.Governance)
		products.MountRoutes(r, h.Products)
		runtimeassets.MountRoutes(r, h.RuntimeAssets)
		explore.MountRoutes(r, h.Explore)
		notebooks.MountRoutes(r, h.Notebooks)
		dashboards.MountRoutes(r, h.Dashboards)
		macros.MountRoutes(r, h.Macros)
		models.MountRoutes(r, h.Models)
		semantic.MountRoutes(r, h.Semantic)
		pipelines.MountRoutes(r, h.Pipelines)
	})
}
