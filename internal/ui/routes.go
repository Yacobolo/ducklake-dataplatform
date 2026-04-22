package ui

import (
	"io/fs"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/Yacobolo/quackstack/internal/ui/assets"
	"github.com/Yacobolo/quackstack/internal/ui/auth"
	"github.com/Yacobolo/quackstack/internal/ui/catalogs"
	"github.com/Yacobolo/quackstack/internal/ui/components"
	"github.com/Yacobolo/quackstack/internal/ui/compute"
	"github.com/Yacobolo/quackstack/internal/ui/dashboards"
	"github.com/Yacobolo/quackstack/internal/ui/explore"
	"github.com/Yacobolo/quackstack/internal/ui/governance"
	"github.com/Yacobolo/quackstack/internal/ui/macros"
	"github.com/Yacobolo/quackstack/internal/ui/models"
	"github.com/Yacobolo/quackstack/internal/ui/notebooks"
	"github.com/Yacobolo/quackstack/internal/ui/overview"
	"github.com/Yacobolo/quackstack/internal/ui/pipelines"
	"github.com/Yacobolo/quackstack/internal/ui/projects"
	"github.com/Yacobolo/quackstack/internal/ui/runtimeassets"
	"github.com/Yacobolo/quackstack/internal/ui/security"
	"github.com/Yacobolo/quackstack/internal/ui/semantic"
	"github.com/Yacobolo/quackstack/internal/ui/storage"
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
		runtimeassets.MountRoutes(r, h.RuntimeAssets)
		projects.MountRoutes(r, h.Projects)
		explore.MountRoutes(r, h.Explore)
		notebooks.MountRoutes(r, h.Notebooks)
		dashboards.MountRoutes(r, h.Dashboards)
		macros.MountRoutes(r, h.Macros)
		models.MountRoutes(r, h.Models)
		semantic.MountRoutes(r, h.Semantic)
		pipelines.MountRoutes(r, h.Pipelines)
	})
}
