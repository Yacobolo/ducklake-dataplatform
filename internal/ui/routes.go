package ui

import (
	"io/fs"
	"net/http"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/ui/assets"
)

func MountRoutes(r chi.Router, h *Handler) {
	r.Get("/login", h.LoginPage)
	r.Post("/login", h.LoginSubmit)
	r.Get("/login/oidc", h.OIDCLoginStart)
	r.Get("/login/oidc/callback", h.OIDCLoginCallback)
	r.Post("/logout", h.Logout)

	staticFS, err := fs.Sub(assets.StaticFS(), "static")
	if err == nil {
		r.Handle("/static/*", http.StripPrefix("/ui/static/", http.FileServer(http.FS(staticFS))))
	}

	r.Group(func(r chi.Router) {
		r.Use(h.RequireWebSession)
		r.Use(h.EnsureCSRFToken)
		r.Use(h.RequireCSRF)
		r.Get("/", h.Home)
		r.Get("/components", h.ComponentsPage)
		r.Get("/sql", h.SQLEditorPage)
		r.Post("/sql/run", h.SQLEditorRun)
		r.Post("/sql/download.csv", h.SQLEditorDownloadCSV)

		r.Get("/catalogs", h.CatalogsList)
		r.Get("/catalogs/{catalogName}", h.CatalogsDetail)
		r.Get("/catalogs/new", h.CatalogsNew)
		r.Post("/catalogs", h.CatalogsCreate)
		r.Get("/catalogs/{catalogName}/edit", h.CatalogsEdit)
		r.Post("/catalogs/{catalogName}/update", h.CatalogsUpdate)
		r.Post("/catalogs/{catalogName}/delete", h.CatalogsDelete)
		r.Post("/catalogs/{catalogName}/set-default", h.CatalogsSetDefault)
		r.Get("/catalogs/{catalogName}/schemas/new", h.CatalogSchemasNew)
		r.Post("/catalogs/{catalogName}/schemas", h.CatalogSchemasCreate)
		r.Get("/catalogs/{catalogName}/schemas/{schemaName}", h.CatalogSchemasDetail)
		r.Get("/catalogs/{catalogName}/schemas/{schemaName}/edit", h.CatalogSchemasEdit)
		r.Post("/catalogs/{catalogName}/schemas/{schemaName}/update", h.CatalogSchemasUpdate)
		r.Post("/catalogs/{catalogName}/schemas/{schemaName}/delete", h.CatalogSchemasDelete)
		r.Get("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}", h.CatalogTablesDetail)
		r.Get("/catalogs/{catalogName}/schemas/{schemaName}/views/{viewName}", h.CatalogViewsDetail)

		r.Get("/security", h.SecurityHome)
		r.Get("/security/principals", h.SecurityPrincipalsList)
		r.Get("/security/principals/new", h.SecurityPrincipalsNew)
		r.Post("/security/principals", h.SecurityPrincipalsCreate)
		r.Get("/security/principals/{principalID}", h.SecurityPrincipalsDetail)
		r.Post("/security/principals/{principalID}/delete", h.SecurityPrincipalsDelete)
		r.Post("/security/principals/{principalID}/admin", h.SecurityPrincipalsSetAdmin)
		r.Get("/security/groups", h.SecurityGroupsList)
		r.Get("/security/groups/new", h.SecurityGroupsNew)
		r.Post("/security/groups", h.SecurityGroupsCreate)
		r.Get("/security/groups/{groupID}", h.SecurityGroupsDetail)
		r.Post("/security/groups/{groupID}/delete", h.SecurityGroupsDelete)
		r.Post("/security/groups/{groupID}/members", h.SecurityGroupsAddMember)
		r.Post("/security/groups/{groupID}/members/delete", h.SecurityGroupsRemoveMember)
		r.Get("/security/grants", h.SecurityGrantsList)
		r.Post("/security/grants", h.SecurityGrantsCreate)
		r.Post("/security/grants/{grantID}/delete", h.SecurityGrantsDelete)
		r.Get("/security/api-keys", h.SecurityAPIKeysList)
		r.Post("/security/api-keys", h.SecurityAPIKeysCreate)
		r.Post("/security/api-keys/{keyID}/delete", h.SecurityAPIKeysDelete)
		r.Post("/security/api-keys/cleanup", h.SecurityAPIKeysCleanup)

		r.Get("/assets", h.AssetsList)
		r.Get("/assets/{assetKey}", h.AssetsDetail)
		r.Post("/assets/{assetKey}/materialize", h.AssetMaterialize)
		r.Post("/assets/{assetKey}/backfills", h.AssetBackfillCreate)

		r.Get("/notebooks", h.NotebooksList)
		r.Get("/notebooks/{notebookID}", h.NotebooksDetail)
		r.Get("/notebooks/new", h.NotebooksNew)
		r.Post("/notebooks", h.NotebooksCreate)
		r.Get("/notebooks/{notebookID}/edit", h.NotebooksEdit)
		r.Post("/notebooks/{notebookID}/update", h.NotebooksUpdate)
		r.Post("/notebooks/{notebookID}/delete", h.NotebooksDelete)
		r.Get("/notebooks/{notebookID}/cells/new", h.NotebookCellsNew)
		r.Post("/notebooks/{notebookID}/cells", h.NotebookCellsCreate)
		r.Get("/notebooks/{notebookID}/cells/{cellID}/edit", h.NotebookCellsEdit)
		r.Post("/notebooks/{notebookID}/cells/{cellID}/update", h.NotebookCellsUpdate)
		r.Post("/notebooks/{notebookID}/cells/{cellID}/run", h.NotebookCellsRun)
		r.Post("/notebooks/{notebookID}/cells/{cellID}/move", h.NotebookCellsMove)
		r.Get("/notebooks/{notebookID}/cells/{cellID}/download.csv", h.NotebookCellsDownloadCSV)
		r.Post("/notebooks/{notebookID}/cells/{cellID}/delete", h.NotebookCellsDelete)
		r.Post("/notebooks/{notebookID}/cells/reorder", h.NotebookCellsReorder)
		r.Post("/notebooks/{notebookID}/run-all", h.NotebookRunAll)

		r.Get("/macros", h.MacrosList)
		r.Get("/macros/{macroName}", h.MacrosDetail)
		r.Get("/macros/new", h.MacrosNew)
		r.Post("/macros", h.MacrosCreate)
		r.Get("/macros/{macroName}/edit", h.MacrosEdit)
		r.Post("/macros/{macroName}/update", h.MacrosUpdate)
		r.Post("/macros/{macroName}/delete", h.MacrosDelete)

		r.Get("/models", h.ModelsList)
		r.Get("/models/{projectName}/{modelName}", h.ModelsDetail)
		r.Get("/models/new", h.ModelsNew)
		r.Post("/models", h.ModelsCreate)
		r.Get("/models/{projectName}/{modelName}/edit", h.ModelsEdit)
		r.Post("/models/{projectName}/{modelName}/update", h.ModelsUpdate)
		r.Post("/models/{projectName}/{modelName}/delete", h.ModelsDelete)
		r.Get("/models/{projectName}/{modelName}/tests/new", h.ModelTestsNew)
		r.Post("/models/{projectName}/{modelName}/tests", h.ModelTestsCreate)
		r.Post("/models/{projectName}/{modelName}/tests/{testID}/delete", h.ModelTestsDelete)
		r.Post("/models/runs/trigger", h.ModelRunsTrigger)
		r.Post("/models/runs/{runID}/cancel", h.ModelRunsCancel)
		r.Post("/models/runs/manual-cancel", h.ModelRunsManualCancel)
	})
}
