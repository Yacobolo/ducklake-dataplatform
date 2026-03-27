package ui

import (
	"duck-demo/internal/config"
	assetsvc "duck-demo/internal/service/asset"
	authsvc "duck-demo/internal/service/auth"
	"duck-demo/internal/service/catalog"
	"duck-demo/internal/service/macro"
	"duck-demo/internal/service/model"
	"duck-demo/internal/service/notebook"
	"duck-demo/internal/service/orchestration"
	"duck-demo/internal/service/pipeline"
	"duck-demo/internal/service/query"
	uiauth "duck-demo/internal/ui/auth"
	"duck-demo/internal/ui/catalogs"
	"duck-demo/internal/ui/components"
	"duck-demo/internal/ui/compute"
	"duck-demo/internal/ui/core"
	"duck-demo/internal/ui/dashboards"
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

type PrincipalResolver = core.PrincipalResolver

// Handler is the public UI facade used by cmd/server and integration tests.
// It composes feature handlers over shared UI dependencies.
type Handler struct {
	*core.Dependencies
	Auth          *uiauth.Handler
	Catalogs      *catalogs.Handler
	Compute       *compute.Handler
	Overview      *overview.Handler
	Components    *components.Handler
	Dashboards    *dashboards.Handler
	Governance    *governance.Handler
	Macros        *macros.Handler
	Models        *models.Handler
	Notebooks     *notebooks.Handler
	Pipelines     *pipelines.Handler
	Products      *products.Handler
	RuntimeAssets *runtimeassets.Handler
	Security      *security.Handler
	Semantic      *semantic.Handler
	Storage       *storage.Handler
}

func NewHandler(
	catalogRegistration *catalog.CatalogRegistrationService,
	catalogSvc *catalog.CatalogService,
	querySvc *query.QueryService,
	viewSvc *catalog.ViewService,
	pipelineSvc *pipeline.Service,
	assetSvc *assetsvc.Service,
	backfillSvc *orchestration.BackfillService,
	notebookSvc *notebook.Service,
	notebookFolderSvc *notebook.FolderService,
	sessionManager *notebook.SessionManager,
	macroSvc *macro.Service,
	modelSvc *model.Service,
	authService *authsvc.Service,
	webSessionService *authsvc.SessionService,
	principalResolver PrincipalResolver,
	authCfg config.AuthConfig,
	production bool,
) *Handler {
	deps := &core.Dependencies{
		CatalogRegistration: catalogRegistration,
		Catalog:             catalogSvc,
		Query:               querySvc,
		View:                viewSvc,
		Pipeline:            pipelineSvc,
		Asset:               assetSvc,
		Backfill:            backfillSvc,
		Notebook:            notebookSvc,
		NotebookFolders:     notebookFolderSvc,
		SessionManager:      sessionManager,
		Macro:               macroSvc,
		Model:               modelSvc,
		AuthService:         authService,
		WebSessionService:   webSessionService,
		PrincipalResolver:   principalResolver,
		Auth:                authCfg,
		Production:          production,
	}
	handler := &Handler{
		Dependencies: deps,
		Auth:         uiauth.New(authService, webSessionService, principalResolver, authCfg, production),
		Overview:     overview.New(),
		Components:   components.New(),
	}
	handler.Catalogs = catalogs.New(handler.Dependencies)
	handler.Compute = compute.New(handler.Dependencies)
	handler.Dashboards = dashboards.New(handler.Dependencies)
	handler.Governance = governance.New(handler.Dependencies)
	handler.Macros = macros.New(handler.Dependencies)
	handler.Models = models.New(handler.Dependencies)
	handler.Notebooks = notebooks.New(handler.Dependencies)
	handler.Pipelines = pipelines.New(handler.Dependencies)
	handler.Products = products.New(handler.Dependencies)
	handler.RuntimeAssets = runtimeassets.New(handler.Dependencies)
	handler.Security = security.New(handler.Dependencies)
	handler.Semantic = semantic.New(handler.Dependencies)
	handler.Storage = storage.New(handler.Dependencies)
	return handler
}

func (h *Handler) SyncDependencies() {
	if h == nil || h.Dependencies == nil {
		return
	}
}
