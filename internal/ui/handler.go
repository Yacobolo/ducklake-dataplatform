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
	"duck-demo/internal/ui/dashboards"
	"duck-demo/internal/ui/governance"
	"duck-demo/internal/ui/legacy"
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

type PrincipalResolver = legacy.PrincipalResolver

// Handler is the public UI facade used by cmd/server and integration tests.
// The current implementation is delegated to the legacy UI package while
// top-level route ownership moves into feature packages.
type Handler struct {
	*legacy.Handler
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
	sessionManager *notebook.SessionManager,
	macroSvc *macro.Service,
	modelSvc *model.Service,
	authService *authsvc.Service,
	webSessionService *authsvc.SessionService,
	principalResolver PrincipalResolver,
	authCfg config.AuthConfig,
	production bool,
) *Handler {
	legacyHandler := legacy.NewHandler(
		catalogRegistration,
		catalogSvc,
		querySvc,
		viewSvc,
		pipelineSvc,
		assetSvc,
		backfillSvc,
		notebookSvc,
		sessionManager,
		macroSvc,
		modelSvc,
		authService,
		webSessionService,
		principalResolver,
		authCfg,
		production,
	)
	return &Handler{
		Handler:       legacyHandler,
		Auth:          uiauth.New(authService, webSessionService, principalResolver, authCfg, production),
		Catalogs:      catalogs.New(legacyHandler),
		Compute:       compute.New(legacyHandler),
		Overview:      overview.New(),
		Components:    components.New(),
		Dashboards:    dashboards.New(legacyHandler),
		Governance:    governance.New(legacyHandler),
		Macros:        macros.New(legacyHandler),
		Models:        models.New(legacyHandler),
		Notebooks:     notebooks.New(legacyHandler),
		Pipelines:     pipelines.New(legacyHandler),
		Products:      products.New(legacyHandler),
		RuntimeAssets: runtimeassets.New(legacyHandler),
		Security:      security.New(legacyHandler),
		Semantic:      semantic.New(legacyHandler),
		Storage:       storage.New(legacyHandler),
	}
}
