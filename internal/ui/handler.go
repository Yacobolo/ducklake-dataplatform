package ui

import (
	"github.com/Yacobolo/quackstack/internal/config"
	assetsvc "github.com/Yacobolo/quackstack/internal/service/asset"
	authsvc "github.com/Yacobolo/quackstack/internal/service/auth"
	"github.com/Yacobolo/quackstack/internal/service/catalog"
	exploresvc "github.com/Yacobolo/quackstack/internal/service/explore"
	"github.com/Yacobolo/quackstack/internal/service/macro"
	"github.com/Yacobolo/quackstack/internal/service/model"
	"github.com/Yacobolo/quackstack/internal/service/notebook"
	"github.com/Yacobolo/quackstack/internal/service/orchestration"
	"github.com/Yacobolo/quackstack/internal/service/pipeline"
	projectsvc "github.com/Yacobolo/quackstack/internal/service/project"
	"github.com/Yacobolo/quackstack/internal/service/query"
	"github.com/Yacobolo/quackstack/internal/service/resourceaccess"
	"github.com/Yacobolo/quackstack/internal/service/savedresource"
	workspacesvc "github.com/Yacobolo/quackstack/internal/service/workspace"
	uiauth "github.com/Yacobolo/quackstack/internal/ui/auth"
	"github.com/Yacobolo/quackstack/internal/ui/catalogs"
	"github.com/Yacobolo/quackstack/internal/ui/components"
	"github.com/Yacobolo/quackstack/internal/ui/compute"
	"github.com/Yacobolo/quackstack/internal/ui/core"
	"github.com/Yacobolo/quackstack/internal/ui/dashboards"
	"github.com/Yacobolo/quackstack/internal/ui/explore"
	"github.com/Yacobolo/quackstack/internal/ui/governance"
	"github.com/Yacobolo/quackstack/internal/ui/macros"
	"github.com/Yacobolo/quackstack/internal/ui/models"
	"github.com/Yacobolo/quackstack/internal/ui/notebooks"
	"github.com/Yacobolo/quackstack/internal/ui/overview"
	"github.com/Yacobolo/quackstack/internal/ui/pipelines"
	"github.com/Yacobolo/quackstack/internal/ui/products"
	uiprojects "github.com/Yacobolo/quackstack/internal/ui/projects"
	"github.com/Yacobolo/quackstack/internal/ui/runtimeassets"
	"github.com/Yacobolo/quackstack/internal/ui/security"
	"github.com/Yacobolo/quackstack/internal/ui/semantic"
	"github.com/Yacobolo/quackstack/internal/ui/storage"
	"github.com/Yacobolo/quackstack/internal/ui/core"
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
	Explore       *explore.Handler
	Governance    *governance.Handler
	Macros        *macros.Handler
	Models        *models.Handler
	Notebooks     *notebooks.Handler
	Pipelines     *pipelines.Handler
	Projects      *uiprojects.Handler
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
	workspaceSvc *workspacesvc.Service,
	projectSvc *projectsvc.Service,
	exploreSvc *exploresvc.Service,
	sessionManager *notebook.SessionManager,
	macroSvc *macro.Service,
	modelSvc *model.Service,
	authService *authsvc.Service,
	webSessionService *authsvc.SessionService,
	resourceAccessService *resourceaccess.Service,
	savedResourceService *savedresource.Service,
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
		Workspace:           workspaceSvc,
		Project:             projectSvc,
		Explore:             exploreSvc,
		SessionManager:      sessionManager,
		Macro:               macroSvc,
		Model:               modelSvc,
		AuthService:         authService,
		WebSessionService:   webSessionService,
		ResourceAccess:      resourceAccessService,
		SavedResource:       savedResourceService,
		PrincipalResolver:   principalResolver,
		Auth:                authCfg,
		Production:          production,
	}
	handler := &Handler{
		Dependencies: deps,
		Auth:         uiauth.New(authService, webSessionService, principalResolver, authCfg, production),
		Overview:     overview.New(deps),
		Components:   components.New(),
	}
	notebookHandler := notebooks.New(handler.Dependencies)
	handler.Catalogs = catalogs.New(handler.Dependencies)
	handler.Compute = compute.New(handler.Dependencies)
	handler.Dashboards = dashboards.New(handler.Dependencies)
	handler.Explore = explore.New(handler.Dependencies)
	handler.Governance = governance.New(handler.Dependencies)
	handler.Macros = macros.New(handler.Dependencies)
	handler.Models = models.New(handler.Dependencies)
	handler.Notebooks = notebookHandler
	handler.Pipelines = pipelines.New(handler.Dependencies)
	handler.Projects = uiprojects.New(handler.Dependencies)
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
