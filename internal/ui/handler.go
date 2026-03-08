package ui

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"duck-demo/internal/config"
	"duck-demo/internal/domain"
	assetsvc "duck-demo/internal/service/asset"
	authsvc "duck-demo/internal/service/auth"
	"duck-demo/internal/service/catalog"
	svccompute "duck-demo/internal/service/compute"
	"duck-demo/internal/service/governance"
	"duck-demo/internal/service/macro"
	"duck-demo/internal/service/model"
	"duck-demo/internal/service/notebook"
	"duck-demo/internal/service/orchestration"
	"duck-demo/internal/service/pipeline"
	"duck-demo/internal/service/query"
	"duck-demo/internal/service/security"
	"duck-demo/internal/service/storage"

	gomponents "maragu.dev/gomponents"
)

type Handler struct {
	CatalogRegistration *catalog.CatalogRegistrationService
	Catalog             *catalog.CatalogService
	Query               *query.QueryService
	View                *catalog.ViewService
	Pipeline            *pipeline.Service
	Asset               *assetsvc.Service
	Backfill            *orchestration.BackfillService
	Notebook            *notebook.Service
	SessionManager      *notebook.SessionManager
	Macro               *macro.Service
	Model               *model.Service
	Principal           *security.PrincipalService
	Group               *security.GroupService
	Grant               *security.GrantService
	RowFilter           *security.RowFilterService
	ColumnMask          *security.ColumnMaskService
	APIKey              *security.APIKeyService
	StorageCredential   *storage.StorageCredentialService
	ExternalLocation    *storage.ExternalLocationService
	Volume              *storage.VolumeService
	ComputeEndpoint     *svccompute.ComputeEndpointService
	Search              *catalog.SearchService
	Tag                 *governance.TagService
	Audit               *governance.AuditService
	QueryHistory        *governance.QueryHistoryService
	Lineage             *governance.LineageService
	AuthService         *authsvc.Service
	WebSessionService   *authsvc.SessionService
	PrincipalResolver   PrincipalResolver
	Auth                config.AuthConfig
	Production          bool
}

type PrincipalResolver interface {
	ResolveOrProvision(ctx context.Context, req domain.ResolveOrProvisionRequest) (*domain.Principal, error)
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
	auth config.AuthConfig,
	production bool,
) *Handler {
	return &Handler{
		CatalogRegistration: catalogRegistration,
		Catalog:             catalogSvc,
		Query:               querySvc,
		View:                viewSvc,
		Pipeline:            pipelineSvc,
		Asset:               assetSvc,
		Backfill:            backfillSvc,
		Notebook:            notebookSvc,
		SessionManager:      sessionManager,
		Macro:               macroSvc,
		Model:               modelSvc,
		AuthService:         authService,
		WebSessionService:   webSessionService,
		PrincipalResolver:   principalResolver,
		Auth:                auth,
		Production:          production,
	}
}

func pageFromRequest(r *http.Request, defaultPageSize int) domain.PageRequest {
	maxResults := defaultPageSize
	if maxResults <= 0 {
		maxResults = 25
	}
	if raw := r.URL.Query().Get("max_results"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			maxResults = parsed
		}
	}
	if maxResults < 1 {
		maxResults = 1
	}
	if maxResults > 200 {
		maxResults = 200
	}
	return domain.PageRequest{
		MaxResults: maxResults,
		PageToken:  r.URL.Query().Get("page_token"),
	}
}

func renderHTML(w http.ResponseWriter, status int, node gomponents.Node) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = node.Render(w)
}

func principalFromContext(ctx context.Context) domain.ContextPrincipal {
	p, ok := domain.PrincipalFromContext(ctx)
	if !ok {
		return domain.ContextPrincipal{Name: "unknown", Type: "user"}
	}
	return p
}

func principalLabel(ctx context.Context) (string, bool) {
	p, ok := domain.PrincipalFromContext(ctx)
	if !ok {
		return "unknown", false
	}
	if strings.TrimSpace(p.Name) == "" {
		return "unknown", p.IsAdmin
	}
	return p.Name, p.IsAdmin
}
