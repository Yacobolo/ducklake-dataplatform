package ui

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"duck-demo/internal/config"
	"duck-demo/internal/domain"
	"duck-demo/internal/service/catalog"
	"duck-demo/internal/service/macro"
	"duck-demo/internal/service/model"
	"duck-demo/internal/service/notebook"
	"duck-demo/internal/service/pipeline"
	"duck-demo/internal/service/query"

	gomponents "maragu.dev/gomponents"
)

type Handler struct {
	CatalogRegistration *catalog.CatalogRegistrationService
	Catalog             *catalog.CatalogService
	Query               *query.QueryService
	View                *catalog.ViewService
	Pipeline            *pipeline.Service
	Notebook            *notebook.Service
	SessionManager      *notebook.SessionManager
	Macro               *macro.Service
	Model               *model.Service
	Auth                config.AuthConfig
	Production          bool
}

func NewHandler(
	catalogRegistration *catalog.CatalogRegistrationService,
	catalogSvc *catalog.CatalogService,
	querySvc *query.QueryService,
	viewSvc *catalog.ViewService,
	pipelineSvc *pipeline.Service,
	notebookSvc *notebook.Service,
	sessionManager *notebook.SessionManager,
	macroSvc *macro.Service,
	modelSvc *model.Service,
	auth config.AuthConfig,
	production bool,
) *Handler {
	return &Handler{
		CatalogRegistration: catalogRegistration,
		Catalog:             catalogSvc,
		Query:               querySvc,
		View:                viewSvc,
		Pipeline:            pipelineSvc,
		Notebook:            notebookSvc,
		SessionManager:      sessionManager,
		Macro:               macroSvc,
		Model:               modelSvc,
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
