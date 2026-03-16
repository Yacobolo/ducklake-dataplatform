package core

import (
	"context"
	"net/http"
	"strings"

	"duck-demo/internal/config"
	"duck-demo/internal/domain"
	assetsvc "duck-demo/internal/service/asset"
	authsvc "duck-demo/internal/service/auth"
	"duck-demo/internal/service/catalog"
	svccompute "duck-demo/internal/service/compute"
	"duck-demo/internal/service/dashboard"
	"duck-demo/internal/service/governance"
	"duck-demo/internal/service/macro"
	"duck-demo/internal/service/model"
	"duck-demo/internal/service/notebook"
	"duck-demo/internal/service/orchestration"
	"duck-demo/internal/service/pipeline"
	productsvc "duck-demo/internal/service/product"
	"duck-demo/internal/service/query"
	"duck-demo/internal/service/security"
	"duck-demo/internal/service/semantic"
	"duck-demo/internal/service/storage"

	gomponents "maragu.dev/gomponents"
	html "maragu.dev/gomponents/html"
)

type PrincipalResolver interface {
	ResolveOrProvision(ctx context.Context, req domain.ResolveOrProvisionRequest) (*domain.Principal, error)
}

type Dependencies struct {
	CatalogRegistration *catalog.CatalogRegistrationService
	Catalog             *catalog.CatalogService
	Query               *query.QueryService
	Manifest            *query.ManifestService
	View                *catalog.ViewService
	Pipeline            *pipeline.Service
	Product             *productsvc.Service
	Asset               *assetsvc.Service
	Backfill            *orchestration.BackfillService
	Notebook            *notebook.Service
	SessionManager      *notebook.SessionManager
	GitService          *notebook.GitService
	Macro               *macro.Service
	Model               *model.Service
	Semantic            *semantic.Service
	Dashboard           *dashboard.Service
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

func (d *Dependencies) CSRFFieldProvider(r *http.Request) func() gomponents.Node {
	return func() gomponents.Node {
		token, _ := r.Context().Value(csrfContextKey{}).(string)
		if token == "" {
			token = readCSRFCookie(r)
		}
		return html.Input(
			html.Type("hidden"),
			html.Name("csrf_token"),
			html.Value(token),
		)
	}
}

type csrfContextKey struct{}

const csrfCookieName = "ui_csrf"

func readCSRFCookie(r *http.Request) string {
	cookie, err := r.Cookie(csrfCookieName)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(cookie.Value)
}
