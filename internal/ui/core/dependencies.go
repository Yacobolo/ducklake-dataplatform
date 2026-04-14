package core

import (
	"context"
	"net/http"
	"strings"

	"github.com/Yacobolo/quackstack/internal/config"
	"github.com/Yacobolo/quackstack/internal/domain"
	assetsvc "github.com/Yacobolo/quackstack/internal/service/asset"
	authsvc "github.com/Yacobolo/quackstack/internal/service/auth"
	"github.com/Yacobolo/quackstack/internal/service/catalog"
	svccompute "github.com/Yacobolo/quackstack/internal/service/compute"
	"github.com/Yacobolo/quackstack/internal/service/dashboard"
	exploresvc "github.com/Yacobolo/quackstack/internal/service/explore"
	"github.com/Yacobolo/quackstack/internal/service/governance"
	"github.com/Yacobolo/quackstack/internal/service/macro"
	"github.com/Yacobolo/quackstack/internal/service/model"
	"github.com/Yacobolo/quackstack/internal/service/notebook"
	"github.com/Yacobolo/quackstack/internal/service/orchestration"
	"github.com/Yacobolo/quackstack/internal/service/pipeline"
	productsvc "github.com/Yacobolo/quackstack/internal/service/product"
	"github.com/Yacobolo/quackstack/internal/service/query"
	"github.com/Yacobolo/quackstack/internal/service/resourceaccess"
	"github.com/Yacobolo/quackstack/internal/service/savedresource"
	"github.com/Yacobolo/quackstack/internal/service/security"
	"github.com/Yacobolo/quackstack/internal/service/semantic"
	"github.com/Yacobolo/quackstack/internal/service/storage"

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
	NotebookFolders     *notebook.FolderService
	Explore             *exploresvc.Service
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
	ResourceAccess      *resourceaccess.Service
	SavedResource       *savedresource.Service
	PrincipalResolver   PrincipalResolver
	Auth                config.AuthConfig
	Production          bool
}

func (d *Dependencies) CSRFFieldProvider(r *http.Request) func() gomponents.Node {
	return func() gomponents.Node {
		token := d.CSRFToken(r)
		return html.Input(
			html.Type("hidden"),
			html.Name("csrf_token"),
			html.Value(token),
		)
	}
}

func (d *Dependencies) CSRFToken(r *http.Request) string {
	token, _ := r.Context().Value(csrfContextKey{}).(string)
	if token == "" {
		token = readCSRFCookie(r)
	}
	return token
}

func WithCSRFToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, csrfContextKey{}, strings.TrimSpace(token))
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
