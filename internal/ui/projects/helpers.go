package projects

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"
)

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
	return domain.PageRequest{MaxResults: maxResults, PageToken: r.URL.Query().Get("page_token")}
}

func renderServiceError(w http.ResponseWriter, err error) {
	status, message := core.ServiceErrorStatus(err)
	title := "Unexpected Error"
	switch status {
	case http.StatusNotFound:
		title = "Not Found"
	case http.StatusForbidden:
		title = "Access Denied"
	case http.StatusBadRequest:
		title = "Invalid Request"
	case http.StatusConflict:
		title = "Conflict"
	}
	core.RenderHTML(w, status, core.ErrorPage(title, message))
}

func principalName(r *http.Request) string {
	p := core.PrincipalFromContext(r.Context())
	if strings.TrimSpace(p.Name) == "" {
		return "unknown"
	}
	return p.Name
}

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	return ts.UTC().Format("2006-01-02 15:04 UTC")
}

func valueOrDash(v string) string {
	if strings.TrimSpace(v) == "" {
		return "-"
	}
	return v
}

func ptrString(v string) *string {
	return &v
}

func projectKindLabel(kind string) string {
	switch strings.TrimSpace(kind) {
	case domain.ProjectKindPersonal:
		return "Personal"
	case domain.ProjectKindShared:
		return "Shared"
	case domain.ProjectKindLibrary:
		return "Library"
	default:
		return valueOrDash(kind)
	}
}

func environmentKindLabel(kind string) string {
	switch strings.TrimSpace(kind) {
	case domain.EnvironmentKindDevelopment:
		return "Development"
	case domain.EnvironmentKindStaging:
		return "Staging"
	case domain.EnvironmentKindProduction:
		return "Production"
	default:
		return valueOrDash(kind)
	}
}

func buildStateLabel(state string) string {
	switch strings.TrimSpace(state) {
	case domain.BuildStateDraft:
		return "Draft"
	case domain.BuildStateReady:
		return "Ready"
	case domain.BuildStateReleased:
		return "Released"
	case domain.BuildStateSuperseded:
		return "Superseded"
	default:
		return valueOrDash(state)
	}
}

func ownerSummary(project domain.Project) string {
	switch {
	case project.OwnerPrincipal != nil && strings.TrimSpace(*project.OwnerPrincipal) != "":
		return *project.OwnerPrincipal
	case project.OwnerTeamID != nil && strings.TrimSpace(*project.OwnerTeamID) != "":
		return *project.OwnerTeamID
	default:
		return "-"
	}
}

func productSummary(project domain.Project) string {
	if project.ProductID == nil || strings.TrimSpace(*project.ProductID) == "" {
		return "Unlinked"
	}
	return *project.ProductID
}

func modelsListURL(projectName string) string {
	return "/ui/models?project=" + url.QueryEscape(projectName)
}

func macrosListURL(projectName string) string {
	return "/ui/macros?project=" + url.QueryEscape(projectName)
}

func semanticListURL(projectName string) string {
	return "/ui/semantic/models?project=" + url.QueryEscape(projectName)
}

func newModelURL(projectName string) string {
	return "/ui/models/new?project=" + url.QueryEscape(projectName)
}

func newMacroURL(projectName string) string {
	return "/ui/macros/new?project=" + url.QueryEscape(projectName)
}

func newSemanticURL(projectName string) string {
	return "/ui/semantic/models/new?project=" + url.QueryEscape(projectName)
}

func projectTab(baseURL, tab string) string {
	if tab == "" || tab == projectTabOverview {
		return baseURL
	}
	return baseURL + "?tab=" + url.QueryEscape(tab)
}

const (
	projectTabOverview     = "overview"
	projectTabAssets       = "assets"
	projectTabEnvironments = "environments"
	projectTabBuilds       = "builds"
)

func normalizedProjectTab(v string) string {
	switch strings.TrimSpace(v) {
	case projectTabAssets, projectTabEnvironments, projectTabBuilds:
		return strings.TrimSpace(v)
	default:
		return projectTabOverview
	}
}
