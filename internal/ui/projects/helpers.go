package projects

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
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

func parseFormOrRenderBadRequest(w http.ResponseWriter, r *http.Request) bool {
	if err := r.ParseForm(); err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Unable to parse form."))
		return false
	}
	return true
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

func formString(values map[string][]string, key string) string {
	if values == nil {
		return ""
	}
	return strings.TrimSpace(first(values[key]))
}

func formOptionalString(values map[string][]string, key string) *string {
	value := formString(values, key)
	if value == "" {
		return nil
	}
	return &value
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
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

func projectDetailURL(projectID string) string {
	return "/ui/projects/" + url.PathEscape(projectID)
}

func projectEnvironmentURL(projectID, environmentID string) string {
	return projectDetailURL(projectID) + "/environments/" + url.PathEscape(environmentID)
}

func projectEnvironmentNewURL(projectID string) string {
	return projectDetailURL(projectID) + "/environments/new"
}

func projectEnvironmentEditURL(projectID, environmentID string) string {
	return projectEnvironmentURL(projectID, environmentID) + "/edit"
}

func projectEnvironmentUpdateURL(projectID, environmentID string) string {
	return projectEnvironmentURL(projectID, environmentID) + "/update"
}

func projectEnvironmentDeleteURL(projectID, environmentID string) string {
	return projectEnvironmentURL(projectID, environmentID) + "/delete"
}

func projectBuildURL(projectID, buildID string) string {
	return projectDetailURL(projectID) + "/builds/" + url.PathEscape(buildID)
}

func stringMapEditorValue(values map[string]string) string {
	if len(values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	lines := make([]string, 0, len(keys))
	for _, key := range keys {
		lines = append(lines, fmt.Sprintf("%s=%s", key, values[key]))
	}
	return strings.Join(lines, "\n")
}

func parseStringMapEditor(raw string) map[string]string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	lines := strings.Split(raw, "\n")
	out := make(map[string]string, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			key = line
			value = ""
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = strings.TrimSpace(value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func projectTab(baseURL, tab string) string {
	if tab == "" || tab == projectTabModels {
		return baseURL
	}
	return baseURL + "?tab=" + url.QueryEscape(tab)
}

const (
	projectTabModels       = "models"
	projectTabMacros       = "macros"
	projectTabSemantic     = "semantic"
	projectTabEnvironments = "environments"
	projectTabBuilds       = "builds"
)

func normalizedProjectTab(v string) string {
	switch strings.TrimSpace(v) {
	case projectTabMacros, projectTabSemantic, projectTabEnvironments, projectTabBuilds:
		return strings.TrimSpace(v)
	default:
		return projectTabModels
	}
}
