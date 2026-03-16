package governance

import (
	"net/http"
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

func parseFormOrRenderBadRequest(w http.ResponseWriter, r *http.Request) bool {
	if err := r.ParseForm(); err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Unable to parse form."))
		return false
	}
	return true
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

func formString(values map[string][]string, key string) string {
	if values == nil {
		return ""
	}
	return strings.TrimSpace(first(values[key]))
}

func formOptionalString(values map[string][]string, key string) *string {
	v := formString(values, key)
	if v == "" {
		return nil
	}
	return &v
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func strOrDash(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return "-"
	}
	return *value
}

func valueOrDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	return ts.UTC().Format("2006-01-02 15:04 UTC")
}

func optionalQueryValue(r *http.Request, key string) *string {
	value := strings.TrimSpace(r.URL.Query().Get(key))
	if value == "" {
		return nil
	}
	return &value
}
