package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
)

var uuidPathPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type bodyValidationTarget int

const (
	bodyValidationNone bodyValidationTarget = iota
	bodyValidationCreatePrincipal
	bodyValidationCreateNotebook
	bodyValidationCreateAPIKey
)

// RequestValidationMiddleware enforces HTTP contract validation that the
// generated strict handler does not currently apply itself.
func RequestValidationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := validatePaginationParams(r); err != nil {
			writeBadRequest(w, err)
			return
		}
		if err := validateUUIDPathParams(r); err != nil {
			writeBadRequest(w, err)
			return
		}

		if err := normalizeAndValidateJSONBody(r); err != nil {
			writeBadRequest(w, err)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func validatePaginationParams(r *http.Request) error {
	qs := r.URL.Query()

	if raw := qs.Get("max_results"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > domain.MaxMaxResults {
			return domain.ErrValidation("max_results must be between 1 and %d", domain.MaxMaxResults)
		}
	}

	if raw := qs.Get("page_token"); raw != "" {
		if _, err := decodePageToken(raw); err != nil {
			return err
		}
	}

	return nil
}

func decodePageToken(token string) (int, error) {
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0, domain.ErrValidation("page_token must be a valid cursor")
	}
	offset, err := strconv.Atoi(string(decoded))
	if err != nil || offset < 0 {
		return 0, domain.ErrValidation("page_token must be a valid cursor")
	}
	return offset, nil
}

func validateUUIDPathParams(r *http.Request) error {
	for _, spec := range uuidRouteSpecs {
		pathParts := normalizedPathParts(r.URL.Path)
		if !spec.matches(r.Method, pathParts) {
			continue
		}
		for _, param := range spec.params {
			value := chi.URLParam(r, param.name)
			if value == "" {
				value = routeParamFromPath(pathParts, param.segmentIndex)
			}
			if value == "" {
				continue
			}
			if !uuidPathPattern.MatchString(value) {
				return domain.ErrValidation("%s must be a valid UUID", param.name)
			}
		}
	}

	return nil
}

type uuidRouteSpec struct {
	method  string
	pattern []string
	params  []uuidRouteParam
}

type uuidRouteParam struct {
	name         string
	segmentIndex int
}

var uuidRouteSpecs = []uuidRouteSpec{
	{method: http.MethodGet, pattern: []string{"principals", "*"}, params: []uuidRouteParam{{name: "principalId", segmentIndex: 1}}},
	{method: http.MethodDelete, pattern: []string{"principals", "*"}, params: []uuidRouteParam{{name: "principalId", segmentIndex: 1}}},
	{method: http.MethodPatch, pattern: []string{"principals", "*"}, params: []uuidRouteParam{{name: "principalId", segmentIndex: 1}}},
	{method: http.MethodGet, pattern: []string{"groups", "*"}, params: []uuidRouteParam{{name: "groupId", segmentIndex: 1}}},
	{method: http.MethodDelete, pattern: []string{"groups", "*"}, params: []uuidRouteParam{{name: "groupId", segmentIndex: 1}}},
	{method: http.MethodGet, pattern: []string{"groups", "*", "members"}, params: []uuidRouteParam{{name: "groupId", segmentIndex: 1}}},
	{method: http.MethodPost, pattern: []string{"groups", "*", "members"}, params: []uuidRouteParam{{name: "groupId", segmentIndex: 1}}},
	{method: http.MethodDelete, pattern: []string{"groups", "*", "members"}, params: []uuidRouteParam{{name: "groupId", segmentIndex: 1}}},
	{method: http.MethodDelete, pattern: []string{"grants", "*"}, params: []uuidRouteParam{{name: "grantId", segmentIndex: 1}}},
	{method: http.MethodDelete, pattern: []string{"row-filters", "*"}, params: []uuidRouteParam{{name: "rowFilterId", segmentIndex: 1}}},
	{method: http.MethodDelete, pattern: []string{"column-masks", "*"}, params: []uuidRouteParam{{name: "columnMaskId", segmentIndex: 1}}},
	{method: http.MethodDelete, pattern: []string{"api-keys", "*"}, params: []uuidRouteParam{{name: "apiKeyId", segmentIndex: 1}}},
	{method: http.MethodDelete, pattern: []string{"tags", "*"}, params: []uuidRouteParam{{name: "tagId", segmentIndex: 1}}},
	{method: http.MethodDelete, pattern: []string{"lineage", "edges", "*"}, params: []uuidRouteParam{{name: "edgeId", segmentIndex: 2}}},
	{method: http.MethodGet, pattern: []string{"model-runs", "*"}, params: []uuidRouteParam{{name: "runId", segmentIndex: 1}}},
	{method: http.MethodGet, pattern: []string{"model-runs", "*", "steps"}, params: []uuidRouteParam{{name: "runId", segmentIndex: 1}}},
	{method: http.MethodGet, pattern: []string{"model-runs", "*", "steps", "*", "test-results"}, params: []uuidRouteParam{{name: "runId", segmentIndex: 1}, {name: "stepId", segmentIndex: 3}}},
	{method: http.MethodPost, pattern: []string{"model-runs", "*", "cancel"}, params: []uuidRouteParam{{name: "runId", segmentIndex: 1}}},
}

func (s uuidRouteSpec) matches(method string, pathParts []string) bool {
	if s.method != method {
		return false
	}
	if len(pathParts) != len(s.pattern) {
		return false
	}
	for i, part := range s.pattern {
		if part == "*" {
			continue
		}
		if pathParts[i] != part {
			return false
		}
	}
	return true
}

func normalizedPathParts(path string) []string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) > 0 && parts[0] == "v1" {
		return parts[1:]
	}
	return parts
}

func routeParamFromPath(parts []string, index int) string {
	if index >= len(parts) {
		return ""
	}
	return parts[index]
}

func normalizeAndValidateJSONBody(r *http.Request) error {
	if r.Body == nil || !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		return nil
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return domain.ErrValidation("read request body: %v", err)
	}

	if isSetDefaultCatalogRoute(r.Method, r.URL.Path) && len(bytes.TrimSpace(body)) == 0 {
		body = []byte("{}")
	}

	target := bodyValidationForRoute(r.Method, r.URL.Path)
	if target != bodyValidationNone && len(bytes.TrimSpace(body)) > 0 {
		if err := validateJSONBody(body, target); err != nil {
			return err
		}
	}

	r.Body = io.NopCloser(bytes.NewReader(body))
	return nil
}

func isSetDefaultCatalogRoute(method, path string) bool {
	return method == http.MethodPost && uuidRouteSpec{method: http.MethodPost, pattern: []string{"catalogs", "*", "set-default"}}.matches(method, normalizedPathParts(path))
}

func bodyValidationForRoute(method, path string) bodyValidationTarget {
	if method != http.MethodPost {
		return bodyValidationNone
	}
	switch path {
	case "/principals", "/v1/principals":
		return bodyValidationCreatePrincipal
	case "/notebooks", "/v1/notebooks":
		return bodyValidationCreateNotebook
	case "/api-keys", "/v1/api-keys":
		return bodyValidationCreateAPIKey
	default:
		return bodyValidationNone
	}
}

func validateJSONBody(body []byte, target bodyValidationTarget) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()

	switch target {
	case bodyValidationCreatePrincipal:
		var req CreatePrincipalJSONRequestBody
		if err := decoder.Decode(&req); err != nil {
			return domain.ErrValidation("invalid JSON body: %v", err)
		}
	case bodyValidationCreateNotebook:
		var req CreateNotebookJSONRequestBody
		if err := decoder.Decode(&req); err != nil {
			return domain.ErrValidation("invalid JSON body: %v", err)
		}
	case bodyValidationCreateAPIKey:
		var req CreateAPIKeyJSONRequestBody
		if err := decoder.Decode(&req); err != nil {
			return domain.ErrValidation("invalid JSON body: %v", err)
		}
	}

	if decoder.More() {
		return domain.ErrValidation("invalid JSON body: unexpected trailing content")
	}

	return nil
}

func writeBadRequest(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(Error{Code: 400, Message: err.Error()})
}
