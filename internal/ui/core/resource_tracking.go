package core

import (
	"net/http"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/service/resourceref"
)

func TrackResourceVisit(r *http.Request, deps *Dependencies, resource domain.ResourceRef) error {
	if deps == nil || deps.ResourceAccess == nil {
		return nil
	}
	return deps.ResourceAccess.TrackVisit(r.Context(), PrincipalFromContext(r.Context()), resource)
}

func SafeUIReturnPath(raw string) string {
	candidate := strings.TrimSpace(raw)
	if candidate == "" || candidate == "/ui" {
		return "/ui"
	}
	normalized, err := resourceref.NormalizeHref(candidate)
	if err != nil {
		return "/ui"
	}
	return normalized
}
