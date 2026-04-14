package core

import (
	"context"
	"net/http"

	"github.com/Yacobolo/quackstack/internal/domain"

	gomponents "maragu.dev/gomponents"
)

func RenderHTML(w http.ResponseWriter, status int, node gomponents.Node) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = node.Render(w)
}

func PrincipalFromContext(ctx context.Context) domain.ContextPrincipal {
	p, ok := domain.PrincipalFromContext(ctx)
	if !ok {
		return domain.ContextPrincipal{Name: "unknown", Type: "user"}
	}
	return p
}
