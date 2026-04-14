package components

import (
	"net/http"

	"github.com/Yacobolo/quackstack/internal/ui/core"
)

type Handler struct{}

func New() *Handler { return &Handler{} }

func (h *Handler) ComponentsPage(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, componentsPage(core.PrincipalFromContext(r.Context())))
}
