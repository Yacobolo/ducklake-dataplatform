package macros

import (
	"net/http"

	"duck-demo/internal/ui/legacy"
)

type Handler struct{ legacy *legacy.Handler }

func New(h *legacy.Handler) *Handler { return &Handler{legacy: h} }

func (h *Handler) MacrosList(w http.ResponseWriter, r *http.Request)   { h.legacy.MacrosList(w, r) }
func (h *Handler) MacrosDiff(w http.ResponseWriter, r *http.Request)   { h.legacy.MacrosDiff(w, r) }
func (h *Handler) MacrosImpact(w http.ResponseWriter, r *http.Request) { h.legacy.MacrosImpact(w, r) }
func (h *Handler) MacrosDetail(w http.ResponseWriter, r *http.Request) { h.legacy.MacrosDetail(w, r) }
func (h *Handler) MacrosNew(w http.ResponseWriter, r *http.Request)    { h.legacy.MacrosNew(w, r) }
func (h *Handler) MacrosCreate(w http.ResponseWriter, r *http.Request) { h.legacy.MacrosCreate(w, r) }
func (h *Handler) MacrosEdit(w http.ResponseWriter, r *http.Request)   { h.legacy.MacrosEdit(w, r) }
func (h *Handler) MacrosUpdate(w http.ResponseWriter, r *http.Request) { h.legacy.MacrosUpdate(w, r) }
func (h *Handler) MacrosDelete(w http.ResponseWriter, r *http.Request) { h.legacy.MacrosDelete(w, r) }
