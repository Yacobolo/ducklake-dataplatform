package runtimeassets

import (
	"net/http"

	"duck-demo/internal/ui/legacy"
)

type Handler struct{ legacy *legacy.Handler }

func New(h *legacy.Handler) *Handler { return &Handler{legacy: h} }

func (h *Handler) AssetsList(w http.ResponseWriter, r *http.Request)   { h.legacy.AssetsList(w, r) }
func (h *Handler) AssetsDetail(w http.ResponseWriter, r *http.Request) { h.legacy.AssetsDetail(w, r) }
func (h *Handler) AssetMaterialize(w http.ResponseWriter, r *http.Request) {
	h.legacy.AssetMaterialize(w, r)
}
func (h *Handler) AssetBackfillCreate(w http.ResponseWriter, r *http.Request) {
	h.legacy.AssetBackfillCreate(w, r)
}
