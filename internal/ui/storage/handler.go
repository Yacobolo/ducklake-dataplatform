package storage

import (
	"net/http"

	"duck-demo/internal/ui/legacy"
)

type Handler struct{ legacy *legacy.Handler }

func New(h *legacy.Handler) *Handler { return &Handler{legacy: h} }

func (h *Handler) StorageHome(w http.ResponseWriter, r *http.Request) { h.legacy.StorageHome(w, r) }
func (h *Handler) StorageCredentialsList(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageCredentialsList(w, r)
}
func (h *Handler) StorageCredentialsNew(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageCredentialsNew(w, r)
}
func (h *Handler) StorageCredentialsCreate(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageCredentialsCreate(w, r)
}
func (h *Handler) StorageCredentialsDetail(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageCredentialsDetail(w, r)
}
func (h *Handler) StorageCredentialsEdit(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageCredentialsEdit(w, r)
}
func (h *Handler) StorageCredentialsUpdate(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageCredentialsUpdate(w, r)
}
func (h *Handler) StorageCredentialsDelete(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageCredentialsDelete(w, r)
}
func (h *Handler) StorageLocationsList(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageLocationsList(w, r)
}
func (h *Handler) StorageLocationsNew(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageLocationsNew(w, r)
}
func (h *Handler) StorageLocationsCreate(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageLocationsCreate(w, r)
}
func (h *Handler) StorageLocationsDetail(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageLocationsDetail(w, r)
}
func (h *Handler) StorageLocationsEdit(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageLocationsEdit(w, r)
}
func (h *Handler) StorageLocationsUpdate(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageLocationsUpdate(w, r)
}
func (h *Handler) StorageLocationsDelete(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageLocationsDelete(w, r)
}
func (h *Handler) StorageVolumesList(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageVolumesList(w, r)
}
func (h *Handler) StorageVolumesNew(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageVolumesNew(w, r)
}
func (h *Handler) StorageVolumesCreate(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageVolumesCreate(w, r)
}
func (h *Handler) StorageVolumesDetail(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageVolumesDetail(w, r)
}
func (h *Handler) StorageVolumesEdit(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageVolumesEdit(w, r)
}
func (h *Handler) StorageVolumesUpdate(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageVolumesUpdate(w, r)
}
func (h *Handler) StorageVolumesDelete(w http.ResponseWriter, r *http.Request) {
	h.legacy.StorageVolumesDelete(w, r)
}
