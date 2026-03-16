package storage

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	StorageHome(http.ResponseWriter, *http.Request)
	StorageCredentialsList(http.ResponseWriter, *http.Request)
	StorageCredentialsNew(http.ResponseWriter, *http.Request)
	StorageCredentialsCreate(http.ResponseWriter, *http.Request)
	StorageCredentialsDetail(http.ResponseWriter, *http.Request)
	StorageCredentialsEdit(http.ResponseWriter, *http.Request)
	StorageCredentialsUpdate(http.ResponseWriter, *http.Request)
	StorageCredentialsDelete(http.ResponseWriter, *http.Request)
	StorageLocationsList(http.ResponseWriter, *http.Request)
	StorageLocationsNew(http.ResponseWriter, *http.Request)
	StorageLocationsCreate(http.ResponseWriter, *http.Request)
	StorageLocationsDetail(http.ResponseWriter, *http.Request)
	StorageLocationsEdit(http.ResponseWriter, *http.Request)
	StorageLocationsUpdate(http.ResponseWriter, *http.Request)
	StorageLocationsDelete(http.ResponseWriter, *http.Request)
	StorageVolumesList(http.ResponseWriter, *http.Request)
	StorageVolumesNew(http.ResponseWriter, *http.Request)
	StorageVolumesCreate(http.ResponseWriter, *http.Request)
	StorageVolumesDetail(http.ResponseWriter, *http.Request)
	StorageVolumesEdit(http.ResponseWriter, *http.Request)
	StorageVolumesUpdate(http.ResponseWriter, *http.Request)
	StorageVolumesDelete(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/storage", h.StorageHome)
	r.Get("/storage/credentials", h.StorageCredentialsList)
	r.Get("/storage/credentials/new", h.StorageCredentialsNew)
	r.Post("/storage/credentials", h.StorageCredentialsCreate)
	r.Get("/storage/credentials/{credentialName}", h.StorageCredentialsDetail)
	r.Get("/storage/credentials/{credentialName}/edit", h.StorageCredentialsEdit)
	r.Post("/storage/credentials/{credentialName}/update", h.StorageCredentialsUpdate)
	r.Post("/storage/credentials/{credentialName}/delete", h.StorageCredentialsDelete)
	r.Get("/storage/locations", h.StorageLocationsList)
	r.Get("/storage/locations/new", h.StorageLocationsNew)
	r.Post("/storage/locations", h.StorageLocationsCreate)
	r.Get("/storage/locations/{locationName}", h.StorageLocationsDetail)
	r.Get("/storage/locations/{locationName}/edit", h.StorageLocationsEdit)
	r.Post("/storage/locations/{locationName}/update", h.StorageLocationsUpdate)
	r.Post("/storage/locations/{locationName}/delete", h.StorageLocationsDelete)
	r.Get("/storage/volumes", h.StorageVolumesList)
	r.Get("/storage/volumes/new", h.StorageVolumesNew)
	r.Post("/storage/volumes", h.StorageVolumesCreate)
	r.Get("/storage/volumes/{catalogName}/{schemaName}/{volumeName}", h.StorageVolumesDetail)
	r.Get("/storage/volumes/{catalogName}/{schemaName}/{volumeName}/edit", h.StorageVolumesEdit)
	r.Post("/storage/volumes/{catalogName}/{schemaName}/{volumeName}/update", h.StorageVolumesUpdate)
	r.Post("/storage/volumes/{catalogName}/{schemaName}/{volumeName}/delete", h.StorageVolumesDelete)
}
