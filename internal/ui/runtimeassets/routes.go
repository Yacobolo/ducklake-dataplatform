package runtimeassets

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	AssetsList(http.ResponseWriter, *http.Request)
	AssetsDetail(http.ResponseWriter, *http.Request)
	AssetMaterialize(http.ResponseWriter, *http.Request)
	AssetBackfillCreate(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/assets", h.AssetsList)
	r.Get("/assets/{assetKey}", h.AssetsDetail)
	r.Post("/assets/{assetKey}/materialize", h.AssetMaterialize)
	r.Post("/assets/{assetKey}/backfills", h.AssetBackfillCreate)
}
