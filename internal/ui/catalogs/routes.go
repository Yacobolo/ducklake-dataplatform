package catalogs

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	CatalogsList(http.ResponseWriter, *http.Request)
	CatalogsDetail(http.ResponseWriter, *http.Request)
	CatalogsNew(http.ResponseWriter, *http.Request)
	CatalogsCreate(http.ResponseWriter, *http.Request)
	CatalogsEdit(http.ResponseWriter, *http.Request)
	CatalogsUpdate(http.ResponseWriter, *http.Request)
	CatalogsDelete(http.ResponseWriter, *http.Request)
	CatalogsSetDefault(http.ResponseWriter, *http.Request)
	CatalogSchemasNew(http.ResponseWriter, *http.Request)
	CatalogSchemasCreate(http.ResponseWriter, *http.Request)
	CatalogSchemasDetail(http.ResponseWriter, *http.Request)
	CatalogSchemasEdit(http.ResponseWriter, *http.Request)
	CatalogSchemasUpdate(http.ResponseWriter, *http.Request)
	CatalogSchemasDelete(http.ResponseWriter, *http.Request)
	CatalogTablesDetail(http.ResponseWriter, *http.Request)
	CatalogViewsDetail(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/catalogs", h.CatalogsList)
	r.Get("/catalogs/{catalogName}", h.CatalogsDetail)
	r.Get("/catalogs/new", h.CatalogsNew)
	r.Post("/catalogs", h.CatalogsCreate)
	r.Get("/catalogs/{catalogName}/edit", h.CatalogsEdit)
	r.Post("/catalogs/{catalogName}/update", h.CatalogsUpdate)
	r.Post("/catalogs/{catalogName}/delete", h.CatalogsDelete)
	r.Post("/catalogs/{catalogName}/set-default", h.CatalogsSetDefault)
	r.Get("/catalogs/{catalogName}/schemas/new", h.CatalogSchemasNew)
	r.Post("/catalogs/{catalogName}/schemas", h.CatalogSchemasCreate)
	r.Get("/catalogs/{catalogName}/schemas/{schemaName}", h.CatalogSchemasDetail)
	r.Get("/catalogs/{catalogName}/schemas/{schemaName}/edit", h.CatalogSchemasEdit)
	r.Post("/catalogs/{catalogName}/schemas/{schemaName}/update", h.CatalogSchemasUpdate)
	r.Post("/catalogs/{catalogName}/schemas/{schemaName}/delete", h.CatalogSchemasDelete)
	r.Get("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}", h.CatalogTablesDetail)
	r.Get("/catalogs/{catalogName}/schemas/{schemaName}/views/{viewName}", h.CatalogViewsDetail)
}
