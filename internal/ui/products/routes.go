package products

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	ProductsList(http.ResponseWriter, *http.Request)
	ProductsNew(http.ResponseWriter, *http.Request)
	ProductsCreate(http.ResponseWriter, *http.Request)
	ProductsDetail(http.ResponseWriter, *http.Request)
	ProductsVersionDetail(http.ResponseWriter, *http.Request)
	ProductsCreateVersion(http.ResponseWriter, *http.Request)
	ProductsPublish(http.ResponseWriter, *http.Request)
	ProductsDeprecate(http.ResponseWriter, *http.Request)
	ProductsRetire(http.ResponseWriter, *http.Request)
	ProductsAddDependency(http.ResponseWriter, *http.Request)
	ProductsSubscribe(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/products", h.ProductsList)
	r.Get("/products/new", h.ProductsNew)
	r.Post("/products", h.ProductsCreate)
	r.Get("/products/{productSlug}", h.ProductsDetail)
	r.Get("/products/{productSlug}/versions/{version}", h.ProductsVersionDetail)
	r.Post("/products/{productSlug}/versions", h.ProductsCreateVersion)
	r.Post("/products/{productSlug}/publish", h.ProductsPublish)
	r.Post("/products/{productSlug}/deprecate", h.ProductsDeprecate)
	r.Post("/products/{productSlug}/retire", h.ProductsRetire)
	r.Post("/products/{productSlug}/dependencies", h.ProductsAddDependency)
	r.Post("/products/{productSlug}/subscriptions", h.ProductsSubscribe)
}
