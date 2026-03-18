package auth

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	LoginPage(http.ResponseWriter, *http.Request)
	LoginSubmit(http.ResponseWriter, *http.Request)
	OIDCLoginStart(http.ResponseWriter, *http.Request)
	OIDCLoginCallback(http.ResponseWriter, *http.Request)
	Logout(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/login", h.LoginPage)
	r.Post("/login", h.LoginSubmit)
	r.Get("/login/oidc", h.OIDCLoginStart)
	r.Get("/login/oidc/callback", h.OIDCLoginCallback)
	r.Post("/logout", h.Logout)
}
