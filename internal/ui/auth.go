package ui

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"duck-demo/internal/domain"

	gomponents "maragu.dev/gomponents"
	html "maragu.dev/gomponents/html"
)

const (
	bearerCookieName = "ui_bearer"
	apiKeyCookieName = "ui_api_key"
)

func (h *Handler) LoginPage(w http.ResponseWriter, r *http.Request) {
	if _, ok := domain.PrincipalFromContext(r.Context()); ok {
		http.Redirect(w, r, "/ui", http.StatusSeeOther)
		return
	}
	renderHTML(w, http.StatusOK, loginPage(strings.TrimSpace(r.URL.Query().Get("error"))))
}

func (h *Handler) LoginSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Redirect(w, r, "/ui/login?error=invalid+form", http.StatusSeeOther)
		return
	}
	kind := strings.TrimSpace(r.Form.Get("kind"))
	token := strings.TrimSpace(r.Form.Get("token"))
	if token == "" {
		http.Redirect(w, r, "/ui/login?error=token+is+required", http.StatusSeeOther)
		return
	}

	expires := time.Now().Add(24 * time.Hour)
	bearerCookie := &http.Cookie{
		Name:     bearerCookieName,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.Production,
		SameSite: http.SameSiteLaxMode,
		Expires:  expires,
	}
	apiKeyCookie := &http.Cookie{
		Name:     apiKeyCookieName,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.Production,
		SameSite: http.SameSiteLaxMode,
		Expires:  expires,
	}

	switch kind {
	case "api_key":
		apiKeyCookie.Value = token
		bearerCookie.MaxAge = -1
	default:
		bearerCookie.Value = token
		apiKeyCookie.MaxAge = -1
	}
	http.SetCookie(w, bearerCookie)
	http.SetCookie(w, apiKeyCookie)
	http.Redirect(w, r, "/ui", http.StatusSeeOther)
}

func (h *Handler) Logout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     bearerCookieName,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.Production,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
	http.SetCookie(w, &http.Cookie{
		Name:     apiKeyCookieName,
		Path:     "/",
		HttpOnly: true,
		Secure:   h.Production,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
	http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
}

func (h *Handler) CookieHeaderBridge(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			if cookie, err := r.Cookie(bearerCookieName); err == nil && strings.TrimSpace(cookie.Value) != "" {
				r.Header.Set("Authorization", "Bearer "+strings.TrimSpace(cookie.Value))
			}
		}
		if h.Auth.APIKeyEnabled && r.Header.Get(h.Auth.APIKeyHeader) == "" {
			if cookie, err := r.Cookie(apiKeyCookieName); err == nil && strings.TrimSpace(cookie.Value) != "" {
				r.Header.Set(h.Auth.APIKeyHeader, strings.TrimSpace(cookie.Value))
			}
		}
		next.ServeHTTP(w, r)
	})
}

func RedirectToLogin(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/ui") {
		http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
		return
	}
	w.WriteHeader(http.StatusUnauthorized)
}

func loginPage(errMsg string) gomponents.Node {
	content := []gomponents.Node{
		html.H1(gomponents.Text("Duck Platform")),
		html.P(gomponents.Text("Sign in with an API token for the read-only UI.")),
		html.Form(
			html.Method("post"),
			html.Action("/ui/login"),
			html.Class("login-form"),
			html.Label(gomponents.Text("Credential type")),
			html.Select(
				html.Name("kind"),
				html.Option(html.Value("bearer"), gomponents.Text("JWT bearer token")),
				html.Option(html.Value("api_key"), gomponents.Text("API key")),
			),
			html.Label(gomponents.Text("Token")),
			html.Textarea(
				html.Name("token"),
				html.Placeholder("Paste token here"),
				html.Required(),
			),
			html.Button(
				html.Type("submit"),
				html.Class("btn btn-primary"),
				gomponents.Text("Sign In"),
			),
		),
	}
	if errMsg != "" {
		content = append([]gomponents.Node{html.P(html.Class("error"), gomponents.Text(fmt.Sprintf("Error: %s", errMsg)))}, content...)
	}

	return html.HTML(
		html.Lang("en"),
		html.Head(
			html.Meta(html.Charset("utf-8")),
			html.Meta(html.Name("viewport"), html.Content("width=device-width, initial-scale=1")),
			html.TitleEl(gomponents.Text("Sign in | Duck UI")),
			html.Link(html.Rel("preconnect"), html.Href("https://fonts.googleapis.com")),
			html.Link(html.Rel("preconnect"), html.Href("https://fonts.gstatic.com"), gomponents.Attr("crossorigin", "")),
			html.Link(html.Rel("stylesheet"), html.Href("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap")),
			html.Link(html.Rel("stylesheet"), html.Href("https://cdn.jsdelivr.net/npm/@primer/css@22.1.0/dist/primer.min.css")),
			html.Link(html.Rel("stylesheet"), html.Href("/ui/static/app.css")),
		),
		html.Body(
			html.Class("login-body"),
			html.Main(html.Class("login-wrap"), gomponents.Group(content)),
		),
	)
}
