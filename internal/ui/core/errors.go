package core

import (
	"errors"
	"net/http"

	"github.com/Yacobolo/quackstack/internal/domain"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func ErrorPage(title, message string) Node {
	return HTML(
		Lang("en"),
		Attr("data-color-mode", "auto"),
		Attr("data-light-theme", "light"),
		Attr("data-dark-theme", "dark"),
		Head(
			Meta(Charset("utf-8")),
			Meta(Name("viewport"), Content("width=device-width, initial-scale=1")),
			TitleEl(Text(title+" | Duck UI")),
			Link(Rel("icon"), Href("data:,")),
			Script(Raw(ThemeInitScript)),
			Link(Rel("stylesheet"), Href(UIStylesheetHref())),
		),
		Body(
			Main(
				Class("mx-auto flex min-h-screen max-w-[62rem] flex-col gap-4 px-6 py-8 text-[var(--fgColor-default)]"),
				H1(Class("m-0 text-2xl font-semibold leading-tight"), Text(title)),
				P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(message)),
				P(Class("m-0"), TextLink("/ui", Text("Back to overview"))),
			),
			Script(Raw(ThemeBehaviorScript)),
		),
	)
}

func ServiceErrorStatus(err error) (int, string) {
	status := http.StatusInternalServerError
	message := "An unexpected error occurred while loading this page."

	var notFound *domain.NotFoundError
	var accessDenied *domain.AccessDeniedError
	var validation *domain.ValidationError
	var conflict *domain.ConflictError
	if errors.As(err, &notFound) {
		return http.StatusNotFound, notFound.Error()
	}
	if errors.As(err, &accessDenied) {
		return http.StatusForbidden, accessDenied.Error()
	}
	if errors.As(err, &validation) {
		return http.StatusBadRequest, validation.Error()
	}
	if errors.As(err, &conflict) {
		return http.StatusConflict, conflict.Error()
	}
	return status, message
}
