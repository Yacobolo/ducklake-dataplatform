package ui

import (
	"fmt"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func loginPage(errMsg string, showOIDC bool) Node {
	content := []Node{
		H1(Text("Duck Platform")),
		P(Text("Sign in with local credentials or OIDC.")),
	}

	if showOIDC {
		content = append(content,
			A(
				Href("/ui/login/oidc"),
				Class("btn btn-primary"),
				Text("Continue with OIDC"),
			),
			P(Text("or sign in with local credentials")),
		)
	}

	content = append(content,
		Form(
			Method("post"),
			Action("/ui/login"),
			Class("login-form"),
			Label(Text("Username")),
			Input(
				Type("text"),
				Name("username"),
				Placeholder("admin"),
				Required(),
			),
			Label(Text("Password")),
			Input(
				Type("password"),
				Name("password"),
				Placeholder("••••••••••••"),
				Required(),
			),
			Button(
				Type("submit"),
				Class("btn btn-primary"),
				Text("Sign In"),
			),
		),
	)
	if errMsg != "" {
		content = append([]Node{P(Class("error"), Text(fmt.Sprintf("Error: %s", errMsg)))}, content...)
	}

	return HTML(
		Lang("en"),
		Attr("data-color-mode", "auto"),
		Attr("data-light-theme", "light"),
		Attr("data-dark-theme", "dark"),
		Head(
			Meta(Charset("utf-8")),
			Meta(Name("viewport"), Content("width=device-width, initial-scale=1")),
			TitleEl(Text("Sign in | Duck UI")),
			Script(Raw(themeInitScript)),
			Link(Rel("preconnect"), Href("https://fonts.googleapis.com")),
			Link(Rel("preconnect"), Href("https://fonts.gstatic.com"), Attr("crossorigin", "")),
			Link(Rel("stylesheet"), Href("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap")),
			Link(Rel("stylesheet"), Href(uiStylesheetHref())),
		),
		Body(
			Class("login-body"),
			Main(Class("login-wrap"), Group(content)),
			Script(Raw(themeBehaviorScript)),
		),
	)
}
