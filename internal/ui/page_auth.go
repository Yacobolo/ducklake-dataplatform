package ui

import (
	"fmt"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func loginPage(errMsg string) Node {
	content := []Node{
		H1(Text("Duck Platform")),
		P(Text("Sign in with an API token for the read-only UI.")),
		Form(
			Method("post"),
			Action("/ui/login"),
			Class("login-form"),
			Label(Text("Credential type")),
			Select(
				Name("kind"),
				Option(Value("bearer"), Text("JWT bearer token")),
				Option(Value("api_key"), Text("API key")),
			),
			Label(Text("Token")),
			Textarea(
				Name("token"),
				Placeholder("Paste token here"),
				Required(),
			),
			Button(
				Type("submit"),
				Class("btn btn-primary"),
				Text("Sign In"),
			),
		),
	}
	if errMsg != "" {
		content = append([]Node{P(Class("error"), Text(fmt.Sprintf("Error: %s", errMsg)))}, content...)
	}

	return HTML(
		Lang("en"),
		Head(
			Meta(Charset("utf-8")),
			Meta(Name("viewport"), Content("width=device-width, initial-scale=1")),
			TitleEl(Text("Sign in | Duck UI")),
			Link(Rel("preconnect"), Href("https://fonts.googleapis.com")),
			Link(Rel("preconnect"), Href("https://fonts.gstatic.com"), Attr("crossorigin", "")),
			Link(Rel("stylesheet"), Href("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap")),
			Link(Rel("stylesheet"), Href("https://cdn.jsdelivr.net/npm/@primer/css@22.1.0/dist/primer.min.css")),
			Link(Rel("stylesheet"), Href("/ui/static/app.css")),
		),
		Body(
			Class("login-body"),
			Main(Class("login-wrap"), Group(content)),
		),
	)
}
