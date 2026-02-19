package ui

import (
	"fmt"

	gomponents "maragu.dev/gomponents"
	html "maragu.dev/gomponents/html"
)

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
