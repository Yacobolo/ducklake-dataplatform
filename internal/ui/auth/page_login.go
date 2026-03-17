package auth

import (
	"fmt"

	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func loginPage(errMsg string, showOIDC bool) Node {
	content := []Node{
		H1(Class("m-0 text-3xl font-semibold tracking-tight"), Text("Duck Platform")),
		P(Class("m-0 text-sm text-muted"), Text("Sign in with local credentials or OIDC.")),
	}

	if showOIDC {
		content = append(content,
			core.PrimaryLink("/ui/login/oidc", "", Text("Continue with OIDC")),
			P(Class("m-0 text-sm text-muted"), Text("or sign in with local credentials")),
		)
	}

	content = append(content,
		Form(
			Method("post"),
			Action("/ui/login"),
			Class("grid gap-3"),
			Label(Class("mb-0 text-xs font-semibold text-muted"), Text("Username")),
			Input(Type("text"), Name("username"), Placeholder("admin"), Class("w-full rounded-xl border border-border bg-background px-3 py-2 text-sm text-foreground"), Required()),
			Label(Class("mb-0 text-xs font-semibold text-muted"), Text("Password")),
			Input(Type("password"), Name("password"), Placeholder("••••••••••••"), Class("w-full rounded-xl border border-border bg-background px-3 py-2 text-sm text-foreground"), Required()),
			core.PrimaryButton("", Type("submit"), Text("Sign In")),
		),
	)
	if errMsg != "" {
		content = append([]Node{P(Class("m-0 rounded-lg border border-border-danger bg-danger-muted px-3 py-2 text-sm text-danger-text"), Text(fmt.Sprintf("Error: %s", errMsg)))}, content...)
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
			Script(Raw(core.ThemeInitScript)),
			Link(Rel("preconnect"), Href("https://fonts.googleapis.com")),
			Link(Rel("preconnect"), Href("https://fonts.gstatic.com"), Attr("crossorigin", "")),
			Link(Rel("stylesheet"), Href("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap")),
			Link(Rel("stylesheet"), Href(core.UIStylesheetHref())),
		),
		Body(
			Class("grid min-h-screen place-items-center bg-background px-5 py-8 text-foreground"),
			Main(Class("grid w-full max-w-[42rem] gap-4 rounded-2xl border border-border bg-background p-6 shadow-md"), Group(content)),
			Script(Raw(core.ThemeBehaviorScript)),
		),
	)
}
