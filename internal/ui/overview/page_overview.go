package overview

import (
	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type overviewLinkData struct {
	Label string
	Copy  string
	Href  string
}

type overviewSectionData struct {
	Kicker      string
	Title       string
	Description string
	StartHref   string
	StartLabel  string
	StartCopy   string
	Links       []overviewLinkData
}

func overviewPage(principal domain.ContextPrincipal, sections []overviewSectionData) Node {
	nodes := make([]Node, 0, len(sections))
	for i := range sections {
		section := sections[i]
		linkNodes := make([]Node, 0, len(section.Links))
		for j := range section.Links {
			link := section.Links[j]
			linkNodes = append(linkNodes, A(
				Href(link.Href),
				Class("grid gap-1 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] px-4 py-3 text-inherit no-underline transition-colors hover:border-[var(--borderColor-accent-emphasis)] hover:bg-[var(--bgColor-default)]"),
				Span(Class("text-sm font-semibold text-[var(--fgColor-default)]"), Text(link.Label)),
				P(Class("m-0 text-sm leading-6 text-[var(--fgColor-muted)]"), Text(link.Copy)),
			))
		}

		nodes = append(nodes, core.SectionSurface(
			core.SectionHeader(section.Title, section.Description),
			Div(Class("grid gap-4 xl:grid-cols-[minmax(0,1.1fr)_minmax(0,1.4fr)]"),
				Div(Class("grid gap-3 rounded-2xl border border-[var(--borderColor-default)] bg-[linear-gradient(135deg,var(--bgColor-muted)_0%,var(--bgColor-default)_65%)] p-5"),
					core.Kicker(section.Kicker),
					H2(Class("m-0 text-2xl font-semibold leading-tight text-[var(--fgColor-default)]"), Text(section.StartLabel)),
					P(Class("m-0 text-sm leading-6 text-[var(--fgColor-muted)]"), Text(section.StartCopy)),
					Div(Class("flex flex-wrap items-center gap-3"),
						core.PrimaryLink(section.StartHref, "", Text("Start here")),
					),
				),
				Div(Class("grid gap-3"), Group(linkNodes)),
			),
		))
	}

	return core.AppPage("Overview", "home", principal,
		core.PageHeader(
			"Workspace orientation",
			"Move through the platform by goal, not by internal subsystem.",
			"Discover published interfaces, build trusted data products, and operate the platform from a smaller set of clearer workspaces.",
		),
		Div(Class("grid gap-4"), Group(nodes)),
		Section(
			Class("grid gap-3 border-t border-[var(--borderColor-default)] pt-4"),
			H2(Class("m-0 text-lg font-semibold"), Text("Internal tools")),
			P(Class("m-0 text-sm leading-6 text-[var(--fgColor-muted)]"), Text("Design-system and implementation surfaces stay reachable, but they no longer compete with the main product navigation.")),
			Div(Class("flex flex-wrap items-center gap-3"),
				core.SecondaryLink("/ui/components", "", Text("Component library")),
				core.SecondaryLink("/ui/macros", "", Text("Macro workspace")),
			),
		),
	)
}
