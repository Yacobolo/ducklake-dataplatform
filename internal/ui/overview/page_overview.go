package overview

import (
	"fmt"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type overviewPageData struct {
	Principal domain.ContextPrincipal
	Recent    []domain.ResourceAccessEvent
	Saved     []domain.SavedResource
	CSRFField func() Node
}

func overviewPage(data overviewPageData) Node {
	return core.AppPage("Home", "home", data.Principal,
		Div(
			Class("grid gap-20"),
			overviewHero(data.Principal, len(data.Recent), len(data.Saved)),
			overviewTables(data.Recent, data.Saved, data.CSRFField),
		),
		StyleEl(Text(`
.recent-save-button:hover,
.recent-save-button:focus-visible {
  color: var(--fgColor-accent);
}

.recent-save-button:disabled {
  color: var(--fgColor-muted);
  opacity: 0.55;
  cursor: not-allowed;
}
`)),
		Script(Src(core.UIScriptHref("home-hero.js"))),
	)
}

func overviewHero(principal domain.ContextPrincipal, recentCount int, savedCount int) Node {
	name := principal.Name
	if name == "" {
		name = "there"
	}

	return Div(
		Class("relative"),
		El("duck-home-hero",
			Attr("display-name", name),
		),
	)
}

func overviewTables(recent []domain.ResourceAccessEvent, saved []domain.SavedResource, csrfField func() Node) Node {
	savedByIdentity := make(map[string]struct{}, len(saved))
	for i := range saved {
		savedByIdentity[resourceIdentity(saved[i].ResourceType, saved[i].ResourceKey)] = struct{}{}
	}

	return Div(
		Class("grid gap-8 pt-6 xl:grid-cols-2"),
		overviewRecentTable(recent, savedByIdentity, csrfField),
		overviewSavedTable(saved, csrfField),
	)
}

func overviewRecentTable(items []domain.ResourceAccessEvent, savedByIdentity map[string]struct{}, csrfField func() Node) Node {
	rows := make([]Node, 0, len(items))
	for i := range items {
		_, isSaved := savedByIdentity[resourceIdentity(items[i].ResourceType, items[i].ResourceKey)]
		rows = append(rows, Tr(
			Td(overviewResourceCell(items[i].ResourceRef)),
			Td(overviewPathCell(items[i].ResourcePath)),
			Td(Span(Class("text-sm font-medium text-[var(--fgColor-default)]"), Text(core.ResourceKindLabel(items[i].ResourceType)))),
			Td(Span(Class("text-sm text-[var(--fgColor-muted)]"), Text(formatRelativeTime(items[i].AccessedAt)))),
			Td(Class("text-right"), overviewRecentSaveForm(items[i].ResourceRef, isSaved, csrfField)),
		))
	}

	return overviewTable(
		"Recent resources",
		"clock-3",
		len(items),
		"",
		[]string{"Resource", "Path", "Type", "Visited", ""},
		rows,
		"No recent resources yet.",
	)
}

func overviewSavedTable(items []domain.SavedResource, csrfField func() Node) Node {
	rows := make([]Node, 0, len(items))
	for i := range items {
		when := formatRelativeTime(items[i].SavedAt)

		rows = append(rows, Tr(
			Td(overviewResourceCell(items[i].ResourceRef)),
			Td(overviewPathCell(items[i].ResourcePath)),
			Td(Span(Class("text-sm font-medium text-[var(--fgColor-default)]"), Text(core.ResourceKindLabel(items[i].ResourceType)))),
			Td(Span(Class("text-sm text-[var(--fgColor-muted)]"), Text(when))),
			Td(Class("text-right"), overviewSavedRemoveForm(items[i].ResourceRef, csrfField)),
		))
	}

	return overviewTable(
		"Saved resources",
		"bookmark",
		len(items),
		"",
		[]string{"Resource", "Path", "Type", "Saved", ""},
		rows,
		"No saved resources yet.",
	)
}

func overviewTable(title string, icon string, count int, description string, headers []string, rows []Node, empty string) Node {
	headerNodes := make([]Node, 0, len(headers))
	for _, header := range headers {
		if header == "" {
			headerNodes = append(headerNodes, Th(Scope("col"), Class("relative"), Span(Class("sr-only"), Text("Actions"))))
			continue
		}
		headerNodes = append(headerNodes, Th(
			Scope("col"),
			Text(header),
		))
	}

	body := Node(nil)
	if len(rows) == 0 {
		body = TBody(
			Tr(
				Td(
					Class("text-sm text-[var(--fgColor-muted)]"),
					ColSpan(fmt.Sprintf("%d", len(headers))),
					Text(empty),
				),
			),
		)
	} else {
		body = TBody(Group(rows))
	}

	return Div(
		Class("grid gap-4"),
		Div(Class("flex items-center justify-between px-1"),
			Div(Class("flex min-w-0 items-center gap-2"),
				Span(
					Class("inline-flex shrink-0 items-center justify-center text-[var(--fgColor-muted)]"),
					core.Icon(icon, Class("h-[18px] w-[18px]")),
				),
				H2(Class("m-0 text-base font-bold tracking-[-0.03em] text-[var(--fgColor-default)]"), Text(title)),
				Span(
					Class("inline-flex shrink-0 items-center rounded-full bg-[var(--bgColor-muted)] px-2 py-0.5 text-xs font-medium text-[var(--fgColor-muted)]"),
					Text(fmt.Sprintf("%d", count)),
				),
			),
			func() Node {
				if description == "" {
					return nil
				}
				return P(Class("m-0 max-w-[34rem] text-sm leading-6 text-[var(--fgColor-muted)]"), Text(description))
			}(),
		),
		core.TableContainer("",
			core.DataTable("min-w-[40rem]",
				THead(Tr(Group(headerNodes))),
				body,
			),
		),
	)
}

func overviewResourceCell(item domain.ResourceRef) Node {
	linkClass := "font-semibold text-[var(--fgColor-accent)] no-underline visited:text-[var(--fgColor-accent)] hover:text-[var(--fgColor-accent)] hover:underline active:text-[var(--fgColor-accent)]"
	return Div(
		Class("flex items-center gap-3"),
		core.ResourceIcon(item.ResourceType),
		Div(Class("min-w-0 flex-1"),
			A(
				Href(item.Href),
				Class(linkClass),
				Text(item.DisplayName),
			),
		),
	)
}

func overviewPathCell(resourcePath string) Node {
	if resourcePath == "" {
		return Span(Class("text-sm text-[var(--fgColor-muted)]"), Text("—"))
	}
	return Span(
		Class("block max-w-[14rem] truncate text-sm text-[var(--fgColor-muted)]"),
		Title(resourcePath),
		Text(resourcePath),
	)
}

func overviewRecentSaveForm(item domain.ResourceRef, isSaved bool, csrfField func() Node) Node {
	fields := []Node{
		Input(Type("hidden"), Name("resource_type"), Value(item.ResourceType)),
		Input(Type("hidden"), Name("resource_key"), Value(item.ResourceKey)),
		Input(Type("hidden"), Name("display_name"), Value(item.DisplayName)),
		Input(Type("hidden"), Name("resource_path"), Value(item.ResourcePath)),
		Input(Type("hidden"), Name("section"), Value(item.Section)),
		Input(Type("hidden"), Name("return_to"), Value("/ui")),
	}
	buttonAttrs := []Node{
		Type("submit"),
		Class("recent-save-button ui-table-action inline-flex h-8 w-8 items-center justify-center rounded-md p-1 transition-colors duration-150 ease-out focus-visible:outline-2 focus-visible:outline-offset-0 focus-visible:outline-[var(--borderColor-accent-emphasis)]"),
		Title("Save resource"),
		Attr("aria-label", "Save resource"),
	}
	if isSaved {
		buttonAttrs = append(buttonAttrs,
			Disabled(),
			Title("Resource already saved"),
			Attr("aria-label", "Resource already saved"),
		)
	}
	button := Button(
		append(buttonAttrs,
			core.Icon("bookmark", Class("h-[18px] w-[18px]"), Attr("style", "stroke-width:2")),
			Span(Class("sr-only"), Text("Save resource")),
		)...,
	)
	if csrfField != nil {
		fields = append(fields, csrfField())
	}
	fields = append(fields, button)
	return Form(Method("post"), Action("/ui/resources/save"), Group(fields))
}

func overviewSavedRemoveForm(item domain.ResourceRef, csrfField func() Node) Node {
	fields := []Node{
		Input(Type("hidden"), Name("resource_type"), Value(item.ResourceType)),
		Input(Type("hidden"), Name("resource_key"), Value(item.ResourceKey)),
		Input(Type("hidden"), Name("return_to"), Value("/ui")),
	}
	if csrfField != nil {
		fields = append(fields, csrfField())
	}
	fields = append(fields,
		Button(
			Type("submit"),
			Class("ui-table-action ui-table-action--danger inline-flex h-8 w-8 items-center justify-center rounded-md p-1 transition-colors duration-150 ease-out focus-visible:outline-2 focus-visible:outline-offset-0 focus-visible:outline-[var(--borderColor-danger-emphasis)]"),
			Title("Remove saved resource"),
			Attr("aria-label", "Remove saved resource"),
			core.Icon("x", Class("h-4 w-4"), Attr("style", "stroke-width:2.25")),
			Span(Class("sr-only"), Text("Remove saved resource")),
		),
	)
	return Form(Method("post"), Action("/ui/resources/unsave"), Group(fields))
}

func resourceIdentity(resourceType string, resourceKey string) string {
	return resourceType + "::" + resourceKey
}

func formatRelativeTime(ts time.Time) string {
	diff := time.Since(ts)
	switch {
	case diff < time.Minute:
		return "just now"
	case diff < time.Hour:
		return fmt.Sprintf("%dm ago", int(diff.Minutes()))
	case diff < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(diff.Hours()))
	case diff < 7*24*time.Hour:
		return fmt.Sprintf("%dd ago", int(diff.Hours()/24))
	default:
		return ts.Format("2 Jan 2006")
	}
}
