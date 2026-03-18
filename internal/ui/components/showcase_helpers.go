package components

import (
	"fmt"
	"strconv"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type segmentedTabItem struct {
	Label  string
	Active bool
}

type formFieldConfig struct {
	Label        string
	Name         string
	Type         string
	Value        string
	Placeholder  string
	Required     bool
	HelpText     string
	ErrorMessage string
	Invalid      bool
}

type treeViewItem struct {
	Label    string
	Icon     string
	Href     string
	Open     bool
	Active   bool
	Children []treeViewItem
}

type breadcrumbItem struct {
	Label  string
	Href   string
	Active bool
}

type avatarConfig struct {
	Label string
	Tone  string
	Size  string
}

func breadcrumbs(items []breadcrumbItem) Node {
	if len(items) == 0 {
		return nil
	}

	nodes := make([]Node, 0, len(items))
	for i := range items {
		item := items[i]
		linkClass := "rounded-md px-2 py-1 text-xs text-[var(--fgColor-accent)] no-underline hover:bg-[var(--bgColor-muted)]"
		ariaCurrent := Node(nil)
		if item.Active {
			linkClass += " font-semibold text-[var(--fgColor-default)]"
			ariaCurrent = Attr("aria-current", "page")
		}
		nodes = append(nodes,
			Li(
				Class("inline-flex items-center gap-1"),
				A(Href(core.FallbackString(item.Href, "#")), Class(linkClass), ariaCurrent, Text(item.Label)),
			),
		)
	}

	return Nav(
		Attr("aria-label", "Breadcrumb"),
		Ol(Class("flex flex-wrap items-center gap-1 text-xs text-[var(--fgColor-muted)]"), Group(nodes)),
	)
}

func avatar(cfg avatarConfig) Node {
	initials := avatarInitials(cfg.Label)
	size := strings.TrimSpace(cfg.Size)
	if size == "" {
		size = "medium"
	}

	tone := strings.TrimSpace(cfg.Tone)
	if tone == "" {
		tone = "neutral"
	}

	return Span(
		Class(core.ClassNames("inline-flex items-center justify-center rounded-full font-semibold uppercase tracking-wide", avatarSizeClass(size), avatarToneClass(tone))),
		Attr("aria-label", cfg.Label),
		Text(initials),
	)
}

func avatarSizeClass(size string) string {
	switch size {
	case "small":
		return "h-8 w-8 text-xs"
	case "large":
		return "h-12 w-12 text-base"
	default:
		return "h-10 w-10 text-sm"
	}
}

func avatarToneClass(tone string) string {
	switch tone {
	case "accent":
		return "bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]"
	case "success":
		return "bg-[var(--bgColor-success-muted)] text-[var(--fgColor-success)]"
	case "attention":
		return "bg-[var(--bgColor-attention-muted)] text-[var(--fgColor-attention)]"
	case "danger":
		return "bg-[var(--bgColor-danger-muted)] text-[var(--fgColor-danger)]"
	default:
		return "bg-[var(--bgColor-muted)] text-[var(--fgColor-default)]"
	}
}

func avatarInitials(label string) string {
	parts := strings.Fields(strings.TrimSpace(label))
	if len(parts) == 0 {
		return "?"
	}
	if len(parts) == 1 {
		r := []rune(parts[0])
		if len(r) == 0 {
			return "?"
		}
		return strings.ToUpper(string(r[0]))
	}
	r1 := []rune(parts[0])
	r2 := []rune(parts[1])
	if len(r1) == 0 || len(r2) == 0 {
		return "?"
	}
	return strings.ToUpper(string(r1[0]) + string(r2[0]))
}

func formField(cfg formFieldConfig) Node {
	inputType := strings.TrimSpace(cfg.Type)
	if inputType == "" {
		inputType = "text"
	}

	inputClass := "w-full rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 py-2 text-sm text-[var(--fgColor-default)] shadow-xs transition-colors placeholder:text-[var(--fgColor-muted)] focus:border-[var(--borderColor-accent-emphasis)] focus:outline-none focus:ring-2 focus:ring-[var(--focus-outlineColor)]"
	if cfg.Invalid {
		inputClass += " border-[var(--borderColor-danger-muted)]"
	}

	labelNodes := []Node{Text(cfg.Label)}
	if cfg.Required {
		labelNodes = append(labelNodes, Span(Class("ml-1 text-[var(--fgColor-danger)]"), Text("*")))
	}

	inputNodes := []Node{
		Type(inputType),
		Class(inputClass),
		Name(cfg.Name),
		Value(cfg.Value),
	}
	if strings.TrimSpace(cfg.Placeholder) != "" {
		inputNodes = append(inputNodes, Placeholder(cfg.Placeholder))
	}
	if cfg.Required {
		inputNodes = append(inputNodes, Required())
	}
	if cfg.Invalid {
		inputNodes = append(inputNodes, Attr("aria-invalid", "true"))
	}

	helpNode := Node(nil)
	if strings.TrimSpace(cfg.HelpText) != "" {
		helpNode = P(Class("m-0 text-xs leading-4 text-[var(--fgColor-muted)]"), Text(cfg.HelpText))
	}

	errorNode := Node(nil)
	if strings.TrimSpace(cfg.ErrorMessage) != "" {
		errorNode = P(Class("m-0 text-xs leading-4 text-[var(--fgColor-danger)]"), Text(cfg.ErrorMessage))
	}

	return Div(
		Class("flex flex-col gap-1"),
		Label(Class("text-xs font-semibold text-[var(--fgColor-muted)]"), Group(labelNodes)),
		Input(inputNodes...),
		helpNode,
		errorNode,
	)
}

func actionBar() Node {
	return Div(
		Class("grid gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs lg:grid-cols-[minmax(0,1fr)_minmax(16rem,20rem)_minmax(12rem,14rem)_auto] lg:items-end"),
		data.Signals(map[string]any{"q": "", "sort": "updated"}),
		Div(
			Class("flex flex-col gap-1"),
			core.Badge("Component Catalog", ""),
			P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text("Filter and sort reusable building blocks before composing screens.")),
		),
		Div(
			Class("flex flex-col gap-1"),
			Label(For("component-search"), Class("text-xs font-semibold text-[var(--fgColor-muted)]"), Text("Search")),
			core.InputControl("", ID("component-search"), Type("search"), Placeholder("Search components"), AutoComplete("off"), data.Bind("q")),
		),
		Div(
			Class("flex flex-col gap-1"),
			Label(For("component-sort"), Class("text-xs font-semibold text-[var(--fgColor-muted)]"), Text("Sort")),
			core.SelectControl("", ID("component-sort"), data.Bind("sort"), Option(Value("updated"), Text("Recently updated")), Option(Value("name"), Text("Name")), Option(Value("category"), Text("Category"))),
		),
		Div(
			Class("flex flex-wrap items-center gap-2"),
			core.SecondaryButton("", Type("button"), Text("Reset")),
			core.PrimaryButton("", Type("button"), Text("Create component")),
		),
	)
}

func quickFilterCardWithValue(placeholder, initialValue string, extraControls ...Node) Node {
	controls := []Node{
		Div(
			Class("flex min-w-[min(20rem,100%)] flex-1 flex-col gap-1"),
			Label(Class("sr-only"), Text("Quick filter")),
			core.InputControl("", Type("search"), Name("q"), Placeholder(placeholder), data.Bind("q"), AutoComplete("off"), Attr("data-quick-filter-input", "true")),
		),
	}
	controls = append(controls, extraControls...)
	syncScript := `(function(){
  var input=document.querySelector('[data-quick-filter-input="true"]');
  if(!(input instanceof HTMLInputElement)){ return; }

  function syncURL(value){
    var url=new URL(window.location.href);
    if(value){
      url.searchParams.set('q', value);
    } else {
      url.searchParams.delete('q');
    }
    url.searchParams.delete('page_token');
    var next=url.pathname;
    var query=url.searchParams.toString();
    if(query){ next+='?'+query; }
    if(next!==window.location.pathname+window.location.search){
      window.history.replaceState({}, '', next);
    }
  }

  input.addEventListener('input', function(){
    syncURL(input.value.trim());
  });
})();`

	return Div(
		Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
		data.Signals(map[string]any{"q": initialValue}),
		Div(Class("flex flex-wrap items-center gap-3"), Group(controls)),
		Script(Raw(syncScript)),
	)
}

func pageToolbar(newHref, newLabel string) Node {
	return Div(
		Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
		Div(
			Class("flex flex-wrap items-center justify-between gap-3"),
			Div(
				Class("flex min-w-0 flex-col gap-1"),
				core.Badge("Workspace", ""),
				P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text("Browse and manage resources.")),
			),
			core.PrimaryLink(newHref, "", Text(newLabel)),
		),
	)
}

func emptyStateCard(message, ctaLabel, ctaHref string) Node {
	cta := Node(nil)
	if ctaLabel != "" && ctaHref != "" {
		cta = core.PrimaryLink(ctaHref, "", Text(ctaLabel))
	}
	return Div(
		Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 text-center shadow-xs"),
		Div(Class("mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-[var(--bgColor-muted)] text-[var(--fgColor-accent)]"), I(Class(core.NavIconClass()), Attr("data-lucide", "inbox"), Attr("aria-hidden", "true"))),
		Div(
			Class("flex flex-col items-center gap-2 text-center"),
			P(Class("m-0 text-lg font-semibold"), Text("No results yet")),
			P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(message)),
			cta,
		),
	)
}

func paginationCard(basePath string, page domain.PageRequest, total int64) Node {
	shown := min(page.Limit(), int(total))
	summary := fmt.Sprintf("Showing %d of %d entries.", shown, total)
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	if nextToken == "" {
		return Div(
			Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
			Div(
				Class("flex items-center justify-between gap-3 max-sm:flex-col max-sm:items-start"),
				Div(Class("flex min-w-0 flex-col gap-1"), P(Class("m-0 text-sm font-semibold text-[var(--fgColor-default)]"), Text("Pagination")), P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(summary))),
				Span(Class("inline-flex min-h-8 items-center justify-center rounded-lg border border-[var(--borderColor-default)] px-3 text-sm font-medium text-[var(--fgColor-default)] opacity-60 pointer-events-none"), Attr("aria-disabled", "true"), Text("Next")),
			),
		)
	}
	url := fmt.Sprintf("%s?max_results=%d&page_token=%s", basePath, page.Limit(), nextToken)
	return Div(
		Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
		Div(
			Class("flex items-center justify-between gap-3 max-sm:flex-col max-sm:items-start"),
			Div(Class("flex min-w-0 flex-col gap-1"), P(Class("m-0 text-sm font-semibold text-[var(--fgColor-default)]"), Text("Pagination")), P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(summary))),
			core.SecondaryLink(url, "small", Text("Next page")),
		),
	)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func actionMenuLink(href, label string) Node {
	icon := actionIconForLabel(label)
	return A(Href(href), Class(core.DropdownItemClass("text-[var(--fgColor-default)]")), I(Class(core.NavIconClass()), Attr("data-lucide", icon), Attr("aria-hidden", "true")), Span(Text(label)))
}

func actionMenuPost(action, label string, csrfField func() Node, danger bool) Node {
	btnClass := core.DropdownItemClass()
	if danger {
		btnClass += " text-[var(--fgColor-danger)] hover:bg-[var(--bgColor-danger-muted)]"
	} else {
		btnClass += " text-[var(--fgColor-default)]"
	}
	icon := actionIconForLabel(label)
	button := Form(
		Method("post"),
		Action(action),
		csrfField(),
		Button(Type("submit"), Class(btnClass), I(Class(core.NavIconClass()), Attr("data-lucide", icon), Attr("aria-hidden", "true")), Span(Text(label))),
	)
	if danger {
		return Group([]Node{Div(Class("dropdown-divider my-1 border-t border-[var(--borderColor-muted)]")), button})
	}
	return button
}

func actionIconForLabel(label string) string {
	lower := strings.ToLower(strings.TrimSpace(label))
	switch {
	case strings.Contains(lower, "open"):
		return "arrow-up-right"
	case strings.Contains(lower, "edit"):
		return "pencil"
	case strings.Contains(lower, "delete"), strings.Contains(lower, "remove"):
		return "trash-2"
	case strings.Contains(lower, "download"):
		return "download"
	default:
		return "circle"
	}
}

func banner(level, title, message string) Node {
	className := "flex items-start gap-3 rounded-xl border border-[var(--borderColor-accent-emphasis)] bg-[var(--bgColor-accent-muted)] px-4 py-3 text-sm text-[var(--fgColor-default)]"
	icon := "info"
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "success":
		className = "flex items-start gap-3 rounded-xl border border-[var(--borderColor-success-emphasis)] bg-[var(--bgColor-success-muted)] px-4 py-3 text-sm text-[var(--fgColor-default)]"
		icon = "check-circle-2"
	case "attention", "warning":
		className = "flex items-start gap-3 rounded-xl border border-[var(--borderColor-attention-emphasis)] bg-[var(--bgColor-attention-muted)] px-4 py-3 text-sm text-[var(--fgColor-default)]"
		icon = "triangle-alert"
	case "danger", "error":
		className = "flex items-start gap-3 rounded-xl border border-[var(--borderColor-danger-muted)] bg-[var(--bgColor-danger-muted)] px-4 py-3 text-sm text-[var(--fgColor-default)]"
		icon = "circle-x"
	}

	return Div(
		Class(className),
		I(Class(core.NavIconClass("mt-0.5")), Attr("data-lucide", icon), Attr("aria-hidden", "true")),
		Div(Class("flex min-w-0 flex-col gap-1"), Strong(Class("font-semibold"), Text(title)), P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(message))),
	)
}

func spinner() Node {
	return Span(Class("inline-block h-4 w-4 animate-spin rounded-full border-2 border-[var(--borderColor-muted)] border-t-accent"), Attr("aria-hidden", "true"))
}

func progressBar(value, max int) Node {
	if max <= 0 {
		max = 100
	}
	if value < 0 {
		value = 0
	}
	if value > max {
		value = max
	}

	return Div(
		Class("h-2 w-full overflow-hidden rounded-full bg-[var(--bgColor-muted)]"),
		Attr("role", "progressbar"),
		Attr("aria-valuemin", "0"),
		Attr("aria-valuemax", strconv.Itoa(max)),
		Attr("aria-valuenow", strconv.Itoa(value)),
		Div(Class("h-full rounded-full bg-[var(--bgColor-accent-emphasis)] transition-[width] duration-200 ease-out"), Style("width: "+strconv.Itoa((value*100)/max)+"%;")),
	)
}

func metricCard(label, value, meta, tone string) Node {
	classes := "relative overflow-hidden rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-sm before:absolute before:inset-y-0 before:left-0 before:w-1 before:bg-[var(--borderColor-accent-emphasis)] before:content-['']"
	trimmedTone := strings.TrimSpace(tone)
	if trimmedTone != "" {
		switch trimmedTone {
		case "success":
			classes += " bg-[linear-gradient(135deg,var(--bgColor-success-muted)_0%,var(--bgColor-default)_45%)] before:bg-[var(--borderColor-success-emphasis)]"
		case "attention":
			classes += " bg-[linear-gradient(135deg,var(--bgColor-attention-muted)_0%,var(--bgColor-default)_45%)] before:bg-[var(--borderColor-attention-emphasis)]"
		default:
			classes += " bg-[linear-gradient(135deg,var(--bgColor-accent-muted)_0%,var(--bgColor-default)_45%)]"
		}
	}

	metaNode := Node(nil)
	if strings.TrimSpace(meta) != "" {
		metaNode = P(Class("text-xs text-[var(--fgColor-muted)]"), Text(meta))
	}

	return Div(
		Class(classes),
		P(Class("m-0 text-xs font-semibold text-[var(--fgColor-default)]"), Text(label)),
		P(Class("my-1 text-3xl font-semibold leading-tight text-[var(--fgColor-default)]"), Text(value)),
		metaNode,
	)
}

func segmentedTabs(items []segmentedTabItem) Node {
	nodes := make([]Node, 0, len(items))
	for i := range items {
		item := items[i]
		className := "inline-flex min-h-9 items-center justify-center rounded-lg px-3 py-2 text-sm font-medium text-[var(--fgColor-muted)] transition-colors hover:text-[var(--fgColor-default)]"
		if item.Active {
			className += " bg-[var(--bgColor-default)] text-[var(--fgColor-default)] shadow-xs"
		}
		nodes = append(nodes, Button(Type("button"), Class(className), Attr("aria-pressed", strconv.FormatBool(item.Active)), Text(item.Label)))
	}

	return Div(Class("inline-flex flex-wrap gap-1 rounded-xl bg-[var(--bgColor-muted)] p-1"), Group(nodes))
}

func treeView(items []treeViewItem) Node {
	if len(items) == 0 {
		return P(Class("text-sm text-[var(--fgColor-muted)]"), Text("No items"))
	}

	nodes := make([]Node, 0, len(items))
	for i := range items {
		nodes = append(nodes, treeViewNode(items[i]))
	}

	return Ul(Class("grid gap-1"), Group(nodes))
}

func treeViewNode(item treeViewItem) Node {
	icon := strings.TrimSpace(item.Icon)
	if icon == "" {
		icon = "circle"
	}

	linkClass := "inline-flex min-h-9 items-center gap-2 rounded-lg px-3 py-2 text-sm text-[var(--fgColor-default)] no-underline hover:bg-[var(--bgColor-muted)]"
	if item.Active {
		linkClass += " bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]"
	}

	link := A(Href(core.FallbackString(item.Href, "#")), Class(linkClass), I(Class(core.NavIconClass()), Attr("data-lucide", icon), Attr("aria-hidden", "true")), Span(Text(item.Label)))
	if len(item.Children) == 0 {
		return Li(link)
	}

	childNodes := make([]Node, 0, len(item.Children))
	for i := range item.Children {
		childNodes = append(childNodes, treeViewNode(item.Children[i]))
	}

	openAttr := Node(nil)
	if item.Open {
		openAttr = Attr("open", "")
	}

	return Li(
		Details(
			Class("group"),
			openAttr,
			Summary(Class(core.DetailsSummaryClass("flex items-center gap-2")), I(Class(core.NavIconClass("transition-transform group-open:rotate-90")), Attr("data-lucide", "chevron-right"), Attr("aria-hidden", "true")), link),
			Ul(Class("ml-6 mt-1 grid gap-1 border-l border-[var(--borderColor-muted)] pl-2"), Group(childNodes)),
		),
	)
}
