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
		linkClass := "rounded-md px-2 py-1 text-xs text-accent no-underline hover:bg-surface-muted"
		ariaCurrent := Node(nil)
		if item.Active {
			linkClass += " font-semibold text-foreground"
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
		Ol(Class("flex flex-wrap items-center gap-1 text-xs text-muted"), Group(nodes)),
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
		return "bg-accent-muted text-accent"
	case "success":
		return "bg-success-muted text-success-text"
	case "attention":
		return "bg-warning-muted text-warning-text"
	case "danger":
		return "bg-danger-muted text-danger-text"
	default:
		return "bg-surface-muted text-foreground"
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

	inputClass := "w-full rounded-xl border border-border bg-background px-3 py-2 text-sm text-foreground shadow-xs transition-colors placeholder:text-muted focus:border-border-accent focus:outline-none focus:ring-2 focus:ring-[var(--color-ring)]"
	if cfg.Invalid {
		inputClass += " border-border-danger"
	}

	labelNodes := []Node{Text(cfg.Label)}
	if cfg.Required {
		labelNodes = append(labelNodes, Span(Class("ml-1 text-danger-text"), Text("*")))
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
		helpNode = P(Class("m-0 text-xs leading-[var(--text-xs--line-height)] text-muted"), Text(cfg.HelpText))
	}

	errorNode := Node(nil)
	if strings.TrimSpace(cfg.ErrorMessage) != "" {
		errorNode = P(Class("m-0 text-xs leading-[var(--text-xs--line-height)] text-danger-text"), Text(cfg.ErrorMessage))
	}

	return Div(
		Class("flex flex-col gap-1"),
		Label(Class("text-xs font-semibold text-muted"), Group(labelNodes)),
		Input(inputNodes...),
		helpNode,
		errorNode,
	)
}

func checkboxOption(id, name, label string, checked bool) Node {
	checkedNode := Node(nil)
	if checked {
		checkedNode = Checked()
	}

	return Label(
		Class("inline-flex items-center gap-2 text-sm font-medium text-foreground"),
		Input(Type("checkbox"), ID(id), Name(name), Value(label), checkedNode, Class("m-0 inline-grid h-5 w-5 shrink-0 appearance-none place-content-center rounded-md border border-border-muted bg-background transition-colors after:h-2.5 after:w-1.5 after:origin-center after:rotate-45 after:scale-0 after:border-b-[3px] after:border-r-[3px] after:border-b-primary-foreground after:border-r-primary-foreground after:content-[''] checked:border-[var(--control-checked-borderColor-rest)] checked:bg-[var(--control-checked-bgColor-rest)] checked:after:scale-100 hover:border-[var(--control-borderColor-emphasis)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--color-ring)] focus-visible:outline-offset-0 disabled:cursor-not-allowed disabled:border-[var(--control-borderColor-disabled)] disabled:bg-[var(--control-bgColor-disabled)]")),
		Span(Text(label)),
	)
}

func radioOption(id, name, label string, checked bool) Node {
	checkedNode := Node(nil)
	if checked {
		checkedNode = Checked()
	}

	return Label(
		Class("inline-flex items-center gap-2 text-sm font-medium text-foreground"),
		Input(Type("radio"), ID(id), Name(name), Value(label), checkedNode, Class("m-0 inline-grid h-4 w-4 shrink-0 appearance-none place-content-center rounded-full border border-border-muted bg-background transition-colors after:h-2 after:w-2 after:scale-0 after:rounded-full after:bg-primary-foreground after:transition-transform after:content-[''] checked:border-[var(--control-checked-borderColor-rest)] checked:bg-background checked:after:scale-100 checked:after:bg-[var(--control-checked-bgColor-rest)] hover:border-[var(--control-borderColor-emphasis)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-[var(--color-ring)] focus-visible:outline-offset-0 disabled:cursor-not-allowed disabled:border-[var(--control-borderColor-disabled)] disabled:bg-[var(--control-bgColor-disabled)]")),
		Span(Text(label)),
	)
}

func toggleSwitch(id, name, label string, checked bool) Node {
	checkedNode := Node(nil)
	stateLabel := "Off"
	if checked {
		checkedNode = Checked()
		stateLabel = "On"
	}

	return Label(
		Class("inline-grid grid-cols-[1fr_auto_auto] items-center gap-3 text-sm font-medium text-foreground"),
		Span(Class("text-foreground"), Text(label)),
		Span(Class("min-w-8 text-right text-xs leading-[var(--text-xs--line-height)] text-muted"), Text(stateLabel)),
		Input(Type("checkbox"), ID(id), Name(name), checkedNode, Class("peer sr-only")),
		Span(Class("relative inline-flex h-5 w-10 items-center justify-start rounded-full border border-[var(--controlTrack-borderColor-rest)] bg-[var(--controlTrack-bgColor-rest)] p-0.5 transition-colors peer-checked:border-[var(--control-checked-borderColor-rest)] peer-checked:bg-[var(--control-checked-bgColor-rest)] peer-focus-visible:outline peer-focus-visible:outline-2 peer-focus-visible:outline-[var(--color-ring)] peer-focus-visible:outline-offset-0"),
			Span(Class("h-4 w-4 rounded-full bg-[var(--controlKnob-bgColor-rest)] shadow-xs transition-transform peer-checked:translate-x-5 peer-checked:bg-background")),
		),
	)
}

func actionBar() Node {
	return Div(
		Class("grid gap-3 rounded-xl border border-border bg-background p-4 shadow-xs lg:grid-cols-[minmax(0,1fr)_minmax(16rem,20rem)_minmax(12rem,14rem)_auto] lg:items-end"),
		data.Signals(map[string]any{"q": "", "sort": "updated"}),
		Div(
			Class("flex flex-col gap-1"),
			core.Badge("Component Catalog", ""),
			P(Class("m-0 text-xs text-muted"), Text("Filter and sort reusable building blocks before composing screens.")),
		),
		Div(
			Class("flex flex-col gap-1"),
			Label(For("component-search"), Class("text-xs font-semibold text-muted"), Text("Search")),
			core.InputControl("", ID("component-search"), Type("search"), Placeholder("Search components"), AutoComplete("off"), data.Bind("q")),
		),
		Div(
			Class("flex flex-col gap-1"),
			Label(For("component-sort"), Class("text-xs font-semibold text-muted"), Text("Sort")),
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
		Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
		data.Signals(map[string]any{"q": initialValue}),
		Div(Class("flex flex-wrap items-center gap-3"), Group(controls)),
		Script(Raw(syncScript)),
	)
}

func pageToolbar(newHref, newLabel string) Node {
	return Div(
		Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
		Div(
			Class("flex flex-wrap items-center justify-between gap-3"),
			Div(
				Class("flex min-w-0 flex-col gap-1"),
				core.Badge("Workspace", ""),
				P(Class("m-0 text-xs text-muted"), Text("Browse and manage resources.")),
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
		Class("rounded-xl border border-border bg-background p-4 text-center shadow-xs"),
		Div(Class("mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-surface-muted text-accent"), I(Class(core.NavIconClass()), Attr("data-lucide", "inbox"), Attr("aria-hidden", "true"))),
		Div(
			Class("flex flex-col items-center gap-2 text-center"),
			P(Class("m-0 text-lg font-semibold"), Text("No results yet")),
			P(Class("m-0 text-sm text-muted"), Text(message)),
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
			Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
			Div(
				Class("flex items-center justify-between gap-3 max-sm:flex-col max-sm:items-start"),
				Div(Class("flex min-w-0 flex-col gap-1"), P(Class("m-0 text-sm font-semibold text-foreground"), Text("Pagination")), P(Class("m-0 text-xs text-muted"), Text(summary))),
				Span(Class("inline-flex min-h-8 items-center justify-center rounded-lg border border-border px-3 text-sm font-medium text-foreground opacity-60 pointer-events-none"), Attr("aria-disabled", "true"), Text("Next")),
			),
		)
	}
	url := fmt.Sprintf("%s?max_results=%d&page_token=%s", basePath, page.Limit(), nextToken)
	return Div(
		Class("rounded-xl border border-border bg-background p-4 shadow-xs"),
		Div(
			Class("flex items-center justify-between gap-3 max-sm:flex-col max-sm:items-start"),
			Div(Class("flex min-w-0 flex-col gap-1"), P(Class("m-0 text-sm font-semibold text-foreground"), Text("Pagination")), P(Class("m-0 text-xs text-muted"), Text(summary))),
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
	return A(Href(href), Class(core.DropdownItemClass("text-foreground")), I(Class(core.NavIconClass()), Attr("data-lucide", icon), Attr("aria-hidden", "true")), Span(Text(label)))
}

func actionMenuPost(action, label string, csrfField func() Node, danger bool) Node {
	btnClass := core.DropdownItemClass()
	if danger {
		btnClass += " text-danger-text hover:bg-danger-muted"
	} else {
		btnClass += " text-foreground"
	}
	icon := actionIconForLabel(label)
	button := Form(
		Method("post"),
		Action(action),
		csrfField(),
		Button(Type("submit"), Class(btnClass), I(Class(core.NavIconClass()), Attr("data-lucide", icon), Attr("aria-hidden", "true")), Span(Text(label))),
	)
	if danger {
		return Group([]Node{Div(Class("dropdown-divider my-1 border-t border-border-muted")), button})
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
	className := "flex items-start gap-3 rounded-xl border border-border-accent bg-accent-muted px-4 py-3 text-sm text-foreground"
	icon := "info"
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "success":
		className = "flex items-start gap-3 rounded-xl border border-border-success bg-success-muted px-4 py-3 text-sm text-foreground"
		icon = "check-circle-2"
	case "attention", "warning":
		className = "flex items-start gap-3 rounded-xl border border-[color:var(--color-warning)] bg-warning-muted px-4 py-3 text-sm text-foreground"
		icon = "triangle-alert"
	case "danger", "error":
		className = "flex items-start gap-3 rounded-xl border border-border-danger bg-danger-muted px-4 py-3 text-sm text-foreground"
		icon = "circle-x"
	}

	return Div(
		Class(className),
		I(Class(core.NavIconClass("mt-0.5")), Attr("data-lucide", icon), Attr("aria-hidden", "true")),
		Div(Class("flex min-w-0 flex-col gap-1"), Strong(Class("font-semibold"), Text(title)), P(Class("m-0 text-sm text-muted"), Text(message))),
	)
}

func spinner() Node {
	return Span(Class("inline-block h-4 w-4 animate-spin rounded-full border-2 border-border-muted border-t-accent"), Attr("aria-hidden", "true"))
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
		Class("h-2 w-full overflow-hidden rounded-full bg-surface-muted"),
		Attr("role", "progressbar"),
		Attr("aria-valuemin", "0"),
		Attr("aria-valuemax", strconv.Itoa(max)),
		Attr("aria-valuenow", strconv.Itoa(value)),
		Div(Class("h-full rounded-full bg-accent transition-[width] duration-200 ease-out"), Style("width: "+strconv.Itoa((value*100)/max)+"%;")),
	)
}

func metricCard(label, value, meta, tone string) Node {
	classes := "relative overflow-hidden rounded-xl border border-border bg-background p-4 shadow-sm before:absolute before:inset-y-0 before:left-0 before:w-1 before:bg-border-accent before:content-['']"
	trimmedTone := strings.TrimSpace(tone)
	if trimmedTone != "" {
		switch trimmedTone {
		case "success":
			classes += " bg-[linear-gradient(135deg,var(--color-success-muted)_0%,var(--color-background)_45%)] before:bg-border-success"
		case "attention":
			classes += " bg-[linear-gradient(135deg,var(--color-warning-muted)_0%,var(--color-background)_45%)] before:bg-warning"
		default:
			classes += " bg-[linear-gradient(135deg,var(--color-accent-muted)_0%,var(--color-background)_45%)]"
		}
	}

	metaNode := Node(nil)
	if strings.TrimSpace(meta) != "" {
		metaNode = P(Class("text-xs text-muted"), Text(meta))
	}

	return Div(
		Class(classes),
		P(Class("m-0 text-xs font-semibold text-foreground"), Text(label)),
		P(Class("my-1 text-3xl font-semibold leading-tight text-foreground"), Text(value)),
		metaNode,
	)
}

func segmentedTabs(items []segmentedTabItem) Node {
	nodes := make([]Node, 0, len(items))
	for i := range items {
		item := items[i]
		className := "inline-flex min-h-9 items-center justify-center rounded-lg px-3 py-2 text-sm font-medium text-muted transition-colors hover:text-foreground"
		if item.Active {
			className += " bg-background text-foreground shadow-xs"
		}
		nodes = append(nodes, Button(Type("button"), Class(className), Attr("aria-pressed", strconv.FormatBool(item.Active)), Text(item.Label)))
	}

	return Div(Class("inline-flex flex-wrap gap-1 rounded-xl bg-surface-muted p-1"), Group(nodes))
}

func treeView(items []treeViewItem) Node {
	if len(items) == 0 {
		return P(Class("text-sm text-muted"), Text("No items"))
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

	linkClass := "inline-flex min-h-9 items-center gap-2 rounded-lg px-3 py-2 text-sm text-foreground no-underline hover:bg-surface-muted"
	if item.Active {
		linkClass += " bg-accent-muted text-accent"
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
			Ul(Class("ml-6 mt-1 grid gap-1 border-l border-border-muted pl-2"), Group(childNodes)),
		),
	)
}
