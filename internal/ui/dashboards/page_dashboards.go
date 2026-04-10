package dashboards

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"
	dashboardsvc "duck-demo/internal/service/dashboard"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type dashboardListRowData struct {
	Name        string
	Description string
	URL         string
	Owner       string
	Updated     string
}

type dashboardDetailPageData struct {
	Principal         domain.ContextPrincipal
	Dashboard         *domain.Dashboard
	Widgets           []dashboardsvc.ResolvedWidget
	PageTabs          []core.SectionTab
	CurrentPageName   string
	CurrentPageKey    string
	EditMode          bool
	Freshness         *domain.AssetFreshnessStatus
	FreshnessExplain  *domain.AssetFreshnessNode
	BaseURL           string
	ViewURL           string
	StudioURL         string
	EditURL           string
	DeleteURL         string
	CreateWidgetURL   string
	SurfaceURL        string
	UpdatesStreamURL  string
	UpdatesApplyURL   string
	TablePageURL      string
	StreamID          string
	ActiveFilters     []dashboardsvc.InteractiveFilter
	FilterKey         string
	UpdateVersion     string
	PendingWidgetIDs  []string
	CSRFToken         string
	CSRFFieldProvider func() Node
}

type dashboardWidgetFormData struct {
	Title             string
	Action            string
	SubmitLabel       string
	PageName          string
	Name              string
	Description       string
	SourceKind        string
	SQL               string
	NotebookID        string
	CellID            string
	SemanticModelID   string
	Metrics           string
	Dimensions        string
	Filters           string
	OrderBy           string
	Limit             string
	TimeGrain         string
	VisualKind        string
	ChartType         string
	VisualTitle       string
	VisualSubtitle    string
	VisualLegendMode  string
	VisualLegendPos   string
	VisualX           string
	VisualY           string
	VisualSeries      string
	VisualLabel       string
	VisualValue       string
	VisualSecondary   string
	LayoutX           string
	LayoutY           string
	LayoutW           string
	LayoutH           string
}

func dashboardsListPage(principal domain.ContextPrincipal, rows []dashboardListRowData, page domain.PageRequest, total int64) Node {
	tableRows := make([]Node, 0, len(rows))
	for _, row := range rows {
		tableRows = append(tableRows, Tr(
			core.TablePrimaryCell(core.ResourceIcon("dashboard"), core.TablePrimaryLink(row.URL, row.Name)),
			Td(Text(row.Description)),
			Td(core.TableMetaText(row.Owner)),
			Td(core.TableMetaText(row.Updated)),
		))
	}
	tableNode := Node(core.ListPageBody(
		core.WorkspaceEmptyState("layout-panel-top", "No dashboards yet.", "Create a dashboard to start collecting widgets and semantic views.", core.PrimaryLink("/ui/dashboards/new", "", Text("New dashboard"))),
	))
	if len(tableRows) > 0 {
		tableNode = core.ListPageBody(core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Name")), Th(Text("Description")), Th(Text("Owner")), Th(Text("Updated")))),
				TBody(Group(tableRows)),
			),
		), core.ListPagination("/ui/dashboards", page, total))
	}
	return core.AppPage("Dashboards", "dashboards", principal,
		core.PageHeader("Discover", "Dashboards", "Browse and manage dashboard resources.", core.PrimaryLink("/ui/dashboards/new", "", Text("New dashboard"))),
		tableNode,
	)
}

func dashboardsNewPage(principal domain.ContextPrincipal, folderID string, csrfFieldProvider func() Node) Node {
	fields := []Node{
		core.FieldLabel("Name"),
		core.InputControl("", Name("name"), Required()),
		core.FieldLabel("Description"),
		core.TextareaControl("", Name("description")),
		core.FieldLabel("Semantic project"),
		core.InputControl("", Name("semantic_project_name")),
		core.FieldLabel("Semantic model"),
		core.InputControl("", Name("semantic_model_name")),
		core.FieldLabel("Compute mode"),
		dashboardComputeModeSelect(domain.DashboardComputePolicy{}.Normalize().Mode),
		core.FieldLabel("Compute endpoint"),
		core.InputControl("", Name("compute_endpoint_name")),
		core.Checkbox("dashboard-compute-fallback-local", "compute_fallback_local", "true", "Fallback to local if shared endpoint is unavailable", false),
	}
	if strings.TrimSpace(folderID) != "" {
		fields = append(fields,
			Input(Type("hidden"), Name("folder_id"), Value(folderID)),
			P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("This dashboard will be created in the current Explore folder.")),
		)
	}
	return formPage(principal, "New Dashboard", "dashboards", "/ui/dashboards", csrfFieldProvider, fields...)
}

func dashboardsEditPage(principal domain.ContextPrincipal, dashboard *domain.Dashboard, csrfFieldProvider func() Node) Node {
	compute := dashboard.Compute.Normalize()
	return formPage(principal, "Edit Dashboard", "dashboards", "/ui/dashboards/"+dashboard.ID+"/update", csrfFieldProvider,
		core.FieldLabel("Name"),
		core.InputControl("", Name("name"), Value(dashboard.Name), Required()),
		core.FieldLabel("Description"),
		core.TextareaControl("", Name("description"), Text(dashboard.Description)),
		core.FieldLabel("Semantic project"),
		core.InputControl("", Name("semantic_project_name"), Value(dashboard.SemanticProjectName)),
		core.FieldLabel("Semantic model"),
		core.InputControl("", Name("semantic_model_name"), Value(dashboard.SemanticModelName)),
		core.FieldLabel("Compute mode"),
		dashboardComputeModeSelect(compute.Mode),
		core.FieldLabel("Compute endpoint"),
		core.InputControl("", Name("compute_endpoint_name"), Value(compute.EndpointName)),
		core.Checkbox("dashboard-compute-fallback-local", "compute_fallback_local", "true", "Fallback to local if shared endpoint is unavailable", compute.FallbackLocal),
	)
}

func dashboardWidgetEditPage(principal domain.ContextPrincipal, dashboard *domain.Dashboard, widget *domain.DashboardWidget, csrfFieldProvider func() Node) Node {
	return core.AppPage(
		"Edit Widget: "+widget.Name,
		"dashboards",
		principal,
		dashboardWidgetFormCard(widgetFormDataFromWidget(widget, "/ui/dashboards/"+dashboard.ID+"/widgets/"+widget.ID+"/update", "Update widget", "Edit Widget"), csrfFieldProvider),
	)
}

func dashboardsDetailPage(d dashboardDetailPageData) Node {
	widgetNodes := dashboardWidgetNodes(d.Widgets, d.BaseURL, d.CSRFFieldProvider, d.EditMode)
	if len(widgetNodes) == 0 {
		emptyAction := Node(nil)
		if d.EditMode {
			emptyAction = core.PrimaryLink("#dashboard-widget-form", "", Text("Add widget in Studio"))
		}
		widgetNodes = append(widgetNodes, Div(
			Class("dashboard-canvas-empty rounded-[1.75rem] border border-dashed border-[var(--borderColor-default)] bg-[color-mix(in_srgb,var(--bgColor-default)_84%,white_16%)] p-6"),
			core.EmptyState("inbox", "No widgets yet.", "Switch to Studio mode to start arranging tiles and authoring widget queries.", emptyAction),
		))
	}

	viewModeTabs := core.SectionTabs([]core.SectionTab{
		{Label: "View", Href: d.ViewURL, Active: !d.EditMode},
		{Label: "Studio", Href: d.StudioURL, Active: d.EditMode},
	})

	headerActions := []Node{
		viewModeTabs,
		core.SecondaryLink(d.EditURL, "", Text("Edit metadata")),
	}
	if d.EditMode {
		headerActions = append(headerActions, Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete"))))
	}

	body := []Node{}
	if len(d.PageTabs) > 1 {
		body = append(body, dashboardPageTabBar(d.PageTabs))
	}
	if !d.EditMode {
		body = append(body, dashboardViewMetaBar(d.Dashboard.Compute))
	}
	if d.EditMode {
		body = append(body,
			dashboardHero(d),
			dashboardFreshnessCard(d.Freshness, d.FreshnessExplain),
		)
	}
	body = append(body, dashboardViewStreamRoot(d, widgetNodes))
	if d.EditMode {
		body = append(body, dashboardStudioRail(d))
	}

	return core.AppPage(
		"Dashboard: "+d.Dashboard.Name,
		"dashboards",
		d.Principal,
		core.PageHeader("Discover", d.Dashboard.Name, d.Dashboard.Description, headerActions...),
		Group(body),
		Script(Src(core.UIScriptHref("dashboard.js"))),
	)
}

func dashboardWidgetNodes(widgets []dashboardsvc.ResolvedWidget, baseURL string, csrfFieldProvider func() Node, editMode bool) []Node {
	widgetNodes := make([]Node, 0, len(widgets))
	for _, widget := range widgets {
		widgetNodes = append(widgetNodes, dashboardWidgetCard(widget, baseURL, csrfFieldProvider, editMode))
	}
	return widgetNodes
}

func dashboardViewStreamRoot(d dashboardDetailPageData, widgetNodes []Node) Node {
	if d.EditMode || strings.TrimSpace(d.UpdatesStreamURL) == "" {
		return dashboardViewContent(d, widgetNodes)
	}
	return Div(
		ID("dashboard-view-root"),
		Attr("data-dashboard-surface", "true"),
		Attr("data-dashboard-stream-root", "true"),
		Attr("data-dashboard-filter-key", d.FilterKey),
		Attr("data-dashboard-update-version", d.UpdateVersion),
		Attr("data-dashboard-pending-widget-ids", strings.Join(d.PendingWidgetIDs, ",")),
		Attr("data-dashboard-view-url", d.ViewURL),
		Attr("data-dashboard-surface-url", d.SurfaceURL),
		Attr("data-dashboard-stream-id", d.StreamID),
		Attr("data-dashboard-updates-url", d.UpdatesStreamURL),
		Attr("data-dashboard-apply-url", d.UpdatesApplyURL),
		Attr("data-dashboard-table-page-url", d.TablePageURL),
		Attr("data-dashboard-csrf-token", d.CSRFToken),
		data.Signals(map[string]any{}),
		data.Init("@get('"+d.UpdatesStreamURL+"')"),
		Div(Class("hidden"), d.CSRFFieldProvider()),
		El("style", Raw(`
html[data-dashboard-loading='true'] [data-dashboard-loading-indicator='true'] {
  opacity: 1;
  transform: translateY(0);
}
html[data-dashboard-loading='true'] [data-dashboard-loading-surface='true'] {
  cursor: progress;
}
html[data-dashboard-loading='true'] [data-dashboard-loading-canvas='true'] {
  opacity: 0.68;
}
[data-dashboard-widget-card='true'][data-dashboard-widget-loading='true'] [data-dashboard-widget-loading-indicator='true'] {
  opacity: 1;
}
`)),
		dashboardViewContent(d, widgetNodes),
	)
}

func dashboardViewContent(d dashboardDetailPageData, widgetNodes []Node) Node {
	body := dashboardViewSurfaceContents(d, widgetNodes)
	return Div(
		ID("dashboard-view-content"),
		Class("shrink-0 grid gap-4"),
		Group(body),
	)
}

func dashboardViewSurfaceContents(d dashboardDetailPageData, widgetNodes []Node) []Node {
	body := []Node{}
	if !d.EditMode && dashboardShowsInteractiveToolbar(d.Dashboard) {
		body = append(body, dashboardInteractiveToolbar(d.ActiveFilters))
	}
	body = append(body, dashboardCanvas(widgetNodes, d.EditMode))
	return body
}

func dashboardPageTabBar(tabs []core.SectionTab) Node {
	if len(tabs) <= 1 {
		return nil
	}
	nodes := make([]Node, 0, len(tabs))
	for i := range tabs {
		tab := tabs[i]
		className := "inline-flex min-h-10 items-center border-b-2 border-transparent px-1 text-sm font-medium text-[var(--fgColor-muted)] no-underline transition-colors hover:text-[var(--fgColor-default)]"
		current := Node(nil)
		if tab.Active {
			className = "inline-flex min-h-10 items-center border-b-2 border-[var(--borderColor-accent-emphasis)] px-1 text-sm font-semibold text-[var(--fgColor-default)] no-underline"
			current = Attr("aria-current", "page")
		}
		nodes = append(nodes, A(
			Href(tab.Href),
			Class(className),
			Attr("data-dashboard-page-link", ""),
			current,
			Text(tab.Label),
		))
	}
	return Div(
		Class("dashboard-page-tabs"),
		Attr("data-ignore", ""),
		Nav(
			Class("flex flex-wrap items-center gap-5 border-b border-[var(--borderColor-default)]"),
			Attr("aria-label", "Section navigation"),
			Group(nodes),
		),
	)
}

func dashboardShowsInteractiveToolbar(dashboard *domain.Dashboard) bool {
	return dashboard != nil && strings.TrimSpace(dashboard.SemanticProjectName) != "" && strings.TrimSpace(dashboard.SemanticModelName) != ""
}

func dashboardFilterSignalMap(filters []dashboardsvc.InteractiveFilter) map[string][]string {
	if len(filters) == 0 {
		return map[string][]string{}
	}
	out := make(map[string][]string, len(filters))
	for _, filter := range filters {
		out[filter.Dimension] = append([]string(nil), filter.Values...)
	}
	return out
}

func dashboardInteractiveToolbar(filters []dashboardsvc.InteractiveFilter) Node {
	displayFilters := dashboardAggregateDisplayFilters(filters)
	chips := []Node{
		P(Class("m-0 text-[10px] font-black uppercase tracking-[0.15em] text-[var(--fgColor-muted)]"), Text("Cross Filters")),
	}
	if len(displayFilters) == 0 {
		chips = append(chips,
			P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("No active filters")),
		)
	} else {
		filterNodes := make([]Node, 0)
		for _, filter := range displayFilters {
			for _, value := range filter.Values {
				filterNodes = append(filterNodes, Button(
					Type("button"),
					Class("inline-flex items-center gap-2 rounded-full border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] px-3 py-1 text-xs font-semibold text-[var(--fgColor-default)] transition-colors hover:border-[var(--borderColor-accent-emphasis)] hover:text-[var(--fgColor-accent)]"),
					Attr("data-dashboard-remove-filter", "true"),
					Attr("data-dashboard-filter-dimension", filter.Dimension),
					Attr("data-dashboard-filter-value", value),
					Span(Text(dashboardDisplayFilterDimension(filter.Dimension))),
					Span(Class("text-[var(--fgColor-muted)]"), Text(value)),
					Span(Class("text-[var(--fgColor-muted)]"), Raw("&times;")),
				))
			}
		}
		chips = append(chips, Div(Class("flex flex-wrap items-center gap-2"), Group(filterNodes)))
	}

	clearAction := Node(nil)
	if len(displayFilters) > 0 {
		clearAction = Button(
			Type("button"),
			Class("inline-flex items-center rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 py-1.5 text-xs font-semibold text-[var(--fgColor-muted)] transition-colors hover:border-[var(--borderColor-accent-emphasis)] hover:text-[var(--fgColor-accent)]"),
			Attr("data-dashboard-clear-filters", "true"),
			Text("Clear all"),
		)
	}

	return Div(
		Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-4 py-3 shadow-sm"),
		Div(Class("flex flex-wrap items-start justify-between gap-3"),
			Div(Class("grid gap-2"), Group(chips)),
			Div(Class("flex flex-wrap items-center gap-2"),
				Span(
					Class("inline-flex translate-y-1 items-center gap-2 rounded-full border border-[var(--borderColor-accent-emphasis)] bg-[var(--bgColor-accent-muted)] px-3 py-1 text-[11px] font-semibold text-[var(--fgColor-accent)] opacity-0 transition-all duration-150"),
					Attr("data-dashboard-loading-indicator", "true"),
					Raw(`<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" class="animate-spin"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>`),
					Span(Text("Updating")),
				),
				clearAction,
			),
		),
	)
}

func dashboardAggregateDisplayFilters(filters []dashboardsvc.InteractiveFilter) []dashboardsvc.InteractiveFilter {
	if len(filters) == 0 {
		return nil
	}

	order := make([]string, 0, len(filters))
	grouped := make(map[string][]string, len(filters))
	seen := make(map[string]map[string]struct{}, len(filters))
	for _, filter := range filters {
		dimension := strings.TrimSpace(filter.Dimension)
		if dimension == "" {
			continue
		}
		if _, ok := grouped[dimension]; !ok {
			grouped[dimension] = []string{}
			order = append(order, dimension)
			seen[dimension] = make(map[string]struct{})
		}
		for _, value := range filter.Values {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if _, ok := seen[dimension][value]; ok {
				continue
			}
			seen[dimension][value] = struct{}{}
			grouped[dimension] = append(grouped[dimension], value)
		}
	}

	out := make([]dashboardsvc.InteractiveFilter, 0, len(order))
	for _, dimension := range order {
		if len(grouped[dimension]) == 0 {
			continue
		}
		out = append(out, dashboardsvc.InteractiveFilter{
			Dimension: dimension,
			Values:    grouped[dimension],
		})
	}
	return out
}

func dashboardDisplayFilterDimension(dimension string) string {
	parts := strings.SplitN(strings.TrimSpace(dimension), "@", 2)
	label := strings.ReplaceAll(parts[0], "_", " ")
	label = strings.ReplaceAll(label, ".", " ")
	label = strings.TrimSpace(label)
	if len(parts) == 2 && strings.TrimSpace(parts[1]) != "" {
		label += " (" + strings.TrimSpace(parts[1]) + ")"
	}
	return label
}

func dashboardHero(d dashboardDetailPageData) Node {
	modeBadge := core.Badge("View Mode", "accent")
	modeCopy := "Read-only dashboard surface tuned for review and presentation."
	if d.EditMode {
		modeBadge = core.Badge("Studio Mode", "attention")
		modeCopy = "Arrange tiles, inspect backing queries, and manage widget definitions without mixing controls into the viewer."
	}

	stats := []Node{
		dashboardHeroStat("Widgets", strconv.Itoa(len(d.Widgets))),
		dashboardHeroStat("Owner", dashIfEmpty(d.Dashboard.Owner)),
		dashboardHeroStat("Compute", dashboardComputePolicyLabel(d.Dashboard.Compute)),
	}
	if d.Freshness != nil {
		stats = append(stats, dashboardHeroStat("Freshness", d.Freshness.FreshnessStatus))
	}

	return Div(
		Class("shrink-0 overflow-hidden rounded-[2rem] border border-[var(--borderColor-default)] bg-[linear-gradient(135deg,color-mix(in_srgb,var(--bgColor-accent-muted)_78%,white_22%)_0%,color-mix(in_srgb,var(--bgColor-default)_92%,white_8%)_48%,color-mix(in_srgb,var(--bgColor-muted)_78%,white_22%)_100%)] p-6 shadow-sm"),
		Div(Class("grid gap-6 xl:grid-cols-[minmax(0,1.7fr)_minmax(18rem,0.9fr)]"),
			Div(Class("grid gap-4"),
				Div(Class("flex flex-wrap items-center gap-3"),
					modeBadge,
					Span(Class("text-xs font-semibold uppercase tracking-[0.08em] text-[var(--fgColor-muted)]"), Text("Dashboard Canvas")),
				),
				H2(Class("m-0 text-4xl font-black tracking-tight text-[var(--fgColor-default)]"), Text(d.Dashboard.Name)),
				P(Class("m-0 max-w-3xl text-sm leading-7 text-[var(--fgColor-muted)]"), Text(modeCopy)),
			),
			Div(Class("grid gap-3 sm:grid-cols-3 xl:grid-cols-1"), Group(stats)),
		),
	)
}

func dashboardHeroStat(label, value string) Node {
	return Div(
		Class("rounded-2xl border border-[var(--borderColor-default)] bg-[color-mix(in_srgb,var(--bgColor-default)_88%,white_12%)] p-4 shadow-xs"),
		P(Class("m-0 text-[11px] font-semibold uppercase tracking-[0.08em] text-[var(--fgColor-muted)]"), Text(label)),
		P(Class("mt-2 mb-0 text-xl font-semibold text-[var(--fgColor-default)]"), Text(value)),
	)
}

func dashboardViewMetaBar(policy domain.DashboardComputePolicy) Node {
	return Div(
		Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-4 py-3 shadow-sm"),
		Div(Class("flex flex-wrap items-center justify-between gap-3"),
			Div(Class("grid gap-1"),
				P(Class("m-0 text-[10px] font-black uppercase tracking-[0.15em] text-[var(--fgColor-muted)]"), Text("Compute Policy")),
				P(Class("m-0 text-sm text-[var(--fgColor-default)]"), Text(dashboardComputePolicyLabel(policy))),
			),
			Span(Class("text-xs text-[var(--fgColor-muted)]"), Text("Viewer routing is read-only")),
		),
	)
}

func dashboardComputeModeSelect(selected string) Node {
	return core.SelectControl("", Name("compute_mode"),
		Option(Value(domain.ComputeModeAuto), selectedValue(strings.ToUpper(strings.TrimSpace(selected)), domain.ComputeModeAuto), Text("AUTO")),
		Option(Value(domain.ComputeModeByocLocal), selectedValue(strings.ToUpper(strings.TrimSpace(selected)), domain.ComputeModeByocLocal), Text("BYOC_LOCAL")),
		Option(Value(domain.ComputeModeSharedEndpoint), selectedValue(strings.ToUpper(strings.TrimSpace(selected)), domain.ComputeModeSharedEndpoint), Text("SHARED_ENDPOINT")),
	)
}

func dashboardComputePolicyLabel(policy domain.DashboardComputePolicy) string {
	policy = policy.Normalize()
	switch policy.Mode {
	case domain.ComputeModeSharedEndpoint:
		label := "Shared endpoint"
		if policy.EndpointName != "" {
			label += ": " + policy.EndpointName
		}
		if policy.FallbackLocal {
			label += " (fallback local)"
		}
		return label
	case domain.ComputeModeByocLocal:
		return "Local"
	default:
		return "Auto"
	}
}

func dashboardCanvas(widgetNodes []Node, editMode bool) Node {
	canvasClass := "dashboard-canvas-grid grid auto-rows-[minmax(8rem,auto)] gap-5 md:grid-cols-2 xl:gap-6"
	if editMode {
		canvasClass += " dashboard-canvas-grid--edit"
	}
	shellClass := "dashboard-canvas-shell shrink-0 overflow-hidden rounded-[2rem] border p-4 sm:p-5"
	shellStyle := ""
	if editMode {
		shellClass += " border-[var(--borderColor-default)] bg-[radial-gradient(circle_at_top_left,color-mix(in_srgb,var(--bgColor-accent-muted)_55%,transparent)_0%,transparent_32%),linear-gradient(180deg,color-mix(in_srgb,var(--bgColor-muted)_86%,white_14%)_0%,var(--bgColor-default)_100%)] shadow-sm"
	} else {
		shellClass += " border-[var(--borderColor-default)]"
		shellStyle = "background-color: var(--bgColor-muted); background-image: radial-gradient(color-mix(in srgb, var(--fgColor-muted) 26%, transparent) 1.5px, transparent 1.5px); background-size: 24px 24px;"
	}
	canvasBody := []Node{
		El("style", Raw(`
@media (min-width: 1280px) {
  .dashboard-canvas-grid {
    grid-template-columns: repeat(12, minmax(0, 1fr));
    grid-auto-rows: 7.5rem;
  }
  .dashboard-canvas-grid > .dashboard-widget-tile {
    grid-column: var(--dash-col-start) / span var(--dash-col-span);
    grid-row: var(--dash-row-start) / span var(--dash-row-span);
  }
}
`)),
		Div(Class(canvasClass), Attr("data-dashboard-loading-canvas", "true"), Group(widgetNodes)),
	}
	if editMode {
		canvasBody = append([]Node{
			Div(Class("mb-4 flex items-center justify-between gap-3"),
				Div(
					P(Class("m-0 text-xs font-semibold uppercase tracking-[0.1em] text-[var(--fgColor-muted)]"), Text("Canvas")),
					P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("Tiles now read as a single dashboard surface instead of isolated admin cards.")),
				),
			),
		}, canvasBody...)
	}
	return Div(
		Class(shellClass),
		Attr("data-dashboard-loading-surface", "true"),
		Style(shellStyle),
		Group(canvasBody),
	)
}

func dashboardStudioRail(d dashboardDetailPageData) Node {
	widgetItems := make([]Node, 0, len(d.Widgets))
	for _, widget := range d.Widgets {
		widgetItems = append(widgetItems, dashboardStudioWidgetItem(widget, d.BaseURL, d.CSRFFieldProvider))
	}
	if len(widgetItems) == 0 {
		widgetItems = append(widgetItems, P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("No widgets yet. Use the form below to add the first tile.")))
	}

	return Div(
		Class("shrink-0 grid gap-4 xl:grid-cols-[minmax(0,1.35fr)_minmax(20rem,0.85fr)]"),
		Div(Class("min-w-0"),
			core.SectionSurface(
				core.SectionHeader("Canvas Guidance", "Keep the dashboard viewer quiet by moving authoring controls into Studio mode."),
				Ul(
					Class("m-0 grid gap-2 pl-5 text-sm leading-6 text-[var(--fgColor-muted)]"),
					Li(Text("Use View mode for presentation and stakeholder review.")),
					Li(Text("Use Studio mode to add widgets, inspect source SQL, and manage tile metadata.")),
					Li(Text("Treat widget layout width and height as canvas slots rather than separate page sections.")),
				),
			),
		),
		Div(Class("grid gap-4"),
			core.SectionSurface(
				ID("dashboard-widget-form"),
				core.SectionHeader("Studio Rail", "Manage existing tiles and add new ones from a dedicated authoring panel."),
				Div(Class("grid gap-3"), Group(widgetItems)),
			),
			dashboardWidgetFormCard(defaultWidgetFormData(d.CreateWidgetURL), d.CSRFFieldProvider),
		),
	)
}

func dashboardFreshnessCard(status *domain.AssetFreshnessStatus, explanation *domain.AssetFreshnessNode) Node {
	if status == nil {
		return nil
	}

	upstream := make([]Node, 0)
	if explanation != nil {
		for _, child := range explanation.Upstream {
			upstream = append(upstream, Li(
				Strong(Text(child.AssetKey)),
				Text(" "),
				statusLabel(child.FreshnessStatus, dashboardFreshnessTone(child.FreshnessStatus)),
				Text(" "),
				Span(Class("text-xs text-[var(--fgColor-muted)]"), Text(child.Reason)),
			))
		}
	}
	upstreamNode := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No upstream blockers detected.")))
	if len(upstream) > 0 {
		upstreamNode = Ul(Group(upstream))
	}

	return Div(
		Class("shrink-0 rounded-2xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
		H2(Text("Freshness")),
		Div(Class("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex"),
			statusLabel(status.FreshnessStatus, dashboardFreshnessTone(status.FreshnessStatus)),
			Span(Class("text-xs text-[var(--fgColor-muted)]"), Text(status.Reason)),
		),
		P(Text("Effective max lag: "+strconv.FormatInt(status.EffectiveMaxLagSeconds, 10)+"s")),
		P(Text("Last materialized: "+formatTimePtr(status.LastMaterializedAt))),
		P(Text("Stale since: "+formatTimePtr(status.StaleSince))),
		H3(Text("Upstream status")),
		upstreamNode,
	)
}

func dashboardWidgetCard(widget dashboardsvc.ResolvedWidget, deleteBaseURL string, csrfFieldProvider func() Node, editMode bool) Node {
	content := Node(nil)
	tileStyle := fmt.Sprintf("--dash-col-start:%d;--dash-row-start:%d;--dash-col-span:%d;--dash-row-span:%d;", dashboardCanvasColStart(widget.Widget.Layout), dashboardCanvasRowStart(widget.Widget.Layout), dashboardCanvasColSpan(widget.Widget.Layout), dashboardCanvasRowSpan(widget.Widget.Layout))
	switch {
	case widget.Widget.VisualSpec != nil && widget.Widget.VisualSpec.Kind == domain.VisualOutputMetric:
		if !editMode {
			content = metricHost(widget, false)
			break
		}
		field := ""
		if widget.Widget.VisualSpec.Encodings.Value != nil {
			field = widget.Widget.VisualSpec.Encodings.Value.Field
		}
		var value interface{} = "-"
		if len(widget.Rows) > 0 {
			idx := 0
			for i, col := range widget.Columns {
				if col == field {
					idx = i
					break
				}
			}
			if len(widget.Rows[0]) > idx {
				value = widget.Rows[0][idx]
			}
		}
		content = visualMetricCard(defaultVisualTitle(widget.Widget.VisualSpec, widget.Widget.Name), value, dashboardRowCountLabel(widget.RowCount))
	case widget.Widget.VisualSpec != nil && widget.Widget.VisualSpec.Kind == domain.VisualOutputChart:
		content = chartHost(widget, editMode)
	case widget.Widget.VisualSpec != nil && widget.Widget.VisualSpec.Kind == domain.VisualOutputTable:
		content = tableHost(widget, editMode)
		tileStyle += fmt.Sprintf("height:%.2frem;", float64(dashboardCanvasRowSpan(widget.Widget.Layout))*7.5)
	default:
		content = dashboardWidgetTable(widget)
	}

	generatedSQL := Node(nil)
	if editMode && widget.GeneratedSQL != "" {
		generatedSQL = Details(Summary(Text("Generated SQL")), Pre(Text(widget.GeneratedSQL)))
	}

	actions := Node(nil)
	if editMode {
		actions = core.ActionMenu("Actions",
			core.ActionMenuLink(deleteBaseURL+"/widgets/"+widget.Widget.ID+"/edit", "Edit widget"),
			core.ActionMenuPost(deleteBaseURL+"/widgets/"+widget.Widget.ID+"/delete", "Delete widget", csrfFieldProvider, true),
		)
	}

	details := Node(nil)
	if editMode {
		details = dashboardWidgetDataDetails(widget)
	}

	auxiliary := []Node{}
	if details != nil {
		auxiliary = append(auxiliary, details)
	}
	if generatedSQL != nil {
		auxiliary = append(auxiliary, generatedSQL)
	}
	if !editMode && widget.Interaction != nil && !widget.Interaction.Participates && strings.TrimSpace(widget.Interaction.DisabledReason) != "" {
		auxiliary = append(auxiliary, P(Class("m-0 text-xs font-medium text-[var(--fgColor-muted)]"), Text(widget.Interaction.DisabledReason)))
	}

	return Div(
		Class("dashboard-widget-tile group flex h-full min-h-0 flex-col overflow-hidden rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] shadow-sm transition-all duration-150 hover:-translate-y-[1px] hover:border-[var(--borderColor-accent-emphasis)] hover:shadow-md md:col-span-2"),
		Attr("data-dashboard-widget-card", "true"),
		Attr("data-widget-id", widget.Widget.ID),
		Style(tileStyle),
		dashboardWidgetHeader(widget, actions, editMode),
		Div(Class(dashboardWidgetBodyClass(widget)), content),
		Iff(len(auxiliary) > 0, func() Node {
			return Div(
				Class("grid gap-3 border-t border-[color-mix(in_srgb,var(--borderColor-default)_72%,transparent)] px-5 py-4"),
				Group(auxiliary),
			)
		}),
	)
}

func dashboardWidgetHeader(widget dashboardsvc.ResolvedWidget, actions Node, editMode bool) Node {
	descriptionNode := Node(nil)
	if editMode && strings.TrimSpace(widget.Widget.Description) != "" {
		descriptionNode = P(Class("mt-1 mb-0 max-w-2xl text-xs leading-5 text-[var(--fgColor-muted)]"), Text(widget.Widget.Description))
	}

	return Div(
		Class("flex items-center justify-between gap-3 border-b border-[var(--borderColor-default)] px-4 py-3"),
		Div(Class("min-w-0"),
			P(Class("m-0 text-[10px] font-black uppercase tracking-[0.15em] text-[var(--fgColor-muted)]"), Text(widget.Widget.Name)),
			descriptionNode,
		),
		Div(Class("flex items-center gap-2"),
			Iff(!editMode, func() Node {
				return Span(
					Class("inline-flex items-center gap-1 rounded-full border border-[var(--borderColor-accent-emphasis)] bg-[var(--bgColor-accent-muted)] px-2 py-0.5 text-[10px] font-semibold text-[var(--fgColor-accent)] opacity-0 transition-opacity duration-150"),
					Attr("data-dashboard-widget-loading-indicator", "true"),
					Attr("aria-hidden", "true"),
					Raw(`<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" class="animate-spin"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>`),
					Span(Text("Updating")),
				)
			}),
			dashboardWidgetHeaderAction(actions, editMode),
		),
	)
}

func dashboardWidgetHeaderAction(actions Node, editMode bool) Node {
	if editMode {
		return actions
	}
	return Button(
		Type("button"),
		Class("rounded p-1 text-[var(--fgColor-muted)] transition-colors hover:bg-[var(--bgColor-muted)] hover:text-[var(--fgColor-default)]"),
		Title("Widget options"),
		Attr("aria-label", "Widget options"),
		Raw(`<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="1"></circle><circle cx="12" cy="5" r="1"></circle><circle cx="12" cy="19" r="1"></circle></svg>`),
	)
}

func dashboardRowCountLabel(count int) string {
	if count == 1 {
		return "1 row"
	}
	return fmt.Sprintf("%d rows", count)
}

func dashboardWidgetBodyClass(widget dashboardsvc.ResolvedWidget) string {
	if widget.Widget.VisualSpec != nil && widget.Widget.VisualSpec.Kind == domain.VisualOutputMetric {
		return "flex min-h-0 flex-1 px-6 py-6"
	}
	if widget.Widget.VisualSpec != nil && widget.Widget.VisualSpec.Kind == domain.VisualOutputTable {
		return "min-h-0 h-0 flex-1 overflow-hidden"
	}
	return "min-h-0 flex-1 px-5 pb-5 pt-5"
}

func dashboardWidgetDataDetails(widget dashboardsvc.ResolvedWidget) Node {
	return Details(
		Summary(Text("View data")),
		dashboardWidgetTable(widget),
	)
}

func dashboardWidgetTable(widget dashboardsvc.ResolvedWidget) Node {
	headers := make([]Node, 0, len(widget.Columns))
	for _, col := range widget.Columns {
		alignClass := "text-left"
		if dashboardColumnLooksNumeric(col) {
			alignClass = "text-right"
		}
		headers = append(headers, Th(Class("px-5 py-3 "+alignClass), Text(strings.ReplaceAll(col, "_", " "))))
	}
	rows := make([]Node, 0, len(widget.Rows))
	for _, row := range widget.Rows {
		cells := make([]Node, 0, len(row))
		for i, cell := range row {
			col := ""
			if i < len(widget.Columns) {
				col = widget.Columns[i]
			}
			cellClass := "px-5 py-4 align-middle text-sm text-[var(--fgColor-default)]"
			if i == 0 {
				cellClass += " font-semibold"
			}
			if dashboardValueIsNumeric(cell) || dashboardColumnLooksNumeric(col) {
				cellClass += " text-right font-medium tabular-nums text-[color-mix(in_srgb,var(--fgColor-default)_82%,var(--fgColor-muted)_18%)]"
			}
			cells = append(cells, Td(Class(cellClass), Text(formatDashboardCellValue(col, cell))))
		}
		rows = append(rows, Tr(Class("border-b border-[color-mix(in_srgb,var(--borderColor-default)_56%,transparent)] transition-colors hover:bg-[color-mix(in_srgb,var(--bgColor-accent-muted)_32%,transparent)]"), Group(cells)))
	}
	return Div(
		Class("min-h-0 overflow-auto"),
		Table(
			Class("min-w-full border-separate border-spacing-0 text-left text-[13px]"),
			THead(Class("sticky top-0 z-[1] bg-[color-mix(in_srgb,var(--bgColor-muted)_82%,white_18%)] text-[10px] font-black uppercase tracking-[0.14em] text-[var(--fgColor-muted)] backdrop-blur-sm"),
				Tr(Group(headers)),
			),
			TBody(Class("bg-transparent"), Group(rows)),
		),
	)
}

func dashboardStudioWidgetItem(widget dashboardsvc.ResolvedWidget, baseURL string, csrfFieldProvider func() Node) Node {
	return Div(
		Class("rounded-2xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-3"),
		Div(Class("flex items-start justify-between gap-3"),
			Div(Class("min-w-0"),
				P(Class("m-0 text-sm font-semibold text-[var(--fgColor-default)]"), Text(widget.Widget.Name)),
				P(Class("m-0 text-xs leading-5 text-[var(--fgColor-muted)]"), Text(widget.Widget.Description)),
				P(Class("m-0 text-[11px] uppercase tracking-[0.08em] text-[var(--fgColor-muted)]"), Text("Page: "+domain.NormalizeDashboardPageName(widget.Widget.PageName))),
				P(Class("m-0 text-[11px] uppercase tracking-[0.08em] text-[var(--fgColor-muted)]"), Text(fmt.Sprintf("%d row(s) rendered", widget.RowCount))),
			),
			core.ActionMenu("Actions",
				core.ActionMenuLink(baseURL+"/widgets/"+widget.Widget.ID+"/edit", "Edit widget"),
				core.ActionMenuPost(baseURL+"/widgets/"+widget.Widget.ID+"/delete", "Delete widget", csrfFieldProvider, true),
			),
		),
	)
}

func dashboardCanvasColStart(layout domain.DashboardWidgetLayout) int {
	switch {
	case layout.X < 0:
		return 1
	case layout.X >= 12:
		return 12
	default:
		return layout.X + 1
	}
}

func dashboardCanvasRowStart(layout domain.DashboardWidgetLayout) int {
	switch {
	case layout.Y < 0:
		return 1
	case layout.Y >= 24:
		return 24
	default:
		return layout.Y + 1
	}
}

func dashboardCanvasColSpan(layout domain.DashboardWidgetLayout) int {
	switch {
	case layout.W <= 0:
		return 4
	case layout.W > 12:
		return 12
	default:
		return layout.W
	}
}

func dashboardCanvasRowSpan(layout domain.DashboardWidgetLayout) int {
	switch {
	case layout.H <= 0:
		return 3
	case layout.H > 8:
		return 8
	default:
		return layout.H
	}
}

func dashboardFreshnessTone(status string) string {
	switch status {
	case domain.AssetFreshnessStatusFresh:
		return "success"
	case domain.AssetFreshnessStatusRefreshing:
		return "accent"
	case domain.AssetFreshnessStatusStale:
		return "attention"
	case domain.AssetFreshnessStatusBlocked:
		return "severe"
	default:
		return ""
	}
}

func dashboardWidgetFormCard(data dashboardWidgetFormData, csrfFieldProvider func() Node) Node {
	return core.SectionSurface(
		ID("dashboard-widget-form"),
		core.SectionHeader(data.Title, "Widget authoring uses the shared single-surface form framing."),
		Form(
			Class("grid gap-3"),
			Method("post"),
			Action(data.Action),
			csrfFieldProvider(),
			core.FieldLabel("Page"),
			core.InputControl("", Name("page_name"), Value(data.PageName)),
			core.FieldLabel("Name"),
			core.InputControl("", Name("name"), Value(data.Name), Required()),
			core.FieldLabel("Description"),
			core.TextareaControl("", Name("description"), Text(data.Description)),
			core.FieldLabel("Source kind"),
			core.SelectControl("", Name("source_kind"),
				Option(Value("sql_query"), selectedValue(data.SourceKind, "sql_query"), Text("sql_query")),
				Option(Value("notebook_cell"), selectedValue(data.SourceKind, "notebook_cell"), Text("notebook_cell")),
				Option(Value("semantic_query"), selectedValue(data.SourceKind, "semantic_query"), Text("semantic_query")),
			),
			core.FieldLabel("SQL query"),
			core.TextareaControl("", Name("sql"), Text(data.SQL)),
			core.FieldLabel("Notebook ID"),
			core.InputControl("", Name("notebook_id"), Value(data.NotebookID)),
			core.FieldLabel("Cell ID"),
			core.InputControl("", Name("cell_id"), Value(data.CellID)),
			core.FieldLabel("Semantic model ID"),
			core.InputControl("", Name("semantic_model_id"), Value(data.SemanticModelID)),
			core.FieldLabel("Metrics (comma separated)"),
			core.InputControl("", Name("metrics"), Value(data.Metrics)),
			core.FieldLabel("Dimensions (comma separated)"),
			core.InputControl("", Name("dimensions"), Value(data.Dimensions)),
			core.FieldLabel("Filters (comma separated)"),
			core.InputControl("", Name("filters"), Value(data.Filters)),
			core.FieldLabel("Order by (comma separated)"),
			core.InputControl("", Name("order_by"), Value(data.OrderBy)),
			core.FieldLabel("Limit"),
			core.InputControl("", Name("limit"), Value(data.Limit)),
			core.FieldLabel("Time grain"),
			core.InputControl("", Name("time_grain"), Value(data.TimeGrain)),
			core.FieldLabel("Visual kind"),
			core.SelectControl("", Name("visual_kind"),
				Option(Value("table"), selectedValue(data.VisualKind, "table"), Text("table")),
				Option(Value("metric"), selectedValue(data.VisualKind, "metric"), Text("metric")),
				Option(Value("chart"), selectedValue(data.VisualKind, "chart"), Text("chart")),
			),
			core.FieldLabel("Chart type"),
			core.SelectControl("", Name("chart_type"),
				Option(Value("bar"), selectedValue(data.ChartType, "bar"), Text("bar")),
				Option(Value("line"), selectedValue(data.ChartType, "line"), Text("line")),
				Option(Value("area"), selectedValue(data.ChartType, "area"), Text("area")),
				Option(Value("pie"), selectedValue(data.ChartType, "pie"), Text("pie")),
				Option(Value("doughnut"), selectedValue(data.ChartType, "doughnut"), Text("doughnut")),
				Option(Value("scatter"), selectedValue(data.ChartType, "scatter"), Text("scatter")),
				Option(Value("stacked_bar"), selectedValue(data.ChartType, "stacked_bar"), Text("stacked_bar")),
			),
			core.FieldLabel("Title"),
			core.InputControl("", Name("visual_title"), Value(data.VisualTitle)),
			core.FieldLabel("Subtitle"),
			core.InputControl("", Name("visual_subtitle"), Value(data.VisualSubtitle)),
			core.FieldLabel("Legend"),
			core.SelectControl("", Name("visual_legend_mode"),
				Option(Value("auto"), selectedValue(data.VisualLegendMode, "auto"), Text("auto")),
				Option(Value("show"), selectedValue(data.VisualLegendMode, "show"), Text("show")),
				Option(Value("hide"), selectedValue(data.VisualLegendMode, "hide"), Text("hide")),
			),
			core.FieldLabel("Legend position"),
			core.SelectControl("", Name("visual_legend_position"),
				Option(Value("top"), selectedValue(data.VisualLegendPos, "top"), Text("top")),
				Option(Value("right"), selectedValue(data.VisualLegendPos, "right"), Text("right")),
				Option(Value("bottom"), selectedValue(data.VisualLegendPos, "bottom"), Text("bottom")),
				Option(Value("left"), selectedValue(data.VisualLegendPos, "left"), Text("left")),
			),
			core.FieldLabel("X field"),
			core.InputControl("", Name("visual_x"), Value(data.VisualX)),
			core.FieldLabel("Y field"),
			core.InputControl("", Name("visual_y"), Value(data.VisualY)),
			core.FieldLabel("Series field"),
			core.InputControl("", Name("visual_series"), Value(data.VisualSeries)),
			core.FieldLabel("Label field"),
			core.InputControl("", Name("visual_label"), Value(data.VisualLabel)),
			core.FieldLabel("Value field"),
			core.InputControl("", Name("visual_value"), Value(data.VisualValue)),
			core.FieldLabel("Secondary field"),
			core.InputControl("", Name("visual_secondary"), Value(data.VisualSecondary)),
			core.FieldLabel("Layout X"),
			core.InputControl("", Name("layout_x"), Value(data.LayoutX)),
			core.FieldLabel("Layout Y"),
			core.InputControl("", Name("layout_y"), Value(data.LayoutY)),
			core.FieldLabel("Layout W"),
			core.InputControl("", Name("layout_w"), Value(data.LayoutW)),
			core.FieldLabel("Layout H"),
			core.InputControl("", Name("layout_h"), Value(data.LayoutH)),
			core.PrimaryButton("", Type("submit"), Text(data.SubmitLabel)),
		),
	)
}

func defaultWidgetFormData(action string) dashboardWidgetFormData {
	return dashboardWidgetFormData{
		Title:            "Add widget",
		Action:           action,
		SubmitLabel:      "Create widget",
		PageName:         domain.DefaultDashboardPageName,
		SourceKind:       "sql_query",
		VisualKind:       "table",
		VisualLegendMode: "auto",
		VisualLegendPos:  "top",
		LayoutX:          "0",
		LayoutY:          "0",
		LayoutW:          "4",
		LayoutH:          "3",
	}
}

func widgetFormDataFromWidget(widget *domain.DashboardWidget, action, submitLabel, title string) dashboardWidgetFormData {
	data := defaultWidgetFormData(action)
	data.Title = title
	data.SubmitLabel = submitLabel
	data.PageName = domain.NormalizeDashboardPageName(widget.PageName)
	data.Name = widget.Name
	data.Description = widget.Description
	data.SourceKind = string(widget.Source.Kind)
	data.LayoutX = strconv.Itoa(widget.Layout.X)
	data.LayoutY = strconv.Itoa(widget.Layout.Y)
	data.LayoutW = strconv.Itoa(widget.Layout.W)
	data.LayoutH = strconv.Itoa(widget.Layout.H)

	switch widget.Source.Kind {
	case domain.DashboardWidgetSourceSQLQuery:
		if widget.Source.SQLQuery != nil {
			data.SQL = widget.Source.SQLQuery.SQL
		}
	case domain.DashboardWidgetSourceNotebookCell:
		if widget.Source.NotebookCell != nil {
			data.NotebookID = widget.Source.NotebookCell.NotebookID
			data.CellID = widget.Source.NotebookCell.CellID
		}
	case domain.DashboardWidgetSourceSemanticQuery:
		if widget.Source.SemanticQuery != nil {
			data.SemanticModelID = widget.Source.SemanticQuery.SemanticModelID
			data.Metrics = strings.Join(widget.Source.SemanticQuery.Metrics, ", ")
			data.Dimensions = strings.Join(widget.Source.SemanticQuery.Dimensions, ", ")
			data.Filters = strings.Join(widget.Source.SemanticQuery.Filters, ", ")
			data.OrderBy = strings.Join(widget.Source.SemanticQuery.OrderBy, ", ")
			if widget.Source.SemanticQuery.Limit != nil {
				data.Limit = strconv.Itoa(*widget.Source.SemanticQuery.Limit)
			}
			if widget.Source.SemanticQuery.TimeGrain != nil {
				data.TimeGrain = *widget.Source.SemanticQuery.TimeGrain
			}
		}
	}

	if widget.VisualSpec != nil {
		data.VisualKind = string(widget.VisualSpec.Kind)
		data.VisualTitle = widget.VisualSpec.Title
		data.VisualSubtitle = widget.VisualSpec.Subtitle
		data.VisualLegendMode = "auto"
		if widget.VisualSpec.Legend != nil {
			if *widget.VisualSpec.Legend {
				data.VisualLegendMode = "show"
			} else {
				data.VisualLegendMode = "hide"
			}
		}
		data.VisualLegendPos = "top"
		if widget.VisualSpec.LegendPosition != nil && strings.TrimSpace(string(*widget.VisualSpec.LegendPosition)) != "" {
			data.VisualLegendPos = string(*widget.VisualSpec.LegendPosition)
		}
		if widget.VisualSpec.ChartType != nil {
			data.ChartType = string(*widget.VisualSpec.ChartType)
		}
		if widget.VisualSpec.Encodings.X != nil {
			data.VisualX = widget.VisualSpec.Encodings.X.Field
		}
		if widget.VisualSpec.Encodings.Y != nil {
			data.VisualY = widget.VisualSpec.Encodings.Y.Field
		}
		if widget.VisualSpec.Encodings.Series != nil {
			data.VisualSeries = widget.VisualSpec.Encodings.Series.Field
		}
		if widget.VisualSpec.Encodings.Label != nil {
			data.VisualLabel = widget.VisualSpec.Encodings.Label.Field
		}
		if widget.VisualSpec.Encodings.Value != nil {
			data.VisualValue = widget.VisualSpec.Encodings.Value.Field
		}
		if widget.VisualSpec.Encodings.Secondary != nil {
			data.VisualSecondary = widget.VisualSpec.Encodings.Secondary.Field
		}
	}

	return data
}

func formPage(principal domain.ContextPrincipal, title, active, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	return core.AppPage(
		title,
		active,
		principal,
		core.FormPageLayout("Discover", title, "Dashboard authoring uses the shared single-surface form layout.",
			Form(
				Class("stack-form [&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0"),
				Method("post"),
				Action(action),
				Group(nodes),
				Div(Class("form-actions mt-2"), core.PrimaryButton("", Type("submit"), Text("Save"))),
			),
		),
	)
}

func selectedValue(current, expected string) Node {
	if current == expected {
		return Selected()
	}
	return nil
}

func parseIntWithDefault(value string, fallback int) int {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	return core.FormatTimeDisplay(ts)
}

func formatTimePtr(ts *time.Time) string {
	if ts == nil || ts.IsZero() {
		return "-"
	}
	return core.FormatTimeDisplay(*ts)
}

func labelClass(tone string) string {
	base := "inline-flex items-center rounded-full border border-transparent px-2 py-0.5 text-xs font-medium"
	switch tone {
	case "accent":
		return core.ClassNames(base, "bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]")
	case "attention":
		return core.ClassNames(base, "bg-[var(--bgColor-attention-muted)] text-[var(--fgColor-attention)]")
	case "success":
		return core.ClassNames(base, "bg-[var(--bgColor-success-muted)] text-[var(--fgColor-success)]")
	case "severe":
		return core.ClassNames(base, "bg-[var(--bgColor-danger-muted)] text-[var(--fgColor-danger)]")
	default:
		return core.ClassNames(base, "bg-[var(--bgColor-muted)] text-[var(--fgColor-default)]")
	}
}

func statusLabel(text, tone string) Node {
	return Span(Class(labelClass(tone)), Text(text))
}

func chartHost(widget dashboardsvc.ResolvedWidget, editMode bool) Node {
	nodes := []Node{
		Class("dashboard-chart-host block min-h-[19rem] w-full"),
		Attr("data-widget-id", widget.Widget.ID),
		Attr("data-widget-origin-key", widget.Widget.FilterOriginKey),
	}
	if editMode {
		nodes = append(nodes, Attr("data-chart-payload", widgetPayload(widget.Widget.Name, widget.Columns, widget.Rows, widget.RowCount, dashboardChartVisual(widget.Widget.VisualSpec), widget.Interaction, widget.Page, widget.Sort)))
	} else {
		nodes = append(nodes, Attr("data-ignore-morph", ""))
	}
	return El("duck-chart", nodes...)
}

func metricHost(widget dashboardsvc.ResolvedWidget, editMode bool) Node {
	nodes := []Node{
		Class("dashboard-metric-host block min-h-[10.5rem] w-full"),
		Attr("data-widget-id", widget.Widget.ID),
		Attr("data-widget-origin-key", widget.Widget.FilterOriginKey),
	}
	if editMode {
		nodes = append(nodes, Attr("data-metric-payload", widgetPayload(widget.Widget.Name, widget.Columns, widget.Rows, widget.RowCount, widget.Widget.VisualSpec, widget.Interaction, widget.Page, widget.Sort)))
	} else {
		nodes = append(nodes, Attr("data-ignore-morph", ""))
	}
	return El("duck-metric", nodes...)
}

func tableHost(widget dashboardsvc.ResolvedWidget, editMode bool) Node {
	nodes := []Node{
		Class("dashboard-table-host block h-full min-h-0 w-full"),
		Attr("data-widget-id", widget.Widget.ID),
		Attr("data-widget-origin-key", widget.Widget.FilterOriginKey),
	}
	if editMode {
		nodes = append(nodes, Attr("data-table-payload", widgetPayload(widget.Widget.Name, widget.Columns, widget.Rows, widget.RowCount, widget.Widget.VisualSpec, widget.Interaction, widget.Page, widget.Sort)))
	} else {
		nodes = append(nodes, Attr("data-ignore-morph", ""))
	}
	return El("duck-table", nodes...)
}

type widgetRenderPayload struct {
	Name        string                                  `json:"name"`
	Columns     []string                                `json:"columns"`
	Rows        [][]interface{}                         `json:"rows"`
	RowCount    int                                     `json:"row_count"`
	Visual      *domain.VisualSpec                      `json:"visual"`
	Interaction *dashboardsvc.ResolvedWidgetInteraction `json:"interaction,omitempty"`
	Page        *dashboardsvc.ResolvedWidgetPage        `json:"page,omitempty"`
	Sort        *dashboardsvc.ResolvedWidgetSort        `json:"sort,omitempty"`
}

func dashboardChartVisual(visual *domain.VisualSpec) *domain.VisualSpec {
	if visual == nil {
		return nil
	}
	copy := *visual
	copy.Title = ""
	copy.Subtitle = ""
	return &copy
}

func widgetPayload(name string, columns []string, rows [][]interface{}, rowCount int, visual *domain.VisualSpec, interaction *dashboardsvc.ResolvedWidgetInteraction, page *dashboardsvc.ResolvedWidgetPage, sort *dashboardsvc.ResolvedWidgetSort) string {
	payload, err := json.Marshal(widgetRenderPayload{Name: name, Columns: columns, Rows: rows, RowCount: rowCount, Visual: visual, Interaction: interaction, Page: page, Sort: sort})
	if err != nil {
		return "{}"
	}
	return string(payload)
}

type dashboardWidgetPayloadEvent struct {
	WidgetID string              `json:"widget_id"`
	Version  string              `json:"version"`
	Payload  widgetRenderPayload `json:"payload"`
}

func dashboardWidgetPayload(widget dashboardsvc.ResolvedWidget, version string) (dashboardWidgetPayloadEvent, bool) {
	if widget.Widget.ID == "" || widget.Widget.VisualSpec == nil {
		return dashboardWidgetPayloadEvent{}, false
	}
	switch widget.Widget.VisualSpec.Kind {
	case domain.VisualOutputChart, domain.VisualOutputTable, domain.VisualOutputMetric:
	default:
		return dashboardWidgetPayloadEvent{}, false
	}
	return dashboardWidgetPayloadEvent{
		WidgetID: widget.Widget.ID,
		Version:  version,
		Payload: widgetRenderPayload{
			Name:        widget.Widget.Name,
			Columns:     widget.Columns,
			Rows:        widget.Rows,
			RowCount:    widget.RowCount,
			Visual:      dashboardChartVisual(widget.Widget.VisualSpec),
			Interaction: widget.Interaction,
			Page:        widget.Page,
			Sort:        widget.Sort,
		},
	}, true
}

func visualMetricCard(title string, value interface{}, secondary string) Node {
	return Div(
		Class("flex min-h-[10.5rem] flex-1 flex-col items-center justify-center gap-3 text-center"),
		P(Class("m-0 text-[clamp(2.9rem,6vw,4.9rem)] font-black leading-none tracking-[-0.06em] text-[var(--fgColor-default)]"), Text(formatDashboardMetricValue(title, value))),
		P(Class("m-0 text-[11px] font-bold uppercase tracking-[0.12em] text-[var(--fgColor-muted)]"), Text(secondary)),
	)
}

func formatDashboardMetricValue(title string, value interface{}) string {
	numeric, ok := dashboardNumericValue(value)
	if !ok {
		return fmt.Sprint(value)
	}
	if dashboardColumnLooksCurrency(title) {
		return formatDashboardCompactCurrency(numeric)
	}
	if math.Abs(numeric-math.Round(numeric)) < 0.000001 {
		return formatDashboardInteger(int64(math.Round(numeric)))
	}
	return formatDashboardDecimal(numeric, 2)
}

func formatDashboardCellValue(column string, value interface{}) string {
	numeric, ok := dashboardNumericValue(value)
	if !ok {
		return fmt.Sprint(value)
	}
	if dashboardColumnLooksCurrency(column) {
		return formatDashboardDecimal(numeric, 2)
	}
	if math.Abs(numeric-math.Round(numeric)) < 0.000001 {
		return formatDashboardInteger(int64(math.Round(numeric)))
	}
	return formatDashboardDecimal(numeric, 2)
}

func dashboardColumnLooksCurrency(text string) bool {
	text = strings.ToLower(text)
	for _, token := range []string{"revenue", "amount", "gross", "price", "fare", "sales", "cost"} {
		if strings.Contains(text, token) {
			return true
		}
	}
	return false
}

func dashboardColumnLooksNumeric(text string) bool {
	text = strings.ToLower(text)
	for _, token := range []string{"count", "total", "sum", "number", "amount", "revenue", "fare", "price", "share"} {
		if strings.Contains(text, token) {
			return true
		}
	}
	return false
}

func dashboardValueIsNumeric(value interface{}) bool {
	_, ok := dashboardNumericValue(value)
	return ok
}

func dashboardNumericValue(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func formatDashboardCompactCurrency(value float64) string {
	sign := ""
	if value < 0 {
		sign = "-"
		value = math.Abs(value)
	}
	switch {
	case value >= 1_000_000_000:
		return fmt.Sprintf("%s$%sB", sign, trimTrailingZeros(value/1_000_000_000, 2))
	case value >= 1_000_000:
		return fmt.Sprintf("%s$%sM", sign, trimTrailingZeros(value/1_000_000, 2))
	case value >= 1_000:
		return fmt.Sprintf("%s$%sK", sign, trimTrailingZeros(value/1_000, 1))
	default:
		return fmt.Sprintf("%s$%s", sign, formatDashboardDecimal(value, 2))
	}
}

func formatDashboardDecimal(value float64, decimals int) string {
	sign := ""
	if value < 0 {
		sign = "-"
		value = math.Abs(value)
	}
	rounded := fmt.Sprintf("%.*f", decimals, value)
	parts := strings.SplitN(rounded, ".", 2)
	if len(parts) == 1 {
		return sign + formatDashboardIntegerString(parts[0])
	}
	return sign + formatDashboardIntegerString(parts[0]) + "." + parts[1]
}

func formatDashboardInteger(value int64) string {
	sign := ""
	if value < 0 {
		sign = "-"
		value = -value
	}
	return sign + formatDashboardIntegerString(strconv.FormatInt(value, 10))
}

func formatDashboardIntegerString(raw string) string {
	if len(raw) <= 3 {
		return raw
	}
	var parts []string
	for len(raw) > 3 {
		parts = append([]string{raw[len(raw)-3:]}, parts...)
		raw = raw[:len(raw)-3]
	}
	parts = append([]string{raw}, parts...)
	return strings.Join(parts, ",")
}

func trimTrailingZeros(value float64, decimals int) string {
	out := strconv.FormatFloat(value, 'f', decimals, 64)
	out = strings.TrimRight(out, "0")
	out = strings.TrimRight(out, ".")
	return out
}

func defaultVisualTitle(spec *domain.VisualSpec, fallback string) string {
	if spec == nil || strings.TrimSpace(spec.Title) == "" {
		return fallback
	}
	return spec.Title
}

func parseFormOrRenderBadRequest(w http.ResponseWriter, r *http.Request) bool {
	if err := r.ParseForm(); err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Unable to parse form."))
		return false
	}
	return true
}

func formString(values map[string][]string, key string) string {
	if values == nil {
		return ""
	}
	return strings.TrimSpace(first(values[key]))
}

func formOptionalString(values map[string][]string, key string) *string {
	v := formString(values, key)
	if v == "" {
		return nil
	}
	return &v
}

func formCSV(values map[string][]string, key string) []string {
	raw := formString(values, key)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func dashIfEmpty(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

func visualSpecFromForm(values url.Values) (*domain.VisualSpec, error) {
	if values.Get("visual_kind") == "" &&
		values.Get("chart_type") == "" &&
		values.Get("visual_title") == "" &&
		values.Get("visual_x") == "" &&
		values.Get("visual_y") == "" &&
		values.Get("visual_value") == "" &&
		values.Get("visual_label") == "" {
		return nil, nil
	}
	kind := domain.VisualOutputKind(formString(values, "visual_kind"))
	if kind == "" {
		kind = domain.VisualOutputTable
	}
	spec := &domain.VisualSpec{
		Kind:         kind,
		Title:        formString(values, "visual_title"),
		Subtitle:     formString(values, "visual_subtitle"),
		ColorPalette: formString(values, "visual_palette"),
	}
	switch legendMode := formString(values, "visual_legend_mode"); legendMode {
	case "show":
		v := true
		spec.Legend = &v
	case "hide":
		v := false
		spec.Legend = &v
	}
	if legendPosition := formString(values, "visual_legend_position"); legendPosition != "" {
		position := domain.VisualLegendPosition(legendPosition)
		spec.LegendPosition = &position
	}
	if stacked := values.Get("visual_stacked"); stacked != "" {
		v := stacked == "on" || stacked == "true"
		spec.Stacked = &v
	}
	if chartType := formString(values, "chart_type"); chartType != "" {
		ct := domain.VisualChartType(chartType)
		spec.ChartType = &ct
	}
	if field := formString(values, "visual_x"); field != "" {
		spec.Encodings.X = &domain.VisualFieldBinding{Field: field}
	}
	if field := formString(values, "visual_y"); field != "" {
		spec.Encodings.Y = &domain.VisualFieldBinding{Field: field}
	}
	if field := formString(values, "visual_series"); field != "" {
		spec.Encodings.Series = &domain.VisualFieldBinding{Field: field}
	}
	if field := formString(values, "visual_label"); field != "" {
		spec.Encodings.Label = &domain.VisualFieldBinding{Field: field}
	}
	if field := formString(values, "visual_value"); field != "" {
		spec.Encodings.Value = &domain.VisualFieldBinding{Field: field}
	}
	if field := formString(values, "visual_secondary"); field != "" {
		spec.Encodings.Secondary = &domain.VisualFieldBinding{Field: field}
	}
	return spec, spec.Validate()
}

func dashboardWidgetSourceFromForm(values url.Values) (domain.DashboardWidgetSource, error) {
	kind := domain.DashboardWidgetSourceKind(formString(values, "source_kind"))
	switch kind {
	case domain.DashboardWidgetSourceSQLQuery:
		return domain.DashboardWidgetSource{
			Kind: kind,
			SQLQuery: &domain.DashboardSQLQuerySource{
				SQL: formString(values, "sql"),
			},
		}, nil
	case domain.DashboardWidgetSourceNotebookCell:
		return domain.DashboardWidgetSource{
			Kind: kind,
			NotebookCell: &domain.DashboardNotebookCellSource{
				NotebookID: formString(values, "notebook_id"),
				CellID:     formString(values, "cell_id"),
			},
		}, nil
	case domain.DashboardWidgetSourceSemanticQuery:
		source := domain.DashboardWidgetSource{
			Kind: kind,
			SemanticQuery: &domain.DashboardSemanticQuerySource{
				SemanticModelID: formString(values, "semantic_model_id"),
				Metrics:         formCSV(values, "metrics"),
				Dimensions:      formCSV(values, "dimensions"),
				Filters:         formCSV(values, "filters"),
				OrderBy:         formCSV(values, "order_by"),
			},
		}
		if rawLimit := formString(values, "limit"); rawLimit != "" {
			limit, err := strconv.Atoi(rawLimit)
			if err != nil {
				return domain.DashboardWidgetSource{}, fmt.Errorf("limit must be an integer")
			}
			source.SemanticQuery.Limit = &limit
		}
		if timeGrain := strings.TrimSpace(formString(values, "time_grain")); timeGrain != "" {
			source.SemanticQuery.TimeGrain = &timeGrain
		}
		return source, nil
	default:
		return domain.DashboardWidgetSource{}, fmt.Errorf("unsupported source kind %q", string(kind))
	}
}

func parseInt(v string) (int, error) {
	return strconv.Atoi(v)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func renderHTMLString(node Node) (string, error) {
	var buf bytes.Buffer
	if err := node.Render(&buf); err != nil {
		return "", err
	}
	return buf.String(), nil
}
