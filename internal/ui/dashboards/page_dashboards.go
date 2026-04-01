package dashboards

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"
	dashboardsvc "duck-demo/internal/service/dashboard"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
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
	Freshness         *domain.AssetFreshnessStatus
	FreshnessExplain  *domain.AssetFreshnessNode
	BaseURL           string
	EditURL           string
	DeleteURL         string
	CreateWidgetURL   string
	CSRFFieldProvider func() Node
}

type dashboardWidgetFormData struct {
	Title             string
	Action            string
	SubmitLabel       string
	Name              string
	Description       string
	SourceKind        string
	SQL               string
	NotebookID        string
	CellID            string
	ProjectName       string
	semanticModelName string
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
	return formPage(principal, "Edit Dashboard", "dashboards", "/ui/dashboards/"+dashboard.ID+"/update", csrfFieldProvider,
		core.FieldLabel("Name"),
		core.InputControl("", Name("name"), Value(dashboard.Name), Required()),
		core.FieldLabel("Description"),
		core.TextareaControl("", Name("description"), Text(dashboard.Description)),
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
	widgetNodes := make([]Node, 0, len(d.Widgets))
	for _, widget := range d.Widgets {
		widgetNodes = append(widgetNodes, dashboardWidgetCard(widget, d.BaseURL, d.CSRFFieldProvider))
	}
	if len(widgetNodes) == 0 {
		widgetNodes = append(widgetNodes, core.SectionSurface(core.EmptyState("inbox", "No widgets yet.", "Add the first widget below to turn this dashboard into a working surface.", core.PrimaryLink("#dashboard-widget-form", "", Text("Add widget below")))))
	}

	return core.AppPage(
		"Dashboard: "+d.Dashboard.Name,
		"dashboards",
		d.Principal,
		core.PageHeader("Discover", d.Dashboard.Name, d.Dashboard.Description,
			core.SecondaryLink(d.EditURL, "", Text("Edit")),
			Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete"))),
		),
		dashboardFreshnessCard(d.Freshness, d.FreshnessExplain),
		Div(Class("grid gap-4 md:grid-cols-2 xl:grid-cols-12 [&>*]:xl:col-span-6"), Group(widgetNodes)),
		dashboardWidgetFormCard(defaultWidgetFormData(d.CreateWidgetURL), d.CSRFFieldProvider),
		Script(Src(core.UIScriptHref("dashboard.js"))),
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
		Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-xs"),
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

func dashboardWidgetCard(widget dashboardsvc.ResolvedWidget, deleteBaseURL string, csrfFieldProvider func() Node) Node {
	content := Node(nil)
	switch {
	case widget.Widget.VisualSpec != nil && widget.Widget.VisualSpec.Kind == domain.VisualOutputMetric:
		field := ""
		if widget.Widget.VisualSpec.Encodings.Value != nil {
			field = widget.Widget.VisualSpec.Encodings.Value.Field
		}
		value := "-"
		if len(widget.Rows) > 0 {
			idx := 0
			for i, col := range widget.Columns {
				if col == field {
					idx = i
					break
				}
			}
			if len(widget.Rows[0]) > idx {
				value = fmt.Sprint(widget.Rows[0][idx])
			}
		}
		content = visualMetricCard(defaultVisualTitle(widget.Widget.VisualSpec, widget.Widget.Name), value, fmt.Sprintf("%d row(s)", widget.RowCount))
	case widget.Widget.VisualSpec != nil && widget.Widget.VisualSpec.Kind == domain.VisualOutputChart:
		content = chartHost(widget.Columns, widget.Rows, widget.Widget.VisualSpec)
	default:
		content = dashboardWidgetTable(widget)
	}

	generatedSQL := Node(nil)
	if widget.GeneratedSQL != "" {
		generatedSQL = Details(Summary(Text("Generated SQL")), Pre(Text(widget.GeneratedSQL)))
	}

	return Div(
		Class("grid gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-sm"),
		core.SectionHeader(widget.Widget.Name, widget.Widget.Description, core.SecondaryLink(deleteBaseURL+"/widgets/"+widget.Widget.ID+"/edit", "", Text("Edit widget"))),
		content,
		dashboardWidgetDataDetails(widget),
		generatedSQL,
		Form(Method("post"), Action(deleteBaseURL+"/widgets/"+widget.Widget.ID+"/delete"), csrfFieldProvider(), core.DangerButton("small", Type("submit"), Text("Delete widget"))),
	)
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
		headers = append(headers, Th(Text(col)))
	}
	rows := make([]Node, 0, len(widget.Rows))
	for _, row := range widget.Rows {
		cells := make([]Node, 0, len(row))
		for _, cell := range row {
			cells = append(cells, Td(Text(fmt.Sprint(cell))))
		}
		rows = append(rows, Tr(Group(cells)))
	}
	return core.TableContainer("", core.DataTable("", THead(Tr(Group(headers))), TBody(Group(rows))))
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
			core.FieldLabel("Project name"),
			core.InputControl("", Name("project_name"), Value(data.ProjectName)),
			core.FieldLabel("Semantic model"),
			core.InputControl("", Name("semantic_model_name"), Value(data.semanticModelName)),
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
		Title:       "Add widget",
		Action:      action,
		SubmitLabel: "Create widget",
		SourceKind:  "sql_query",
		VisualKind:  "table",
		LayoutX:     "0",
		LayoutY:     "0",
		LayoutW:     "4",
		LayoutH:     "3",
	}
}

func widgetFormDataFromWidget(widget *domain.DashboardWidget, action, submitLabel, title string) dashboardWidgetFormData {
	data := defaultWidgetFormData(action)
	data.Title = title
	data.SubmitLabel = submitLabel
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
			data.ProjectName = widget.Source.SemanticQuery.ProjectName
			data.semanticModelName = widget.Source.SemanticQuery.SemanticModelName
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

func chartHost(columns []string, rows [][]interface{}, visual *domain.VisualSpec) Node {
	return El("duck-chart", Class("block min-h-[20rem]"), Attr("data-chart-payload", chartPayload(columns, rows, visual)))
}

type chartRenderPayload struct {
	Columns []string           `json:"columns"`
	Rows    [][]interface{}    `json:"rows"`
	Visual  *domain.VisualSpec `json:"visual"`
}

func chartPayload(columns []string, rows [][]interface{}, visual *domain.VisualSpec) string {
	payload, err := json.Marshal(chartRenderPayload{Columns: columns, Rows: rows, Visual: visual})
	if err != nil {
		return "{}"
	}
	return string(payload)
}

func visualMetricCard(title string, value interface{}, secondary string) Node {
	return Div(
		Class("relative overflow-hidden rounded-xl border border-[var(--borderColor-default)] bg-[linear-gradient(135deg,var(--bgColor-accent-muted)_0%,var(--bgColor-default)_45%)] p-4 shadow-sm before:absolute before:inset-y-0 before:left-0 before:w-1 before:bg-[var(--borderColor-accent-emphasis)] before:content-['']"),
		P(Class("m-0 text-xs font-semibold text-[var(--fgColor-default)]"), Text(title)),
		P(Class("my-1 text-3xl font-semibold leading-tight text-[var(--fgColor-default)]"), Text(fmt.Sprint(value))),
		P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(secondary)),
	)
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
	if legend := values.Get("visual_legend"); legend != "" {
		v := legend == "on" || legend == "true"
		spec.Legend = &v
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
				ProjectName:       formString(values, "project_name"),
				SemanticModelName: formString(values, "semantic_model_name"),
				Metrics:           formCSV(values, "metrics"),
				Dimensions:        formCSV(values, "dimensions"),
				Filters:           formCSV(values, "filters"),
				OrderBy:           formCSV(values, "order_by"),
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
