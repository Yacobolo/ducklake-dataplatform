package ui

import (
	"fmt"
	"strconv"
	"strings"

	"duck-demo/internal/domain"
	dashboardsvc "duck-demo/internal/service/dashboard"

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
	SemanticModelName string
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
			Td(A(Href(row.URL), Text(row.Name))),
			Td(Text(row.Description)),
			Td(Text(row.Owner)),
			Td(Text(row.Updated)),
		))
	}
	tableNode := Node(emptyStateCard("No dashboards yet.", "New dashboard", "/ui/dashboards/new"))
	if len(tableRows) > 0 {
		tableNode = Div(Class(cardClass("table-wrap")), Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("Description")), Th(Text("Owner")), Th(Text("Updated")))), TBody(Group(tableRows))))
	}
	return appPage("Dashboards", "dashboards", principal, pageToolbar("/ui/dashboards/new", "New dashboard"), tableNode, paginationCard("/ui/dashboards", page, total))
}

func dashboardsNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Dashboard", "dashboards", "/ui/dashboards", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Description")),
		Textarea(Name("description")),
	)
}

func dashboardsEditPage(principal domain.ContextPrincipal, dashboard *domain.Dashboard, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Edit Dashboard", "dashboards", "/ui/dashboards/"+dashboard.ID+"/update", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Value(dashboard.Name), Required()),
		Label(Text("Description")),
		Textarea(Name("description"), Text(dashboard.Description)),
	)
}

func dashboardWidgetEditPage(principal domain.ContextPrincipal, dashboard *domain.Dashboard, widget *domain.DashboardWidget, csrfFieldProvider func() Node) Node {
	return appPage(
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
		widgetNodes = append(widgetNodes, emptyStateCard("No widgets yet.", "Add widget below", "#dashboard-widget-form"))
	}

	return appPage(
		"Dashboard: "+d.Dashboard.Name,
		"dashboards",
		d.Principal,
		Div(
			Class("dashboard-toolbar"),
			H1(Text(d.Dashboard.Name)),
			P(Class("color-fg-muted"), Text(d.Dashboard.Description)),
			Div(Class("button-row"),
				A(Href(d.EditURL), Class(secondaryButtonClass()), Text("Edit")),
				Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldProvider(), Button(Type("submit"), Class("btn btn-danger"), Text("Delete"))),
			),
		),
		dashboardFreshnessCard(d.Freshness, d.FreshnessExplain),
		Div(Class("dashboard-grid"), Group(widgetNodes)),
		dashboardWidgetFormCard(defaultWidgetFormData(d.CreateWidgetURL), d.CSRFFieldProvider),
		Script(Src(uiScriptHref("dashboard.js"))),
	)
}

func dashboardFreshnessCard(status *domain.AssetFreshnessStatus, explanation *domain.AssetFreshnessNode) Node {
	if status == nil {
		return Node(nil)
	}

	upstream := make([]Node, 0)
	if explanation != nil {
		for _, child := range explanation.Upstream {
			upstream = append(upstream, Li(
				Strong(Text(child.AssetKey)),
				Text(" "),
				statusLabel(child.FreshnessStatus, dashboardFreshnessTone(child.FreshnessStatus)),
				Text(" "),
				Span(Class(mutedClass()), Text(child.Reason)),
			))
		}
	}
	upstreamNode := Node(P(Class(mutedClass()), Text("No upstream blockers detected.")))
	if len(upstream) > 0 {
		upstreamNode = Ul(Group(upstream))
	}

	return Div(
		Class(cardClass()),
		H2(Text("Freshness")),
		Div(Class("button-row"),
			statusLabel(status.FreshnessStatus, dashboardFreshnessTone(status.FreshnessStatus)),
			Span(Class(mutedClass()), Text(status.Reason)),
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
		Class("dashboard-widget"),
		H2(Text(widget.Widget.Name)),
		P(Class("color-fg-muted"), Text(widget.Widget.Description)),
		Div(Class("button-row"),
			A(Href(deleteBaseURL+"/widgets/"+widget.Widget.ID+"/edit"), Class(secondaryButtonClass()), Text("Edit widget")),
		),
		content,
		dashboardWidgetDataDetails(widget),
		generatedSQL,
		Form(Method("post"), Action(deleteBaseURL+"/widgets/"+widget.Widget.ID+"/delete"), csrfFieldProvider(), Button(Type("submit"), Class("btn btn-danger btn-sm"), Text("Delete widget"))),
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
	return Div(Class("table-wrap"), Table(Class("data-table"), THead(Tr(Group(headers))), TBody(Group(rows))))
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
	return Div(
		Class(cardClass()),
		ID("dashboard-widget-form"),
		H2(Text(data.Title)),
		Form(
			Method("post"),
			Action(data.Action),
			csrfFieldProvider(),
			Label(Text("Name")),
			Input(Name("name"), Value(data.Name), Required()),
			Label(Text("Description")),
			Textarea(Name("description"), Text(data.Description)),
			Label(Text("Source kind")),
			Select(Name("source_kind"),
				Option(Value("sql_query"), selectedValue(data.SourceKind, "sql_query"), Text("sql_query")),
				Option(Value("notebook_cell"), selectedValue(data.SourceKind, "notebook_cell"), Text("notebook_cell")),
				Option(Value("semantic_query"), selectedValue(data.SourceKind, "semantic_query"), Text("semantic_query")),
			),
			Label(Text("SQL query")),
			Textarea(Name("sql"), Text(data.SQL)),
			Label(Text("Notebook ID")),
			Input(Name("notebook_id"), Value(data.NotebookID)),
			Label(Text("Cell ID")),
			Input(Name("cell_id"), Value(data.CellID)),
			Label(Text("Project name")),
			Input(Name("project_name"), Value(data.ProjectName)),
			Label(Text("Semantic model")),
			Input(Name("semantic_model_name"), Value(data.SemanticModelName)),
			Label(Text("Metrics (comma separated)")),
			Input(Name("metrics"), Value(data.Metrics)),
			Label(Text("Dimensions (comma separated)")),
			Input(Name("dimensions"), Value(data.Dimensions)),
			Label(Text("Filters (comma separated)")),
			Input(Name("filters"), Value(data.Filters)),
			Label(Text("Order by (comma separated)")),
			Input(Name("order_by"), Value(data.OrderBy)),
			Label(Text("Limit")),
			Input(Name("limit"), Value(data.Limit)),
			Label(Text("Time grain")),
			Input(Name("time_grain"), Value(data.TimeGrain)),
			Label(Text("Visual kind")),
			Select(Name("visual_kind"),
				Option(Value("table"), selectedValue(data.VisualKind, "table"), Text("table")),
				Option(Value("metric"), selectedValue(data.VisualKind, "metric"), Text("metric")),
				Option(Value("chart"), selectedValue(data.VisualKind, "chart"), Text("chart")),
			),
			Label(Text("Chart type")),
			Select(Name("chart_type"),
				Option(Value("bar"), selectedValue(data.ChartType, "bar"), Text("bar")),
				Option(Value("line"), selectedValue(data.ChartType, "line"), Text("line")),
				Option(Value("area"), selectedValue(data.ChartType, "area"), Text("area")),
				Option(Value("pie"), selectedValue(data.ChartType, "pie"), Text("pie")),
				Option(Value("doughnut"), selectedValue(data.ChartType, "doughnut"), Text("doughnut")),
				Option(Value("scatter"), selectedValue(data.ChartType, "scatter"), Text("scatter")),
				Option(Value("stacked_bar"), selectedValue(data.ChartType, "stacked_bar"), Text("stacked_bar")),
			),
			Label(Text("Title")),
			Input(Name("visual_title"), Value(data.VisualTitle)),
			Label(Text("Subtitle")),
			Input(Name("visual_subtitle"), Value(data.VisualSubtitle)),
			Label(Text("X field")),
			Input(Name("visual_x"), Value(data.VisualX)),
			Label(Text("Y field")),
			Input(Name("visual_y"), Value(data.VisualY)),
			Label(Text("Series field")),
			Input(Name("visual_series"), Value(data.VisualSeries)),
			Label(Text("Label field")),
			Input(Name("visual_label"), Value(data.VisualLabel)),
			Label(Text("Value field")),
			Input(Name("visual_value"), Value(data.VisualValue)),
			Label(Text("Secondary field")),
			Input(Name("visual_secondary"), Value(data.VisualSecondary)),
			Label(Text("Layout X")),
			Input(Name("layout_x"), Value(data.LayoutX)),
			Label(Text("Layout Y")),
			Input(Name("layout_y"), Value(data.LayoutY)),
			Label(Text("Layout W")),
			Input(Name("layout_w"), Value(data.LayoutW)),
			Label(Text("Layout H")),
			Input(Name("layout_h"), Value(data.LayoutH)),
			Button(Type("submit"), Class(primaryButtonClass()), Text(data.SubmitLabel)),
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
			data.SemanticModelName = widget.Source.SemanticQuery.SemanticModelName
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
