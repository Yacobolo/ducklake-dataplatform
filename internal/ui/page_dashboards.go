package ui

import (
	"fmt"
	"strconv"

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
	BaseURL           string
	EditURL           string
	DeleteURL         string
	CreateWidgetURL   string
	CSRFFieldProvider func() Node
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
		Div(Class("dashboard-grid"), Group(widgetNodes)),
		dashboardWidgetCreateCard(d.Dashboard.ID, d.CreateWidgetURL, d.CSRFFieldProvider),
		Script(Src(uiScriptHref("dashboard.js"))),
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

func dashboardWidgetCreateCard(dashboardID, action string, csrfFieldProvider func() Node) Node {
	return Div(
		Class(cardClass()),
		ID("dashboard-widget-form"),
		H2(Text("Add widget")),
		Form(
			Method("post"),
			Action(action),
			csrfFieldProvider(),
			Label(Text("Name")),
			Input(Name("name"), Required()),
			Label(Text("Description")),
			Textarea(Name("description")),
			Label(Text("Source kind")),
			Select(Name("source_kind"),
				Option(Value("sql_query"), Text("sql_query")),
				Option(Value("notebook_cell"), Text("notebook_cell")),
				Option(Value("semantic_query"), Text("semantic_query")),
			),
			Label(Text("SQL query")),
			Textarea(Name("sql")),
			Label(Text("Notebook ID")),
			Input(Name("notebook_id")),
			Label(Text("Cell ID")),
			Input(Name("cell_id")),
			Label(Text("Project name")),
			Input(Name("project_name")),
			Label(Text("Semantic model")),
			Input(Name("semantic_model_name")),
			Label(Text("Metrics (comma separated)")),
			Input(Name("metrics")),
			Label(Text("Dimensions (comma separated)")),
			Input(Name("dimensions")),
			Label(Text("Filters (comma separated)")),
			Input(Name("filters")),
			Label(Text("Order by (comma separated)")),
			Input(Name("order_by")),
			Label(Text("Limit")),
			Input(Name("limit")),
			Label(Text("Time grain")),
			Input(Name("time_grain")),
			Label(Text("Visual kind")),
			Select(Name("visual_kind"),
				Option(Value("table"), Text("table")),
				Option(Value("metric"), Text("metric")),
				Option(Value("chart"), Text("chart")),
			),
			Label(Text("Chart type")),
			Select(Name("chart_type"),
				Option(Value("bar"), Text("bar")),
				Option(Value("line"), Text("line")),
				Option(Value("area"), Text("area")),
				Option(Value("pie"), Text("pie")),
				Option(Value("doughnut"), Text("doughnut")),
				Option(Value("scatter"), Text("scatter")),
				Option(Value("stacked_bar"), Text("stacked_bar")),
			),
			Label(Text("Title")),
			Input(Name("visual_title")),
			Label(Text("Subtitle")),
			Input(Name("visual_subtitle")),
			Label(Text("X field")),
			Input(Name("visual_x")),
			Label(Text("Y field")),
			Input(Name("visual_y")),
			Label(Text("Series field")),
			Input(Name("visual_series")),
			Label(Text("Label field")),
			Input(Name("visual_label")),
			Label(Text("Value field")),
			Input(Name("visual_value")),
			Label(Text("Layout X")),
			Input(Name("layout_x"), Value("0")),
			Label(Text("Layout Y")),
			Input(Name("layout_y"), Value("0")),
			Label(Text("Layout W")),
			Input(Name("layout_w"), Value("4")),
			Label(Text("Layout H")),
			Input(Name("layout_h"), Value("3")),
			Button(Type("submit"), Class(primaryButtonClass()), Text("Create widget")),
		),
	)
}

func parseIntWithDefault(value string, fallback int) int {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
