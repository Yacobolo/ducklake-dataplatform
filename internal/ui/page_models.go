package ui

import (
	"duck-demo/internal/domain"

	gomponents "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	html "maragu.dev/gomponents/html"
)

type modelsListRowData struct {
	FilterValue   string
	DetailURL     string
	ModelName     string
	Materialized  string
	Dependencies  string
	UpdatedAtText string
}

type modelsListPageData struct {
	Principal domain.ContextPrincipal
	Rows      []modelsListRowData
	Page      domain.PageRequest
	Total     int64
}

func modelsListPage(d modelsListPageData) gomponents.Node {
	rows := make([]gomponents.Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		rows = append(rows, html.Tr(
			data.Show(containsExpr(row.FilterValue)),
			html.Td(html.A(html.Href(row.DetailURL), gomponents.Text(row.ModelName))),
			html.Td(gomponents.Text(row.Materialized)),
			html.Td(gomponents.Text(row.Dependencies)),
			html.Td(gomponents.Text(row.UpdatedAtText)),
		))
	}

	return appPage(
		"Models",
		"models",
		d.Principal,
		html.Div(html.Class(cardClass()), html.A(html.Href("/ui/models/new"), gomponents.Text("+ New model"))),
		html.Div(
			data.Signals(map[string]any{"q": ""}),
			html.Div(html.Class(cardClass()), html.Label(gomponents.Text("Quick filter")), html.Input(html.Type("text"), data.Bind("q"), html.Placeholder("Filter by project.model or materialization"))),
			html.Div(html.Class(cardClass("table-wrap")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Model")), html.Th(gomponents.Text("Materialization")), html.Th(gomponents.Text("Dependencies")), html.Th(gomponents.Text("Updated")))), html.TBody(gomponents.Group(rows)))),
		),
		paginationCard("/ui/models", d.Page, d.Total),
	)
}

type modelTestRowData struct {
	Name      string
	TestType  string
	Column    string
	DeleteURL string
}

type modelsDetailPageData struct {
	Principal         domain.ContextPrincipal
	ProjectName       string
	ModelName         string
	QualifiedName     string
	Materialization   string
	Owner             string
	DependsOn         string
	ConfigText        string
	EditURL           string
	DeleteURL         string
	NewTestURL        string
	TriggerRunURL     string
	CancelRunURL      string
	DefaultSelector   string
	SQL               string
	Tests             []modelTestRowData
	TriggerProject    string
	TriggerModel      string
	CSRFFieldProvider func() gomponents.Node
}

func modelsDetailPage(d modelsDetailPageData) gomponents.Node {
	testRows := make([]gomponents.Node, 0, len(d.Tests))
	for i := range d.Tests {
		t := d.Tests[i]
		testRows = append(testRows, html.Tr(
			html.Td(gomponents.Text(t.Name)),
			html.Td(gomponents.Text(t.TestType)),
			html.Td(gomponents.Text(t.Column)),
			html.Td(
				html.Form(
					html.Method("post"),
					html.Action(t.DeleteURL),
					d.CSRFFieldProvider(),
					html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Delete")),
				),
			),
		))
	}

	return appPage(
		"Model: "+d.QualifiedName,
		"models",
		d.Principal,
		html.Div(
			html.Class(cardClass()),
			html.P(gomponents.Text("Materialization: "+d.Materialization)),
			html.P(gomponents.Text("Owner: "+d.Owner)),
			html.P(gomponents.Text("Depends on: "+d.DependsOn)),
			html.P(gomponents.Text("Config: "+d.ConfigText)),
			html.A(html.Href(d.EditURL), gomponents.Text("Edit model")),
			html.Form(html.Method("post"), html.Action(d.DeleteURL), d.CSRFFieldProvider(), html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Delete model"))),
			html.A(html.Href(d.NewTestURL), gomponents.Text("+ New test")),
			html.Form(
				html.Method("post"),
				html.Action(d.TriggerRunURL),
				d.CSRFFieldProvider(),
				html.Input(html.Type("hidden"), html.Name("project_name"), html.Value(d.TriggerProject)),
				html.Input(html.Type("hidden"), html.Name("model_name"), html.Value(d.TriggerModel)),
				html.Label(gomponents.Text("Target catalog")),
				html.Input(html.Name("target_catalog"), html.Required()),
				html.Label(gomponents.Text("Target schema")),
				html.Input(html.Name("target_schema"), html.Required()),
				html.Label(gomponents.Text("Selector")),
				html.Input(html.Name("selector"), html.Value(d.DefaultSelector)),
				html.Button(html.Type("submit"), html.Class(primaryButtonClass()), gomponents.Text("Trigger model run")),
			),
			html.Form(
				html.Method("post"),
				html.Action(d.CancelRunURL),
				d.CSRFFieldProvider(),
				html.Label(gomponents.Text("Run ID to cancel")),
				html.Input(html.Name("run_id")),
				html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Cancel model run")),
			),
		),
		html.Div(html.Class(cardClass()), html.H2(gomponents.Text("SQL")), html.Pre(gomponents.Text(d.SQL))),
		html.Div(html.Class(cardClass("table-wrap")), html.H2(gomponents.Text("Tests")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Type")), html.Th(gomponents.Text("Column")), html.Th(gomponents.Text("Actions")))), html.TBody(gomponents.Group(testRows)))),
	)
}
