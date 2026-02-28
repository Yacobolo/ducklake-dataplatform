package ui

import (
	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
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

func modelsListPage(d modelsListPageData) Node {
	rows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		rows = append(rows, Tr(
			data.Show(containsExpr(row.FilterValue)),
			Td(A(Href(row.DetailURL), Text(row.ModelName))),
			Td(statusLabel(row.Materialized, "accent")),
			Td(Text(row.Dependencies)),
			Td(Text(row.UpdatedAtText)),
		))
	}
	tableNode := Node(emptyStateCard("No models available.", "New model", "/ui/models/new"))
	if len(rows) > 0 {
		tableNode = Div(Class(cardClass("table-wrap")), Table(Class("data-table"), THead(Tr(Th(Text("Model")), Th(Text("Materialization")), Th(Text("Dependencies")), Th(Text("Updated")))), TBody(Group(rows))))
	}

	return appPage(
		"Models",
		"models",
		d.Principal,
		pageToolbar("/ui/models/new", "New model"),
		quickFilterCard("Filter by project.model or materialization"),
		tableNode,
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
	CSRFFieldProvider func() Node
}

func modelsDetailPage(d modelsDetailPageData) Node {
	testRows := make([]Node, 0, len(d.Tests))
	for i := range d.Tests {
		t := d.Tests[i]
		testRows = append(testRows, Tr(
			Td(Text(t.Name)),
			Td(Text(t.TestType)),
			Td(Text(t.Column)),
			Td(Class("text-right"), actionMenu("Actions", actionMenuPost(t.DeleteURL, "Delete test", d.CSRFFieldProvider, true))),
		))
	}

	return appPage(
		"Model: "+d.QualifiedName,
		"models",
		d.Principal,
		Div(
			Class(cardClass()),
			P(Text("Materialization: "), statusLabel(d.Materialization, "accent")),
			P(Text("Owner: "+d.Owner)),
			P(Text("Depends on: "+d.DependsOn)),
			P(Text("Config: "+d.ConfigText)),
			Div(Class("BtnGroup"), A(Href(d.EditURL), Class(secondaryButtonClass()), Text("Edit")), A(Href(d.NewTestURL), Class(primaryButtonClass()), Text("New test")), Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldProvider(), Button(Type("submit"), Class("btn btn-danger"), Text("Delete")))),
			Form(
				Method("post"),
				Action(d.TriggerRunURL),
				d.CSRFFieldProvider(),
				Input(Type("hidden"), Name("project_name"), Value(d.TriggerProject)),
				Input(Type("hidden"), Name("model_name"), Value(d.TriggerModel)),
				Label(Text("Target catalog")),
				Input(Name("target_catalog"), Class("form-control"), Required()),
				Label(Text("Target schema")),
				Input(Name("target_schema"), Class("form-control"), Required()),
				Label(Text("Selector")),
				Input(Name("selector"), Class("form-control"), Value(d.DefaultSelector)),
				Button(Type("submit"), Class(primaryButtonClass()), Text("Trigger model run")),
			),
			Form(
				Method("post"),
				Action(d.CancelRunURL),
				d.CSRFFieldProvider(),
				Label(Text("Run ID to cancel")),
				Input(Name("run_id"), Class("form-control")),
				Button(Type("submit"), Class(secondaryButtonClass()), Text("Cancel model run")),
			),
		),
		Div(Class(cardClass()), H2(Text("SQL")), Pre(Text(d.SQL))),
		Div(Class(cardClass("table-wrap")), H2(Text("Tests")), Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Column")), Th(Class("text-right"), Text("Actions")))), TBody(Group(testRows)))),
	)
}

func modelsNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Model", "models", "/ui/models", csrfFieldProvider,
		Label(Text("Project")),
		Input(Name("project_name"), Required()),
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Materialization")),
		Select(Name("materialization"), Option(Value("VIEW"), Text("VIEW")), Option(Value("TABLE"), Text("TABLE")), Option(Value("INCREMENTAL"), Text("INCREMENTAL")), Option(Value("EPHEMERAL"), Text("EPHEMERAL"))),
		Label(Text("Description")),
		Textarea(Name("description")),
		Label(Text("Tags (comma separated)")),
		Input(Name("tags")),
		Label(Text("SQL")),
		Textarea(Name("sql"), Required()),
	)
}

func modelsEditPage(principal domain.ContextPrincipal, projectName, modelName string, model *domain.Model, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Edit Model", "models", "/ui/models/"+projectName+"/"+modelName+"/update", csrfFieldProvider,
		Label(Text("Materialization")),
		Select(Name("materialization"), optionSelected("VIEW", model.Materialization), optionSelected("TABLE", model.Materialization), optionSelected("INCREMENTAL", model.Materialization), optionSelected("EPHEMERAL", model.Materialization)),
		Label(Text("Description")),
		Textarea(Name("description"), Text(model.Description)),
		Label(Text("Tags (comma separated)")),
		Input(Name("tags"), Value(csvValues(model.Tags))),
		Label(Text("SQL")),
		Textarea(Name("sql"), Text(model.SQL), Required()),
	)
}

func modelTestsNewPage(principal domain.ContextPrincipal, projectName, modelName string, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Model Test", "models", "/ui/models/"+projectName+"/"+modelName+"/tests", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Type")),
		Select(Name("test_type"), Option(Value("not_null"), Text("not_null")), Option(Value("unique"), Text("unique")), Option(Value("accepted_values"), Text("accepted_values")), Option(Value("relationships"), Text("relationships")), Option(Value("custom_sql"), Text("custom_sql"))),
		Label(Text("Column")),
		Input(Name("column")),
		Label(Text("Values (accepted_values, comma separated)")),
		Input(Name("values")),
		Label(Text("To Model (relationships)")),
		Input(Name("to_model")),
		Label(Text("To Column (relationships)")),
		Input(Name("to_column")),
		Label(Text("SQL (custom_sql)")),
		Textarea(Name("test_sql")),
	)
}
