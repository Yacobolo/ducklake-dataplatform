package ui

import (
	"strconv"
	"strings"

	"duck-demo/internal/domain"

	gomponents "maragu.dev/gomponents"
	html "maragu.dev/gomponents/html"
)

func formPage(principal domain.ContextPrincipal, title, active, action string, csrfFieldProvider func() gomponents.Node, fields ...gomponents.Node) gomponents.Node {
	nodes := []gomponents.Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	return appPage(
		title,
		active,
		principal,
		html.Div(
			html.Class(cardClass()),
			html.Form(
				html.Method("post"),
				html.Action(action),
				gomponents.Group(nodes),
				html.Div(html.StyleAttr("margin-top: 12px"), html.Button(html.Type("submit"), html.Class(primaryButtonClass()), gomponents.Text("Save"))),
			),
		),
	)
}

func catalogsNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "New Catalog", "catalogs", "/ui/catalogs", csrfFieldProvider,
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Metastore Type")),
		html.Select(html.Name("metastore_type"), html.Option(html.Value("sqlite"), gomponents.Text("sqlite")), html.Option(html.Value("postgres"), gomponents.Text("postgres"))),
		html.Label(gomponents.Text("DSN")),
		html.Input(html.Name("dsn"), html.Required()),
		html.Label(gomponents.Text("Data Path")),
		html.Input(html.Name("data_path"), html.Required()),
		html.Label(gomponents.Text("Comment")),
		html.Textarea(html.Name("comment")),
	)
}

func catalogsEditPage(principal domain.ContextPrincipal, catalogName string, catalog *domain.CatalogRegistration, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "Edit Catalog", "catalogs", "/ui/catalogs/"+catalogName+"/update", csrfFieldProvider,
		html.Label(gomponents.Text("Comment")),
		html.Textarea(html.Name("comment"), gomponents.Text(catalog.Comment)),
		html.Label(gomponents.Text("Data Path")),
		html.Input(html.Name("data_path"), html.Value(catalog.DataPath)),
		html.Label(gomponents.Text("DSN")),
		html.Input(html.Name("dsn"), html.Value(catalog.DSN)),
	)
}

func catalogSchemasNewPage(principal domain.ContextPrincipal, catalogName string, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "New Schema", "catalogs", "/ui/catalogs/"+catalogName+"/schemas", csrfFieldProvider,
		html.Label(gomponents.Text("Schema Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Comment")),
		html.Textarea(html.Name("comment")),
		html.Label(gomponents.Text("Location Name")),
		html.Input(html.Name("location_name")),
	)
}

func catalogSchemasEditPage(principal domain.ContextPrincipal, catalogName, schemaName string, schema *domain.SchemaDetail, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "Edit Schema", "catalogs", "/ui/catalogs/"+catalogName+"/schemas/"+schemaName+"/update", csrfFieldProvider,
		html.Label(gomponents.Text("Comment")),
		html.Textarea(html.Name("comment"), gomponents.Text(schema.Comment)),
	)
}

func pipelinesNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "New Pipeline", "pipelines", "/ui/pipelines", csrfFieldProvider,
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description")),
		html.Label(gomponents.Text("Schedule Cron")),
		html.Input(html.Name("schedule_cron")),
		html.Label(gomponents.Text("Concurrency Limit")),
		html.Input(html.Name("concurrency_limit"), html.Value("1")),
		html.Label(gomponents.Text("Paused")),
		html.Input(html.Type("checkbox"), html.Name("is_paused")),
	)
}

func pipelinesEditPage(principal domain.ContextPrincipal, pipelineName string, pipeline *domain.Pipeline, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	pausedInput := []gomponents.Node{html.Type("checkbox"), html.Name("is_paused")}
	if pipeline.IsPaused {
		pausedInput = append(pausedInput, html.Checked())
	}

	return formPage(principal, "Edit Pipeline", "pipelines", "/ui/pipelines/"+pipelineName+"/update", csrfFieldProvider,
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description"), gomponents.Text(pipeline.Description)),
		html.Label(gomponents.Text("Schedule Cron")),
		html.Input(html.Name("schedule_cron"), html.Value(optionalStringValue(pipeline.ScheduleCron))),
		html.Label(gomponents.Text("Concurrency Limit")),
		html.Input(html.Name("concurrency_limit"), html.Value(strconv.Itoa(pipeline.ConcurrencyLimit))),
		html.Label(gomponents.Text("Paused")),
		html.Input(pausedInput...),
	)
}

func pipelineJobsNewPage(principal domain.ContextPrincipal, pipelineName string, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "New Pipeline Job", "pipelines", "/ui/pipelines/"+pipelineName+"/jobs", csrfFieldProvider,
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Type")),
		html.Select(html.Name("job_type"), html.Option(html.Value("NOTEBOOK"), gomponents.Text("NOTEBOOK")), html.Option(html.Value("MODEL_RUN"), gomponents.Text("MODEL_RUN"))),
		html.Label(gomponents.Text("Notebook ID")),
		html.Input(html.Name("notebook_id")),
		html.Label(gomponents.Text("Model Selector")),
		html.Input(html.Name("model_selector")),
		html.Label(gomponents.Text("Depends On (comma separated job names)")),
		html.Input(html.Name("depends_on")),
	)
}

func notebooksNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "New Notebook", "notebooks", "/ui/notebooks", csrfFieldProvider,
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description")),
		html.Label(gomponents.Text("Initial SQL Source")),
		html.Textarea(html.Name("source")),
	)
}

func notebooksEditPage(principal domain.ContextPrincipal, notebookID string, notebook *domain.Notebook, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "Edit Notebook", "notebooks", "/ui/notebooks/"+notebookID+"/update", csrfFieldProvider,
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Value(notebook.Name), html.Required()),
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description"), gomponents.Text(optionalStringValue(notebook.Description))),
	)
}

func notebookCellsNewPage(principal domain.ContextPrincipal, notebookID string, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "New Notebook Cell", "notebooks", "/ui/notebooks/"+notebookID+"/cells", csrfFieldProvider,
		html.Label(gomponents.Text("Cell Type")),
		html.Select(html.Name("cell_type"), html.Option(html.Value("sql"), gomponents.Text("sql")), html.Option(html.Value("markdown"), gomponents.Text("markdown"))),
		html.Label(gomponents.Text("Content")),
		html.Textarea(html.Name("content"), html.Required()),
		html.Label(gomponents.Text("Position (optional)")),
		html.Input(html.Name("position")),
	)
}

func notebookCellsEditPage(principal domain.ContextPrincipal, notebookID, cellID string, cell *domain.Cell, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "Edit Notebook Cell", "notebooks", "/ui/notebooks/"+notebookID+"/cells/"+cellID+"/update", csrfFieldProvider,
		html.Label(gomponents.Text("Content")),
		html.Textarea(html.Name("content"), gomponents.Text(cell.Content), html.Required()),
		html.Label(gomponents.Text("Position")),
		html.Input(html.Name("position"), html.Value(strconv.Itoa(cell.Position))),
	)
}

func macrosNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "New Macro", "macros", "/ui/macros", csrfFieldProvider,
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Type")),
		html.Select(html.Name("macro_type"), html.Option(html.Value("SCALAR"), gomponents.Text("SCALAR")), html.Option(html.Value("TABLE"), gomponents.Text("TABLE"))),
		html.Label(gomponents.Text("Visibility")),
		html.Select(html.Name("visibility"), html.Option(html.Value("project"), gomponents.Text("project")), html.Option(html.Value("catalog_global"), gomponents.Text("catalog_global")), html.Option(html.Value("system"), gomponents.Text("system"))),
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description")),
		html.Label(gomponents.Text("Parameters (comma separated)")),
		html.Input(html.Name("parameters")),
		html.Label(gomponents.Text("Body")),
		html.Textarea(html.Name("body"), html.Required()),
	)
}

func macrosEditPage(principal domain.ContextPrincipal, macroName string, macro *domain.Macro, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "Edit Macro", "macros", "/ui/macros/"+macroName+"/update", csrfFieldProvider,
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description"), gomponents.Text(macro.Description)),
		html.Label(gomponents.Text("Visibility")),
		html.Select(html.Name("visibility"), optionSelected("project", macro.Visibility), optionSelected("catalog_global", macro.Visibility), optionSelected("system", macro.Visibility)),
		html.Label(gomponents.Text("Parameters (comma separated)")),
		html.Input(html.Name("parameters"), html.Value(csvValues(macro.Parameters))),
		html.Label(gomponents.Text("Body")),
		html.Textarea(html.Name("body"), gomponents.Text(macro.Body), html.Required()),
		html.Label(gomponents.Text("Status")),
		html.Select(html.Name("status"), optionSelected("ACTIVE", macro.Status), optionSelected("DEPRECATED", macro.Status)),
	)
}

func modelsNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "New Model", "models", "/ui/models", csrfFieldProvider,
		html.Label(gomponents.Text("Project")),
		html.Input(html.Name("project_name"), html.Required()),
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Materialization")),
		html.Select(html.Name("materialization"), html.Option(html.Value("VIEW"), gomponents.Text("VIEW")), html.Option(html.Value("TABLE"), gomponents.Text("TABLE")), html.Option(html.Value("INCREMENTAL"), gomponents.Text("INCREMENTAL")), html.Option(html.Value("EPHEMERAL"), gomponents.Text("EPHEMERAL"))),
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description")),
		html.Label(gomponents.Text("Tags (comma separated)")),
		html.Input(html.Name("tags")),
		html.Label(gomponents.Text("SQL")),
		html.Textarea(html.Name("sql"), html.Required()),
	)
}

func modelsEditPage(principal domain.ContextPrincipal, projectName, modelName string, model *domain.Model, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "Edit Model", "models", "/ui/models/"+projectName+"/"+modelName+"/update", csrfFieldProvider,
		html.Label(gomponents.Text("Materialization")),
		html.Select(html.Name("materialization"), optionSelected("VIEW", model.Materialization), optionSelected("TABLE", model.Materialization), optionSelected("INCREMENTAL", model.Materialization), optionSelected("EPHEMERAL", model.Materialization)),
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description"), gomponents.Text(model.Description)),
		html.Label(gomponents.Text("Tags (comma separated)")),
		html.Input(html.Name("tags"), html.Value(csvValues(model.Tags))),
		html.Label(gomponents.Text("SQL")),
		html.Textarea(html.Name("sql"), gomponents.Text(model.SQL), html.Required()),
	)
}

func modelTestsNewPage(principal domain.ContextPrincipal, projectName, modelName string, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	return formPage(principal, "New Model Test", "models", "/ui/models/"+projectName+"/"+modelName+"/tests", csrfFieldProvider,
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Type")),
		html.Select(html.Name("test_type"), html.Option(html.Value("not_null"), gomponents.Text("not_null")), html.Option(html.Value("unique"), gomponents.Text("unique")), html.Option(html.Value("accepted_values"), gomponents.Text("accepted_values")), html.Option(html.Value("relationships"), gomponents.Text("relationships")), html.Option(html.Value("custom_sql"), gomponents.Text("custom_sql"))),
		html.Label(gomponents.Text("Column")),
		html.Input(html.Name("column")),
		html.Label(gomponents.Text("Values (accepted_values, comma separated)")),
		html.Input(html.Name("values")),
		html.Label(gomponents.Text("To Model (relationships)")),
		html.Input(html.Name("to_model")),
		html.Label(gomponents.Text("To Column (relationships)")),
		html.Input(html.Name("to_column")),
		html.Label(gomponents.Text("SQL (custom_sql)")),
		html.Textarea(html.Name("test_sql")),
	)
}

func optionSelected(value, selected string) gomponents.Node {
	if value == selected {
		return html.Option(html.Value(value), html.Selected(), gomponents.Text(value))
	}
	return html.Option(html.Value(value), gomponents.Text(value))
}

func optionalStringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func csvValues(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.Join(values, ", ")
}
