package ui

import (
	"fmt"
	"net/url"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"

	gomponents "maragu.dev/gomponents"
	html "maragu.dev/gomponents/html"
)

func sqlEditorPage(principal domain.ContextPrincipal, sqlText string, result *query.QueryResult, runError string, state sqlEditorContext, csrfFieldProvider func() gomponents.Node) gomponents.Node {
	resultNode := gomponents.Node(html.P(html.Class(mutedClass()), gomponents.Text("Run a query to see results.")))

	if runError != "" {
		resultNode = html.Div(
			html.Class(cardClass()),
			html.H2(gomponents.Text("Query Error")),
			html.Pre(gomponents.Text(runError)),
		)
	} else if result != nil {
		headerCols := make([]gomponents.Node, 0, len(result.Columns))
		for i := range result.Columns {
			headerCols = append(headerCols, html.Th(gomponents.Text(result.Columns[i])))
		}

		displayRows := result.Rows
		truncated := false
		if len(displayRows) > sqlEditorMaxRows {
			displayRows = displayRows[:sqlEditorMaxRows]
			truncated = true
		}

		rows := make([]gomponents.Node, 0, len(displayRows))
		for i := range displayRows {
			cells := make([]gomponents.Node, 0, len(displayRows[i]))
			for j := range displayRows[i] {
				cells = append(cells, html.Td(gomponents.Text(sqlCellString(displayRows[i][j]))))
			}
			rows = append(rows, html.Tr(gomponents.Group(cells)))
		}

		meta := fmt.Sprintf("%d row(s)", result.RowCount)
		if truncated {
			meta = fmt.Sprintf("%d row(s), showing first %d", result.RowCount, sqlEditorMaxRows)
		}

		resultNode = html.Div(
			html.Class(cardClass("table-wrap")),
			html.H2(gomponents.Text("Results")),
			html.P(html.Class(mutedClass()), gomponents.Text(meta)),
			html.Table(
				html.THead(html.Tr(gomponents.Group(headerCols))),
				html.TBody(gomponents.Group(rows)),
			),
		)
	}

	snippetsNode := snippetLinks(state.SelectedCatalog, state.SelectedSchema)
	catalogOptions := make([]gomponents.Node, 0, len(state.Catalogs)+1)
	catalogOptions = append(catalogOptions, optionSelectedValue("", state.SelectedCatalog, "(choose catalog)"))
	for i := range state.Catalogs {
		catalogOptions = append(catalogOptions, optionSelectedValue(state.Catalogs[i].Name, state.SelectedCatalog, state.Catalogs[i].Name))
	}

	schemaOptions := make([]gomponents.Node, 0, len(state.Schemas)+1)
	schemaOptions = append(schemaOptions, optionSelectedValue("", state.SelectedSchema, "(choose schema)"))
	for i := range state.Schemas {
		schemaOptions = append(schemaOptions, optionSelectedValue(state.Schemas[i].Name, state.SelectedSchema, state.Schemas[i].Name))
	}

	return appPage(
		"SQL Editor",
		"sql",
		principal,
		html.Div(
			html.Class(cardClass()),
			html.Form(
				html.Method("get"),
				html.Action("/ui/sql"),
				html.Label(gomponents.Text("Catalog")),
				html.Select(html.Name("catalog"), gomponents.Group(catalogOptions)),
				html.Label(gomponents.Text("Schema")),
				html.Select(html.Name("schema"), gomponents.Group(schemaOptions)),
				html.Div(html.Class("button-row"), html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Set context"))),
			),
			html.P(html.Class(mutedClass()), gomponents.Text("Context does not rewrite SQL automatically. It powers snippets and export naming.")),
			html.H2(gomponents.Text("Default snippets")),
			snippetsNode,
		),
		html.Div(
			html.Class(cardClass()),
			html.Form(
				html.Method("post"),
				html.Action("/ui/sql/run"),
				csrfFieldProvider(),
				html.Input(html.Type("hidden"), html.Name("catalog"), html.Value(state.SelectedCatalog)),
				html.Input(html.Type("hidden"), html.Name("schema"), html.Value(state.SelectedSchema)),
				html.Label(gomponents.Text("SQL")),
				html.Textarea(html.Name("sql"), html.Required(), gomponents.Text(sqlText)),
				html.Div(
					html.Class("button-row"),
					html.Button(html.Type("submit"), html.Class(primaryButtonClass()), gomponents.Text("Run query")),
					html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), html.FormAction("/ui/sql/download.csv"), gomponents.Text("Download CSV")),
				),
			),
		),
		resultNode,
	)
}

func optionSelectedValue(value, selected, label string) gomponents.Node {
	if value == selected {
		return html.Option(html.Value(value), html.Selected(), gomponents.Text(label))
	}
	return html.Option(html.Value(value), gomponents.Text(label))
}

func snippetLinks(catalogName, schemaName string) gomponents.Node {
	snippets := []struct {
		ID    string
		Label string
	}{
		{ID: "show_tables", Label: "Show tables"},
		{ID: "show_views", Label: "Show views"},
		{ID: "describe_table", Label: "Describe table"},
		{ID: "sample_rows", Label: "Sample rows"},
	}

	links := make([]gomponents.Node, 0, len(snippets))
	for i := range snippets {
		q := url.Values{}
		q.Set("snippet", snippets[i].ID)
		if catalogName != "" {
			q.Set("catalog", catalogName)
		}
		if schemaName != "" {
			q.Set("schema", schemaName)
		}
		links = append(links, html.A(html.Href("/ui/sql?"+q.Encode()), gomponents.Text(snippets[i].Label)))
	}
	return html.Div(html.Class("snippet-list"), gomponents.Group(links))
}
