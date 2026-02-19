package ui

import (
	"fmt"
	"net/url"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func sqlEditorPage(principal domain.ContextPrincipal, sqlText string, result *query.QueryResult, runError string, state sqlEditorContext, csrfFieldProvider func() Node) Node {
	resultNode := Node(P(Class(mutedClass()), Text("Run a query to see results.")))

	if runError != "" {
		resultNode = Div(
			Class(cardClass("flash flash-error")),
			H2(Text("Query Error")),
			Pre(Text(runError)),
		)
	} else if result != nil {
		headerCols := make([]Node, 0, len(result.Columns))
		for i := range result.Columns {
			headerCols = append(headerCols, Th(Text(result.Columns[i])))
		}

		displayRows := result.Rows
		truncated := false
		if len(displayRows) > sqlEditorMaxRows {
			displayRows = displayRows[:sqlEditorMaxRows]
			truncated = true
		}

		rows := make([]Node, 0, len(displayRows))
		for i := range displayRows {
			cells := make([]Node, 0, len(displayRows[i]))
			for j := range displayRows[i] {
				cells = append(cells, Td(Text(sqlCellString(displayRows[i][j]))))
			}
			rows = append(rows, Tr(Group(cells)))
		}

		meta := fmt.Sprintf("%d row(s)", result.RowCount)
		if truncated {
			meta = fmt.Sprintf("%d row(s), showing first %d", result.RowCount, sqlEditorMaxRows)
		}

		resultNode = Div(
			Class(cardClass("table-wrap")),
			H2(Text("Results")),
			P(Class(mutedClass()), Text(meta)),
			Table(
				Class("data-table"),
				THead(Tr(Group(headerCols))),
				TBody(Group(rows)),
			),
		)
	}

	snippetsNode := snippetLinks(state.SelectedCatalog, state.SelectedSchema)
	catalogOptions := make([]Node, 0, len(state.Catalogs)+1)
	catalogOptions = append(catalogOptions, optionSelectedValue("", state.SelectedCatalog, "(choose catalog)"))
	for i := range state.Catalogs {
		catalogOptions = append(catalogOptions, optionSelectedValue(state.Catalogs[i].Name, state.SelectedCatalog, state.Catalogs[i].Name))
	}

	schemaOptions := make([]Node, 0, len(state.Schemas)+1)
	schemaOptions = append(schemaOptions, optionSelectedValue("", state.SelectedSchema, "(choose schema)"))
	for i := range state.Schemas {
		schemaOptions = append(schemaOptions, optionSelectedValue(state.Schemas[i].Name, state.SelectedSchema, state.Schemas[i].Name))
	}

	return appPage(
		"SQL Editor",
		"sql",
		principal,
		Div(
			Class(cardClass()),
			Form(
				Method("get"),
				Action("/ui/sql"),
				Label(Text("Catalog")),
				Select(Class("form-select"), Name("catalog"), Group(catalogOptions)),
				Label(Text("Schema")),
				Select(Class("form-select"), Name("schema"), Group(schemaOptions)),
				Div(Class("button-row"), Button(Type("submit"), Class(secondaryButtonClass()), Text("Set context"))),
			),
			P(Class(mutedClass()), Text("Context does not rewrite SQL automatically. It powers snippets and export naming.")),
			H2(Text("Default snippets")),
			snippetsNode,
		),
		Div(
			Class(cardClass()),
			Form(
				Method("post"),
				Action("/ui/sql/run"),
				csrfFieldProvider(),
				Input(Type("hidden"), Name("catalog"), Value(state.SelectedCatalog)),
				Input(Type("hidden"), Name("schema"), Value(state.SelectedSchema)),
				Label(Text("SQL")),
				Textarea(Class("form-control"), Name("sql"), Required(), Text(sqlText)),
				Div(
					Class("button-row"),
					Button(Type("submit"), Class(primaryButtonClass()), Text("Run query")),
					Button(Type("submit"), Class(secondaryButtonClass()), FormAction("/ui/sql/download.csv"), Text("Download CSV")),
				),
			),
		),
		resultNode,
	)
}

func optionSelectedValue(value, selected, label string) Node {
	if value == selected {
		return Option(Value(value), Selected(), Text(label))
	}
	return Option(Value(value), Text(label))
}

func snippetLinks(catalogName, schemaName string) Node {
	snippets := []struct {
		ID    string
		Label string
	}{
		{ID: "show_tables", Label: "Show tables"},
		{ID: "show_views", Label: "Show views"},
		{ID: "describe_table", Label: "Describe table"},
		{ID: "sample_rows", Label: "Sample rows"},
	}

	links := make([]Node, 0, len(snippets))
	for i := range snippets {
		q := url.Values{}
		q.Set("snippet", snippets[i].ID)
		if catalogName != "" {
			q.Set("catalog", catalogName)
		}
		if schemaName != "" {
			q.Set("schema", schemaName)
		}
		links = append(links, A(Href("/ui/sql?"+q.Encode()), Text(snippets[i].Label)))
	}
	return Div(Class("snippet-list"), Group(links))
}
