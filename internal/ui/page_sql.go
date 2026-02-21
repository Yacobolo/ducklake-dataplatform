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
	resultNode := Node(Div(
		Class("sql-result-card sql-results-empty"),
		Div(
			Class("sql-results-empty-body"),
			I(Class("sql-results-empty-icon"), Attr("data-lucide", "table"), Attr("aria-hidden", "true")),
			P(Class("sql-results-empty-title"), Text("No results yet")),
			P(Class(mutedClass()), Text("Run a query to preview rows here.")),
			Code(Text("SELECT * FROM <schema>.<table> LIMIT 50;")),
		),
	))

	if runError != "" {
		resultNode = Div(
			Class("sql-result-card flash flash-error"),
			H2(Class("sql-results-title"), Text("Query Error")),
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
			Class("sql-result-card table-wrap"),
			Div(
				Class("sql-results-header"),
				H2(Class("sql-results-title"), Text("Results (Table View)")),
				P(Class(mutedClass()), Text(meta)),
			),
			Div(
				Class("sql-results-scroll"),
				Table(
					Class("data-table"),
					THead(Tr(Group(headerCols))),
					TBody(Group(rows)),
				),
			),
		)
	}

	snippetsNode := snippetLinks(state.SelectedCatalog, state.SelectedSchema)
	snippetsMenuNode := snippetMenu(state.SelectedCatalog, state.SelectedSchema)
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
			Class("sql-workspace"),
			Form(
				Method("get"),
				Action("/ui/sql"),
				Class("sql-context-bar"),
				Div(
					Class("sql-context-fields"),
					Div(
						Class("sql-context-field"),
						Label(Text("Catalog")),
						Select(Class("form-select"), Name("catalog"), Attr("onchange", "this.form.submit()"), Group(catalogOptions)),
					),
					Div(
						Class("sql-context-field"),
						Label(Text("Schema")),
						Select(Class("form-select"), Name("schema"), Attr("onchange", "this.form.submit()"), Group(schemaOptions)),
					),
				),
			),
			Div(Class("sql-divider")),
			Form(
				Method("post"),
				Action("/ui/sql/run"),
				Class("sql-editor-frame"),
				csrfFieldProvider(),
				Input(Type("hidden"), Name("catalog"), Value(state.SelectedCatalog)),
				Input(Type("hidden"), Name("schema"), Value(state.SelectedSchema)),
				Div(
					Class("sql-editor-toolbar"),
					Div(
						Class("button-row"),
						Button(
							Type("submit"),
							Class(primaryButtonClass()),
							I(Class("btn-icon-glyph"), Attr("data-lucide", "play"), Attr("aria-hidden", "true")),
							Span(Text("Run query")),
						),
						Button(
							Type("submit"),
							Class(secondaryButtonClass()),
							FormAction("/ui/sql/download.csv"),
							I(Class("btn-icon-glyph"), Attr("data-lucide", "download"), Attr("aria-hidden", "true")),
							Span(Text("Download CSV")),
						),
					),
					Div(
						Class("sql-snippet-toolbar"),
						P(Class("sql-snippet-label"), Text("Snippets:")),
						Div(Class("sql-snippet-inline"), snippetsNode),
						Div(Class("sql-snippet-collapsed"), snippetsMenuNode),
					),
				),
				Div(
					Class("sql-editor-host"),
					El(
						"sql-editor-surface",
						Textarea(
							Class("form-control sql-editor-textarea"),
							Name("sql"),
							Required(),
							Attr("spellcheck", "false"),
							Text(sqlText),
						),
					),
				),
			),
		),
		Div(
			Class("sql-results-panel"),
			resultNode,
		),
		Script(Src(uiScriptHref("sql-editor.js"))),
	)
}

func optionSelectedValue(value, selected, label string) Node {
	if value == selected {
		return Option(Value(value), Selected(), Text(label))
	}
	return Option(Value(value), Text(label))
}

func snippetLinks(catalogName, schemaName string) Node {
	snippets := sqlSnippets()

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
		links = append(links,
			A(
				Href("/ui/sql?"+q.Encode()),
				Class("btn btn-sm"),
				I(Class("btn-icon-glyph"), Attr("data-lucide", snippetIcon(snippets[i].ID)), Attr("aria-hidden", "true")),
				Span(Text(snippets[i].Label)),
			),
		)
	}
	return Div(Class("snippet-list sql-snippet-list"), Group(links))
}

func snippetMenu(catalogName, schemaName string) Node {
	snippets := sqlSnippets()
	items := make([]Node, 0, len(snippets))
	for i := range snippets {
		q := url.Values{}
		q.Set("snippet", snippets[i].ID)
		if catalogName != "" {
			q.Set("catalog", catalogName)
		}
		if schemaName != "" {
			q.Set("schema", schemaName)
		}
		items = append(items,
			A(
				Href("/ui/sql?"+q.Encode()),
				Class("dropdown-item"),
				I(Class("dropdown-item-icon"), Attr("data-lucide", snippetIcon(snippets[i].ID)), Attr("aria-hidden", "true")),
				Span(Text(snippets[i].Label)),
			),
		)
	}

	return Details(
		Class("dropdown details-reset details-overlay d-inline-block sql-snippet-dropdown"),
		Summary(Class("btn btn-sm"), Text("Snippets")),
		Div(
			Class("dropdown-menu dropdown-menu-sw"),
			Group(items),
		),
	)
}

type sqlSnippet struct {
	ID    string
	Label string
}

func sqlSnippets() []sqlSnippet {
	return []sqlSnippet{
		{ID: "show_tables", Label: "Show tables"},
		{ID: "show_views", Label: "Show views"},
		{ID: "describe_table", Label: "Describe table"},
		{ID: "sample_rows", Label: "Sample rows"},
	}
}

func snippetIcon(id string) string {
	switch id {
	case "show_tables":
		return "table"
	case "show_views":
		return "eye"
	case "describe_table":
		return "list"
	case "sample_rows":
		return "file-text"
	default:
		return "sparkles"
	}
}
