package ui

import (
	"fmt"
	"net/url"
	"strconv"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

func sqlEditorPage(principal domain.ContextPrincipal, sqlText string, result *query.QueryResult, runError string, state sqlEditorContext, csrfFieldProvider func() Node) Node {
	csvActionNode := Node(nil)
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
		csvActionNode = Form(
			Method("post"),
			Action("/ui/sql/download.csv"),
			Class("sql-results-actions"),
			csrfFieldProvider(),
			Input(Type("hidden"), Name("catalog"), Value(state.SelectedCatalog)),
			Input(Type("hidden"), Name("schema"), Value(state.SelectedSchema)),
			Input(Type("hidden"), Name("sql"), Value(sqlText)),
			Button(
				Type("submit"),
				Class("btn btn-sm"),
				I(Class("btn-icon-glyph"), Attr("data-lucide", "download"), Attr("aria-hidden", "true")),
				Span(Text("Download CSV")),
			),
		)

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
				Div(
					Class("sql-results-meta"),
					H2(Class("sql-results-title"), Text("Results (Table View)")),
					P(Class(mutedClass()), Text(meta)),
				),
				csvActionNode,
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

	selectedCatalogLabel := state.SelectedCatalog
	if selectedCatalogLabel == "" {
		selectedCatalogLabel = "Choose catalog"
	}

	selectedSchemaLabel := state.SelectedSchema
	if selectedSchemaLabel == "" {
		selectedSchemaLabel = "Choose schema"
	}

	catalogMenuItems := make([]Node, 0, len(state.Catalogs)+1)
	catalogMenuItems = append(catalogMenuItems,
		A(
			Href(sqlContextURL("", "")),
			Class("dropdown-item"),
			I(Class("dropdown-item-icon"), Attr("data-lucide", "circle"), Attr("aria-hidden", "true")),
			Span(Text("Choose catalog")),
		),
	)
	for i := range state.Catalogs {
		catalog := state.Catalogs[i]
		catalogMenuItems = append(catalogMenuItems,
			A(
				Href(sqlContextURL(catalog.Name, "")),
				Class("dropdown-item"),
				I(Class("dropdown-item-icon"), Attr("data-lucide", "database"), Attr("aria-hidden", "true")),
				Span(Text(catalog.Name)),
			),
		)
	}

	schemaMenuItems := make([]Node, 0, len(state.Schemas)+1)
	if state.SelectedCatalog == "" {
		schemaMenuItems = append(schemaMenuItems,
			Div(
				Class("dropdown-item color-fg-muted"),
				Attr("aria-disabled", "true"),
				I(Class("dropdown-item-icon"), Attr("data-lucide", "info"), Attr("aria-hidden", "true")),
				Span(Text("Choose a catalog first")),
			),
		)
	} else {
		schemaMenuItems = append(schemaMenuItems,
			A(
				Href(sqlContextURL(state.SelectedCatalog, "")),
				Class("dropdown-item"),
				I(Class("dropdown-item-icon"), Attr("data-lucide", "circle"), Attr("aria-hidden", "true")),
				Span(Text("Choose schema")),
			),
		)
		for i := range state.Schemas {
			schema := state.Schemas[i]
			schemaMenuItems = append(schemaMenuItems,
				A(
					Href(sqlContextURL(state.SelectedCatalog, schema.Name)),
					Class("dropdown-item"),
					I(Class("dropdown-item-icon"), Attr("data-lucide", "folder"), Attr("aria-hidden", "true")),
					Span(Text(schema.Name)),
				),
			)
		}
	}

	explorerCatalogs := make([]catalogExplorerCatalogItem, 0, len(state.Catalogs))
	for i := range state.Catalogs {
		catalog := state.Catalogs[i]
		catalogItem := catalogExplorerCatalogItem{
			Name:      catalog.Name,
			URL:       sqlContextURL(catalog.Name, ""),
			Active:    catalog.Name == state.SelectedCatalog,
			Open:      catalog.Name == state.SelectedCatalog,
			EmptyText: "No schemas in this catalog.",
		}
		if catalog.Name == state.SelectedCatalog {
			schemaItems := make([]catalogExplorerSchemaItem, 0, len(state.Schemas))
			for j := range state.Schemas {
				schema := state.Schemas[j]
				schemaItems = append(schemaItems, catalogExplorerSchemaItem{
					Name:   schema.Name,
					URL:    sqlContextURL(catalog.Name, schema.Name),
					Active: schema.Name == state.SelectedSchema,
				})
			}
			catalogItem.Schemas = schemaItems
		}
		explorerCatalogs = append(explorerCatalogs, catalogItem)
	}

	explorerPanel := catalogExplorerPanel(catalogExplorerPanelData{
		Title:             "Catalog Explorer",
		FilterPlaceholder: "Filter catalogs or schemas",
		Catalogs:          explorerCatalogs,
		EmptyCatalogsText: "No catalogs found.",
	})

	return appPage(
		"SQL Editor",
		"sql",
		principal,
		data.Signals(map[string]any{"q": ""}),
		workspaceLayout(
			"sql-workspace-layout",
			workspaceAside(
				"sql-workspace",
				"sql-aside",
				[]workspaceAsideTab{
					{ID: "explorer", Label: "Explorer", Icon: "database", Count: strconv.Itoa(len(state.Catalogs)), Content: explorerPanel, PanelClass: "sql-aside-explorer-panel"},
				},
				"explorer",
			),
			Div(
				Class("sql-main"),
				Div(
					Class("sql-editor-panel"),
					Div(
						Class("sql-context-bar"),
						Div(
							Class("sql-context-pickers"),
							Details(
								Class("dropdown details-reset details-overlay d-inline-block sql-context-picker"),
								Summary(
									Class("btn btn-sm sql-context-picker-button"),
									Title("Select catalog"),
									Attr("aria-label", "Select catalog"),
									I(Class("btn-icon-glyph"), Attr("data-lucide", "database"), Attr("aria-hidden", "true")),
									Span(Class("sql-context-picker-label"), Text(selectedCatalogLabel)),
									I(Class("btn-icon-glyph"), Attr("data-lucide", "chevrons-up-down"), Attr("aria-hidden", "true")),
								),
								Div(
									Class("dropdown-menu dropdown-menu-sw sql-context-picker-menu"),
									Group(catalogMenuItems),
								),
							),
							Details(
								Class("dropdown details-reset details-overlay d-inline-block sql-context-picker"),
								Summary(
									Class("btn btn-sm sql-context-picker-button"),
									Title("Select schema"),
									Attr("aria-label", "Select schema"),
									I(Class("btn-icon-glyph"), Attr("data-lucide", "folder"), Attr("aria-hidden", "true")),
									Span(Class("sql-context-picker-label"), Text(selectedSchemaLabel)),
									I(Class("btn-icon-glyph"), Attr("data-lucide", "chevrons-up-down"), Attr("aria-hidden", "true")),
								),
								Div(
									Class("dropdown-menu dropdown-menu-sw sql-context-picker-menu"),
									Group(schemaMenuItems),
								),
							),
						),
					),
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
									ID("sql-run-query"),
									Class(primaryButtonClass()),
									I(Class("btn-icon-glyph"), Attr("data-lucide", "play"), Attr("aria-hidden", "true")),
									Span(Text("Run query")),
								),
								Button(
									Type("button"),
									ID("sql-format-query"),
									Class(secondaryButtonClass()),
									I(Class("btn-icon-glyph"), Attr("data-lucide", "align-left"), Attr("aria-hidden", "true")),
									Span(Text("Format SQL")),
								),
								Span(Class("sr-only"), Text("Shortcuts: Run Cmd or Ctrl plus Enter. Format Cmd or Ctrl plus Shift plus F.")),
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
			),
		),
		Script(Src(uiScriptHref("sql-editor.js"))),
	)
}

func sqlContextURL(catalogName, schemaName string) string {
	q := url.Values{}
	if catalogName != "" {
		q.Set("catalog", catalogName)
	}
	if schemaName != "" {
		q.Set("schema", schemaName)
	}
	encoded := q.Encode()
	if encoded == "" {
		return "/ui/sql"
	}
	return "/ui/sql?" + encoded
}
