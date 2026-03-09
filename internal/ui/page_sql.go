package ui

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type sqlComputeOption struct {
	Label       string
	Description string
	Active      bool
}

func sqlComputeModeOption(value, label, selected string) Node {
	if value == selected {
		return Option(Value(value), Selected(), Text(label))
	}
	return Option(Value(value), Text(label))
}

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

	computeOptions := []sqlComputeOption{
		{Label: "Local", Description: "Recommended for interactive queries. DuckDB runs in the client runtime.", Active: true},
		{Label: "Shared Endpoint", Description: "Use managed compute for heavier or unattended workloads."},
	}
	runtimeTone := "attention"
	if strings.EqualFold(state.BrowserRuntime.Status, "READY") && state.BrowserRuntime.Supported {
		runtimeTone = "success"
	}
	browserRuntimeCard := Div(
		Class(cardClass("sql-compute-card")),
		Attr("data-sql-browser-runtime", "true"),
		Attr("data-runtime-manifest-endpoint", "/ui/sql/runtime/manifest"),
		H2(Class("sql-results-title"), Text("Compute")),
		P(Class(mutedClass()), Text("Interactive UI will default to local compute once the browser DuckDB WASM runner is enabled. The browser will use the same manifest and duck_access contract as the CLI.")),
		segmentedTabs(func() []segmentedTabItem {
			items := make([]segmentedTabItem, 0, len(computeOptions))
			for i := range computeOptions {
				items = append(items, segmentedTabItem{Label: computeOptions[i].Label, Active: computeOptions[i].Active})
			}
			return items
		}()),
		P(Class(mutedClass()), Text(computeOptions[0].Description)),
		Div(
			Class("Banner Banner-"+runtimeTone),
			Attr("data-sql-runtime-banner", "true"),
			I(Class("nav-icon"), Attr("data-lucide", "cpu"), Attr("aria-hidden", "true")),
			Div(
				Strong(Attr("data-sql-browser-runtime-title", "true"), Text("Browser runtime")),
				P(Attr("data-sql-browser-runtime-message", "true"), Text(state.BrowserRuntime.StatusReason)),
			),
		),
		Div(
			Class("sql-compute-meta"),
			Div(
				Class("sql-compute-meta-item"),
				P(Class("sql-compute-meta-label"), Text("Contract")),
				Code(Text(state.BrowserRuntime.ContractVersion)),
			),
			Div(
				Class("sql-compute-meta-item"),
				P(Class("sql-compute-meta-label"), Text("Engine")),
				Code(Text(state.BrowserRuntime.Engine)),
			),
			Div(
				Class("sql-compute-meta-item"),
				P(Class("sql-compute-meta-label"), Text("Adapter")),
				Code(Text(state.BrowserRuntime.Adapter)),
			),
			Div(
				Class("sql-compute-meta-item"),
				P(Class("sql-compute-meta-label"), Text("Auth")),
				Code(Text(strings.Join(state.BrowserRuntime.RequiredAuthModes, ", "))),
			),
			Div(
				Class("sql-compute-meta-item"),
				P(Class("sql-compute-meta-label"), Text("Files")),
				Code(Text(strings.Join(state.BrowserRuntime.SupportedFileURLTypes, ", "))),
			),
			Div(
				Class("sql-compute-meta-item"),
				P(Class("sql-compute-meta-label"), Text("Guidance")),
				Span(Text(fmt.Sprintf("~%d rows / %d MB browser memory", state.BrowserRuntime.RecommendedMaxRows, state.BrowserRuntime.RecommendedMemoryMB))),
			),
		),
		Ul(
			Class("color-fg-muted text-small"),
			Li(Text("Local browser execution supports a bounded read-only subset, including multi-table queries over up to four manifest-backed tables.")),
			Li(Text("Use an explicit LIMIT within the browser guidance before running locally.")),
			Li(Text("Auto will prefer local browser execution when the query and runtime satisfy guardrails, otherwise it will fall through to managed compute.")),
			Li(Text("If the browser runtime or query shape is unsupported, switch to Shared Endpoint or Auto and rerun.")),
		),
		Div(
			Class("button-row"),
			Button(
				Type("button"),
				ID("sql-reset-local-runtime"),
				Class(secondaryButtonClass()),
				I(Class("btn-icon-glyph"), Attr("data-lucide", "rotate-ccw"), Attr("aria-hidden", "true")),
				Span(Text("Reset local runtime")),
			),
			Button(
				Type("button"),
				ID("sql-cancel-local-run"),
				Class(secondaryButtonClass()),
				Disabled(),
				I(Class("btn-icon-glyph"), Attr("data-lucide", "square"), Attr("aria-hidden", "true")),
				Span(Text("Cancel local run")),
			),
		),
		P(Class(mutedClass()), Attr("data-sql-browser-runtime-preflight", "true"), Text("")),
	)
	modeOptions := []Node{
		sqlComputeModeOption(domain.ComputeModeAuto, "Auto", state.ComputeRequest.Mode),
		sqlComputeModeOption(domain.ComputeModeByocLocal, "Local (BYOC)", state.ComputeRequest.Mode),
		sqlComputeModeOption(domain.ComputeModeSharedEndpoint, "Shared endpoint", state.ComputeRequest.Mode),
	}
	endpointOptions := []Node{
		Option(Value(""), Text("Platform default")),
	}
	for i := range state.ComputeTargets {
		target := state.ComputeTargets[i]
		if target.Mode != domain.ComputeModeSharedEndpoint || target.EndpointName == "" {
			continue
		}
		label := target.Label
		if target.Default {
			label += " (default)"
		}
		if !target.Selectable && target.AvailabilityReason != "" {
			label += " - " + target.AvailabilityReason
		}
		optionAttrs := []Node{Value(target.EndpointName)}
		if state.ComputeRequest.EndpointName == target.EndpointName {
			optionAttrs = append(optionAttrs, Selected())
		}
		if !target.Selectable {
			optionAttrs = append(optionAttrs, Disabled())
		}
		endpointOptions = append(endpointOptions, Option(Group(optionAttrs), Text(label)))
	}

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
					browserRuntimeCard,
					Form(
						Method("post"),
						Action("/ui/sql/run"),
						Class("sql-editor-frame"),
						Attr("data-sql-editor-form", "true"),
						csrfFieldProvider(),
						Input(Type("hidden"), Name("catalog"), Value(state.SelectedCatalog)),
						Input(Type("hidden"), Name("schema"), Value(state.SelectedSchema)),
						Input(Type("hidden"), Name("workload_type"), Value(state.ComputeRequest.WorkloadType)),
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
									Type("submit"),
									FormAction("/ui/sql/run-async"),
									ID("sql-run-query-async"),
									Class(secondaryButtonClass()),
									I(Class("btn-icon-glyph"), Attr("data-lucide", "timer-reset"), Attr("aria-hidden", "true")),
									Span(Text("Run async")),
								),
								Button(
									Type("button"),
									ID("sql-format-query"),
									Class(secondaryButtonClass()),
									I(Class("btn-icon-glyph"), Attr("data-lucide", "align-left"), Attr("aria-hidden", "true")),
									Span(Text("Format SQL")),
								),
								A(Href("/ui/sql/jobs"), Class(secondaryButtonClass()), Text("Jobs")),
								Span(Class("sr-only"), Text("Shortcuts: Run Cmd or Ctrl plus Enter. Format Cmd or Ctrl plus Shift plus F.")),
							),
							Div(
								Class("sql-compute-controls"),
								Label(
									Class("sr-only"),
									Attr("for", "sql-compute-mode"),
									Text("Compute mode"),
								),
								Select(
									ID("sql-compute-mode"),
									Name("compute_mode"),
									Class("form-select sql-compute-select"),
									Group(modeOptions),
								),
								Label(
									Class("sr-only"),
									Attr("for", "sql-compute-endpoint"),
									Text("Compute endpoint"),
								),
								Select(
									ID("sql-compute-endpoint"),
									Name("endpoint_name"),
									Class("form-select sql-compute-select"),
									Group(endpointOptions),
								),
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
					Attr("data-sql-results-panel", "true"),
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
