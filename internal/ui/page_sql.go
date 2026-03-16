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
		Class(sqlResultCardClass("grid place-items-center border-dashed")),
		Div(
			Class("flex max-w-[var(--overlay-width-small)] flex-col items-center justify-center gap-3 py-8 text-center"),
			I(Class("h-8 w-8 text-[var(--fgColor-muted)]"), Attr("data-lucide", "table"), Attr("aria-hidden", "true")),
			P(Class("m-0 text-lg font-semibold"), Text("No results yet")),
			P(Class(mutedClass()), Text("Run a query to preview rows here.")),
			Code(Class("mt-2 rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-default)] px-3 py-2"), Text("SELECT * FROM <schema>.<table> LIMIT 50;")),
		),
	))

	if runError != "" {
		resultNode = Div(
			Class(sqlResultCardClass("border-[var(--borderColor-danger-emphasis)] bg-[var(--bgColor-danger-muted)]")),
			H2(Class(sqlResultsTitleClass()), Text("Query Error")),
			Pre(Text(runError)),
		)
	} else if result != nil {
		csvActionNode = Form(
			Method("post"),
			Action("/ui/sql/download.csv"),
			Class("flex items-center"),
			csrfFieldProvider(),
			Input(Type("hidden"), Name("catalog"), Value(state.SelectedCatalog)),
			Input(Type("hidden"), Name("schema"), Value(state.SelectedSchema)),
			Input(Type("hidden"), Name("sql"), Value(sqlText)),
			Button(
				Type("submit"),
				Class(secondaryButtonClass("small")),
				I(Class(iconGlyphClass()), Attr("data-lucide", "download"), Attr("aria-hidden", "true")),
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
			Class(sqlResultCardClass()),
			Div(
				Class("sticky top-0 z-[2] mb-3 flex flex-wrap items-start justify-between gap-3 border-b border-[var(--borderColor-muted)] bg-[var(--bgColor-default)] pb-3"),
				Div(
					Class("flex min-w-0 flex-col gap-1"),
					H2(Class(sqlResultsTitleClass()), Text("Results (Table View)")),
					P(Class(mutedClass()), Text(meta)),
				),
				csvActionNode,
			),
			Div(
				Class("min-h-0 flex-1 overflow-auto"),
				Table(
					Class(dataTableClass()),
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
			Class("dropdown-item flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[0.8125rem] text-[var(--fgColor-default)] no-underline hover:bg-[var(--control-bgColor-hover)]"),
			I(Class(navIconClass()), Attr("data-lucide", "circle"), Attr("aria-hidden", "true")),
			Span(Text("Choose catalog")),
		),
	)
	for i := range state.Catalogs {
		catalog := state.Catalogs[i]
		catalogMenuItems = append(catalogMenuItems,
			A(
				Href(sqlContextURL(catalog.Name, "")),
				Class("dropdown-item flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[0.8125rem] text-[var(--fgColor-default)] no-underline hover:bg-[var(--control-bgColor-hover)]"),
				I(Class(navIconClass()), Attr("data-lucide", "database"), Attr("aria-hidden", "true")),
				Span(Text(catalog.Name)),
			),
		)
	}

	schemaMenuItems := make([]Node, 0, len(state.Schemas)+1)
	if state.SelectedCatalog == "" {
		schemaMenuItems = append(schemaMenuItems,
			Div(
				Class("dropdown-item flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[0.8125rem] text-[var(--fgColor-muted)]"),
				Attr("aria-disabled", "true"),
				I(Class(navIconClass()), Attr("data-lucide", "info"), Attr("aria-hidden", "true")),
				Span(Text("Choose a catalog first")),
			),
		)
	} else {
		schemaMenuItems = append(schemaMenuItems,
			A(
				Href(sqlContextURL(state.SelectedCatalog, "")),
				Class("dropdown-item flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[0.8125rem] text-[var(--fgColor-default)] no-underline hover:bg-[var(--control-bgColor-hover)]"),
				I(Class(navIconClass()), Attr("data-lucide", "circle"), Attr("aria-hidden", "true")),
				Span(Text("Choose schema")),
			),
		)
		for i := range state.Schemas {
			schema := state.Schemas[i]
			schemaMenuItems = append(schemaMenuItems,
				A(
					Href(sqlContextURL(state.SelectedCatalog, schema.Name)),
					Class("dropdown-item flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[0.8125rem] text-[var(--fgColor-default)] no-underline hover:bg-[var(--control-bgColor-hover)]"),
					I(Class(navIconClass()), Attr("data-lucide", "folder"), Attr("aria-hidden", "true")),
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
		H2(Class(sqlResultsTitleClass()), Text("Compute")),
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
			Class(func() string {
				switch runtimeTone {
				case "success":
					return "flex items-start gap-3 rounded-xl border border-[var(--borderColor-success-muted)] bg-[var(--bgColor-success-muted)] px-4 py-3 text-sm text-[var(--fgColor-default)]"
				case "danger":
					return "flex items-start gap-3 rounded-xl border border-[var(--borderColor-danger-muted)] bg-[var(--bgColor-danger-muted)] px-4 py-3 text-sm text-[var(--fgColor-default)]"
				default:
					return "flex items-start gap-3 rounded-xl border border-[var(--borderColor-attention-muted)] bg-[var(--bgColor-attention-muted)] px-4 py-3 text-sm text-[var(--fgColor-default)]"
				}
			}()),
			Attr("data-sql-runtime-banner", "true"),
			I(Class(navIconClass()), Attr("data-lucide", "cpu"), Attr("aria-hidden", "true")),
			Div(
				Strong(Attr("data-sql-browser-runtime-title", "true"), Text("Browser runtime")),
				P(Attr("data-sql-browser-runtime-message", "true"), Text(state.BrowserRuntime.StatusReason)),
			),
		),
		Div(
			Class(sqlComputeMetaGridClass()),
			Div(
				Class(sqlComputeMetaItemClass()),
				P(Class(sqlComputeMetaLabelClass()), Text("Contract")),
				Code(Text(state.BrowserRuntime.ContractVersion)),
			),
			Div(
				Class(sqlComputeMetaItemClass()),
				P(Class(sqlComputeMetaLabelClass()), Text("Engine")),
				Code(Text(state.BrowserRuntime.Engine)),
			),
			Div(
				Class(sqlComputeMetaItemClass()),
				P(Class(sqlComputeMetaLabelClass()), Text("Adapter")),
				Code(Text(state.BrowserRuntime.Adapter)),
			),
			Div(
				Class(sqlComputeMetaItemClass()),
				P(Class(sqlComputeMetaLabelClass()), Text("Auth")),
				Code(Text(strings.Join(state.BrowserRuntime.RequiredAuthModes, ", "))),
			),
			Div(
				Class(sqlComputeMetaItemClass()),
				P(Class(sqlComputeMetaLabelClass()), Text("Files")),
				Code(Text(strings.Join(state.BrowserRuntime.SupportedFileURLTypes, ", "))),
			),
			Div(
				Class(sqlComputeMetaItemClass()),
				P(Class(sqlComputeMetaLabelClass()), Text("Guidance")),
				Span(Text(fmt.Sprintf("~%d rows / %d MB browser memory", state.BrowserRuntime.RecommendedMaxRows, state.BrowserRuntime.RecommendedMemoryMB))),
			),
		),
		Ul(
			Class(mutedClass()),
			Li(Text("Local browser execution supports a bounded read-only subset, including multi-table queries over up to four manifest-backed tables.")),
			Li(Text("Use an explicit LIMIT within the browser guidance before running locally.")),
			Li(Text("Auto will prefer local browser execution when the query and runtime satisfy guardrails, otherwise it will fall through to managed compute.")),
			Li(Text("If the browser runtime or query shape is unsupported, switch to Shared Endpoint or Auto and rerun.")),
		),
		Div(
			Class(buttonRowClass()),
			Button(
				Type("button"),
				ID("sql-reset-local-runtime"),
				Class(secondaryButtonClass()),
				I(Class(iconGlyphClass()), Attr("data-lucide", "rotate-ccw"), Attr("aria-hidden", "true")),
				Span(Text("Reset local runtime")),
			),
			Button(
				Type("button"),
				ID("sql-cancel-local-run"),
				Class(secondaryButtonClass()),
				Disabled(),
				I(Class(iconGlyphClass()), Attr("data-lucide", "square"), Attr("aria-hidden", "true")),
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
			"min-h-full",
			workspaceAside(
				"sql-workspace",
				"sql-aside",
				[]workspaceAsideTab{
					{ID: "explorer", Label: "Explorer", Icon: "database", Count: strconv.Itoa(len(state.Catalogs)), Content: explorerPanel, PanelClass: "sql-aside-explorer-panel"},
				},
				"explorer",
			),
			Div(
				Class("grid min-h-0 min-w-0 gap-4 overflow-hidden [grid-template-rows:minmax(0,2fr)_minmax(0,1fr)] max-md:block max-md:overflow-visible"),
				Div(
					Class("flex min-h-0 min-w-0 flex-col gap-3 max-md:mb-4"),
					Div(
						Class("flex flex-wrap items-center justify-between gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-3 max-md:items-stretch"),
						Div(
							Class("flex flex-wrap items-center gap-2 max-md:grid max-md:w-full max-md:grid-cols-1"),
							Details(
								Class(detailsClass("max-md:w-full")),
								Summary(
									Class(detailsSummaryClass(classNames(secondaryButtonClass("small"), "min-w-[calc(var(--overlay-width-xsmall)+var(--space-7))] gap-2 max-md:w-full max-md:justify-start"))),
									Title("Select catalog"),
									Attr("aria-label", "Select catalog"),
									I(Class(iconGlyphClass()), Attr("data-lucide", "database"), Attr("aria-hidden", "true")),
									Span(Class("max-w-[calc(var(--overlay-width-xsmall)+var(--space-4))] truncate text-left max-md:max-w-none"), Text(selectedCatalogLabel)),
									I(Class(iconGlyphClass()), Attr("data-lucide", "chevrons-up-down"), Attr("aria-hidden", "true")),
								),
								Div(
									Class(dropdownMenuClass("left-0 right-auto min-w-[14rem]")),
									Group(catalogMenuItems),
								),
							),
							Details(
								Class(detailsClass("max-md:w-full")),
								Summary(
									Class(detailsSummaryClass(classNames(secondaryButtonClass("small"), "min-w-[calc(var(--overlay-width-xsmall)+var(--space-7))] gap-2 max-md:w-full max-md:justify-start"))),
									Title("Select schema"),
									Attr("aria-label", "Select schema"),
									I(Class(iconGlyphClass()), Attr("data-lucide", "folder"), Attr("aria-hidden", "true")),
									Span(Class("max-w-[calc(var(--overlay-width-xsmall)+var(--space-4))] truncate text-left max-md:max-w-none"), Text(selectedSchemaLabel)),
									I(Class(iconGlyphClass()), Attr("data-lucide", "chevrons-up-down"), Attr("aria-hidden", "true")),
								),
								Div(
									Class(dropdownMenuClass("left-0 right-auto min-w-[14rem]")),
									Group(schemaMenuItems),
								),
							),
						),
					),
					browserRuntimeCard,
					Form(
						Method("post"),
						Action("/ui/sql/run"),
						Class("m-0 flex min-h-0 min-w-0 flex-1 flex-col gap-4 overflow-hidden rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
						Attr("data-sql-editor-form", "true"),
						csrfFieldProvider(),
						Input(Type("hidden"), Name("catalog"), Value(state.SelectedCatalog)),
						Input(Type("hidden"), Name("schema"), Value(state.SelectedSchema)),
						Input(Type("hidden"), Name("workload_type"), Value(state.ComputeRequest.WorkloadType)),
						Div(
							Class("flex flex-wrap items-start justify-between gap-3"),
							Div(
								Class(buttonRowClass()),
								Button(
									Type("submit"),
									ID("sql-run-query"),
									Class(primaryButtonClass()),
									I(Class(iconGlyphClass()), Attr("data-lucide", "play"), Attr("aria-hidden", "true")),
									Span(Text("Run query")),
								),
								Button(
									Type("submit"),
									FormAction("/ui/sql/run-async"),
									ID("sql-run-query-async"),
									Class(secondaryButtonClass()),
									I(Class(iconGlyphClass()), Attr("data-lucide", "timer-reset"), Attr("aria-hidden", "true")),
									Span(Text("Run async")),
								),
								Button(
									Type("button"),
									ID("sql-format-query"),
									Class(secondaryButtonClass()),
									I(Class(iconGlyphClass()), Attr("data-lucide", "align-left"), Attr("aria-hidden", "true")),
									Span(Text("Format SQL")),
								),
								A(Href("/ui/sql/jobs"), Class(secondaryButtonClass()), Text("Jobs")),
								Span(Class("sr-only"), Text("Shortcuts: Run Cmd or Ctrl plus Enter. Format Cmd or Ctrl plus Shift plus F.")),
							),
							Div(
								Class("flex flex-wrap items-center gap-2"),
								Label(
									Class("sr-only"),
									Attr("for", "sql-compute-mode"),
									Text("Compute mode"),
								),
								Select(
									ID("sql-compute-mode"),
									Name("compute_mode"),
									Class(formSelectClass("sql-compute-select w-auto min-w-[11rem]")),
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
									Class(formSelectClass("sql-compute-select w-auto min-w-[12rem]")),
									Group(endpointOptions),
								),
							),
						),
						Div(
							Class("flex min-h-0 min-w-0 flex-1 overflow-hidden"),
							El(
								"sql-editor-surface",
								Textarea(
									Class(formControlClass("sql-editor-textarea min-h-[16rem] font-mono text-[0.8125rem]")),
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
					Class("min-h-0 min-w-0 overflow-hidden"),
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
