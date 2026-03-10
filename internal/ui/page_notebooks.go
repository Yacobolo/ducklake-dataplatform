package ui

import (
	"bytes"
	"fmt"
	"html"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

var markdownRenderer = goldmark.New(
	goldmark.WithExtensions(extension.GFM),
)

type notebooksListRowData struct {
	Filter  string
	Name    string
	URL     string
	Owner   string
	Updated string
}

func notebooksListPage(principal domain.ContextPrincipal, rows []notebooksListRowData, page domain.PageRequest, total int64) Node {
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		r := rows[i]
		tableRows = append(tableRows, Tr(data.Show(containsExpr(r.Filter)), Td(A(Href(r.URL), Text(r.Name))), Td(Text(r.Owner)), Td(Text(r.Updated))))
	}
	tableNode := Node(emptyStateCard("No notebooks yet.", "New notebook", "/ui/notebooks/new"))
	if len(tableRows) > 0 {
		tableNode = Div(Class(cardClass("table-wrap")), Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("Owner")), Th(Text("Updated")))), TBody(Group(tableRows))))
	}
	return appPage("Notebooks", "notebooks", principal, pageToolbar("/ui/notebooks/new", "New notebook"), pageToolbar("/ui/notebooks/git-repos", "Git repos"), quickFilterCard("Filter by notebook or owner"), tableNode, paginationCard("/ui/notebooks", page, total))
}

type notebookCellRowData struct {
	ID           string
	Title        string
	CellType     string
	VisualSpec   *domain.VisualSpec
	Content      string
	Position     int
	LastRunAt    *time.Time
	EditURL      string
	UpdateURL    string
	DeleteURL    string
	RunURL       string
	MoveURL      string
	DownloadURL  string
	OpenInSQLURL string
	LastResult   *notebookCellResultData
}

type notebookCellResultData struct {
	Columns    []string
	RawRows    [][]interface{}
	Rows       [][]string
	RowCount   int
	Error      string
	Duration   time.Duration
	ExecutedAt *time.Time
}

type notebookJobRowData struct {
	ID      string
	URL     string
	State   string
	Updated string
}

type notebookDetailPageData struct {
	Principal       domain.ContextPrincipal
	NotebookID      string
	Name            string
	Owner           string
	Description     string
	SelectedCatalog string
	SelectedSchema  string
	BrowserRuntime  query.ManifestBrowserRuntimeSpec
	ComputeTargets  []sqlComputeTarget
	ComputeRequest  domain.ComputeExecutionRequest
	EditURL         string
	DeleteURL       string
	NewCellURL      string
	RunAllURL       string
	RunAllAsyncURL  string
	ReorderURL      string
	JobsURL         string
	GitRepoURL      string
	PromoteURL      string
	Jobs            []notebookJobRowData
	Cells           []notebookCellRowData
	Explorer        []catalogExplorerCatalogItem
	CSRFFieldFunc   func() Node
}

func notebookDetailPage(d notebookDetailPageData) Node {
	cellNodes := make([]Node, 0, len(d.Cells))
	outlineNodes := make([]Node, 0, len(d.Cells))
	for i := range d.Cells {
		c := d.Cells[i]
		formID := "cell-form-" + c.ID
		isMarkdown := c.CellType == string(domain.CellTypeMarkdown)

		runButton := Node(nil)
		if c.CellType == string(domain.CellTypeSQL) {
			runButton = Button(
				Type("submit"),
				Attr("form", formID),
				FormAction(c.RunURL),
				Class("btn btn-sm btn-icon notebook-cell-gutter-run"),
				Attr("data-run-cell", "true"),
				Attr("data-notebook-run-cell", "true"),
				Title("Run cell"),
				Attr("aria-label", "Run cell"),
				I(Class("btn-icon-glyph"), Attr("data-lucide", "play"), Attr("aria-hidden", "true")),
				Span(Class("sr-only"), Text("Run")),
			)
		}

		editorInput := Node(Textarea(
			Name("content"),
			Class("form-control notebook-editor"),
			Attr("data-cell-editor", "true"),
			Text(c.Content),
		))
		if c.CellType == string(domain.CellTypeSQL) {
			editorInput = Div(
				Class("notebook-sql-editor-host"),
				El(
					"sql-editor-surface",
					Attr("min-lines", "1"),
					Style("--sql-editor-height:auto; --sql-editor-flex:0 0 auto;"),
					Textarea(
						Name("content"),
						Class("form-control sql-editor-textarea"),
						Attr("data-cell-editor", "true"),
						Attr("spellcheck", "false"),
						Text(c.Content),
					),
				),
			)
		}

		editorFormClass := "notebook-cell-form"
		if isMarkdown {
			editorFormClass += " notebook-markdown-editor"
		}

		editorForm := Form(
			Method("post"),
			ID(formID),
			Class(editorFormClass),
			Action(c.UpdateURL),
			Attr("data-notebook-cell-form", "true"),
			d.CSRFFieldFunc(),
			Input(Type("hidden"), Name("catalog"), Value(d.SelectedCatalog)),
			Input(Type("hidden"), Name("schema"), Value(d.SelectedSchema)),
			Input(Type("hidden"), Name("workload_type"), Value(domain.ComputeWorkloadInteractive)),
			Input(Type("hidden"), Name("compute_mode"), Value(d.ComputeRequest.Mode)),
			Input(Type("hidden"), Name("endpoint_name"), Value(d.ComputeRequest.EndpointName)),
			editorInput,
		)

		cellMenuItems := []Node{}
		if !isMarkdown {
			cellMenuItems = append(cellMenuItems, actionMenuLink(c.OpenInSQLURL, "Open cell in SQL editor"))
		}
		cellMenuItems = append(cellMenuItems,
			Form(
				Method("post"),
				Action(c.MoveURL),
				d.CSRFFieldFunc(),
				Input(Type("hidden"), Name("direction"), Value("up")),
				Button(Type("submit"), Class("dropdown-item"), Text("Move cell up")),
			),
			Form(
				Method("post"),
				Action(c.MoveURL),
				d.CSRFFieldFunc(),
				Input(Type("hidden"), Name("direction"), Value("down")),
				Button(Type("submit"), Class("dropdown-item"), Text("Move cell down")),
			),
			Form(
				Method("post"),
				Action("/ui/notebooks/"+d.NotebookID+"/cells"),
				d.CSRFFieldFunc(),
				Input(Type("hidden"), Name("cell_type"), Value("sql")),
				Input(Type("hidden"), Name("content"), Value("")),
				Input(Type("hidden"), Name("position"), Value(strconv.Itoa(c.Position))),
				Button(Type("submit"), Class("dropdown-item"), Attr("data-add-above", "true"), Text("Insert SQL cell above")),
			),
			Form(
				Method("post"),
				Action("/ui/notebooks/"+d.NotebookID+"/cells"),
				d.CSRFFieldFunc(),
				Input(Type("hidden"), Name("cell_type"), Value("sql")),
				Input(Type("hidden"), Name("content"), Value("")),
				Input(Type("hidden"), Name("position"), Value(strconv.Itoa(c.Position+1))),
				Button(Type("submit"), Class("dropdown-item"), Attr("data-add-below", "true"), Text("Insert SQL cell below")),
			),
			actionMenuPost(c.DeleteURL, "Delete cell", d.CSRFFieldFunc, true),
		)

		cellMenu := Details(
			Class("dropdown details-reset details-overlay d-inline-block"),
			Summary(
				Class("btn btn-sm btn-icon"),
				Title("Cell actions"),
				Attr("aria-label", "Cell actions"),
				I(Class("btn-icon-glyph"), Attr("data-lucide", "ellipsis"), Attr("aria-hidden", "true")),
				Span(Class("sr-only"), Text("Cell actions")),
			),
			Div(
				Class("dropdown-menu dropdown-menu-sw"),
				Group(cellMenuItems),
			),
		)

		cellActions := Div(
			Class("notebook-cell-actions"),
			Button(
				Type("button"),
				Class("btn btn-sm btn-icon"),
				Attr("data-drag-handle", "true"),
				Title("Reorder cell (drag)"),
				Attr("aria-label", "Reorder cell (drag)"),
				I(Class("btn-icon-glyph"), Attr("data-lucide", "grip-vertical"), Attr("aria-hidden", "true")),
				Span(Class("sr-only"), Text("Reorder cell (drag)")),
			),
			cellMenu,
		)

		cellNodes = append(cellNodes, notebookInsertRail(d.NotebookID, c.Position, d.CSRFFieldFunc))

		mainContent := Node(
			Div(
				Class("notebook-cell-body"),
				editorForm,
				notebookResultNode(c),
			),
		)
		if isMarkdown {
			mainContent = Div(
				Class("notebook-cell-body"),
				Div(
					Class("notebook-markdown-preview markdown-body"),
					Attr("data-markdown-preview", "true"),
					Attr("tabindex", "0"),
					Title("Double-click to edit markdown"),
					Raw(renderMarkdownHTML(c.Content)),
				),
				editorForm,
			)
		}

		cellNodes = append(cellNodes,
			Article(
				Class("notebook-cell"),
				ID("cell-"+c.ID),
				Attr("data-notebook-cell", "true"),
				Attr("data-cell-id", c.ID),
				Attr("data-cell-type", c.CellType),
				data.Show(containsExpr(c.Title+" "+c.CellType+" "+c.Content)),
				Div(Class("notebook-cell-shell"),
					Aside(
						Class("notebook-cell-gutter notebook-cell-gutter-left"),
						runButton,
						Span(Class("notebook-cell-gutter-index"), Text(strconv.Itoa(c.Position+1))),
					),
					Div(
						Class("notebook-cell-content"),
						Div(
							Class("notebook-cell-frame"),
							mainContent,
						),
					),
					Aside(
						Class("notebook-cell-gutter notebook-cell-gutter-right"),
						cellActions,
					),
				),
			),
		)

		outlineText := strings.TrimSpace(c.Title)
		outlineLevel := 1
		if isMarkdown {
			if heading, level, ok := firstMarkdownHeading(c.Content); ok {
				outlineText = heading
				outlineLevel = level
			} else if outlineText == "" {
				outlineText = "Markdown"
			}
		} else if outlineText == "" {
			outlineText = fmt.Sprintf("SQL %d", c.Position+1)
		}

		outlineKindIcon := "square-terminal"
		outlineKindLabel := "SQL"
		if isMarkdown {
			outlineKindIcon = "pilcrow"
			outlineKindLabel = "Markdown"
		}

		outlineNodes = append(outlineNodes,
			Li(
				data.Show(containsExpr(outlineText+" "+c.CellType+" "+c.Content)),
				A(
					Href("#cell-"+c.ID),
					Class("notebook-outline-link"),
					Attr("data-outline-link", "true"),
					Attr("data-cell-anchor", "cell-"+c.ID),
					Attr("data-outline-level", strconv.Itoa(outlineLevel)),
					Span(Class("notebook-outline-label"), Text(outlineText)),
					Span(
						Class("notebook-outline-kind"),
						I(Class("notebook-outline-kind-icon"), Attr("data-lucide", outlineKindIcon), Attr("aria-hidden", "true")),
						Span(Class("sr-only"), Text(outlineKindLabel)),
					),
				),
			),
		)
	}

	if len(d.Cells) == 0 {
		cellNodes = append(cellNodes, notebookInsertRail(d.NotebookID, 0, d.CSRFFieldFunc))
	} else {
		last := d.Cells[len(d.Cells)-1]
		cellNodes = append(cellNodes, notebookInsertRail(d.NotebookID, last.Position+1, d.CSRFFieldFunc))
	}

	outlinePanel := Details(
		Class("notebook-outline-panel"),
		Attr("open", "open"),
		Summary(
			Class("notebook-outline-summary"),
			Span(Text("Outline")),
			Span(Class("notebook-outline-count"), Text(strconv.Itoa(len(d.Cells))+" cells")),
		),
		Div(
			Class("notebook-outline-body"),
			Div(
				Class("notebook-outline-filter d-flex flex-items-center gap-2"),
				Label(Class("sr-only"), Text("Filter cells")),
				Input(Type("search"), Class("form-control"), Placeholder("Filter cells"), data.Bind("q"), AutoComplete("off")),
			),
			Div(Class("notebook-outline-list"),
				Ul(Group(outlineNodes)),
			),
		),
	)

	explorerPanel := catalogExplorerPanel(catalogExplorerPanelData{
		Title:             "Catalog Explorer",
		FilterPlaceholder: "Filter catalogs or schemas",
		Catalogs:          d.Explorer,
		EmptyCatalogsText: "No catalogs found.",
	})

	descriptionNode := Node(nil)
	if strings.TrimSpace(d.Description) != "" {
		descriptionNode = Span(Class("notebook-toolbar-meta-item"), Text(d.Description))
	}

	toolbarNode := Div(
		Class("notebook-toolbar"),
		Div(Class("notebook-toolbar-main d-flex flex-justify-between flex-wrap flex-items-start gap-2"),
			Div(
				H2(Class("notebook-title"), Text(d.Name)),
				Div(
					Class("notebook-toolbar-meta"),
					Span(Class("notebook-toolbar-meta-item"), Text("Owner "+d.Owner)),
					descriptionNode,
				),
			),
			Div(Class("button-row notebook-toolbar-actions"),
				Form(
					Method("post"),
					Action(d.RunAllURL),
					Attr("data-notebook-run-all-form", "true"),
					d.CSRFFieldFunc(),
					Input(Type("hidden"), Name("compute_mode"), Value(d.ComputeRequest.Mode)),
					Input(Type("hidden"), Name("endpoint_name"), Value(d.ComputeRequest.EndpointName)),
					Input(Type("hidden"), Name("workload_type"), Value(domain.ComputeWorkloadInteractive)),
					Button(Type("submit"), ID("notebook-run-all"), Class(primaryButtonClass()), Text("Run all")),
				),
				Form(
					Method("post"),
					Action(d.RunAllAsyncURL),
					Attr("data-notebook-run-all-async-form", "true"),
					d.CSRFFieldFunc(),
					Input(Type("hidden"), Name("compute_mode"), Value(d.ComputeRequest.Mode)),
					Input(Type("hidden"), Name("endpoint_name"), Value(d.ComputeRequest.EndpointName)),
					Input(Type("hidden"), Name("workload_type"), Value(domain.ComputeWorkloadInteractive)),
					Button(Type("submit"), ID("notebook-run-all-async"), Class(secondaryButtonClass()), Text("Run async")),
				),
				A(Href(d.NewCellURL), Class(secondaryButtonClass()), Text("New cell")),
				A(Href(d.JobsURL), Class(secondaryButtonClass()), Text("Jobs")),
				A(Href(d.GitRepoURL), Class(secondaryButtonClass()), Text("Git repos")),
				Details(
					Class("dropdown details-reset details-overlay d-inline-block"),
					Summary(
						Class("btn btn-sm btn-icon"),
						Title("Notebook actions"),
						Attr("aria-label", "Notebook actions"),
						I(Class("btn-icon-glyph"), Attr("data-lucide", "ellipsis"), Attr("aria-hidden", "true")),
						Span(Class("sr-only"), Text("Notebook actions")),
					),
					Div(
						Class("dropdown-menu dropdown-menu-sw"),
						actionMenuLink(d.EditURL, "Notebook settings"),
						actionMenuPost(d.DeleteURL, "Delete notebook", d.CSRFFieldFunc, true),
					),
				),
			),
		),
	)

	workspaceNode := workspaceLayout(
		"notebook-workspace",
		workspaceAside(
			"notebook-"+d.NotebookID,
			"notebook-aside",
			[]workspaceAsideTab{
				{ID: "outline", Label: "Outline", Icon: "list-tree", Count: strconv.Itoa(len(d.Cells)), Content: outlinePanel, PanelClass: "notebook-outline-panel-wrap"},
				{ID: "explorer", Label: "Explorer", Icon: "database", Content: explorerPanel},
				{ID: "runs", Label: "Runs", Icon: "workflow", Count: strconv.Itoa(len(d.Jobs)), Content: notebookJobsAside(d.Jobs)},
			},
			"outline",
		),
		Div(
			Class("notebook-main"),
			toolbarNode,
			notebookComputeCard(d),
			notebookPromoteCard(d),
			Div(
				Class("notebook-cells"),
				Attr("data-notebook-selected-catalog", d.SelectedCatalog),
				Attr("data-notebook-selected-schema", d.SelectedSchema),
				Attr("data-reorder-url", d.ReorderURL),
				Group(cellNodes),
			),
		),
	)

	return appPage(
		"Notebook: "+d.Name,
		"notebooks",
		d.Principal,
		data.Signals(map[string]any{"q": ""}),
		workspaceNode,
		Script(Src(uiScriptHref("sql-editor.js"))),
		Script(Src(uiScriptHref("notebook.js"))),
	)
}

func notebookJobsAside(jobs []notebookJobRowData) Node {
	if len(jobs) == 0 {
		return P(Class("color-fg-muted text-small"), Text("No async jobs yet."))
	}
	rows := make([]Node, 0, len(jobs))
	for i := range jobs {
		job := jobs[i]
		rows = append(rows, Li(A(Href(job.URL), Text(job.ID)), Text(" "), statusLabel(job.State, notebookJobTone(job.State)), Text(" "), Span(Class(mutedClass()), Text(job.Updated))))
	}
	return Ul(Group(rows))
}

func notebookPromoteCard(d notebookDetailPageData) Node {
	options := []Node{}
	for i := range d.Cells {
		cell := d.Cells[i]
		if cell.CellType != string(domain.CellTypeSQL) {
			continue
		}
		label := fmt.Sprintf("%s (%s)", cell.Title, cell.ID)
		options = append(options, Option(Value(cell.ID), Text(label)))
	}
	if len(options) == 0 {
		return Div(Class(cardClass()), H2(Text("Promote to model")), P(Class(mutedClass()), Text("Add at least one SQL cell before promoting notebook output to a model.")))
	}
	return Div(
		Class(cardClass()),
		H2(Text("Promote to model")),
		Form(
			Method("post"),
			Action(d.PromoteURL),
			d.CSRFFieldFunc(),
			Input(Type("hidden"), Name("notebook_id"), Value(d.NotebookID)),
			Label(Text("Output cell")),
			Select(Name("output_cell_id"), Group(options)),
			Label(Text("Project")),
			Input(Name("project_name"), Required()),
			Label(Text("Model name")),
			Input(Name("name"), Required()),
			Label(Text("Materialization")),
			Select(Name("materialization"), Option(Value("VIEW"), Text("VIEW")), Option(Value("TABLE"), Text("TABLE")), Option(Value("INCREMENTAL"), Text("INCREMENTAL")), Option(Value("EPHEMERAL"), Text("EPHEMERAL"))),
			Button(Type("submit"), Class(primaryButtonClass()), Text("Promote notebook output")),
		),
	)
}

func notebookComputeCard(d notebookDetailPageData) Node {
	modeOptions := []Node{
		sqlComputeModeOption(domain.ComputeModeAuto, "Auto", d.ComputeRequest.Mode),
		sqlComputeModeOption(domain.ComputeModeByocLocal, "Local (BYOC)", d.ComputeRequest.Mode),
		sqlComputeModeOption(domain.ComputeModeSharedEndpoint, "Shared endpoint", d.ComputeRequest.Mode),
	}
	endpointOptions := []Node{Option(Value(""), Text("Platform default"))}
	for i := range d.ComputeTargets {
		target := d.ComputeTargets[i]
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
		if d.ComputeRequest.EndpointName == target.EndpointName {
			optionAttrs = append(optionAttrs, Selected())
		}
		if !target.Selectable {
			optionAttrs = append(optionAttrs, Disabled())
		}
		endpointOptions = append(endpointOptions, Option(Group(optionAttrs), Text(label)))
	}

	return Div(
		Class(cardClass("sql-compute-card")),
		Attr("data-notebook-browser-runtime", "true"),
		Attr("data-runtime-manifest-endpoint", "/ui/sql/runtime/manifest"),
		H2(Class("sql-results-title"), Text("Compute")),
		P(Class(mutedClass()), Text("Interactive SQL cell runs can use the browser-local DuckDB runtime. Notebook Run all and async runs stay on managed compute.")),
		Div(
			Class("button-row"),
			Select(ID("notebook-compute-mode"), Name("notebook_compute_mode"), Class("form-select sql-compute-select"), Group(modeOptions)),
			Select(ID("notebook-compute-endpoint"), Name("notebook_endpoint_name"), Class("form-select sql-compute-select"), Group(endpointOptions)),
			Button(
				Type("button"),
				ID("notebook-reset-local-runtime"),
				Class(secondaryButtonClass()),
				I(Class("btn-icon-glyph"), Attr("data-lucide", "rotate-ccw"), Attr("aria-hidden", "true")),
				Span(Text("Reset local runtime")),
			),
			Button(
				Type("button"),
				ID("notebook-cancel-local-run"),
				Class(secondaryButtonClass()),
				Disabled(),
				I(Class("btn-icon-glyph"), Attr("data-lucide", "square"), Attr("aria-hidden", "true")),
				Span(Text("Cancel local run")),
			),
		),
		Div(
			Class("Banner Banner-attention"),
			Attr("data-notebook-runtime-banner", "true"),
			I(Class("nav-icon"), Attr("data-lucide", "cpu"), Attr("aria-hidden", "true")),
			Div(
				Strong(Attr("data-notebook-browser-runtime-title", "true"), Text("Browser runtime")),
				P(Attr("data-notebook-browser-runtime-message", "true"), Text(d.BrowserRuntime.StatusReason)),
			),
		),
		P(Class(mutedClass()), Attr("data-notebook-browser-runtime-preflight", "true"), Text("")),
	)
}

func notebookInsertRail(notebookID string, position int, csrfField func() Node) Node {
	return Div(
		Class("notebook-insert-rail"),
		Div(
			Class("notebook-insert-actions"),
			Form(
				Method("post"),
				Action("/ui/notebooks/"+notebookID+"/cells"),
				csrfField(),
				Input(Type("hidden"), Name("cell_type"), Value("sql")),
				Input(Type("hidden"), Name("content"), Value("")),
				Input(Type("hidden"), Name("position"), Value(strconv.Itoa(position))),
				Button(Type("submit"), Class("btn btn-sm notebook-insert-btn"), Text("SQL")),
			),
			Form(
				Method("post"),
				Action("/ui/notebooks/"+notebookID+"/cells"),
				csrfField(),
				Input(Type("hidden"), Name("cell_type"), Value("markdown")),
				Input(Type("hidden"), Name("content"), Value("")),
				Input(Type("hidden"), Name("position"), Value(strconv.Itoa(position))),
				Button(Type("submit"), Class("btn btn-sm notebook-insert-btn"), Text("Markdown")),
			),
		),
	)
}

func notebookResultNode(c notebookCellRowData) Node {
	if c.CellType != string(domain.CellTypeSQL) {
		return Div(Class("notebook-output"), Attr("data-notebook-cell-output", "true"), P(Class(mutedClass()), Text("Markdown cell output is rendered by your markdown consumer.")))
	}

	if c.LastResult == nil {
		return Div(Class("notebook-output"), Attr("data-notebook-cell-output", "true"), P(Class(mutedClass()), Text("Run this cell to see output.")))
	}

	if c.LastResult.Error != "" {
		return Div(
			Class("notebook-output flash flash-error"),
			Attr("data-notebook-cell-output", "true"),
			H4(Text("Query Error")),
			P(Class(mutedClass()), Text("Last runtime: "+humanDuration(c.LastResult.Duration))),
			Pre(Text(c.LastResult.Error)),
		)
	}

	if c.VisualSpec != nil && c.VisualSpec.Kind != domain.VisualOutputTable {
		switch c.VisualSpec.Kind {
		case domain.VisualOutputMetric:
			field := ""
			if c.VisualSpec.Encodings.Value != nil {
				field = c.VisualSpec.Encodings.Value.Field
			}
			value, secondary := metricValueFromResult(c.LastResult, field)
			return Div(
				Class("notebook-output"),
				Attr("data-notebook-cell-output", "true"),
				visualMetricCard(defaultVisualTitle(c.VisualSpec, "Metric"), value, secondary),
				notebookResultDataDetails(c),
			)
		case domain.VisualOutputChart:
			return Div(
				Class("notebook-output"),
				Attr("data-notebook-cell-output", "true"),
				chartHost(c.LastResult.Columns, c.LastResult.RawRows, c.VisualSpec),
				notebookResultDataDetails(c),
			)
		}
	}

	return notebookTableResultNode(c)
}

func notebookTableResultNode(c notebookCellRowData) Node {
	if c.LastResult == nil {
		return Div(Class("notebook-output"), Attr("data-notebook-cell-output", "true"), P(Class(mutedClass()), Text("Run this cell to see output.")))
	}

	headers := make([]Node, 0, len(c.LastResult.Columns))
	for i := range c.LastResult.Columns {
		headers = append(headers, Th(Text(c.LastResult.Columns[i])))
	}

	displayRows := c.LastResult.Rows
	truncated := false
	if len(displayRows) > sqlEditorMaxRows {
		displayRows = displayRows[:sqlEditorMaxRows]
		truncated = true
	}

	rows := make([]Node, 0, len(displayRows))
	for i := range displayRows {
		cells := make([]Node, 0, len(displayRows[i]))
		for j := range displayRows[i] {
			cells = append(cells, Td(Text(displayRows[i][j])))
		}
		rows = append(rows, Tr(Group(cells)))
	}

	meta := fmt.Sprintf("%d row(s), runtime %s", c.LastResult.RowCount, humanDuration(c.LastResult.Duration))
	if truncated {
		meta = fmt.Sprintf("%d row(s), showing first %d, runtime %s", c.LastResult.RowCount, sqlEditorMaxRows, humanDuration(c.LastResult.Duration))
	}

	return Div(
		Class("notebook-output table-wrap"),
		Attr("data-notebook-cell-output", "true"),
		Div(Class("d-flex flex-justify-between flex-wrap flex-items-center gap-2"),
			H4(Text("Output")),
			Div(Class("button-row"),
				A(
					Href(c.DownloadURL),
					Class("btn btn-sm btn-icon"),
					Title("Download result CSV"),
					Attr("aria-label", "Download result CSV"),
					I(Class("btn-icon-glyph"), Attr("data-lucide", "download"), Attr("aria-hidden", "true")),
					Span(Class("sr-only"), Text("Download result CSV")),
				),
			),
		),
		P(Class(mutedClass()), Text(meta)),
		Table(Class("data-table"), THead(Tr(Group(headers))), TBody(Group(rows))),
	)
}

func notebookResultDataDetails(c notebookCellRowData) Node {
	return Details(
		Class("notebook-output-data"),
		Summary(Text("View data")),
		notebookTableResultNode(c),
	)
}

func metricValueFromResult(result *notebookCellResultData, field string) (string, string) {
	if result == nil || len(result.RawRows) == 0 || len(result.Columns) == 0 {
		return "-", "No rows"
	}
	colIdx := 0
	if field != "" {
		for i := range result.Columns {
			if result.Columns[i] == field {
				colIdx = i
				break
			}
		}
	}
	if len(result.RawRows[0]) <= colIdx {
		return "-", "No metric value"
	}
	return fmt.Sprint(result.RawRows[0][colIdx]), fmt.Sprintf("%d row(s)", result.RowCount)
}

func defaultVisualTitle(spec *domain.VisualSpec, fallback string) string {
	if spec == nil || strings.TrimSpace(spec.Title) == "" {
		return fallback
	}
	return spec.Title
}

func notebookJobTone(state string) string {
	switch strings.ToLower(state) {
	case string(domain.JobStateComplete):
		return "success"
	case string(domain.JobStateFailed):
		return "severe"
	case string(domain.JobStateRunning):
		return "accent"
	default:
		return "attention"
	}
}

type gitRepoRowData struct {
	ID         string
	URL        string
	Repository string
	Branch     string
	Path       string
	Owner      string
	LastSync   string
}

type notebookGitReposListPageData struct {
	Principal domain.ContextPrincipal
	Rows      []gitRepoRowData
	Page      domain.PageRequest
	Total     int64
}

func notebookGitReposListPage(d notebookGitReposListPageData) Node {
	rows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		rows = append(rows, Tr(
			Td(A(Href(row.URL), Text(row.Repository))),
			Td(Text(row.Branch)),
			Td(Text(row.Path)),
			Td(Text(row.Owner)),
			Td(Text(row.LastSync)),
		))
	}
	tableNode := Node(emptyStateCard("No Git repositories registered.", "Register Git repo", "/ui/notebooks/git-repos/new"))
	if len(rows) > 0 {
		tableNode = Div(Class(cardClass("table-wrap")), Table(Class("data-table"), THead(Tr(Th(Text("Repository")), Th(Text("Branch")), Th(Text("Path")), Th(Text("Owner")), Th(Text("Last sync")))), TBody(Group(rows))))
	}
	return appPage("Notebook Git Repos", "notebooks", d.Principal, pageToolbar("/ui/notebooks/git-repos/new", "Register Git repo"), tableNode, paginationCard("/ui/notebooks/git-repos", d.Page, d.Total))
}

func notebookGitReposNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Register Git Repo", "notebooks", "/ui/notebooks/git-repos", csrfFieldProvider,
		Label(Text("Repository URL")),
		Input(Name("url"), Required()),
		Label(Text("Branch")),
		Input(Name("branch"), Value("main"), Required()),
		Label(Text("Path")),
		Input(Name("path")),
		Label(Text("Auth token")),
		Input(Name("auth_token"), Type("password")),
	)
}

type notebookGitRepoDetailPageData struct {
	Principal     domain.ContextPrincipal
	ID            string
	URL           string
	Branch        string
	Path          string
	Owner         string
	LastSync      string
	LastCommit    string
	DeleteURL     string
	SyncURL       string
	CSRFFieldFunc func() Node
}

func notebookGitRepoDetailPage(d notebookGitRepoDetailPageData) Node {
	return appPage(
		"Git Repo",
		"notebooks",
		d.Principal,
		Div(
			Class(cardClass()),
			P(Text("Repository: "+d.URL)),
			P(Text("Branch: "+d.Branch)),
			P(Text("Path: "+d.Path)),
			P(Text("Owner: "+d.Owner)),
			P(Text("Last sync: "+d.LastSync)),
			P(Text("Last commit: "+d.LastCommit)),
			Div(Class("BtnGroup"),
				Form(Method("post"), Action(d.SyncURL), d.CSRFFieldFunc(), Button(Type("submit"), Class(primaryButtonClass()), Text("Sync repo"))),
				Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldFunc(), Button(Type("submit"), Class("btn btn-danger"), Text("Delete repo"))),
			),
		),
	)
}

type notebookGitRepoSyncResultPageData struct {
	Principal domain.ContextPrincipal
	GitRepoID string
	Result    *domain.GitSyncResult
}

func notebookGitRepoSyncResultPage(d notebookGitRepoSyncResultPageData) Node {
	if d.Result == nil {
		return appPage("Git Sync", "notebooks", d.Principal, emptyStateCard("No sync result available.", "Back to repo", "/ui/notebooks/git-repos/"+d.GitRepoID))
	}
	return appPage("Git Sync", "notebooks", d.Principal, Div(Class(cardClass()), P(Text("Created notebooks: "+strconv.Itoa(d.Result.NotebooksCreated))), P(Text("Updated notebooks: "+strconv.Itoa(d.Result.NotebooksUpdated))), P(Text("Deleted notebooks: "+strconv.Itoa(d.Result.NotebooksDeleted))), P(Text("Commit: "+d.Result.CommitSHA))))
}

type notebookGitRepoSyncUnavailablePageData struct {
	Principal domain.ContextPrincipal
	GitRepoID string
	RepoURL   string
	Branch    string
	Path      string
	Message   string
}

func notebookGitRepoSyncUnavailablePage(d notebookGitRepoSyncUnavailablePageData) Node {
	return appPage(
		"Git Sync Unavailable",
		"notebooks",
		d.Principal,
		pageToolbar("/ui/notebooks/git-repos/"+d.GitRepoID, "Back to repo"),
		Div(
			Class(cardClass()),
			H2(Text("Sync is not available yet")),
			P(Text(d.Message)),
			P(Text("Repository: "+d.RepoURL)),
			P(Text("Branch: "+d.Branch)),
			P(Text("Path: "+d.Path)),
			P(Class(mutedClass()), Text("The repo is registered correctly, but server-side sync execution has not been implemented yet.")),
		),
	)
}

type notebookJobsListPageData struct {
	Principal  domain.ContextPrincipal
	NotebookID string
	Rows       []notebookJobRowData
	Page       domain.PageRequest
	Total      int64
}

func notebookJobsListPage(d notebookJobsListPageData) Node {
	rows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		rows = append(rows, Tr(Td(A(Href(row.URL), Text(row.ID))), Td(statusLabel(row.State, notebookJobTone(row.State))), Td(Text(row.Updated))))
	}
	tableNode := Node(emptyStateCard("No notebook jobs found.", "Back to notebook", "/ui/notebooks/"+d.NotebookID))
	if len(rows) > 0 {
		tableNode = Div(Class(cardClass("table-wrap")), Table(Class("data-table"), THead(Tr(Th(Text("Job ID")), Th(Text("State")), Th(Text("Updated")))), TBody(Group(rows))))
	}
	return appPage("Notebook Jobs", "notebooks", d.Principal, tableNode, paginationCard("/ui/notebooks/"+d.NotebookID+"/jobs", d.Page, d.Total))
}

type notebookJobDetailPageData struct {
	Principal  domain.ContextPrincipal
	NotebookID string
	JobID      string
	State      string
	Result     string
	ErrorText  string
	CreatedAt  string
	UpdatedAt  string
}

func notebookJobDetailPage(d notebookJobDetailPageData) Node {
	return appPage("Notebook Job: "+d.JobID, "notebooks", d.Principal, Div(Class(cardClass()), P(Text("State: "), statusLabel(d.State, notebookJobTone(d.State))), P(Text("Created: "+d.CreatedAt)), P(Text("Updated: "+d.UpdatedAt)), P(Text("Error: "+d.ErrorText))), Div(Class(cardClass()), H2(Text("Result payload")), Pre(Text(d.Result))))
}

func humanDuration(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	if d < time.Millisecond {
		return d.String()
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return d.Truncate(time.Millisecond).String()
}

func humanRelativeTime(ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	d := time.Since(ts)
	if d < 0 {
		d = 0
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
	return fmt.Sprintf("%dd ago", int(d.Hours()/24))
}

func firstMarkdownHeading(source string) (string, int, bool) {
	for _, raw := range strings.Split(source, "\n") {
		line := strings.TrimSpace(raw)
		if !strings.HasPrefix(line, "#") {
			continue
		}

		level := 0
		for level < len(line) && line[level] == '#' {
			level++
		}
		if level == 0 || level > 6 {
			continue
		}
		if level < len(line) && line[level] != ' ' {
			continue
		}

		heading := strings.TrimSpace(line[level:])
		if heading == "" {
			continue
		}
		return heading, level, true
	}

	return "", 0, false
}

func renderMarkdownHTML(source string) string {
	if strings.TrimSpace(source) == "" {
		return `<p class="notebook-markdown-empty">Double-click to add markdown.</p>`
	}

	var out bytes.Buffer
	if err := markdownRenderer.Convert([]byte(source), &out); err != nil {
		return "<pre>" + html.EscapeString(source) + "</pre>"
	}
	return out.String()
}

func notebooksNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Notebook", "notebooks", "/ui/notebooks", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Description")),
		Textarea(Name("description")),
		Label(Text("Initial SQL Source")),
		Textarea(Name("source")),
	)
}

func notebooksEditPage(principal domain.ContextPrincipal, notebookID string, notebook *domain.Notebook, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Edit Notebook", "notebooks", "/ui/notebooks/"+notebookID+"/update", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Value(notebook.Name), Required()),
		Label(Text("Description")),
		Textarea(Name("description"), Text(optionalStringValue(notebook.Description))),
	)
}

func notebookCellsNewPage(principal domain.ContextPrincipal, notebookID string, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Notebook Cell", "notebooks", "/ui/notebooks/"+notebookID+"/cells", csrfFieldProvider,
		Label(Text("Cell Type")),
		Select(Name("cell_type"), Option(Value("sql"), Text("sql")), Option(Value("markdown"), Text("markdown"))),
		Label(Text("Content")),
		Textarea(Name("content"), Required()),
		Label(Text("Position (optional)")),
		Input(Name("position")),
	)
}

func notebookCellsEditPage(principal domain.ContextPrincipal, notebookID, cellID string, cell *domain.Cell, csrfFieldProvider func() Node) Node {
	nodes := []Node{
		Label(Text("Content")),
		Textarea(Name("content"), Text(cell.Content), Required()),
		Label(Text("Position")),
		Input(Name("position"), Value(strconv.Itoa(cell.Position))),
	}
	if cell.CellType == domain.CellTypeSQL {
		visualKind := "table"
		chartType := ""
		title := ""
		subtitle := ""
		xField := ""
		yField := ""
		seriesField := ""
		labelField := ""
		valueField := ""
		if cell.VisualSpec != nil {
			visualKind = string(cell.VisualSpec.Kind)
			title = cell.VisualSpec.Title
			subtitle = cell.VisualSpec.Subtitle
			if cell.VisualSpec.ChartType != nil {
				chartType = string(*cell.VisualSpec.ChartType)
			}
			if cell.VisualSpec.Encodings.X != nil {
				xField = cell.VisualSpec.Encodings.X.Field
			}
			if cell.VisualSpec.Encodings.Y != nil {
				yField = cell.VisualSpec.Encodings.Y.Field
			}
			if cell.VisualSpec.Encodings.Series != nil {
				seriesField = cell.VisualSpec.Encodings.Series.Field
			}
			if cell.VisualSpec.Encodings.Label != nil {
				labelField = cell.VisualSpec.Encodings.Label.Field
			}
			if cell.VisualSpec.Encodings.Value != nil {
				valueField = cell.VisualSpec.Encodings.Value.Field
			}
		}
		nodes = append(nodes,
			Label(Text("Visual kind")),
			Select(Name("visual_kind"),
				optionSelected("table", visualKind),
				optionSelected("metric", visualKind),
				optionSelected("chart", visualKind),
			),
			Label(Text("Chart type")),
			Select(Name("chart_type"),
				optionSelected("bar", chartType),
				optionSelected("line", chartType),
				optionSelected("area", chartType),
				optionSelected("pie", chartType),
				optionSelected("doughnut", chartType),
				optionSelected("scatter", chartType),
				optionSelected("stacked_bar", chartType),
			),
			Label(Text("Visual title")),
			Input(Name("visual_title"), Value(title)),
			Label(Text("Visual subtitle")),
			Input(Name("visual_subtitle"), Value(subtitle)),
			Label(Text("X field")),
			Input(Name("visual_x"), Value(xField)),
			Label(Text("Y field")),
			Input(Name("visual_y"), Value(yField)),
			Label(Text("Series field")),
			Input(Name("visual_series"), Value(seriesField)),
			Label(Text("Label field")),
			Input(Name("visual_label"), Value(labelField)),
			Label(Text("Value field")),
			Input(Name("visual_value"), Value(valueField)),
		)
	}
	return formPage(principal, "Edit Notebook Cell", "notebooks", "/ui/notebooks/"+notebookID+"/cells/"+cellID+"/update", csrfFieldProvider, nodes...)
}
