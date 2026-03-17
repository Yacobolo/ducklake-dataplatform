package notebooks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"
	"duck-demo/internal/ui/core"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

var markdownRenderer = goldmark.New(
	goldmark.WithExtensions(extension.GFM),
)

type notebookCellRow struct {
	ID          string
	Title       string
	CellType    string
	VisualSpec  *domain.VisualSpec
	Content     string
	Position    int
	LastRunAt   *time.Time
	EditURL     string
	UpdateURL   string
	DeleteURL   string
	RunURL      string
	MoveURL     string
	DownloadURL string
	LastResult  *notebookCellResultData
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
	Jobs            []notebookJobRow
	Cells           []notebookCellRow
	Explorer        []core.CatalogExplorerCatalogItem
	CSRFFieldFunc   func() Node
}

func notebookDetailPage(d notebookDetailPageData) Node {
	cellNodes := make([]Node, 0, len(d.Cells))
	outlineNodes := make([]Node, 0, len(d.Cells))
	for i := range d.Cells {
		c := d.Cells[i]
		formID := "cell-form-" + c.ID
		isMarkdown := c.CellType == string(domain.CellTypeMarkdown)
		cellFrameClass := "relative overflow-hidden rounded-xl border border-border-muted bg-transparent before:absolute before:inset-y-0 before:left-0 before:w-[2px] before:bg-border-accent before:opacity-0 before:transition-opacity focus-within:border-border-accent focus-within:before:opacity-100 [.is-active-cell_&]:before:opacity-100"
		cellBodyClass := "flex flex-col gap-3 px-3 pt-2 pb-3"
		markdownPreviewClass := "markdown-body min-h-[6rem] cursor-text rounded-xl border border-border bg-surface-muted px-4 py-3 [&>:first-child]:mt-0 [&>:last-child]:mb-0 [.is-editing-markdown_&]:hidden"
		editorFormClass := "notebook-cell-form"
		if isMarkdown {
			cellFrameClass = "relative overflow-hidden rounded-xl border border-transparent bg-transparent before:absolute before:inset-y-0 before:left-0 before:w-[2px] before:bg-border-accent before:opacity-0 before:transition-opacity [.is-editing-markdown_&]:border-border-muted [.is-editing-markdown_&]:before:opacity-100"
			cellBodyClass = "flex flex-col gap-3 p-0"
			editorFormClass += " hidden [.is-editing-markdown_&]:block"
		}

		runButton := Node(nil)
		if c.CellType == string(domain.CellTypeSQL) {
			runButton = Button(
				Type("submit"),
				Attr("form", formID),
				FormAction(c.RunURL),
				Class("inline-flex min-h-8 min-w-8 items-center justify-center rounded-lg border border-transparent bg-transparent px-2 text-success-text hover:border-border-muted hover:bg-surface-muted"),
				Attr("data-run-cell", "true"),
				Attr("data-notebook-run-cell", "true"),
				Title("Run cell"),
				Attr("aria-label", "Run cell"),
				I(Class(core.IconGlyphClass()), Attr("data-lucide", "play"), Attr("aria-hidden", "true")),
				Span(Class("sr-only"), Text("Run")),
			)
		}

		editorInput := Node(Textarea(
			Name("content"),
			Class("w-full rounded-xl border border-border bg-background px-3 py-2 text-sm text-foreground shadow-xs transition-colors placeholder:text-muted focus:border-border-accent focus:outline-none focus:ring-2 focus:ring-[var(--color-ring)] min-h-[10rem] font-mono text-[0.8125rem]"),
			Attr("data-cell-editor", "true"),
			Text(c.Content),
		))
		if c.CellType == string(domain.CellTypeSQL) {
			editorInput = Div(
				Class("min-w-0 overflow-hidden"),
				El(
					"sql-editor-surface",
					Attr("min-lines", "1"),
					Style("--sql-editor-height:auto; --sql-editor-flex:0 0 auto;"),
					Textarea(
						Name("content"),
						Class("sql-editor-textarea w-full rounded-xl border border-border bg-background px-3 py-2 text-sm text-foreground shadow-xs transition-colors placeholder:text-muted focus:border-border-accent focus:outline-none focus:ring-2 focus:ring-[var(--color-ring)] min-h-[10rem] rounded-none border-0 bg-transparent font-mono text-[0.8125rem] shadow-none"),
						Attr("data-cell-editor", "true"),
						Attr("spellcheck", "false"),
						Text(c.Content),
					),
				),
			)
		} else {
			editorInput = core.TextareaControl("min-h-24 rounded-none border-0 bg-transparent px-4 pt-4 pb-3 font-mono text-[0.8125rem] shadow-none focus-visible:outline-none", Name("content"), Attr("data-cell-editor", "true"), Text(c.Content))
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

		cellMenuItems := []Node{
			Form(Method("post"), Action(c.MoveURL), d.CSRFFieldFunc(), Input(Type("hidden"), Name("direction"), Value("up")), Button(Type("submit"), Class(dropdownItemClass("text-foreground")), Text("Move cell up"))),
			Form(Method("post"), Action(c.MoveURL), d.CSRFFieldFunc(), Input(Type("hidden"), Name("direction"), Value("down")), Button(Type("submit"), Class(dropdownItemClass("text-foreground")), Text("Move cell down"))),
			Form(Method("post"), Action("/ui/notebooks/"+d.NotebookID+"/cells"), d.CSRFFieldFunc(), Input(Type("hidden"), Name("cell_type"), Value("sql")), Input(Type("hidden"), Name("content"), Value("")), Input(Type("hidden"), Name("position"), Value(strconv.Itoa(c.Position))), Button(Type("submit"), Class(dropdownItemClass("text-foreground")), Attr("data-add-above", "true"), Text("Insert SQL cell above"))),
			Form(Method("post"), Action("/ui/notebooks/"+d.NotebookID+"/cells"), d.CSRFFieldFunc(), Input(Type("hidden"), Name("cell_type"), Value("sql")), Input(Type("hidden"), Name("content"), Value("")), Input(Type("hidden"), Name("position"), Value(strconv.Itoa(c.Position+1))), Button(Type("submit"), Class(dropdownItemClass("text-foreground")), Attr("data-add-below", "true"), Text("Insert SQL cell below"))),
			actionMenuPost(c.DeleteURL, "Delete cell", d.CSRFFieldFunc, true),
		}

		cellMenu := Details(
			Class("relative inline-block"),
			Summary(
				Class("list-none [&::-webkit-details-marker]:hidden inline-flex min-h-8 min-w-8 items-center justify-center rounded-lg border border-border bg-background px-2 text-foreground shadow-xs hover:bg-surface-muted"),
				Title("Cell actions"),
				Attr("aria-label", "Cell actions"),
				I(Class(core.IconGlyphClass()), Attr("data-lucide", "ellipsis"), Attr("aria-hidden", "true")),
				Span(Class("sr-only"), Text("Cell actions")),
			),
			Div(Class(dropdownMenuClass("min-w-[14rem]")), Group(cellMenuItems)),
		)

		cellActions := Div(
			Class("flex flex-col items-center gap-2 opacity-0 pointer-events-none transition-opacity group-hover/notebook-cell:opacity-100 group-hover/notebook-cell:pointer-events-auto max-md:flex-row max-md:opacity-100 max-md:pointer-events-auto"),
			core.IconButton("small", Type("button"), Attr("data-drag-handle", "true"), Title("Reorder cell (drag)"), Attr("aria-label", "Reorder cell (drag)"), I(Class(core.IconGlyphClass()), Attr("data-lucide", "grip-vertical"), Attr("aria-hidden", "true")), Span(Class("sr-only"), Text("Reorder cell (drag)"))),
			cellMenu,
		)

		cellNodes = append(cellNodes, notebookInsertRail(d.NotebookID, c.Position, d.CSRFFieldFunc))

		mainContent := Node(Div(Class(cellBodyClass), editorForm, notebookResultNode(c)))
		if isMarkdown {
			mainContent = Div(
				Class(cellBodyClass),
				Div(Class(markdownPreviewClass), Attr("data-markdown-preview", "true"), Attr("tabindex", "0"), Title("Double-click to edit markdown"), Raw(renderMarkdownHTML(c.Content))),
				editorForm,
			)
		}

		cellNodes = append(cellNodes, Article(
			Class("group/notebook-cell relative scroll-mt-[calc(var(--spacing) * 18)] px-1 transition-[background-color,box-shadow] [&.dragging]:opacity-75 [&.drag-over]:shadow-[inset_0_0_0_2px_var(--color-border-accent)]"),
			ID("cell-"+c.ID),
			Attr("data-notebook-cell", "true"),
			Attr("data-cell-id", c.ID),
			Attr("data-cell-type", c.CellType),
			data.Show(coreContainsExpr(c.Title+" "+c.CellType+" "+c.Content)),
			Div(Class("grid gap-2 [grid-template-columns:2rem_minmax(0,1fr)_3rem] max-md:grid-cols-[auto_1fr_auto]"),
				Aside(Class("flex flex-col items-center gap-2 pt-2 max-md:col-start-1 max-md:row-start-1 max-md:flex-row max-md:justify-start max-md:pt-0"), runButton, Span(Class("font-mono text-xs text-muted"), Text(strconv.Itoa(c.Position+1)))),
				Div(Class("min-w-0 max-md:col-span-full max-md:row-start-2"), Div(Class(cellFrameClass), mainContent)),
				Aside(Class("flex flex-col items-center gap-2 pt-2 max-md:col-start-3 max-md:row-start-1 max-md:pt-0"), cellActions),
			),
		))

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

		outlineIndentClass := ""
		if outlineLevel == 2 {
			outlineIndentClass = "pl-[calc(calc(var(--spacing) * 2)+calc(var(--spacing) * 1))]"
		} else if outlineLevel >= 3 {
			outlineIndentClass = "pl-[calc(calc(var(--spacing) * 2)+calc(var(--spacing) * 2))] [&_.notebook-outline-label]:text-[var(--text-xs)]"
		}
		outlineNodes = append(outlineNodes, Li(
			Class("border-b border-border-muted last:border-b-0"),
			data.Show(coreContainsExpr(outlineText+" "+c.CellType+" "+c.Content)),
			A(Href("#cell-"+c.ID), Class(core.ClassNames("flex items-center justify-between gap-2 rounded-lg px-2 py-2 text-sm text-foreground no-underline transition-colors visited:text-foreground hover:text-accent focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-[2px] focus-visible:outline-[var(--color-ring)] [&.is-active-outline-link]:text-accent", outlineIndentClass)), Attr("data-outline-link", "true"), Attr("data-cell-anchor", "cell-"+c.ID), Attr("data-outline-level", strconv.Itoa(outlineLevel)),
				Span(Class("notebook-outline-label min-w-0 flex-1 truncate"), Text(outlineText)),
				Span(Class("inline-flex items-center text-muted"), I(Class("h-4 w-4"), Attr("data-lucide", outlineKindIcon), Attr("aria-hidden", "true")), Span(Class("sr-only"), Text(outlineKindLabel))),
			),
		))
	}

	if len(d.Cells) == 0 {
		cellNodes = append(cellNodes, notebookInsertRail(d.NotebookID, 0, d.CSRFFieldFunc))
	} else {
		last := d.Cells[len(d.Cells)-1]
		cellNodes = append(cellNodes, notebookInsertRail(d.NotebookID, last.Position+1, d.CSRFFieldFunc))
	}

	outlinePanel := Details(
		Class("m-0 border-0 bg-transparent"),
		Attr("open", "open"),
		Summary(Class("list-none [&::-webkit-details-marker]:hidden flex items-center justify-between gap-2 border-b border-border-muted pb-2 text-sm font-semibold"),
			Span(Text("Outline")),
			Span(Class("text-xs font-normal text-muted"), Text(strconv.Itoa(len(d.Cells))+" cells")),
		),
		Div(Class("pt-2"),
			Div(Class("mb-2 flex items-center gap-2"), Label(Class("sr-only"), Text("Filter cells")), core.InputControl("", Type("search"), Placeholder("Filter cells"), data.Bind("q"), AutoComplete("off"))),
			Div(Class("mt-2 max-h-[28rem] overflow-auto"), Ul(Class("m-0 grid list-none gap-0 p-0"), Group(outlineNodes))),
		),
	)

	explorerPanel := core.CatalogExplorerPanel(core.CatalogExplorerPanelData{
		Title:             "Catalog Explorer",
		FilterPlaceholder: "Filter catalogs or schemas",
		Catalogs:          d.Explorer,
		EmptyCatalogsText: "No catalogs found.",
	})

	descriptionNode := Node(nil)
	if strings.TrimSpace(d.Description) != "" {
		descriptionNode = Span(Class("text-sm text-muted"), Text(d.Description))
	}

	toolbarNode := Div(
		Class("sticky top-0 z-20 mb-2 border-b border-border-muted bg-background pb-2"),
		Div(Class("flex flex-wrap items-start justify-between gap-2"),
			Div(H2(Class("m-0 text-2xl font-semibold"), Text(d.Name)), Div(Class("mt-1 flex flex-wrap gap-2"), Span(Class("text-sm text-muted"), Text("Owner "+d.Owner)), descriptionNode)),
			Div(Class("mt-0 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex"),
				Form(Method("post"), Action(d.RunAllURL), Attr("data-notebook-run-all-form", "true"), d.CSRFFieldFunc(), Input(Type("hidden"), Name("compute_mode"), Value(d.ComputeRequest.Mode)), Input(Type("hidden"), Name("endpoint_name"), Value(d.ComputeRequest.EndpointName)), Input(Type("hidden"), Name("workload_type"), Value(domain.ComputeWorkloadInteractive)), core.PrimaryButton("", Type("submit"), ID("notebook-run-all"), Text("Run all"))),
				Form(Method("post"), Action(d.RunAllAsyncURL), Attr("data-notebook-run-all-async-form", "true"), d.CSRFFieldFunc(), Input(Type("hidden"), Name("compute_mode"), Value(d.ComputeRequest.Mode)), Input(Type("hidden"), Name("endpoint_name"), Value(d.ComputeRequest.EndpointName)), Input(Type("hidden"), Name("workload_type"), Value(domain.ComputeWorkloadInteractive)), core.SecondaryButton("", Type("submit"), ID("notebook-run-all-async"), Text("Run async"))),
				core.SecondaryLink(d.NewCellURL, "", Text("New cell")),
				core.SecondaryLink(d.JobsURL, "", Text("Jobs")),
				core.SecondaryLink(d.GitRepoURL, "", Text("Git repos")),
				Details(
					Class("relative inline-block"),
					Summary(Class("list-none [&::-webkit-details-marker]:hidden inline-flex min-h-8 min-w-8 items-center justify-center rounded-lg border border-border bg-background px-2 text-foreground shadow-xs hover:bg-surface-muted"), Title("Notebook actions"), Attr("aria-label", "Notebook actions"), I(Class(core.IconGlyphClass()), Attr("data-lucide", "ellipsis"), Attr("aria-hidden", "true")), Span(Class("sr-only"), Text("Notebook actions"))),
					Div(Class(dropdownMenuClass("min-w-[14rem]")), actionMenuLink(d.EditURL, "Notebook settings"), actionMenuPost(d.DeleteURL, "Delete notebook", d.CSRFFieldFunc, true)),
				),
			),
		),
	)

	workspaceNode := core.WorkspaceLayout(
		"min-h-0",
		core.WorkspaceAside(
			"notebook-"+d.NotebookID,
			"notebook-aside",
			[]core.WorkspaceAsideTab{
				{ID: "outline", Label: "Outline", Icon: "list-tree", Count: strconv.Itoa(len(d.Cells)), Content: outlinePanel},
				{ID: "explorer", Label: "Explorer", Icon: "database", Content: explorerPanel},
				{ID: "runs", Label: "Runs", Icon: "workflow", Count: strconv.Itoa(len(d.Jobs)), Content: notebookJobsAside(d.Jobs)},
			},
			"outline",
		),
		Div(
			Class("min-w-0"),
			toolbarNode,
			notebookComputeCard(d),
			notebookPromoteCard(d),
			Div(Class("grid min-w-0 gap-0"), Attr("data-notebook-selected-catalog", d.SelectedCatalog), Attr("data-notebook-selected-schema", d.SelectedSchema), Attr("data-reorder-url", d.ReorderURL), Group(cellNodes)),
		),
	)

	return core.AppPage(
		"Notebook: "+d.Name,
		"notebooks",
		d.Principal,
		data.Signals(map[string]any{"q": ""}),
		workspaceNode,
		Script(Src(core.UIScriptHref("sql-editor.js"))),
		Script(Src(core.UIScriptHref("notebook.js"))),
	)
}

func notebookJobsAside(jobs []notebookJobRow) Node {
	if len(jobs) == 0 {
		return P(Class("text-xs text-muted"), Text("No async jobs yet."))
	}
	rows := make([]Node, 0, len(jobs))
	for i := range jobs {
		job := jobs[i]
		rows = append(rows, Li(A(Href(job.URL), Text(job.ID)), Text(" "), statusLabel(job.State, notebookJobTone(job.State)), Text(" "), Span(Class("text-xs text-muted"), Text(job.Updated))))
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
		options = append(options, Option(Value(cell.ID), Text(fmt.Sprintf("%s (%s)", cell.Title, cell.ID))))
	}
	if len(options) == 0 {
		return Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"), H2(Text("Promote to model")), P(Class("text-xs text-muted"), Text("Add at least one SQL cell before promoting notebook output to a model.")))
	}
	return Div(Class("rounded-xl border border-border bg-background p-4 shadow-xs"), H2(Text("Promote to model")), Form(Method("post"), Action(d.PromoteURL), Class("grid gap-3"), d.CSRFFieldFunc(), Input(Type("hidden"), Name("notebook_id"), Value(d.NotebookID)), Label(Text("Output cell")), core.SelectControl("", Name("output_cell_id"), Group(options)), Label(Text("Project")), core.InputControl("", Name("project_name"), Required()), Label(Text("Model name")), core.InputControl("", Name("name"), Required()), Label(Text("Materialization")), core.SelectControl("", Name("materialization"), Option(Value("VIEW"), Text("VIEW")), Option(Value("TABLE"), Text("TABLE")), Option(Value("INCREMENTAL"), Text("INCREMENTAL")), Option(Value("EPHEMERAL"), Text("EPHEMERAL"))), core.PrimaryButton("", Type("submit"), Text("Promote notebook output"))))
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
		Class("sql-compute-card rounded-xl border border-border bg-background p-4 shadow-xs"),
		Attr("data-notebook-browser-runtime", "true"),
		Attr("data-runtime-manifest-endpoint", "/ui/notebooks/runtime/manifest"),
		H2(Class("m-0 text-lg font-semibold text-foreground"), Text("Compute")),
		P(Class("text-xs text-muted"), Text("Interactive SQL cell runs can use the browser-local DuckDB runtime. Notebook Run all and async runs stay on managed compute.")),
		Div(Class("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex"),
			core.SelectControl("sql-compute-select w-auto min-w-[11rem]", ID("notebook-compute-mode"), Name("notebook_compute_mode"), Group(modeOptions)),
			core.SelectControl("sql-compute-select w-auto min-w-[12rem]", ID("notebook-compute-endpoint"), Name("notebook_endpoint_name"), Group(endpointOptions)),
			core.SecondaryButton("", Type("button"), ID("notebook-reset-local-runtime"), I(Class(core.IconGlyphClass()), Attr("data-lucide", "rotate-ccw"), Attr("aria-hidden", "true")), Span(Text("Reset local runtime"))),
			core.SecondaryButton("", Type("button"), ID("notebook-cancel-local-run"), Disabled(), I(Class(core.IconGlyphClass()), Attr("data-lucide", "square"), Attr("aria-hidden", "true")), Span(Text("Cancel local run"))),
		),
		Div(Class("flex items-start gap-3 rounded-xl border border-[color:var(--color-warning)] bg-warning-muted px-4 py-3 text-sm"), Attr("data-notebook-runtime-banner", "true"),
			I(Class(core.NavIconClass("mt-0.5")), Attr("data-lucide", "cpu"), Attr("aria-hidden", "true")),
			Div(Strong(Attr("data-notebook-browser-runtime-title", "true"), Text("Browser runtime")), P(Attr("data-notebook-browser-runtime-message", "true"), Text(d.BrowserRuntime.StatusReason))),
		),
		P(Class("text-xs text-muted"), Attr("data-notebook-browser-runtime-preflight", "true"), Text("")),
	)
}

func notebookInsertRail(notebookID string, position int, csrfField func() Node) Node {
	return Div(Class("notebook-insert-rail relative mx-1 flex h-10 items-center justify-center py-1"),
		Div(Class("notebook-insert-actions relative z-[1] inline-flex items-center gap-2 [&_form]:m-0 [&_form]:flex"),
			Form(Method("post"), Action("/ui/notebooks/"+notebookID+"/cells"), csrfField(), Input(Type("hidden"), Name("cell_type"), Value("sql")), Input(Type("hidden"), Name("content"), Value("")), Input(Type("hidden"), Name("position"), Value(strconv.Itoa(position))), core.SecondaryButton("small", Type("submit"), Text("SQL"))),
			Form(Method("post"), Action("/ui/notebooks/"+notebookID+"/cells"), csrfField(), Input(Type("hidden"), Name("cell_type"), Value("markdown")), Input(Type("hidden"), Name("content"), Value("")), Input(Type("hidden"), Name("position"), Value(strconv.Itoa(position))), core.SecondaryButton("small", Type("submit"), Text("Markdown"))),
		),
	)
}

func notebookResultNode(c notebookCellRow) Node {
	if c.CellType != string(domain.CellTypeSQL) {
		return Div(Class("notebook-output flex flex-col gap-2 rounded-xl border border-border bg-surface-muted p-4"), Attr("data-notebook-cell-output", "true"), P(Class("text-xs text-muted"), Text("Markdown cell output is rendered by your markdown consumer.")))
	}
	if c.LastResult == nil {
		return Div(Class("notebook-output flex flex-col gap-2 rounded-xl border border-border bg-surface-muted p-4"), Attr("data-notebook-cell-output", "true"), P(Class("text-xs text-muted"), Text("Run this cell to see output.")))
	}
	if c.LastResult.Error != "" {
		return Div(Class("notebook-output flex flex-col gap-2 rounded-xl border border-border-danger bg-danger-muted p-4"), Attr("data-notebook-cell-output", "true"), H4(Text("Query Error")), P(Class("text-xs text-muted"), Text("Last runtime: "+humanDuration(c.LastResult.Duration))), Pre(Text(c.LastResult.Error)))
	}
	if c.VisualSpec != nil && c.VisualSpec.Kind != domain.VisualOutputTable {
		switch c.VisualSpec.Kind {
		case domain.VisualOutputMetric:
			field := ""
			if c.VisualSpec.Encodings.Value != nil {
				field = c.VisualSpec.Encodings.Value.Field
			}
			value, secondary := metricValueFromResult(c.LastResult, field)
			return Div(Class("notebook-output flex flex-col gap-3 rounded-xl border border-border bg-surface-muted p-4"), Attr("data-notebook-cell-output", "true"), visualMetricCard(defaultVisualTitle(c.VisualSpec, "Metric"), value, secondary), notebookResultDataDetails(c))
		case domain.VisualOutputChart:
			return Div(Class("notebook-output flex flex-col gap-3 rounded-xl border border-border bg-surface-muted p-4"), Attr("data-notebook-cell-output", "true"), chartHost(c.LastResult.Columns, c.LastResult.RawRows, c.VisualSpec), notebookResultDataDetails(c))
		}
	}
	return notebookTableResultNode(c)
}

func notebookTableResultNode(c notebookCellRow) Node {
	if c.LastResult == nil {
		return Div(Class("notebook-output flex flex-col gap-2 rounded-xl border border-border bg-surface-muted p-4"), Attr("data-notebook-cell-output", "true"), P(Class("text-xs text-muted"), Text("Run this cell to see output.")))
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
	return Div(Class("notebook-output flex flex-col gap-3 rounded-xl border border-border bg-surface-muted p-4"), Attr("data-notebook-cell-output", "true"),
		Div(Class("flex flex-wrap items-center justify-between gap-2"), H4(Text("Output")),
			Div(Class("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex"),
				core.SecondaryLink(c.DownloadURL, "small", Title("Download result CSV"), Attr("aria-label", "Download result CSV"), I(Class(core.IconGlyphClass()), Attr("data-lucide", "download"), Attr("aria-hidden", "true")), Span(Class("sr-only"), Text("Download result CSV"))),
			),
		),
		P(Class("text-xs text-muted"), Text(meta)),
		Div(Class("overflow-x-auto"), Table(Class(dataTableClass()), THead(Tr(Group(headers))), TBody(Group(rows)))),
	)
}

func notebookResultDataDetails(c notebookCellRow) Node {
	return Details(Class("mt-3"), Summary(Text("View data")), notebookTableResultNode(c))
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

func chartHost(columns []string, rows [][]interface{}, visual *domain.VisualSpec) Node {
	payload, err := json.Marshal(map[string]any{
		"columns": columns,
		"rows":    rows,
		"visual":  visual,
	})
	if err != nil {
		payload = []byte("{}")
	}
	return El("duck-chart", Class("block min-h-[20rem]"), Attr("data-chart-payload", string(payload)))
}

func visualMetricCard(title string, value interface{}, secondary string) Node {
	return Div(
		Class("relative overflow-hidden rounded-xl border border-border bg-[linear-gradient(135deg,var(--color-accent-muted)_0%,var(--color-background)_45%)] p-4 shadow-sm before:absolute before:inset-y-0 before:left-0 before:w-1 before:bg-border-accent before:content-['']"),
		P(Class("m-0 text-xs font-semibold text-foreground"), Text(title)),
		P(Class("my-1 text-3xl font-semibold leading-tight text-foreground"), Text(fmt.Sprint(value))),
		P(Class("m-0 text-xs text-muted"), Text(secondary)),
	)
}

func sqlComputeModeOption(value, label, selected string) Node {
	if value == selected {
		return Option(Value(value), Selected(), Text(label))
	}
	return Option(Value(value), Text(label))
}

func coreContainsExpr(value string) string {
	lower := strings.ToLower(value)
	return "$q === '' || " + strconv.Quote(lower) + ".includes($q.toLowerCase())"
}

func dropdownMenuClass(extra ...string) string { return core.DropdownMenuClass(extra...) }

func dropdownItemClass(extra ...string) string { return core.DropdownItemClass(extra...) }

func dataTableClass(extra ...string) string {
	return core.ClassNames("min-w-full border-collapse overflow-hidden rounded-xl border border-border bg-background [&_tbody_tr:hover]:bg-surface-muted [&_td]:border-b [&_td]:border-border [&_td]:px-4 [&_td]:py-3 [&_td]:align-top [&_td]:text-[0.8125rem] [&_th]:sticky [&_th]:top-0 [&_th]:z-[1] [&_th]:border-b [&_th]:border-border [&_th]:bg-surface-muted [&_th]:px-4 [&_th]:py-3 [&_th]:text-left [&_th]:text-[0.8125rem] [&_th]:font-semibold [&_th]:uppercase [&_th]:tracking-[0.02em] [&_th]:text-muted", strings.Join(extra, " "))
}

func labelClass(tone string) string {
	base := "inline-flex items-center rounded-full border border-transparent px-2 py-0.5 text-xs font-medium"
	switch tone {
	case "accent":
		return core.ClassNames(base, "bg-accent-muted text-accent")
	case "attention":
		return core.ClassNames(base, "bg-warning-muted text-warning-text")
	case "success":
		return core.ClassNames(base, "bg-success-muted text-success-text")
	case "severe":
		return core.ClassNames(base, "bg-danger-muted text-danger-text")
	default:
		return core.ClassNames(base, "bg-surface-muted text-foreground")
	}
}

func statusLabel(text, tone string) Node {
	return Span(Class(labelClass(tone)), Text(text))
}

func actionMenuLink(href, label string) Node {
	return A(Href(href), Class(dropdownItemClass("text-foreground")), Span(Text(label)))
}

func actionMenuPost(action, label string, csrfField func() Node, danger bool) Node {
	btnClass := dropdownItemClass()
	if danger {
		btnClass += " text-danger-text hover:bg-danger-muted"
	} else {
		btnClass += " text-foreground"
	}
	return Form(Method("post"), Action(action), csrfField(), Button(Type("submit"), Class(btnClass), Span(Text(label))))
}
