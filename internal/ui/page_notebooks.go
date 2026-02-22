package ui

import (
	"bytes"
	"fmt"
	"html"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"

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
	return appPage("Notebooks", "notebooks", principal, pageToolbar("/ui/notebooks/new", "New notebook"), quickFilterCard("Filter by notebook or owner"), tableNode, paginationCard("/ui/notebooks", page, total))
}

type notebookCellRowData struct {
	ID           string
	Title        string
	CellType     string
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
	Rows       [][]string
	RowCount   int
	Error      string
	Duration   time.Duration
	ExecutedAt *time.Time
}

type notebookJobRowData struct {
	ID      string
	State   string
	Updated string
}

type notebookDetailPageData struct {
	Principal     domain.ContextPrincipal
	NotebookID    string
	Name          string
	Owner         string
	Description   string
	EditURL       string
	DeleteURL     string
	NewCellURL    string
	RunAllURL     string
	ReorderURL    string
	Jobs          []notebookJobRowData
	Cells         []notebookCellRowData
	CSRFFieldFunc func() Node
}

func notebookDetailPage(d notebookDetailPageData) Node {
	totalErrors := 0
	for i := range d.Jobs {
		j := d.Jobs[i]
		if strings.EqualFold(j.State, string(domain.JobStateFailed)) {
			totalErrors++
		}
	}
	cellNodes := make([]Node, 0, len(d.Cells))
	outlineNodes := make([]Node, 0, len(d.Cells))
	for i := range d.Cells {
		c := d.Cells[i]
		formID := "cell-form-" + c.ID
		isMarkdown := c.CellType == string(domain.CellTypeMarkdown)

		typeTone := "accent"
		if isMarkdown {
			typeTone = "attention"
		}

		runButton := Node(nil)
		if c.CellType == string(domain.CellTypeSQL) {
			runButton = Button(
				Type("submit"),
				Attr("form", formID),
				FormAction(c.RunURL),
				Class("btn btn-sm btn-icon notebook-cell-gutter-run"),
				Attr("data-run-cell", "true"),
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
					Attr("min-lines", "4"),
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
			d.CSRFFieldFunc(),
			editorInput,
		)

		cellMeta := fmt.Sprintf("Cell %d", c.Position+1)
		lastRunLabel := ""
		lastRunTitle := ""
		if c.CellType == string(domain.CellTypeSQL) {
			cellMeta = fmt.Sprintf("Cell %d, not run", c.Position+1)
			lastRunLabel = "Not run"
			if c.LastResult != nil {
				cellMeta = fmt.Sprintf("Cell %d, runtime %s", c.Position+1, humanDuration(c.LastResult.Duration))
				if c.LastRunAt != nil && !c.LastRunAt.IsZero() {
					lastRunLabel = "Last run " + humanRelativeTime(*c.LastRunAt)
					lastRunTitle = formatTime(*c.LastRunAt)
				} else {
					lastRunLabel = "Last run unavailable"
				}
				if c.LastResult.Error != "" {
					cellMeta = fmt.Sprintf("Cell %d, error in %s", c.Position+1, humanDuration(c.LastResult.Duration))
				} else {
					cellMeta = fmt.Sprintf("Cell %d, %d row(s), %s", c.Position+1, c.LastResult.RowCount, humanDuration(c.LastResult.Duration))
				}
			}
		}

		lastRunNode := Node(nil)
		if lastRunLabel != "" {
			titleNode := Node(nil)
			if lastRunTitle != "" {
				titleNode = Title(lastRunTitle)
			}
			lastRunNode = Span(Class("notebook-cell-timestamp"), titleNode, Text(lastRunLabel))
		}

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
			Class("button-row notebook-cell-actions"),
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
				editorForm,
				notebookResultNode(c),
			),
		)
		if isMarkdown {
			mainContent = Div(
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
			Div(
				Class("notebook-cell"),
				ID("cell-"+c.ID),
				Attr("data-notebook-cell", "true"),
				Attr("data-cell-id", c.ID),
				Attr("data-cell-type", c.CellType),
				data.Show(containsExpr(c.Title+" "+c.CellType+" "+c.Content)),
				Div(Class("notebook-cell-shell"),
					Div(
						Class("notebook-cell-gutter"),
						runButton,
						Span(Class("notebook-cell-gutter-index"), Text(strconv.Itoa(c.Position+1))),
					),
					Div(
						Class("notebook-cell-main"),
						Div(
							Class("notebook-cell-header"),
							Div(
								Class("notebook-cell-title"),
								Span(Class("notebook-cell-name"), Text(c.Title)),
								Span(Class("notebook-cell-meta"), Text(cellMeta)),
							),
							Div(
								Class("notebook-cell-header-right"),
								lastRunNode,
								Span(Class("notebook-cell-type-badge"), statusLabel(c.CellType, typeTone)),
								cellActions,
							),
						),
						mainContent,
					),
				),
			),
		)

		outlineNodes = append(outlineNodes,
			Li(
				data.Show(containsExpr(c.Title+" "+c.CellType+" "+c.Content)),
				A(Href("#cell-"+c.ID), Class("notebook-outline-link"),
					Text(c.Title+" "),
					statusLabel(c.CellType, typeTone),
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

	workspaceNode := Div(
		Class("notebook-workspace"),
		Attr("data-reorder-url", d.ReorderURL),
		Aside(
			Class("notebook-outline"),
			Details(
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
						Class("d-flex flex-items-center gap-2"),
						Label(Class("sr-only"), Text("Filter cells")),
						Input(Type("search"), Class("form-control"), Placeholder("Filter cells"), data.Bind("q"), AutoComplete("off")),
					),
					Div(Class("notebook-outline-list"),
						Ul(Group(outlineNodes)),
					),
				),
			),
		),
		Div(
			Class("notebook-cells"),
			Group(cellNodes),
		),
	)

	descriptionNode := Node(nil)
	if strings.TrimSpace(d.Description) != "" {
		descriptionNode = Span(Class("notebook-toolbar-meta-item"), Text(d.Description))
	}

	return appPage(
		"Notebook: "+d.Name,
		"notebooks",
		d.Principal,
		data.Signals(map[string]any{"q": ""}),
		Div(
			Class("notebook-toolbar"),
			Div(Class("d-flex flex-justify-between flex-wrap flex-items-start gap-2"),
				Div(
					H2(Class("notebook-title"), Text(d.Name)),
					Div(
						Class("notebook-toolbar-meta"),
						Span(Class("notebook-toolbar-meta-item"), Text("Owner "+d.Owner)),
						descriptionNode,
					),
				),
				Div(Class("button-row notebook-toolbar-status"),
					statusLabel(fmt.Sprintf("%d jobs", len(d.Jobs)), "accent"),
					statusLabel(fmt.Sprintf("%d failures", totalErrors), "severe"),
				),
			),
			Div(Class("button-row notebook-toolbar-actions"),
				Form(Method("post"), Action(d.RunAllURL), d.CSRFFieldFunc(), Button(Type("submit"), Class(primaryButtonClass()), Text("Run all"))),
				A(Href(d.NewCellURL), Class(secondaryButtonClass()), Text("New cell")),
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
		workspaceNode,
		Script(Src(uiScriptHref("sql-editor.js"))),
		Script(Src(uiScriptHref("notebook.js"))),
	)
}

func notebookInsertRail(notebookID string, position int, csrfField func() Node) Node {
	return Div(
		Class("notebook-insert-rail"),
		Form(
			Method("post"),
			Action("/ui/notebooks/"+notebookID+"/cells"),
			csrfField(),
			Input(Type("hidden"), Name("cell_type"), Value("sql")),
			Input(Type("hidden"), Name("content"), Value("")),
			Input(Type("hidden"), Name("position"), Value(strconv.Itoa(position))),
			Button(
				Type("submit"),
				Class("btn btn-sm btn-icon notebook-insert-btn"),
				Title("Insert SQL cell"),
				Attr("aria-label", "Insert SQL cell"),
				I(Class("btn-icon-glyph"), Attr("data-lucide", "plus"), Attr("aria-hidden", "true")),
				Span(Class("sr-only"), Text("Insert SQL cell")),
			),
		),
	)
}

func notebookResultNode(c notebookCellRowData) Node {
	if c.CellType != string(domain.CellTypeSQL) {
		return Div(Class("notebook-output"), P(Class(mutedClass()), Text("Markdown cell output is rendered by your markdown consumer.")))
	}

	if c.LastResult == nil {
		return Div(Class("notebook-output"), P(Class(mutedClass()), Text("Run this cell to see output.")))
	}

	if c.LastResult.Error != "" {
		return Div(
			Class("notebook-output flash flash-error"),
			H4(Text("Query Error")),
			P(Class(mutedClass()), Text("Last runtime: "+humanDuration(c.LastResult.Duration))),
			Pre(Text(c.LastResult.Error)),
		)
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
	return formPage(principal, "Edit Notebook Cell", "notebooks", "/ui/notebooks/"+notebookID+"/cells/"+cellID+"/update", csrfFieldProvider,
		Label(Text("Content")),
		Textarea(Name("content"), Text(cell.Content), Required()),
		Label(Text("Position")),
		Input(Name("position"), Value(strconv.Itoa(cell.Position))),
	)
}
