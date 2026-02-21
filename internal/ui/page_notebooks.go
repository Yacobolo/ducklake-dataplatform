package ui

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
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
	Columns  []string
	Rows     [][]string
	RowCount int
	Error    string
	Duration time.Duration
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
	jobRows := make([]Node, 0, len(d.Jobs))
	for i := range d.Jobs {
		j := d.Jobs[i]
		if strings.EqualFold(j.State, string(domain.JobStateFailed)) {
			totalErrors++
		}
		jobRows = append(jobRows,
			Tr(
				Td(Text(j.ID)),
				Td(statusLabel(j.State, notebookJobTone(j.State))),
				Td(Text(j.Updated)),
			),
		)
	}
	cellNodes := make([]Node, 0, len(d.Cells))
	outlineNodes := make([]Node, 0, len(d.Cells))
	for i := range d.Cells {
		c := d.Cells[i]

		typeTone := "accent"
		if c.CellType == string(domain.CellTypeMarkdown) {
			typeTone = "attention"
		}

		runButton := Node(nil)
		if c.CellType == string(domain.CellTypeSQL) {
			runButton = Button(
				Type("submit"),
				Class("btn btn-sm btn-primary"),
				Attr("data-run-cell", "true"),
				I(Class("btn-icon-glyph"), Attr("data-lucide", "play"), Attr("aria-hidden", "true")),
				Span(Text("Run")),
			)
		}

		formAction := c.RunURL
		if c.CellType == string(domain.CellTypeMarkdown) {
			formAction = c.UpdateURL
		}

		saveLabel := "Save markdown"
		saveIconLabel := "Save cell"
		if c.CellType == string(domain.CellTypeMarkdown) {
			saveIconLabel = saveLabel
		}

		saveButton := Node(Button(
			Type("submit"),
			FormAction(c.UpdateURL),
			Class("btn btn-sm btn-icon"),
			Title(saveIconLabel),
			Attr("aria-label", saveIconLabel),
			I(Class("btn-icon-glyph"), Attr("data-lucide", "save"), Attr("aria-hidden", "true")),
			Span(Class("sr-only"), Text(saveIconLabel)),
		))
		if c.CellType == string(domain.CellTypeMarkdown) {
			saveButton = Button(
				Type("submit"),
				FormAction(c.UpdateURL),
				Class("btn btn-sm btn-primary"),
				I(Class("btn-icon-glyph"), Attr("data-lucide", "save"), Attr("aria-hidden", "true")),
				Span(Text(saveLabel)),
			)
		}

		editorForm := Form(
			Method("post"),
			Action(formAction),
			d.CSRFFieldFunc(),
			Textarea(
				Name("content"),
				Class("form-control notebook-editor"),
				Attr("data-cell-editor", "true"),
				Text(c.Content),
			),
			Div(
				Class("button-row notebook-cell-primary-actions"),
				runButton,
				saveButton,
			),
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
				actionMenuLink(c.OpenInSQLURL, "Open cell in SQL editor"),
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

		cellNodes = append(cellNodes,
			Div(
				Class("notebook-cell"),
				ID("cell-"+c.ID),
				Attr("data-notebook-cell", "true"),
				Attr("data-cell-id", c.ID),
				data.Show(containsExpr(c.Title+" "+c.CellType+" "+c.Content)),
				H3(
					Class("notebook-cell-title"),
					Text(c.Title+" "),
					statusLabel(c.CellType, typeTone),
					cellActions,
				),
				editorForm,
				notebookResultNode(c),
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

	jobsCard := Node(emptyStateCard("No jobs yet.", "", ""))
	if len(jobRows) > 0 {
		jobsCard = Div(
			Class(cardClass("notebook-run-strip")),
			H3(Text("Recent runs")),
			Div(
				Class("table-wrap"),
				Table(
					Class("data-table"),
					THead(Tr(Th(Text("Job ID")), Th(Text("State")), Th(Text("Updated")))),
					TBody(Group(jobRows)),
				),
			),
		)
	}

	workspaceNode := Div(
		Class("notebook-workspace"),
		Attr("data-reorder-url", d.ReorderURL),
		Aside(
			Class(cardClass("notebook-outline")),
			H3(Text("Notebook outline")),
			P(Class(mutedClass()), Text("Filter and jump to a cell quickly.")),
			Div(
				Class("d-flex flex-items-center gap-2"),
				Label(Class("sr-only"), Text("Filter cells")),
				Input(Type("search"), Class("form-control"), Placeholder("Filter cells"), data.Bind("q"), AutoComplete("off")),
			),
			Div(Class("notebook-outline-list"),
				Ul(Group(outlineNodes)),
			),
		),
		Div(
			Class("notebook-cells"),
			Div(
				Class(cardClass("notebook-sheet")),
				Group(cellNodes),
			),
		),
	)

	return appPage(
		"Notebook: "+d.Name,
		"notebooks",
		d.Principal,
		data.Signals(map[string]any{"q": ""}),
		Div(
			Class(cardClass("notebook-toolbar")),
			Div(Class("d-flex flex-justify-between flex-wrap flex-items-center gap-2"),
				Div(
					H2(Text(d.Name)),
					P(Class(mutedClass()), Text("Owner: "+d.Owner+" | "+"Description: "+d.Description)),
				),
				Div(Class("button-row"),
					statusLabel(fmt.Sprintf("%d jobs", len(d.Jobs)), "accent"),
					statusLabel(fmt.Sprintf("%d failures", totalErrors), "severe"),
				),
			),
			Div(Class("button-row"),
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
		jobsCard,
		workspaceNode,
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
