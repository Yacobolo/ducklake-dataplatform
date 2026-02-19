package ui

import (
	"strconv"

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
	Title     string
	Content   string
	EditURL   string
	DeleteURL string
}

type notebookJobRowData struct {
	ID      string
	State   string
	Updated string
}

type notebookDetailPageData struct {
	Principal     domain.ContextPrincipal
	Name          string
	Owner         string
	Description   string
	EditURL       string
	DeleteURL     string
	NewCellURL    string
	Jobs          []notebookJobRowData
	Cells         []notebookCellRowData
	CSRFFieldFunc func() Node
}

func notebookDetailPage(d notebookDetailPageData) Node {
	jobRows := make([]Node, 0, len(d.Jobs))
	for i := range d.Jobs {
		j := d.Jobs[i]
		jobRows = append(jobRows, Tr(Td(Text(j.ID)), Td(Text(j.State)), Td(Text(j.Updated))))
	}
	cellNodes := make([]Node, 0, len(d.Cells))
	for i := range d.Cells {
		c := d.Cells[i]
		cellNodes = append(cellNodes, Div(Class(cardClass()), H3(Text(c.Title)), Pre(Text(c.Content)), Div(Class("BtnGroup"), A(Href(c.EditURL), Class("btn btn-sm"), Text("Edit")), Form(Method("post"), Action(c.DeleteURL), d.CSRFFieldFunc(), Button(Type("submit"), Class("btn btn-sm btn-danger"), Text("Delete"))))))
	}
	return appPage("Notebook: "+d.Name, "notebooks", d.Principal, Div(Class(cardClass()), P(Text("Owner: "+d.Owner)), P(Text("Description: "+d.Description)), Div(Class("BtnGroup"), A(Href(d.EditURL), Class(secondaryButtonClass()), Text("Edit")), A(Href(d.NewCellURL), Class(primaryButtonClass()), Text("New cell")), Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldFunc(), Button(Type("submit"), Class("btn btn-danger"), Text("Delete"))))), Div(Class(cardClass("table-wrap")), H2(Text("Recent jobs")), Table(Class("data-table"), THead(Tr(Th(Text("Job ID")), Th(Text("State")), Th(Text("Updated")))), TBody(Group(jobRows)))), Group(cellNodes))
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
