package ui

import (
	"duck-demo/internal/domain"

	gomponents "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	html "maragu.dev/gomponents/html"
)

type notebooksListRowData struct {
	Filter  string
	Name    string
	URL     string
	Owner   string
	Updated string
}

func notebooksListPage(principal domain.ContextPrincipal, rows []notebooksListRowData, page domain.PageRequest, total int64) gomponents.Node {
	tableRows := make([]gomponents.Node, 0, len(rows))
	for i := range rows {
		r := rows[i]
		tableRows = append(tableRows, html.Tr(data.Show(containsExpr(r.Filter)), html.Td(html.A(html.Href(r.URL), gomponents.Text(r.Name))), html.Td(gomponents.Text(r.Owner)), html.Td(gomponents.Text(r.Updated))))
	}
	return appPage("Notebooks", "notebooks", principal, html.Div(html.Class(cardClass()), html.A(html.Href("/ui/notebooks/new"), gomponents.Text("+ New notebook"))), html.Div(data.Signals(map[string]any{"q": ""}), html.Div(html.Class(cardClass()), html.Label(gomponents.Text("Quick filter")), html.Input(html.Type("text"), data.Bind("q"), html.Placeholder("Filter by notebook or owner"))), html.Div(html.Class(cardClass("table-wrap")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Owner")), html.Th(gomponents.Text("Updated")))), html.TBody(gomponents.Group(tableRows))))), paginationCard("/ui/notebooks", page, total))
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
	CSRFFieldFunc func() gomponents.Node
}

func notebookDetailPage(d notebookDetailPageData) gomponents.Node {
	jobRows := make([]gomponents.Node, 0, len(d.Jobs))
	for i := range d.Jobs {
		j := d.Jobs[i]
		jobRows = append(jobRows, html.Tr(html.Td(gomponents.Text(j.ID)), html.Td(gomponents.Text(j.State)), html.Td(gomponents.Text(j.Updated))))
	}
	cellNodes := make([]gomponents.Node, 0, len(d.Cells))
	for i := range d.Cells {
		c := d.Cells[i]
		cellNodes = append(cellNodes, html.Div(html.Class(cardClass()), html.H3(gomponents.Text(c.Title)), html.Pre(gomponents.Text(c.Content)), html.A(html.Href(c.EditURL), gomponents.Text("Edit cell")), html.Form(html.Method("post"), html.Action(c.DeleteURL), d.CSRFFieldFunc(), html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Delete cell")))))
	}
	return appPage("Notebook: "+d.Name, "notebooks", d.Principal, html.Div(html.Class(cardClass()), html.P(gomponents.Text("Owner: "+d.Owner)), html.P(gomponents.Text("Description: "+d.Description)), html.A(html.Href(d.EditURL), gomponents.Text("Edit notebook")), html.Form(html.Method("post"), html.Action(d.DeleteURL), d.CSRFFieldFunc(), html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Delete notebook"))), html.A(html.Href(d.NewCellURL), gomponents.Text("+ New cell"))), html.Div(html.Class(cardClass("table-wrap")), html.H2(gomponents.Text("Recent jobs")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Job ID")), html.Th(gomponents.Text("State")), html.Th(gomponents.Text("Updated")))), html.TBody(gomponents.Group(jobRows)))), gomponents.Group(cellNodes))
}
