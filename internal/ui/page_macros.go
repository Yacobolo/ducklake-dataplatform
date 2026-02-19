package ui

import (
	"duck-demo/internal/domain"

	gomponents "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	html "maragu.dev/gomponents/html"
)

type macrosListRowData struct {
	Filter     string
	Name       string
	URL        string
	Type       string
	Visibility string
	Status     string
}

func macrosListPage(principal domain.ContextPrincipal, rows []macrosListRowData, page domain.PageRequest, total int64) gomponents.Node {
	tableRows := make([]gomponents.Node, 0, len(rows))
	for i := range rows {
		r := rows[i]
		tableRows = append(tableRows, html.Tr(data.Show(containsExpr(r.Filter)), html.Td(html.A(html.Href(r.URL), gomponents.Text(r.Name))), html.Td(gomponents.Text(r.Type)), html.Td(gomponents.Text(r.Visibility)), html.Td(gomponents.Text(r.Status))))
	}
	return appPage("Macros", "macros", principal, html.Div(html.Class(cardClass()), html.A(html.Href("/ui/macros/new"), gomponents.Text("+ New macro"))), html.Div(data.Signals(map[string]any{"q": ""}), html.Div(html.Class(cardClass()), html.Label(gomponents.Text("Quick filter")), html.Input(html.Type("text"), data.Bind("q"), html.Placeholder("Filter by macro name or visibility"))), html.Div(html.Class(cardClass("table-wrap")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Type")), html.Th(gomponents.Text("Visibility")), html.Th(gomponents.Text("Status")))), html.TBody(gomponents.Group(tableRows))))), paginationCard("/ui/macros", page, total))
}

type macroRevisionRowData struct {
	Version   string
	Status    string
	CreatedBy string
	Created   string
}

type macroDetailPageData struct {
	Principal     domain.ContextPrincipal
	Name          string
	Type          string
	Visibility    string
	Status        string
	Owner         string
	EditURL       string
	DeleteURL     string
	Definition    string
	Revisions     []macroRevisionRowData
	CSRFFieldFunc func() gomponents.Node
}

func macroDetailPage(d macroDetailPageData) gomponents.Node {
	revRows := make([]gomponents.Node, 0, len(d.Revisions))
	for i := range d.Revisions {
		r := d.Revisions[i]
		revRows = append(revRows, html.Tr(html.Td(gomponents.Text(r.Version)), html.Td(gomponents.Text(r.Status)), html.Td(gomponents.Text(r.CreatedBy)), html.Td(gomponents.Text(r.Created))))
	}
	return appPage("Macro: "+d.Name, "macros", d.Principal, html.Div(html.Class(cardClass()), html.P(gomponents.Text("Type: "+d.Type)), html.P(gomponents.Text("Visibility: "+d.Visibility)), html.P(gomponents.Text("Status: "+d.Status)), html.P(gomponents.Text("Owner: "+d.Owner)), html.A(html.Href(d.EditURL), gomponents.Text("Edit macro")), html.Form(html.Method("post"), html.Action(d.DeleteURL), d.CSRFFieldFunc(), html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Delete macro")))), html.Div(html.Class(cardClass()), html.H2(gomponents.Text("Definition")), html.Pre(gomponents.Text(d.Definition))), html.Div(html.Class(cardClass("table-wrap")), html.H2(gomponents.Text("Revisions")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Version")), html.Th(gomponents.Text("Status")), html.Th(gomponents.Text("Created by")), html.Th(gomponents.Text("Created")))), html.TBody(gomponents.Group(revRows)))))
}
