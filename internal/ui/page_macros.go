package ui

import (
	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type macrosListRowData struct {
	Filter     string
	Name       string
	URL        string
	Type       string
	Visibility string
	Status     string
}

func macrosListPage(principal domain.ContextPrincipal, rows []macrosListRowData, page domain.PageRequest, total int64) Node {
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		r := rows[i]
		tableRows = append(tableRows, Tr(data.Show(containsExpr(r.Filter)), Td(A(Href(r.URL), Text(r.Name))), Td(statusLabel(r.Type, "accent")), Td(Text(r.Visibility)), Td(statusLabel(r.Status, "attention"))))
	}
	tableNode := Node(emptyStateCard("No macros yet.", "New macro", "/ui/macros/new"))
	if len(tableRows) > 0 {
		tableNode = Div(Class(cardClass("table-wrap")), Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Visibility")), Th(Text("Status")))), TBody(Group(tableRows))))
	}
	return appPage("Macros", "macros", principal, pageToolbar("/ui/macros/new", "New macro"), quickFilterCard("Filter by macro name or visibility"), tableNode, paginationCard("/ui/macros", page, total))
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
	CSRFFieldFunc func() Node
}

func macroDetailPage(d macroDetailPageData) Node {
	revRows := make([]Node, 0, len(d.Revisions))
	for i := range d.Revisions {
		r := d.Revisions[i]
		revRows = append(revRows, Tr(Td(Text(r.Version)), Td(Text(r.Status)), Td(Text(r.CreatedBy)), Td(Text(r.Created))))
	}
	return appPage("Macro: "+d.Name, "macros", d.Principal, Div(Class(cardClass()), P(Text("Type: "+d.Type)), P(Text("Visibility: "+d.Visibility)), P(Text("Status: "+d.Status)), P(Text("Owner: "+d.Owner)), Div(Class("BtnGroup"), A(Href(d.EditURL), Class(secondaryButtonClass()), Text("Edit")), Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldFunc(), Button(Type("submit"), Class("btn btn-danger"), Text("Delete"))))), Div(Class(cardClass()), H2(Text("Definition")), Pre(Text(d.Definition))), Div(Class(cardClass("table-wrap")), H2(Text("Revisions")), Table(Class("data-table"), THead(Tr(Th(Text("Version")), Th(Text("Status")), Th(Text("Created by")), Th(Text("Created")))), TBody(Group(revRows)))))
}

func macrosNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Macro", "macros", "/ui/macros", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Type")),
		Select(Name("macro_type"), Option(Value("SCALAR"), Text("SCALAR")), Option(Value("TABLE"), Text("TABLE"))),
		Label(Text("Visibility")),
		Select(Name("visibility"), Option(Value("project"), Text("project")), Option(Value("catalog_global"), Text("catalog_global")), Option(Value("system"), Text("system"))),
		Label(Text("Description")),
		Textarea(Name("description")),
		Label(Text("Parameters (comma separated)")),
		Input(Name("parameters")),
		Label(Text("Body")),
		Textarea(Name("body"), Required()),
	)
}

func macrosEditPage(principal domain.ContextPrincipal, macroName string, macro *domain.Macro, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Edit Macro", "macros", "/ui/macros/"+macroName+"/update", csrfFieldProvider,
		Label(Text("Description")),
		Textarea(Name("description"), Text(macro.Description)),
		Label(Text("Visibility")),
		Select(Name("visibility"), optionSelected("project", macro.Visibility), optionSelected("catalog_global", macro.Visibility), optionSelected("system", macro.Visibility)),
		Label(Text("Parameters (comma separated)")),
		Input(Name("parameters"), Value(csvValues(macro.Parameters))),
		Label(Text("Body")),
		Textarea(Name("body"), Text(macro.Body), Required()),
		Label(Text("Status")),
		Select(Name("status"), optionSelected("ACTIVE", macro.Status), optionSelected("DEPRECATED", macro.Status)),
	)
}
