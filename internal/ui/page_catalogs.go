package ui

import (
	"duck-demo/internal/domain"

	gomponents "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	html "maragu.dev/gomponents/html"
)

type catalogsListRowData struct {
	Filter    string
	Name      string
	URL       string
	Status    string
	Metastore string
	Updated   string
}

func catalogsListPage(principal domain.ContextPrincipal, rows []catalogsListRowData, page domain.PageRequest, total int64) gomponents.Node {
	tableRows := make([]gomponents.Node, 0, len(rows))
	for i := range rows {
		row := rows[i]
		tableRows = append(tableRows, html.Tr(data.Show(containsExpr(row.Filter)), html.Td(html.A(html.Href(row.URL), gomponents.Text(row.Name))), html.Td(gomponents.Text(row.Status)), html.Td(gomponents.Text(row.Metastore)), html.Td(gomponents.Text(row.Updated))))
	}
	return appPage(
		"Catalogs",
		"catalogs",
		principal,
		html.Div(html.Class(cardClass()), html.A(html.Href("/ui/catalogs/new"), gomponents.Text("+ New catalog"))),
		html.Div(
			data.Signals(map[string]any{"q": ""}),
			html.Div(html.Class(cardClass()), html.Label(gomponents.Text("Quick filter")), html.Input(html.Type("text"), data.Bind("q"), html.Placeholder("Filter by catalog name or status"))),
			html.Div(html.Class(cardClass("table-wrap")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Status")), html.Th(gomponents.Text("Metastore")), html.Th(gomponents.Text("Updated")))), html.TBody(gomponents.Group(tableRows)))),
		),
		paginationCard("/ui/catalogs", page, total),
	)
}

type schemaRowData struct {
	Name      string
	URL       string
	Owner     string
	Updated   string
	EditURL   string
	DeleteURL string
}

type catalogDetailPageData struct {
	Principal      domain.ContextPrincipal
	CatalogName    string
	Status         string
	DataPath       string
	IsDefault      string
	EditURL        string
	SetDefaultURL  string
	DeleteURL      string
	NewSchemaURL   string
	MetastoreItems []string
	Schemas        []schemaRowData
	CSRFField      func() gomponents.Node
}

func catalogDetailPage(d catalogDetailPageData) gomponents.Node {
	summaryNode := gomponents.Node(html.P(gomponents.Text("Metastore summary unavailable")))
	if len(d.MetastoreItems) > 0 {
		list := make([]gomponents.Node, 0, len(d.MetastoreItems))
		for i := range d.MetastoreItems {
			list = append(list, html.Li(gomponents.Text(d.MetastoreItems[i])))
		}
		summaryNode = html.Ul(gomponents.Group(list))
	}

	schemaRows := make([]gomponents.Node, 0, len(d.Schemas))
	for i := range d.Schemas {
		s := d.Schemas[i]
		schemaRows = append(schemaRows, html.Tr(
			html.Td(html.A(html.Href(s.URL), gomponents.Text(s.Name))),
			html.Td(gomponents.Text(s.Owner)),
			html.Td(gomponents.Text(s.Updated)),
			html.Td(
				html.A(html.Href(s.EditURL), gomponents.Text("Edit")),
				html.Form(html.Method("post"), html.Action(s.DeleteURL), d.CSRFField(), html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Delete"))),
			),
		))
	}

	return appPage(
		"Catalog: "+d.CatalogName,
		"catalogs",
		d.Principal,
		html.Div(
			html.Class(cardClass()),
			html.P(gomponents.Text("Status: "+d.Status)),
			html.P(gomponents.Text("Data path: "+d.DataPath)),
			html.P(gomponents.Text("Default: "+d.IsDefault)),
			html.A(html.Href(d.EditURL), gomponents.Text("Edit catalog")),
			html.Form(html.Method("post"), html.Action(d.SetDefaultURL), d.CSRFField(), html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Set default"))),
			html.Form(html.Method("post"), html.Action(d.DeleteURL), d.CSRFField(), html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Delete catalog"))),
			html.A(html.Href(d.NewSchemaURL), gomponents.Text("+ New schema")),
		),
		html.Div(html.Class(cardClass()), html.H2(gomponents.Text("Metastore")), summaryNode),
		html.Div(html.Class(cardClass("table-wrap")), html.H2(gomponents.Text("Schemas")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Owner")), html.Th(gomponents.Text("Updated")), html.Th(gomponents.Text("Actions")))), html.TBody(gomponents.Group(schemaRows)))),
	)
}
