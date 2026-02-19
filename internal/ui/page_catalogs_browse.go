package ui

import (
	"duck-demo/internal/domain"

	gomponents "maragu.dev/gomponents"
	html "maragu.dev/gomponents/html"
)

type schemaTableRowData struct {
	Name    string
	URL     string
	Type    string
	Owner   string
	Updated string
}

type schemaViewRowData struct {
	Name    string
	URL     string
	Owner   string
	Updated string
}

type schemaDetailPageData struct {
	Principal   domain.ContextPrincipal
	CatalogName string
	SchemaName  string
	Owner       string
	Comment     string
	Properties  string
	Tags        string
	BackURL     string
	EditURL     string
	DeleteURL   string
	Tables      []schemaTableRowData
	Views       []schemaViewRowData
	CSRFField   func() gomponents.Node
}

func schemaDetailPage(d schemaDetailPageData) gomponents.Node {
	tableRows := make([]gomponents.Node, 0, len(d.Tables))
	for i := range d.Tables {
		t := d.Tables[i]
		tableRows = append(tableRows, html.Tr(html.Td(html.A(html.Href(t.URL), gomponents.Text(t.Name))), html.Td(gomponents.Text(t.Type)), html.Td(gomponents.Text(t.Owner)), html.Td(gomponents.Text(t.Updated))))
	}
	viewRows := make([]gomponents.Node, 0, len(d.Views))
	for i := range d.Views {
		v := d.Views[i]
		viewRows = append(viewRows, html.Tr(html.Td(html.A(html.Href(v.URL), gomponents.Text(v.Name))), html.Td(gomponents.Text(v.Owner)), html.Td(gomponents.Text(v.Updated))))
	}

	return appPage("Schema: "+d.CatalogName+"."+d.SchemaName, "catalogs", d.Principal,
		html.Div(html.Class(cardClass()), html.P(gomponents.Text("Owner: "+d.Owner)), html.P(gomponents.Text("Comment: "+d.Comment)), html.P(gomponents.Text("Properties: "+d.Properties)), html.P(gomponents.Text("Tags: "+d.Tags)), html.A(html.Href(d.BackURL), gomponents.Text("<- Back to catalog")), html.A(html.Href(d.EditURL), gomponents.Text("Edit schema")), html.Form(html.Method("post"), html.Action(d.DeleteURL), d.CSRFField(), html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Delete schema")))),
		html.Div(html.Class(cardClass("table-wrap")), html.H2(gomponents.Text("Tables")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Type")), html.Th(gomponents.Text("Owner")), html.Th(gomponents.Text("Updated")))), html.TBody(gomponents.Group(tableRows)))),
		html.Div(html.Class(cardClass("table-wrap")), html.H2(gomponents.Text("Views")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Owner")), html.Th(gomponents.Text("Updated")))), html.TBody(gomponents.Group(viewRows)))),
	)
}

type tableColumnRowData struct {
	Name       string
	Type       string
	Nullable   string
	Comment    string
	Properties string
}

type tableDetailPageData struct {
	Principal  domain.ContextPrincipal
	Title      string
	Type       string
	Owner      string
	Comment    string
	Properties string
	Tags       string
	Updated    string
	BackURL    string
	ColumnRows []tableColumnRowData
}

func tableDetailPage(d tableDetailPageData) gomponents.Node {
	rows := make([]gomponents.Node, 0, len(d.ColumnRows))
	for i := range d.ColumnRows {
		c := d.ColumnRows[i]
		rows = append(rows, html.Tr(html.Td(gomponents.Text(c.Name)), html.Td(gomponents.Text(c.Type)), html.Td(gomponents.Text(c.Nullable)), html.Td(gomponents.Text(c.Comment)), html.Td(gomponents.Text(c.Properties))))
	}
	return appPage(d.Title, "catalogs", d.Principal,
		html.Div(html.Class(cardClass()), html.P(gomponents.Text("Type: "+d.Type)), html.P(gomponents.Text("Owner: "+d.Owner)), html.P(gomponents.Text("Comment: "+d.Comment)), html.P(gomponents.Text("Properties: "+d.Properties)), html.P(gomponents.Text("Tags: "+d.Tags)), html.P(gomponents.Text("Updated: "+d.Updated)), html.A(html.Href(d.BackURL), gomponents.Text("<- Back to schema"))),
		html.Div(html.Class(cardClass("table-wrap")), html.H2(gomponents.Text("Columns")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Type")), html.Th(gomponents.Text("Nullable")), html.Th(gomponents.Text("Comment")), html.Th(gomponents.Text("Properties")))), html.TBody(gomponents.Group(rows)))),
	)
}

type viewDetailPageData struct {
	Principal        domain.ContextPrincipal
	Title            string
	Owner            string
	Comment          string
	Properties       string
	SourceTables     string
	Updated          string
	BackURL          string
	Definition       string
	ColumnRows       []tableColumnRowData
	ColumnsAvailable bool
}

func viewDetailPage(d viewDetailPageData) gomponents.Node {
	columnSection := gomponents.Node(html.P(html.Class(mutedClass()), gomponents.Text("Columns unavailable for this view.")))
	if d.ColumnsAvailable {
		rows := make([]gomponents.Node, 0, len(d.ColumnRows))
		for i := range d.ColumnRows {
			c := d.ColumnRows[i]
			rows = append(rows, html.Tr(html.Td(gomponents.Text(c.Name)), html.Td(gomponents.Text(c.Type)), html.Td(gomponents.Text(c.Nullable)), html.Td(gomponents.Text(c.Comment)), html.Td(gomponents.Text(c.Properties))))
		}
		columnSection = html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Type")), html.Th(gomponents.Text("Nullable")), html.Th(gomponents.Text("Comment")), html.Th(gomponents.Text("Properties")))), html.TBody(gomponents.Group(rows)))
	}

	return appPage(d.Title, "catalogs", d.Principal,
		html.Div(html.Class(cardClass()), html.P(gomponents.Text("Owner: "+d.Owner)), html.P(gomponents.Text("Comment: "+d.Comment)), html.P(gomponents.Text("Properties: "+d.Properties)), html.P(gomponents.Text("Tags: -")), html.P(gomponents.Text("Source tables: "+d.SourceTables)), html.P(gomponents.Text("Updated: "+d.Updated)), html.A(html.Href(d.BackURL), gomponents.Text("<- Back to schema"))),
		html.Div(html.Class(cardClass()), html.H2(gomponents.Text("Definition")), html.Pre(gomponents.Text(d.Definition))),
		html.Div(html.Class(cardClass("table-wrap")), html.H2(gomponents.Text("Columns")), columnSection),
	)
}
