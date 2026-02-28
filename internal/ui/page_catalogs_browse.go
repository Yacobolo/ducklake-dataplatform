package ui

import (
	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
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
	CSRFField   func() Node
}

func schemaDetailPage(d schemaDetailPageData) Node {
	tableRows := make([]Node, 0, len(d.Tables))
	for i := range d.Tables {
		t := d.Tables[i]
		tableRows = append(tableRows, Tr(Td(A(Href(t.URL), Text(t.Name))), Td(Text(t.Type)), Td(Text(t.Owner)), Td(Text(t.Updated))))
	}
	viewRows := make([]Node, 0, len(d.Views))
	for i := range d.Views {
		v := d.Views[i]
		viewRows = append(viewRows, Tr(Td(A(Href(v.URL), Text(v.Name))), Td(Text(v.Owner)), Td(Text(v.Updated))))
	}

	return appPage("Schema: "+d.CatalogName+"."+d.SchemaName, "catalogs", d.Principal,
		Div(Class(cardClass()), P(Text("Owner: "+d.Owner)), P(Text("Comment: "+d.Comment)), P(Text("Properties: "+d.Properties)), P(Text("Tags: "+d.Tags)), A(Href(d.BackURL), Text("<- Back to catalog")), A(Href(d.EditURL), Text("Edit schema")), Form(Method("post"), Action(d.DeleteURL), d.CSRFField(), Button(Type("submit"), Class(secondaryButtonClass()), Text("Delete schema")))),
		Div(Class(cardClass("table-wrap")), H2(Text("Tables")), Table(THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Owner")), Th(Text("Updated")))), TBody(Group(tableRows)))),
		Div(Class(cardClass("table-wrap")), H2(Text("Views")), Table(THead(Tr(Th(Text("Name")), Th(Text("Owner")), Th(Text("Updated")))), TBody(Group(viewRows)))),
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

func tableDetailPage(d tableDetailPageData) Node {
	rows := make([]Node, 0, len(d.ColumnRows))
	for i := range d.ColumnRows {
		c := d.ColumnRows[i]
		rows = append(rows, Tr(Td(Text(c.Name)), Td(Text(c.Type)), Td(Text(c.Nullable)), Td(Text(c.Comment)), Td(Text(c.Properties))))
	}
	return appPage(d.Title, "catalogs", d.Principal,
		Div(Class(cardClass()), P(Text("Type: "+d.Type)), P(Text("Owner: "+d.Owner)), P(Text("Comment: "+d.Comment)), P(Text("Properties: "+d.Properties)), P(Text("Tags: "+d.Tags)), P(Text("Updated: "+d.Updated)), A(Href(d.BackURL), Text("<- Back to schema"))),
		Div(Class(cardClass("table-wrap")), H2(Text("Columns")), Table(THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Nullable")), Th(Text("Comment")), Th(Text("Properties")))), TBody(Group(rows)))),
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

func viewDetailPage(d viewDetailPageData) Node {
	columnSection := Node(P(Class(mutedClass()), Text("Columns unavailable for this view.")))
	if d.ColumnsAvailable {
		rows := make([]Node, 0, len(d.ColumnRows))
		for i := range d.ColumnRows {
			c := d.ColumnRows[i]
			rows = append(rows, Tr(Td(Text(c.Name)), Td(Text(c.Type)), Td(Text(c.Nullable)), Td(Text(c.Comment)), Td(Text(c.Properties))))
		}
		columnSection = Table(THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Nullable")), Th(Text("Comment")), Th(Text("Properties")))), TBody(Group(rows)))
	}

	return appPage(d.Title, "catalogs", d.Principal,
		Div(Class(cardClass()), P(Text("Owner: "+d.Owner)), P(Text("Comment: "+d.Comment)), P(Text("Properties: "+d.Properties)), P(Text("Tags: -")), P(Text("Source tables: "+d.SourceTables)), P(Text("Updated: "+d.Updated)), A(Href(d.BackURL), Text("<- Back to schema"))),
		Div(Class(cardClass()), H2(Text("Definition")), Pre(Text(d.Definition))),
		Div(Class(cardClass("table-wrap")), H2(Text("Columns")), columnSection),
	)
}
