package ui

import (
	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type catalogsListRowData struct {
	Filter    string
	Name      string
	URL       string
	Status    string
	Metastore string
	Updated   string
}

func catalogsListPage(principal domain.ContextPrincipal, rows []catalogsListRowData, page domain.PageRequest, total int64) Node {
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		row := rows[i]
		tableRows = append(tableRows, Tr(data.Show(containsExpr(row.Filter)), Td(A(Href(row.URL), Text(row.Name))), Td(statusLabel(row.Status, "accent")), Td(Text(row.Metastore)), Td(Text(row.Updated))))
	}
	tableNode := Node(emptyStateCard("No catalogs found yet.", "Create catalog", "/ui/catalogs/new"))
	if len(tableRows) > 0 {
		tableNode = Div(Class(cardClass("table-wrap")), Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("Status")), Th(Text("Metastore")), Th(Text("Updated")))), TBody(Group(tableRows))))
	}
	return appPage(
		"Catalogs",
		"catalogs",
		principal,
		pageToolbar("/ui/catalogs/new", "New catalog"),
		quickFilterCard("Filter by catalog name or status"),
		tableNode,
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
	CSRFField      func() Node
}

func catalogDetailPage(d catalogDetailPageData) Node {
	summaryNode := Node(P(Text("Metastore summary unavailable")))
	if len(d.MetastoreItems) > 0 {
		list := make([]Node, 0, len(d.MetastoreItems))
		for i := range d.MetastoreItems {
			list = append(list, Li(Text(d.MetastoreItems[i])))
		}
		summaryNode = Ul(Group(list))
	}

	schemaRows := make([]Node, 0, len(d.Schemas))
	for i := range d.Schemas {
		s := d.Schemas[i]
		schemaRows = append(schemaRows, Tr(
			Td(A(Href(s.URL), Text(s.Name))),
			Td(Text(s.Owner)),
			Td(Text(s.Updated)),
			Td(Class("text-right"), actionMenu("Actions", actionMenuLink(s.EditURL, "Edit schema"), actionMenuPost(s.DeleteURL, "Delete schema", d.CSRFField, true))),
		))
	}

	return appPage(
		"Catalog: "+d.CatalogName,
		"catalogs",
		d.Principal,
		Div(
			Class(cardClass()),
			P(Text("Status: "+d.Status)),
			P(Text("Data path: "+d.DataPath)),
			P(Text("Default: "+d.IsDefault)),
			Div(Class("BtnGroup"),
				A(Href(d.EditURL), Class(secondaryButtonClass()), Text("Edit")),
				A(Href(d.NewSchemaURL), Class(primaryButtonClass()), Text("New schema")),
			),
			Div(Class("BtnGroup"),
				Form(Method("post"), Action(d.SetDefaultURL), d.CSRFField(), Button(Type("submit"), Class("btn btn-sm"), Text("Set default"))),
				Form(Method("post"), Action(d.DeleteURL), d.CSRFField(), Button(Type("submit"), Class("btn btn-sm btn-danger"), Text("Delete"))),
			),
		),
		Div(Class(cardClass()), H2(Text("Metastore")), summaryNode),
		Div(Class(cardClass("table-wrap")), H2(Text("Schemas")), Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("Owner")), Th(Text("Updated")), Th(Class("text-right"), Text("Actions")))), TBody(Group(schemaRows)))),
	)
}

func catalogsNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Catalog", "catalogs", "/ui/catalogs", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Metastore Type")),
		Select(Name("metastore_type"), Option(Value("sqlite"), Text("sqlite")), Option(Value("postgres"), Text("postgres"))),
		Label(Text("DSN")),
		Input(Name("dsn"), Required()),
		Label(Text("Data Path")),
		Input(Name("data_path"), Required()),
		Label(Text("Comment")),
		Textarea(Name("comment")),
	)
}

func catalogsEditPage(principal domain.ContextPrincipal, catalogName string, catalog *domain.CatalogRegistration, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Edit Catalog", "catalogs", "/ui/catalogs/"+catalogName+"/update", csrfFieldProvider,
		Label(Text("Comment")),
		Textarea(Name("comment"), Text(catalog.Comment)),
		Label(Text("Data Path")),
		Input(Name("data_path"), Value(catalog.DataPath)),
		Label(Text("DSN")),
		Input(Name("dsn"), Value(catalog.DSN)),
	)
}

func catalogSchemasNewPage(principal domain.ContextPrincipal, catalogName string, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Schema", "catalogs", "/ui/catalogs/"+catalogName+"/schemas", csrfFieldProvider,
		Label(Text("Schema Name")),
		Input(Name("name"), Required()),
		Label(Text("Comment")),
		Textarea(Name("comment")),
		Label(Text("Location Name")),
		Input(Name("location_name")),
	)
}

func catalogSchemasEditPage(principal domain.ContextPrincipal, catalogName, schemaName string, schema *domain.SchemaDetail, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Edit Schema", "catalogs", "/ui/catalogs/"+catalogName+"/schemas/"+schemaName+"/update", csrfFieldProvider,
		Label(Text("Comment")),
		Textarea(Name("comment"), Text(schema.Comment)),
	)
}
