package catalogs

import (
	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func formPage(principal domain.ContextPrincipal, title, active, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	return core.AppPage(
		title,
		active,
		principal,
		Div(
			Class(core.CardClass()),
			Form(
				Class("stack-form [&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0"),
				Method("post"),
				Action(action),
				Group(nodes),
				Div(Class("form-actions mt-2"), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Save"))),
			),
		),
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
