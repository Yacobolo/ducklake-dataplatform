package catalogs

import (
	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/ui/core"

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
		core.Card(
			Form(
				Class("stack-form [&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0"),
				Method("post"),
				Action(action),
				Group(nodes),
				Div(Class("form-actions mt-2"), core.PrimaryButton("", Type("submit"), Text("Save"))),
			),
		),
	)
}

func catalogsNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Catalog", "catalogs", "/ui/catalogs", csrfFieldProvider,
		core.FieldLabel("Name"),
		core.InputControl("", Name("name"), Required()),
		core.FieldLabel("Metastore Type"),
		core.SelectControl("", Name("metastore_type"), Option(Value("sqlite"), Text("sqlite")), Option(Value("postgres"), Text("postgres"))),
		core.FieldLabel("DSN"),
		core.InputControl("", Name("dsn"), Required()),
		core.FieldLabel("Data Path"),
		core.InputControl("", Name("data_path"), Required()),
		core.FieldLabel("Comment"),
		core.TextareaControl("", Name("comment")),
	)
}

func catalogsEditPage(principal domain.ContextPrincipal, catalogName string, catalog *domain.CatalogRegistration, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Edit Catalog", "catalogs", "/ui/catalogs/"+catalogName+"/update", csrfFieldProvider,
		core.FieldLabel("Comment"),
		core.TextareaControl("", Name("comment"), Text(catalog.Comment)),
		core.FieldLabel("Data Path"),
		core.InputControl("", Name("data_path"), Value(catalog.DataPath)),
		core.FieldLabel("DSN"),
		core.InputControl("", Name("dsn"), Value(catalog.DSN)),
	)
}

func catalogSchemasNewPage(principal domain.ContextPrincipal, catalogName string, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Schema", "catalogs", "/ui/catalogs/"+catalogName+"/schemas", csrfFieldProvider,
		core.FieldLabel("Schema Name"),
		core.InputControl("", Name("name"), Required()),
		core.FieldLabel("Comment"),
		core.TextareaControl("", Name("comment")),
		core.FieldLabel("Location Name"),
		core.InputControl("", Name("location_name")),
	)
}

func catalogSchemasEditPage(principal domain.ContextPrincipal, catalogName, schemaName string, schema *domain.SchemaDetail, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Edit Schema", "catalogs", "/ui/catalogs/"+catalogName+"/schemas/"+schemaName+"/update", csrfFieldProvider,
		core.FieldLabel("Comment"),
		core.TextareaControl("", Name("comment"), Text(schema.Comment)),
	)
}
