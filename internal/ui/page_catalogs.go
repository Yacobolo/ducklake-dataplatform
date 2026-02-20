package ui

import (
	"duck-demo/internal/domain"
	"strings"

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

type catalogWorkspaceCatalogLinkData struct {
	Name      string
	Status    string
	URL       string
	IsDefault bool
	Active    bool
}

type catalogWorkspaceObjectNodeData struct {
	Name   string
	URL    string
	Active bool
}

type catalogWorkspaceSchemaNodeData struct {
	Name      string
	Owner     string
	Updated   string
	URL       string
	Active    bool
	Open      bool
	EditURL   string
	DeleteURL string
	Tables    []catalogWorkspaceObjectNodeData
	Views     []catalogWorkspaceObjectNodeData
}

type catalogWorkspaceMetaItemData struct {
	Label string
	Value string
}

type catalogWorkspacePanelData struct {
	Mode             string
	Title            string
	Subtitle         string
	EditURL          string
	DeleteURL        string
	SetDefaultURL    string
	NewSchemaURL     string
	MetaItems        []catalogWorkspaceMetaItemData
	Columns          []tableColumnRowData
	Definition       string
	ColumnsAvailable bool
}

type catalogWorkspacePageData struct {
	Principal          domain.ContextPrincipal
	Catalogs           []catalogWorkspaceCatalogLinkData
	ActiveCatalogName  string
	SelectedSchemaName string
	SelectedType       string
	SelectedName       string
	Schemas            []catalogWorkspaceSchemaNodeData
	Panel              catalogWorkspacePanelData
	QuickFilterMessage string
	CSRFField          func() Node
}

func catalogWorkspacePage(d catalogWorkspacePageData) Node {
	catalogNodes := make([]Node, 0, len(d.Catalogs))
	for i := range d.Catalogs {
		c := d.Catalogs[i]
		className := "catalog-sidebar-link"
		if c.Active {
			className += " active"
		}
		badges := []Node{statusLabel(c.Status, "accent")}
		if c.IsDefault {
			badges = append(badges, statusLabel("default", "success"))
		}
		catalogNodes = append(catalogNodes,
			Li(
				data.Show(containsExpr(c.Name+" "+c.Status)),
				A(Href(c.URL), Class(className),
					Span(Text(c.Name)),
					Span(Class("catalog-sidebar-link-badges"), Group(badges)),
				),
			),
		)
	}

	explorerNodes := make([]Node, 0, len(d.Schemas))
	for i := range d.Schemas {
		schema := d.Schemas[i]
		tableNodes := make([]Node, 0, len(schema.Tables))
		for j := range schema.Tables {
			t := schema.Tables[j]
			className := "catalog-explorer-leaf"
			if t.Active {
				className += " active"
			}
			tableNodes = append(tableNodes, Li(data.Show(containsExpr(schema.Name+" "+t.Name+" table")), A(Href(t.URL), Class(className), I(Class("nav-icon"), Attr("data-lucide", "table"), Attr("aria-hidden", "true")), Span(Text(t.Name)))))
		}

		viewNodes := make([]Node, 0, len(schema.Views))
		for j := range schema.Views {
			v := schema.Views[j]
			className := "catalog-explorer-leaf"
			if v.Active {
				className += " active"
			}
			viewNodes = append(viewNodes, Li(data.Show(containsExpr(schema.Name+" "+v.Name+" view")), A(Href(v.URL), Class(className), I(Class("nav-icon"), Attr("data-lucide", "eye"), Attr("aria-hidden", "true")), Span(Text(v.Name)))))
		}

		schemaClass := "catalog-explorer-schema"
		if schema.Active {
			schemaClass += " active"
		}

		openAttr := Node(nil)
		if schema.Open {
			openAttr = Attr("open", "")
		}

		tablesSection := Node(P(Class(mutedClass()), Text("No tables")))
		if len(tableNodes) > 0 {
			tablesSection = Ul(Class("catalog-explorer-list"), Group(tableNodes))
		}

		viewsSection := Node(P(Class(mutedClass()), Text("No views")))
		if len(viewNodes) > 0 {
			viewsSection = Ul(Class("catalog-explorer-list"), Group(viewNodes))
		}

		explorerNodes = append(explorerNodes,
			Li(Class("catalog-explorer-node"),
				data.Show(containsExpr(schema.Name+" "+schema.Owner+" "+stringsJoin(objectNames(schema.Tables))+" "+stringsJoin(objectNames(schema.Views)))),
				Details(
					Class("details-reset"),
					openAttr,
					Summary(
						Class("catalog-explorer-head"),
						Div(Class("catalog-explorer-toggle"),
							I(Class("nav-icon catalog-explorer-caret"), Attr("data-lucide", "chevron-right"), Attr("aria-hidden", "true")),
							A(Href(schema.URL), Class(schemaClass), I(Class("nav-icon"), Attr("data-lucide", "folder"), Attr("aria-hidden", "true")), Span(Text(schema.Name))),
						),
					),
					Div(Class("catalog-explorer-subhead"),
						P(Class(mutedClass()), Text("Owner: "+schema.Owner+" | Updated: "+schema.Updated)),
						actionMenu("Actions", actionMenuLink(schema.EditURL, "Edit schema"), actionMenuPost(schema.DeleteURL, "Delete schema", d.CSRFField, true)),
					),
					Div(Class("catalog-explorer-group"),
						P(Class("catalog-explorer-group-title"), Text("Tables")),
						tablesSection,
					),
					Div(Class("catalog-explorer-group"),
						P(Class("catalog-explorer-group-title"), Text("Views")),
						viewsSection,
					),
				),
			),
		)
	}

	if len(explorerNodes) == 0 {
		explorerNodes = append(explorerNodes, Li(P(Class(mutedClass()), Text("No schemas in this catalog."))))
	}

	metaNodes := make([]Node, 0, len(d.Panel.MetaItems))
	for i := range d.Panel.MetaItems {
		item := d.Panel.MetaItems[i]
		metaNodes = append(metaNodes,
			Div(Class("catalog-meta-row"),
				Strong(Text(item.Label+": ")),
				Span(Text(dashIfEmpty(item.Value))),
			),
		)
	}

	panelActions := []Node{}
	if d.Panel.NewSchemaURL != "" {
		panelActions = append(panelActions, A(Href(d.Panel.NewSchemaURL), Class(primaryButtonClass()), Text("New schema")))
	}
	if d.Panel.EditURL != "" {
		panelActions = append(panelActions, A(Href(d.Panel.EditURL), Class(secondaryButtonClass()), Text("Edit")))
	}
	if d.Panel.SetDefaultURL != "" {
		panelActions = append(panelActions,
			Form(Method("post"), Action(d.Panel.SetDefaultURL), d.CSRFField(), Button(Type("submit"), Class("btn"), Text("Set default"))),
		)
	}
	if d.Panel.DeleteURL != "" {
		panelActions = append(panelActions, actionMenu("More", actionMenuPost(d.Panel.DeleteURL, "Delete", d.CSRFField, true)))
	}

	columnsNode := Node(nil)
	if d.Panel.Mode == "table" || d.Panel.Mode == "view" {
		if len(d.Panel.Columns) == 0 && d.Panel.Mode == "view" && !d.Panel.ColumnsAvailable {
			columnsNode = P(Class(mutedClass()), Text("Columns unavailable for this view."))
		} else {
			rows := make([]Node, 0, len(d.Panel.Columns))
			for i := range d.Panel.Columns {
				c := d.Panel.Columns[i]
				rows = append(rows, Tr(Td(Text(c.Name)), Td(Text(c.Type)), Td(Text(c.Nullable)), Td(Text(c.Comment)), Td(Text(c.Properties))))
			}
			columnsNode = Div(Class("table-wrap"), Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Nullable")), Th(Text("Comment")), Th(Text("Properties")))), TBody(Group(rows))))
		}
	}

	definitionNode := Node(nil)
	if d.Panel.Definition != "" {
		definitionNode = Div(Class(cardClass()), H3(Text("Definition")), Pre(Text(d.Panel.Definition)))
	}

	return appPage(
		"Catalog: "+d.ActiveCatalogName,
		"catalogs",
		d.Principal,
		data.Signals(map[string]any{"q": ""}),
		quickFilterCard(d.QuickFilterMessage),
		Div(
			Class("catalog-workspace"),
			Aside(
				Class(cardClass("catalog-sidebar")),
				Div(Class("d-flex flex-justify-between flex-items-center"),
					H3(Text("Catalogs")),
					A(Href("/ui/catalogs/new"), Class("btn btn-sm btn-icon"), Title("New catalog"), Attr("aria-label", "New catalog"), I(Class("btn-icon-glyph"), Attr("data-lucide", "plus"), Attr("aria-hidden", "true")), Span(Class("sr-only"), Text("New catalog"))),
				),
				Ul(Class("catalog-sidebar-list"), Group(catalogNodes)),
			),
			Section(
				Class(cardClass("catalog-explorer")),
				Div(Class("d-flex flex-justify-between flex-items-center"),
					H3(Text("Explorer")),
					statusLabel(d.ActiveCatalogName, "accent"),
				),
				Ul(Class("catalog-explorer-root"), Group(explorerNodes)),
			),
			Section(
				Class(cardClass("catalog-view")),
				Div(Class("d-flex flex-justify-between flex-items-center flex-wrap gap-2"),
					Div(
						H2(Text(d.Panel.Title)),
						P(Class(mutedClass()), Text(catalogPanelContextLabel(d))),
					),
					Div(Class("button-row"), Group(panelActions)),
				),
				Div(Class("catalog-meta"), Group(metaNodes)),
				definitionNode,
				columnsNode,
			),
		),
	)
}

func catalogPanelContextLabel(d catalogWorkspacePageData) string {
	parts := []string{d.ActiveCatalogName}
	if d.SelectedSchemaName != "" {
		parts = append(parts, d.SelectedSchemaName)
	}
	if d.SelectedName != "" {
		parts = append(parts, d.SelectedName)
	}
	prefix := strings.ToUpper(d.Panel.Mode)
	if d.Panel.Subtitle != "" {
		prefix += " " + d.Panel.Subtitle
	}
	return prefix + " | " + strings.Join(parts, ".")
}

func objectNames(items []catalogWorkspaceObjectNodeData) []string {
	names := make([]string, 0, len(items))
	for i := range items {
		names = append(names, items[i].Name)
	}
	return names
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
