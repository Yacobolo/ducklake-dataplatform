package ui

import (
	"duck-demo/internal/domain"
	"net/url"
	"strconv"
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
	Name    string
	URL     string
	Active  bool
	Owner   string
	Created string
	Kind    string
}

type catalogWorkspaceSchemaNodeData struct {
	Name      string
	Owner     string
	Created   string
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
	Description      string
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
	ActiveTab          string
	Schemas            []catalogWorkspaceSchemaNodeData
	Panel              catalogWorkspacePanelData
	QuickFilterMessage string
	CSRFField          func() Node
}

func catalogWorkspacePage(d catalogWorkspacePageData) Node {
	explorerNodes := make([]Node, 0, len(d.Schemas))
	for i := range d.Schemas {
		schema := d.Schemas[i]
		tableNodes := make([]Node, 0, len(schema.Tables))
		for j := range schema.Tables {
			t := schema.Tables[j]
			className := "catalog-tree-leaf"
			if t.Active {
				className += " active"
			}
			tableNodes = append(tableNodes, Li(data.Show(containsExpr(schema.Name+" "+t.Name+" table")), A(Href(t.URL), Class(className), I(Class("nav-icon"), Attr("data-lucide", "table"), Attr("aria-hidden", "true")), Span(Text(t.Name)))))
		}

		viewNodes := make([]Node, 0, len(schema.Views))
		for j := range schema.Views {
			v := schema.Views[j]
			className := "catalog-tree-leaf"
			if v.Active {
				className += " active"
			}
			viewNodes = append(viewNodes, Li(data.Show(containsExpr(schema.Name+" "+v.Name+" view")), A(Href(v.URL), Class(className), I(Class("nav-icon"), Attr("data-lucide", "eye"), Attr("aria-hidden", "true")), Span(Text(v.Name)))))
		}

		schemaClass := "catalog-tree-schema-link"
		if schema.Active {
			schemaClass += " active"
		}

		openAttr := Node(nil)
		if schema.Open {
			openAttr = Attr("open", "")
		}

		objectNodes := make([]Node, 0, len(tableNodes)+len(viewNodes))
		objectNodes = append(objectNodes, tableNodes...)
		objectNodes = append(objectNodes, viewNodes...)

		objectSection := Node(P(Class("catalog-tree-empty"), Text("No tables or views")))
		if len(objectNodes) > 0 {
			objectSection = Ul(Class("catalog-tree-list"), Group(objectNodes))
		}

		explorerNodes = append(explorerNodes,
			Li(Class("catalog-tree-node"),
				data.Show(containsExpr(schema.Name+" "+stringsJoin(objectNames(schema.Tables))+" "+stringsJoin(objectNames(schema.Views)))),
				Details(
					Class("details-reset catalog-tree-disclosure"),
					openAttr,
					Summary(
						Class("catalog-tree-summary"),
						Div(Class("catalog-tree-summary-main"),
							I(Class("nav-icon catalog-tree-caret"), Attr("data-lucide", "chevron-right"), Attr("aria-hidden", "true")),
							A(Href(schema.URL), Class(schemaClass), I(Class("nav-icon"), Attr("data-lucide", "folder"), Attr("aria-hidden", "true")), Span(Text(schema.Name))),
						),
					),
					Div(Class("catalog-tree-children"), objectSection),
				),
			),
		)
	}

	catalogTreeNodes := make([]Node, 0, len(d.Catalogs))
	for i := range d.Catalogs {
		c := d.Catalogs[i]
		catalogClass := "catalog-tree-catalog-link"
		if c.Active && d.SelectedType == "catalog" {
			catalogClass += " active"
		}

		showValue := c.Name + " " + c.Status
		if c.Active {
			showValue += " " + stringsJoin(schemaNames(d.Schemas))
		}

		if c.Active {
			childrenNode := Node(P(Class("catalog-tree-empty"), Text("No schemas in this catalog.")))
			if len(explorerNodes) > 0 {
				childrenNode = Ul(Class("catalog-tree-catalog-children"), Group(explorerNodes))
			}
			catalogTreeNodes = append(catalogTreeNodes,
				Li(Class("catalog-tree-catalog-node"),
					data.Show(containsExpr(showValue)),
					Details(
						Class("details-reset catalog-tree-disclosure catalog-tree-catalog-disclosure"),
						Attr("open", ""),
						Summary(
							Class("catalog-tree-summary"),
							Div(Class("catalog-tree-summary-main"),
								I(Class("nav-icon catalog-tree-caret"), Attr("data-lucide", "chevron-right"), Attr("aria-hidden", "true")),
								A(Href(c.URL), Class(catalogClass), I(Class("nav-icon"), Attr("data-lucide", "database"), Attr("aria-hidden", "true")), Span(Text(c.Name))),
							),
						),
						childrenNode,
					),
				),
			)
			continue
		}

		catalogTreeNodes = append(catalogTreeNodes,
			Li(Class("catalog-tree-catalog-node"),
				data.Show(containsExpr(showValue)),
				A(Href(c.URL), Class(catalogClass), I(Class("nav-icon"), Attr("data-lucide", "database"), Attr("aria-hidden", "true")), Span(Text(c.Name))),
			),
		)
	}

	metaNodes := make([]Node, 0, len(d.Panel.MetaItems))
	for i := range d.Panel.MetaItems {
		item := d.Panel.MetaItems[i]
		metaNodes = append(metaNodes,
			Div(Class("catalog-meta-row"),
				Dt(Class("catalog-meta-label"), Text(item.Label)),
				Dd(Class("catalog-meta-value"), Text(dashIfEmpty(item.Value))),
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
			columnsNode = P(Class("catalog-muted"), Text("Columns unavailable for this view."))
		} else {
			rows := make([]Node, 0, len(d.Panel.Columns))
			for i := range d.Panel.Columns {
				c := d.Panel.Columns[i]
				rows = append(rows, Tr(Td(Text(c.Name)), Td(Text(c.Type)), Td(Text(c.Nullable)), Td(Text(c.Comment)), Td(Text(c.Properties))))
			}
			columnsNode = Div(Class("table-wrap catalog-columns-table"), Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Nullable")), Th(Text("Comment")), Th(Text("Properties")))), TBody(Group(rows))))
		}
	}

	definitionNode := Node(nil)
	if d.Panel.Definition != "" {
		definitionNode = Div(Class("catalog-section"), H3(Class("catalog-section-title"), Text("Definition")), Pre(Class("catalog-definition"), Text(d.Panel.Definition)))
	}

	panelStatus := panelMetaValue(d.Panel, "Status")
	if panelStatus == "" {
		panelStatus = strings.ToUpper(d.Panel.Mode)
	}

	tabKeys := catalogTabsForType(d.Panel.Mode)
	tabItems := make([]Node, 0, len(tabKeys))
	for i := range tabKeys {
		tab := tabKeys[i]
		className := "catalog-tab"
		currentAttr := Node(nil)
		if d.ActiveTab == tab {
			className += " active"
			currentAttr = Attr("aria-current", "page")
		}
		tabItems = append(tabItems, A(Href(catalogWorkspaceTabURL(d, tab)), Class(className), currentAttr, Text(catalogTabLabel(tab))))
	}

	overviewContent := catalogOverviewContent(d)
	detailsNodes := []Node{Dl(Class("catalog-meta"), Group(metaNodes)), definitionNode}
	if d.Panel.Mode == "table" || d.Panel.Mode == "view" {
		if columnsNode != nil {
			detailsNodes = append(detailsNodes, Div(Class("catalog-section catalog-section-inline"), H3(Class("catalog-section-title"), Text("Columns")), columnsNode))
		} else {
			detailsNodes = append(detailsNodes, Div(Class("catalog-section catalog-section-inline"), H3(Class("catalog-section-title"), Text("Columns")), P(Class("catalog-muted"), Text("No columns available."))))
		}
	}
	detailsContent := Node(Group(detailsNodes))
	permissionsContent := catalogPlaceholderTab("Permissions", "Permissions for this "+d.Panel.Mode+" will appear here.")
	policiesContent := catalogPlaceholderTab("Policies", "Policies for this "+d.Panel.Mode+" will appear here.")
	workspacesContent := catalogPlaceholderTab("Workspaces", "Workspace assignments for this catalog will appear here.")
	historyContent := catalogPlaceholderTab("History", "History for this "+d.Panel.Mode+" will appear here.")
	lineageContent := catalogPlaceholderTab("Lineage", "Lineage for this "+d.Panel.Mode+" will appear here.")
	insightsContent := catalogPlaceholderTab("Insights", "Insights for this "+d.Panel.Mode+" will appear here.")
	qualityContent := catalogPlaceholderTab("Quality", "Quality rules and checks for this table will appear here.")
	sampleDataContent := catalogPlaceholderTab("Sample Data", "Sample data preview for this table will appear here.")

	detailContent := Node(overviewContent)
	switch d.ActiveTab {
	case "details":
		detailContent = detailsContent
	case "sample-data":
		detailContent = sampleDataContent
	case "permissions":
		detailContent = permissionsContent
	case "policies":
		detailContent = policiesContent
	case "workspaces":
		detailContent = workspacesContent
	case "history":
		detailContent = historyContent
	case "lineage":
		detailContent = lineageContent
	case "insights":
		detailContent = insightsContent
	case "quality":
		detailContent = qualityContent
	}

	return appPage(
		"Catalog: "+d.ActiveCatalogName,
		"catalogs",
		d.Principal,
		data.Signals(map[string]any{"q": "", "childq": ""}),
		Div(
			Class("catalog-workspace"),
			Aside(
				Class("catalog-rail"),
				Div(Class("catalog-rail-head"),
					Div(Class("catalog-rail-head-row"),
						P(Class("catalog-rail-kicker"), Text("Catalog Explorer")),
						A(Href("/ui/catalogs/new"), Class("btn btn-sm btn-icon"), Title("New catalog"), Attr("aria-label", "New catalog"), I(Class("btn-icon-glyph"), Attr("data-lucide", "plus"), Attr("aria-hidden", "true")), Span(Class("sr-only"), Text("New catalog"))),
					),
					Div(Class("catalog-rail-search"),
						I(Class("nav-icon"), Attr("data-lucide", "search"), Attr("aria-hidden", "true")),
						Label(Class("sr-only"), Text("Filter catalog explorer")),
						Input(Type("search"), Class("form-control"), Placeholder(d.QuickFilterMessage), data.Bind("q"), AutoComplete("off")),
					),
				),
				Ul(Class("catalog-tree-root"), Group(catalogTreeNodes)),
			),
			Section(
				Class("catalog-detail"),
				Div(Class("catalog-detail-header"),
					Div(Class("catalog-detail-title-wrap"),
						catalogBreadcrumb(d),
						Div(Class("catalog-detail-heading"),
							H2(Class("catalog-detail-title"), Text(d.Panel.Title)),
							statusLabel(panelStatus, "accent"),
						),
					),
					Div(Class("button-row catalog-detail-actions"), Group(panelActions)),
				),
				Div(Class("catalog-tabs"), Group(tabItems)),
				detailContent,
			),
		),
	)
}

func catalogWorkspaceTabURL(d catalogWorkspacePageData, tab string) string {
	base := catalogExplorerURL(d.ActiveCatalogName, d.SelectedSchemaName, d.SelectedType, d.SelectedName)
	if tab == "overview" {
		return base
	}
	parsed, err := url.Parse(base)
	if err != nil {
		return base
	}
	q := parsed.Query()
	q.Set("tab", tab)
	parsed.RawQuery = q.Encode()
	return parsed.String()
}

func catalogTabLabel(tab string) string {
	switch tab {
	case "overview":
		return "Overview"
	case "details":
		return "Details"
	case "sample-data":
		return "Sample Data"
	case "permissions":
		return "Permissions"
	case "policies":
		return "Policies"
	case "workspaces":
		return "Workspaces"
	case "history":
		return "History"
	case "lineage":
		return "Lineage"
	case "insights":
		return "Insights"
	case "quality":
		return "Quality"
	default:
		return "Overview"
	}
}

func catalogTabsForType(mode string) []string {
	switch mode {
	case "catalog":
		return []string{"overview", "details", "permissions", "policies", "workspaces"}
	case "schema":
		return []string{"overview", "details", "permissions", "policies"}
	case "table":
		return []string{"overview", "sample-data", "details", "permissions", "policies", "history", "lineage", "insights", "quality"}
	case "view":
		return []string{"overview", "details", "permissions", "policies", "history", "lineage", "insights"}
	default:
		return []string{"overview"}
	}
}

func isCatalogTabAllowed(mode, tab string) bool {
	for _, item := range catalogTabsForType(mode) {
		if item == tab {
			return true
		}
	}
	return false
}

func catalogPlaceholderTab(title, text string) Node {
	return Div(Class("catalog-section catalog-section-inline"), H3(Class("catalog-section-title"), Text(title)), P(Class("catalog-muted"), Text(text)))
}

func catalogOverviewContent(d catalogWorkspacePageData) Node {
	childRows := []Node{}
	filterPlaceholder := "Filter child elements"
	countLabelSingular := "item"
	countLabelPlural := "items"

	switch d.Panel.Mode {
	case "catalog":
		filterPlaceholder = "Filter schemas"
		countLabelSingular = "schema"
		countLabelPlural = "schemas"
		for i := range d.Schemas {
			schema := d.Schemas[i]
			childRows = append(childRows,
				Tr(
					data.Show(containsExprSignal(schema.Name+" "+schema.Owner+" "+schema.Created, "childq")),
					Td(A(Href(schema.URL), Text(schema.Name))),
					Td(Text(dashIfEmpty(schema.Owner))),
					Td(Text(dashIfEmpty(schema.Created))),
				),
			)
		}
	case "schema":
		filterPlaceholder = "Filter tables and views"
		countLabelSingular = "child"
		countLabelPlural = "children"
		for i := range d.Schemas {
			schema := d.Schemas[i]
			if schema.Name != d.SelectedSchemaName {
				continue
			}
			for j := range schema.Tables {
				table := schema.Tables[j]
				childRows = append(childRows,
					Tr(
						data.Show(containsExprSignal(table.Name+" "+table.Owner+" "+table.Created+" "+table.Kind, "childq")),
						Td(A(Href(table.URL), Text(table.Name))),
						Td(Text(dashIfEmpty(table.Owner))),
						Td(Text(dashIfEmpty(table.Created))),
					),
				)
			}
			for j := range schema.Views {
				view := schema.Views[j]
				childRows = append(childRows,
					Tr(
						data.Show(containsExprSignal(view.Name+" "+view.Owner+" "+view.Created+" "+view.Kind, "childq")),
						Td(A(Href(view.URL), Text(view.Name))),
						Td(Text(dashIfEmpty(view.Owner))),
						Td(Text(dashIfEmpty(view.Created))),
					),
				)
			}
			break
		}
	case "table", "view":
		filterPlaceholder = "Filter columns"
		countLabelSingular = "column"
		countLabelPlural = "columns"
		for i := range d.Panel.Columns {
			col := d.Panel.Columns[i]
			childRows = append(childRows,
				Tr(
					data.Show(containsExprSignal(col.Name+" "+col.Type+" "+col.Comment, "childq")),
					Td(Text(col.Name)),
					Td(Text(col.Type)),
					Td(Text(col.Nullable)),
				),
			)
		}
	}

	descriptionNode := Node(nil)
	if strings.TrimSpace(d.Panel.Description) != "" {
		descriptionNode = P(Class("catalog-overview-description"), Text(d.Panel.Description))
	}

	headers := []Node{Th(Text("Name")), Th(Text("Owner")), Th(Text("Created at"))}
	if d.Panel.Mode == "table" || d.Panel.Mode == "view" {
		headers = []Node{Th(Text("Name")), Th(Text("Type")), Th(Text("Nullable"))}
	}

	childTable := Node(P(Class("catalog-muted"), Text("No child elements.")))
	if len(childRows) > 0 {
		childTable = Div(Class("table-wrap catalog-overview-table"), Table(Class("data-table"), THead(Tr(Group(headers))), TBody(Group(childRows))))
	}

	return Div(
		Class("catalog-section catalog-section-inline"),
		descriptionNode,
		Div(Class("catalog-overview-toolbar"),
			Div(Class("catalog-overview-filter"),
				I(Class("nav-icon"), Attr("data-lucide", "search"), Attr("aria-hidden", "true")),
				Label(Class("sr-only"), Text("Filter child elements")),
				Input(Type("search"), Class("form-control"), Placeholder(filterPlaceholder), data.Bind("childq"), AutoComplete("off")),
			),
			P(Class("catalog-overview-count"), Text(strconv.Itoa(len(childRows))+" "+pluralize(len(childRows), countLabelSingular, countLabelPlural))),
		),
		childTable,
	)
}

func pluralize(count int, singular, plural string) string {
	if count == 1 {
		return singular
	}
	return plural
}

func catalogBreadcrumb(d catalogWorkspacePageData) Node {
	items := []Node{
		Li(Class("catalog-breadcrumb-item"), A(Href(catalogExplorerURL(d.ActiveCatalogName, "", "catalog", "")), Title(d.ActiveCatalogName), Span(Class("catalog-breadcrumb-label"), Text(d.ActiveCatalogName)))),
	}

	if d.SelectedSchemaName != "" {
		items = append(items,
			Li(Class("catalog-breadcrumb-item"), Span(Class("catalog-breadcrumb-separator"), Attr("aria-hidden", "true"), Text("/")), A(Href(catalogExplorerURL(d.ActiveCatalogName, d.SelectedSchemaName, "schema", "")), Title(d.SelectedSchemaName), Span(Class("catalog-breadcrumb-label"), Text(d.SelectedSchemaName)))),
		)
	}

	if d.SelectedName != "" {
		items = append(items,
			Li(Class("catalog-breadcrumb-item"), Span(Class("catalog-breadcrumb-separator"), Attr("aria-hidden", "true"), Text("/")), Span(Class("catalog-breadcrumb-current catalog-breadcrumb-label"), Title(d.SelectedName), Text(d.SelectedName))),
		)
	}

	return Nav(
		Class("catalog-breadcrumb"),
		Attr("aria-label", "Catalog path"),
		Ol(Class("catalog-breadcrumb-list"), Group(items)),
	)
}

func objectNames(items []catalogWorkspaceObjectNodeData) []string {
	names := make([]string, 0, len(items))
	for i := range items {
		names = append(names, items[i].Name)
	}
	return names
}

func schemaNames(items []catalogWorkspaceSchemaNodeData) []string {
	names := make([]string, 0, len(items))
	for i := range items {
		names = append(names, items[i].Name)
	}
	return names
}

func containsExprSignal(value, signal string) string {
	lower := strings.ToLower(value)
	return "$" + signal + " === '' || " + strconv.Quote(lower) + ".includes($" + signal + ".toLowerCase())"
}

func panelMetaValue(panel catalogWorkspacePanelData, label string) string {
	for i := range panel.MetaItems {
		if strings.EqualFold(panel.MetaItems[i].Label, label) {
			return panel.MetaItems[i].Value
		}
	}
	return ""
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
