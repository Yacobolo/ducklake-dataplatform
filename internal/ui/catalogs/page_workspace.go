package catalogs

import (
	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"
	"net/url"
	"strconv"
	"strings"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type catalogWorkspaceCatalogLinkData struct {
	Name      string
	Status    string
	URL       string
	IsDefault bool
	Active    bool
}

type catalogWorkspaceObjectNodeData struct {
	Name     string
	URL      string
	AssetURL string
	AssetKey string
	Active   bool
	Owner    string
	Created  string
	Kind     string
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
	AssetURL         string
	AssetKey         string
	VersionSummary   *domain.CatalogVersionSummary
	VersionError     string
	HistoryEntries   []domain.CatalogHistoryEntry
	HistoryEntity    string
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
	explorerSchemas := make([]catalogExplorerSchemaItem, 0, len(d.Schemas))
	for i := range d.Schemas {
		schema := d.Schemas[i]
		objects := make([]catalogExplorerObjectItem, 0, len(schema.Tables)+len(schema.Views))
		for j := range schema.Tables {
			t := schema.Tables[j]
			objects = append(objects, catalogExplorerObjectItem{Name: t.Name, URL: t.URL, Icon: "table", Active: t.Active})
		}
		for j := range schema.Views {
			v := schema.Views[j]
			objects = append(objects, catalogExplorerObjectItem{Name: v.Name, URL: v.URL, Icon: "eye", Active: v.Active})
		}

		explorerSchemas = append(explorerSchemas, catalogExplorerSchemaItem{
			Name:      schema.Name,
			URL:       schema.URL,
			Active:    schema.Active,
			Open:      schema.Open,
			Objects:   objects,
			EmptyText: "No tables or views",
		})
	}

	explorerCatalogs := make([]catalogExplorerCatalogItem, 0, len(d.Catalogs))
	for i := range d.Catalogs {
		catalog := d.Catalogs[i]
		catalogItem := catalogExplorerCatalogItem{
			Name:      catalog.Name,
			URL:       catalog.URL,
			Active:    catalog.Active && d.SelectedType == "catalog",
			Open:      catalog.Active,
			EmptyText: "No schemas in this catalog.",
		}
		if catalog.Active {
			catalogItem.Schemas = explorerSchemas
		}
		explorerCatalogs = append(explorerCatalogs, catalogItem)
	}

	explorerPanel := core.CatalogExplorerPanel(catalogExplorerPanelData{
		Title:             "Catalog Explorer",
		FilterPlaceholder: d.QuickFilterMessage,
		Catalogs:          explorerCatalogs,
		EmptyCatalogsText: "No catalogs found.",
	})

	metaNodes := make([]Node, 0, len(d.Panel.MetaItems))
	for i := range d.Panel.MetaItems {
		item := d.Panel.MetaItems[i]
		metaNodes = append(metaNodes,
			Div(Class(catalogMetaRowClass()),
				Dt(Class(catalogMetaLabelClass()), Text(item.Label)),
				Dd(Class(catalogMetaValueClass()), Text(dashIfEmpty(item.Value))),
			),
		)
	}

	panelActions := []Node{}
	if d.Panel.NewSchemaURL != "" {
		panelActions = append(panelActions, core.PrimaryLink(d.Panel.NewSchemaURL, "", Text("New schema")))
	}
	if d.Panel.EditURL != "" {
		panelActions = append(panelActions, core.SecondaryLink(d.Panel.EditURL, "", Text("Edit")))
	}
	if d.Panel.AssetURL != "" {
		label := core.FallbackString(d.Panel.AssetKey, "Open asset")
		panelActions = append(panelActions, core.SecondaryLink(d.Panel.AssetURL, "", Text(label)))
	}
	if d.Panel.SetDefaultURL != "" {
		panelActions = append(panelActions,
			Form(Method("post"), Action(d.Panel.SetDefaultURL), d.CSRFField(), core.SecondaryButton("", Type("submit"), Text("Set default"))),
		)
	}
	if d.Panel.DeleteURL != "" {
		panelActions = append(panelActions, actionMenu("More", actionMenuPost(d.Panel.DeleteURL, "Delete", d.CSRFField, true)))
	}

	columnsNode := Node(nil)
	if d.Panel.Mode == "table" || d.Panel.Mode == "view" {
		if len(d.Panel.Columns) == 0 && d.Panel.Mode == "view" && !d.Panel.ColumnsAvailable {
			columnsNode = P(Class(catalogMutedCopyClass()), Text("Columns unavailable for this view."))
		} else {
			rows := make([]Node, 0, len(d.Panel.Columns))
			for i := range d.Panel.Columns {
				c := d.Panel.Columns[i]
				rows = append(rows, Tr(Td(Text(c.Name)), Td(Text(c.Type)), Td(Text(c.Nullable)), Td(Text(c.Comment)), Td(Text(c.Properties))))
			}
			columnsNode = Div(Class(catalogTableWrapClass("catalog-columns-table")), core.DataTable("", THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Nullable")), Th(Text("Comment")), Th(Text("Properties")))), TBody(Group(rows))))
		}
	}

	definitionNode := Node(nil)
	if d.Panel.Definition != "" {
		definitionNode = Div(Class(catalogSectionClass()),
			H3(Class(catalogSectionTitleClass()), Text("Definition")),
			Pre(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-3"), Text(d.Panel.Definition)),
		)
	}

	panelStatus := panelMetaValue(d.Panel, "Status")
	if panelStatus == "" {
		panelStatus = strings.ToUpper(d.Panel.Mode)
	}

	tabKeys := catalogTabsForType(d.Panel.Mode)
	tabItems := make([]Node, 0, len(tabKeys))
	for i := range tabKeys {
		tab := tabKeys[i]
		className := catalogTabClass(d.ActiveTab == tab)
		currentAttr := Node(nil)
		if d.ActiveTab == tab {
			currentAttr = Attr("aria-current", "page")
		}
		tabItems = append(tabItems, A(Href(catalogWorkspaceTabURL(d, tab)), Class(className), currentAttr, Text(catalogTabLabel(tab))))
	}

	overviewContent := catalogOverviewContent(d)
	detailsNodes := []Node{Dl(Class(catalogMetaListClass()), Group(metaNodes)), definitionNode}
	if d.Panel.Mode == "table" || d.Panel.Mode == "view" {
		if columnsNode != nil {
			detailsNodes = append(detailsNodes, Div(Class(catalogSectionClass()), H3(Class(catalogSectionTitleClass()), Text("Columns")), columnsNode))
		} else {
			detailsNodes = append(detailsNodes, Div(Class(catalogSectionClass()), H3(Class(catalogSectionTitleClass()), Text("Columns")), P(Class(catalogMutedCopyClass()), Text("No columns available."))))
		}
	}
	detailsContent := Node(Group(detailsNodes))
	permissionsContent := catalogPlaceholderTab("Permissions", "Permissions for this "+d.Panel.Mode+" will appear here.")
	policiesContent := catalogPlaceholderTab("Policies", "Policies for this "+d.Panel.Mode+" will appear here.")
	workspacesContent := catalogPlaceholderTab("Workspaces", "Workspace assignments for this catalog will appear here.")
	historyContent := catalogHistoryContent(d)
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

	return core.AppPage(
		"Catalog: "+d.ActiveCatalogName,
		"catalogs",
		d.Principal,
		data.Signals(map[string]any{"q": "", "childq": ""}),
		core.WorkspaceLayout(
			"min-h-0",
			core.WorkspaceAside(
				"catalog-workspace",
				"catalog-aside",
				[]workspaceAsideTab{
					{
						ID:      "explorer",
						Label:   "Explorer",
						Icon:    "database",
						Content: explorerPanel,
					},
				},
				"explorer",
			),
			Section(
				Class("flex min-w-0 flex-col gap-4 rounded-2xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-5 shadow-[var(--shadow-resting-xsmall)] max-md:p-4"),
				Div(Class("flex flex-wrap items-start justify-between gap-3"),
					Div(Class("min-w-0 flex-1"),
						catalogBreadcrumb(d),
						Div(Class("inline-flex flex-wrap items-center gap-3"),
							H2(Class("m-0 text-2xl font-semibold"), Text(d.Panel.Title)),
							core.StatusLabel(panelStatus, "accent"),
						),
					),
					Div(Class(catalogButtonRowClass()), Group(panelActions)),
				),
				Div(Class(catalogTabsClass()), Group(tabItems)),
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
		return []string{"overview", "details", "history", "permissions", "policies", "workspaces"}
	case "schema":
		return []string{"overview", "details", "history", "permissions", "policies"}
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
	return Div(Class(catalogSectionClass()), H3(Class(catalogSectionTitleClass()), Text(title)), P(Class(catalogMutedCopyClass()), Text(text)))
}

func catalogHistoryContent(d catalogWorkspacePageData) Node {
	if d.Panel.Mode == "view" {
		return catalogPlaceholderTab("History", "History for this "+d.Panel.Mode+" will appear here.")
	}
	if d.Panel.VersionError != "" {
		return Div(
			Class(catalogSectionClass()),
			H3(Class(catalogSectionTitleClass()), Text("History")),
			P(Class(catalogMutedCopyClass()), Text(d.Panel.VersionError)),
		)
	}
	if d.Panel.Mode != "catalog" {
		return catalogHistoryEntriesSection(d, false)
	}
	if d.Panel.VersionSummary == nil {
		return Div(
			Class(catalogSectionClass()),
			H3(Class(catalogSectionTitleClass()), Text("History")),
			P(Class(catalogMutedCopyClass()), Text("Version metadata is not available for this catalog.")),
		)
	}
	filterLinks := []struct {
		label string
		value string
	}{
		{label: "All", value: ""},
		{label: "Schemas", value: "schema"},
		{label: "Tables", value: "table"},
		{label: "Columns", value: "column"},
	}
	filterNodes := make([]Node, 0, len(filterLinks))
	for i := range filterLinks {
		item := filterLinks[i]
		className := catalogHistoryFilterClass(d.Panel.HistoryEntity == item.value)
		filterNodes = append(filterNodes, A(Href(catalogHistoryURL(d, item.value)), Class(className), Text(item.label)))
	}

	summary := d.Panel.VersionSummary
	metaItems := []catalogWorkspaceMetaItemData{
		{Label: "Catalog version", Value: summary.Version},
		{Label: "Created by", Value: summary.CreatedBy},
		{Label: "Encrypted", Value: boolPtrLabel(summary.Encrypted)},
		{Label: "Data path", Value: summary.DataPath},
		{Label: "Latest snapshot", Value: snapshotIDLabel(summary.LatestSnapshotID)},
	}
	metaNodes := make([]Node, 0, len(metaItems))
	for i := range metaItems {
		item := metaItems[i]
		metaNodes = append(metaNodes,
			Div(Class(catalogMetaRowClass()),
				Dt(Class(catalogMetaLabelClass()), Text(item.Label)),
				Dd(Class(catalogMetaValueClass()), Text(dashIfEmpty(item.Value))),
			),
		)
	}

	entityRows := []struct {
		name    string
		summary domain.VersionedObjectSummary
	}{
		{name: "Schemas", summary: summary.Schemas},
		{name: "Tables", summary: summary.Tables},
		{name: "Columns", summary: summary.Columns},
	}
	rows := make([]Node, 0, len(entityRows))
	for i := range entityRows {
		row := entityRows[i]
		rows = append(rows,
			Tr(
				Td(Text(row.name)),
				Td(Text(strconv.FormatInt(row.summary.ActiveCount, 10))),
				Td(Text(strconv.FormatInt(row.summary.HistoricalCount, 10))),
				Td(Text(strconv.FormatInt(row.summary.TotalCount, 10))),
				Td(Text(snapshotIDLabel(row.summary.LatestSnapshotID))),
			),
		)
	}

	return Div(
		Class(catalogSectionClass()),
		Div(Class("flex flex-col gap-1"),
			H3(Class(catalogSectionTitleClass()), Text("Version metadata")),
			P(Class(catalogMutedCopyClass()), Text("DuckLake metastore metadata and snapshot-aware counts for this catalog.")),
		),
		Dl(Class(catalogMetaListClass()), Group(metaNodes)),
		Div(
			Class(catalogSectionClass()),
			H3(Class(catalogSectionTitleClass()), Text("Metadata tables")),
			Div(Class(catalogTableWrapClass()),
				core.DataTable("",
					THead(Tr(Th(Text("Entity")), Th(Text("Active")), Th(Text("Historical")), Th(Text("Total")), Th(Text("Latest snapshot")))),
					TBody(Group(rows)),
				),
			),
		),
		Div(
			Class(catalogSectionClass()),
			H3(Class(catalogSectionTitleClass()), Text("Recent history")),
			Div(Class("flex flex-wrap gap-2"), Group(filterNodes)),
			catalogHistoryEntriesTable(d.Panel.HistoryEntries),
		),
	)
}

func catalogHistoryEntriesSection(d catalogWorkspacePageData, showFilters bool) Node {
	filters := Node(nil)
	if showFilters {
		filterLinks := []struct {
			label string
			value string
		}{{"All", ""}, {"Schemas", "schema"}, {"Tables", "table"}, {"Columns", "column"}}
		filterNodes := make([]Node, 0, len(filterLinks))
		for i := range filterLinks {
			item := filterLinks[i]
			className := catalogHistoryFilterClass(d.Panel.HistoryEntity == item.value)
			filterNodes = append(filterNodes, A(Href(catalogHistoryURL(d, item.value)), Class(className), Text(item.label)))
		}
		filters = Div(Class("flex flex-wrap gap-2"), Group(filterNodes))
	}
	return Div(
		Class(catalogSectionClass()),
		H3(Class(catalogSectionTitleClass()), Text("Recent history")),
		filters,
		catalogHistoryEntriesTable(d.Panel.HistoryEntries),
	)
}

func catalogHistoryEntriesTable(entries []domain.CatalogHistoryEntry) Node {
	historyRows := make([]Node, 0, len(entries))
	for i := range entries {
		entry := entries[i]
		historyRows = append(historyRows,
			Tr(
				Td(Text(historyEntityLabel(entry.EntityType))),
				Td(Text(dashIfEmpty(entry.ObjectName))),
				Td(Text(snapshotIDLabel(entry.BeginSnapshotID))),
				Td(Text(snapshotIDLabel(entry.EndSnapshotID))),
				Td(Text(historyStatusLabel(entry))),
			),
		)
	}
	if len(historyRows) == 0 {
		return P(Class(catalogMutedCopyClass()), Text("No history entries match the current filter."))
	}
	return Div(Class(catalogTableWrapClass()), core.DataTable("", THead(Tr(Th(Text("Entity")), Th(Text("Object")), Th(Text("Begin snapshot")), Th(Text("End snapshot")), Th(Text("Status")))), TBody(Group(historyRows))))
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
				assetNode := Node(Text("-"))
				if table.AssetURL != "" {
					assetNode = A(Href(table.AssetURL), Text(core.FallbackString(table.AssetKey, "Open asset")))
				}
				childRows = append(childRows,
					Tr(
						data.Show(containsExprSignal(table.Name+" "+table.Owner+" "+table.Created+" "+table.Kind, "childq")),
						Td(A(Href(table.URL), Text(table.Name))),
						Td(Text(dashIfEmpty(table.Owner))),
						Td(Text(dashIfEmpty(table.Created))),
						Td(assetNode),
					),
				)
			}
			for j := range schema.Views {
				view := schema.Views[j]
				assetNode := Node(Text("-"))
				if view.AssetURL != "" {
					assetNode = A(Href(view.AssetURL), Text(core.FallbackString(view.AssetKey, "Open asset")))
				}
				childRows = append(childRows,
					Tr(
						data.Show(containsExprSignal(view.Name+" "+view.Owner+" "+view.Created+" "+view.Kind, "childq")),
						Td(A(Href(view.URL), Text(view.Name))),
						Td(Text(dashIfEmpty(view.Owner))),
						Td(Text(dashIfEmpty(view.Created))),
						Td(assetNode),
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
		descriptionNode = P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(d.Panel.Description))
	}

	headers := []Node{Th(Text("Name")), Th(Text("Owner")), Th(Text("Created at"))}
	switch d.Panel.Mode {
	case "table", "view":
		headers = []Node{Th(Text("Name")), Th(Text("Type")), Th(Text("Nullable"))}
	case "schema":
		headers = []Node{Th(Text("Name")), Th(Text("Owner")), Th(Text("Created at")), Th(Text("Asset"))}
	}

	childTable := Node(P(Class(catalogMutedCopyClass()), Text("No child elements.")))
	if len(childRows) > 0 {
		childTable = Div(Class(catalogTableWrapClass()), core.DataTable("border-[var(--borderColor-muted)]", THead(Tr(Group(headers))), TBody(Group(childRows))))
	}

	return Div(
		Class(catalogSectionClass()),
		descriptionNode,
		If(d.Panel.AssetURL != "",
			Div(Class(catalogSectionClass()),
				H3(Class(catalogSectionTitleClass()), Text("Linked asset")),
				P(Class(catalogMutedCopyClass()), Text("This object is already represented in the orchestration graph and asset workspace.")),
				core.SecondaryLink(d.Panel.AssetURL, "", Text(core.FallbackString(d.Panel.AssetKey, "Open asset"))),
			),
		),
		Div(Class(catalogOverviewToolbarClass()),
			Div(Class("flex min-w-0 max-w-[calc(var(--overlay-width-small)-var(--space-2))] flex-1 items-center gap-2"),
				I(Class(core.NavIconClass()), Attr("data-lucide", "search"), Attr("aria-hidden", "true")),
				Label(Class("sr-only"), Text("Filter child elements")),
				core.InputControl("w-full", Type("search"), Placeholder(filterPlaceholder), data.Bind("childq"), AutoComplete("off")),
			),
			P(Class("m-0 whitespace-nowrap text-xs text-[var(--fgColor-muted)]"), Text(strconv.Itoa(len(childRows))+" "+pluralize(len(childRows), countLabelSingular, countLabelPlural))),
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
		Li(Class(catalogBreadcrumbItemClass()), A(Href(catalogExplorerURL(d.ActiveCatalogName, "", "catalog", "")), Title(d.ActiveCatalogName), Span(Class(catalogBreadcrumbLabelClass(false)), Text(d.ActiveCatalogName)))),
	}

	if d.SelectedSchemaName != "" {
		items = append(items,
			Li(Class(catalogBreadcrumbItemClass()), Span(Class("catalog-breadcrumb-separator text-[var(--fgColor-muted)]"), Attr("aria-hidden", "true"), Text("/")), A(Href(catalogExplorerURL(d.ActiveCatalogName, d.SelectedSchemaName, "schema", "")), Title(d.SelectedSchemaName), Span(Class(catalogBreadcrumbLabelClass(false)), Text(d.SelectedSchemaName)))),
		)
	}

	if d.SelectedName != "" {
		items = append(items,
			Li(Class(catalogBreadcrumbItemClass()), Span(Class("catalog-breadcrumb-separator text-[var(--fgColor-muted)]"), Attr("aria-hidden", "true"), Text("/")), Span(Class(catalogBreadcrumbLabelClass(true)), Title(d.SelectedName), Text(d.SelectedName))),
		)
	}

	return Nav(
		Class(catalogBreadcrumbClass()),
		Attr("aria-label", "Catalog path"),
		Ol(Class(catalogBreadcrumbListClass()), Group(items)),
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

func snapshotIDLabel(value *int64) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatInt(*value, 10)
}

func boolPtrLabel(value *bool) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatBool(*value)
}

func catalogHistoryURL(d catalogWorkspacePageData, entityType string) string {
	parsed, err := url.Parse(catalogWorkspaceTabURL(d, "history"))
	if err != nil {
		return catalogWorkspaceTabURL(d, "history")
	}
	q := parsed.Query()
	if entityType == "" {
		q.Del("history_entity")
	} else {
		q.Set("history_entity", entityType)
	}
	parsed.RawQuery = q.Encode()
	return parsed.String()
}

func historyStatusLabel(entry domain.CatalogHistoryEntry) string {
	if entry.IsActive {
		return "Active"
	}
	if entry.HasHistory {
		return "Historical"
	}
	return "Inactive"
}

func historyEntityLabel(entityType string) string {
	if entityType == "" {
		return "-"
	}
	return strings.ToUpper(entityType[:1]) + entityType[1:]
}
