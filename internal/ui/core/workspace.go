package core

import (
	"strconv"
	"strings"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type WorkspaceAsideTab struct {
	ID         string
	Label      string
	Icon       string
	Count      string
	PanelClass string
	Content    Node
}

type CatalogExplorerObjectItem struct {
	Name   string
	URL    string
	Icon   string
	Active bool
}

type CatalogExplorerSchemaItem struct {
	Name      string
	URL       string
	Active    bool
	Open      bool
	Objects   []CatalogExplorerObjectItem
	EmptyText string
}

type CatalogExplorerCatalogItem struct {
	Name      string
	URL       string
	Active    bool
	Open      bool
	Schemas   []CatalogExplorerSchemaItem
	EmptyText string
}

type CatalogExplorerPanelData struct {
	Title             string
	FilterPlaceholder string
	Catalogs          []CatalogExplorerCatalogItem
	NewCatalogURL     string
	EmptyCatalogsText string
}

type ExploreNavigatorItem struct {
	Name     string
	URL      string
	Icon     string
	Active   bool
	Open     bool
	Children []ExploreNavigatorItem
}

type ExploreNavigatorPanelData struct {
	Title             string
	FilterPlaceholder string
	Items             []ExploreNavigatorItem
	EmptyText         string
}

func WorkspaceLayout(className string, aside Node, main ...Node) Node {
	isRightAside := strings.Contains(className, "workspace-layout-right-aside")
	baseClasses := "workspace-layout relative grid min-h-0 gap-4 md:[grid-template-columns:18rem_minmax(0,1fr)] [.is-aside-collapsed&]:md:[grid-template-columns:3.5rem_minmax(0,1fr)]"
	mainClass := "workspace-main min-w-0"
	asideNode := aside
	if isRightAside {
		baseClasses = "workspace-layout relative grid min-h-0 gap-4 md:[grid-template-columns:minmax(0,1fr)_18rem] [.is-aside-collapsed&]:md:[grid-template-columns:minmax(0,1fr)_3.5rem]"
		mainClass = "workspace-main min-w-0 md:order-1 max-md:order-2"
		asideNode = Group([]Node{
			Div(Class("workspace-layout-divider hidden self-stretch md:block md:border-l md:border-[var(--borderColor-muted)]")),
			aside,
		})
	} else {
		mainClass += " border-l border-[var(--borderColor-muted)] pl-4 max-md:border-l-0 max-md:pl-0"
	}
	classes := ClassNames(baseClasses, className)
	mainSection := Section(Class(mainClass), Group(main))
	if isRightAside {
		return Div(
			Class(classes),
			Attr("data-workspace-layout", "true"),
			mainSection,
			Div(Class("workspace-aside-rail flex min-h-0 md:order-2 max-md:order-1"), asideNode),
		)
	}
	return Div(
		Class(classes),
		Attr("data-workspace-layout", "true"),
		asideNode,
		mainSection,
	)
}

func WorkspaceAside(storageKey, className string, tabs []WorkspaceAsideTab, defaultTab string) Node {
	isPlain := strings.Contains(className, "workspace-aside-plain")
	isRightRail := strings.Contains(className, "workspace-aside-right")
	baseClasses := "workspace-aside min-w-0 self-stretch pr-2 [.is-aside-collapsed_&]:pr-1 max-md:mb-1 max-md:pb-2 max-md:pr-0"
	if isRightRail {
		baseClasses = "workspace-aside min-w-0 self-stretch pl-4 [.is-aside-collapsed_&]:pl-2 max-md:mb-1 max-md:border-b max-md:border-[var(--borderColor-muted)] max-md:pb-2 max-md:pl-0"
	}
	headClasses := "workspace-aside-head flex items-start justify-between gap-2 px-3 py-3 [.is-aside-collapsed_&]:flex-col [.is-aside-collapsed_&]:items-stretch [.is-aside-collapsed_&]:gap-1 max-md:items-start max-md:[.is-aside-collapsed_&]:flex-row max-md:[.is-aside-collapsed_&]:items-center"
	panelWrapClasses := "workspace-aside-panels flex min-h-0 flex-1 flex-col gap-4 overflow-auto py-3"
	if !isPlain {
		baseClasses += " rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] shadow-xs max-md:border-b max-md:border-[var(--borderColor-muted)]"
		headClasses += " border-b border-[var(--borderColor-default)]"
		panelWrapClasses += " p-3"
	} else {
		panelWrapClasses += " px-3"
	}
	classes := ClassNames(baseClasses, className)
	if len(tabs) == 0 {
		return Aside(Class(classes))
	}

	activeTab := strings.TrimSpace(defaultTab)
	if activeTab == "" {
		activeTab = tabs[0].ID
	}
	if !workspaceTabExists(tabs, activeTab) {
		activeTab = tabs[0].ID
	}

	tabButtons := make([]Node, 0, len(tabs))
	tabPanels := make([]Node, 0, len(tabs))
	for i := range tabs {
		tab := tabs[i]
		tabID := "workspace-tab-" + tab.ID
		panelID := "workspace-panel-" + tab.ID

		tabClass := "workspace-aside-tab inline-flex min-h-10 items-center gap-2 rounded-lg border border-transparent px-3 py-2 text-left text-sm font-medium text-[var(--fgColor-muted)] transition-colors hover:bg-[var(--bgColor-default)] hover:text-[var(--fgColor-default)] [.is-aside-collapsed_&]:justify-center [.is-aside-collapsed_&]:border-transparent [.is-aside-collapsed_&]:bg-transparent [.is-aside-collapsed_&]:px-0 [.is-aside-collapsed_&]:text-[var(--fgColor-muted)] [.is-aside-collapsed_&]:hover:border-[var(--borderColor-muted)] [.is-aside-collapsed_&]:hover:bg-[var(--bgColor-muted)] [.is-aside-collapsed_&]:hover:text-[var(--fgColor-default)] max-md:[.is-aside-collapsed_&]:justify-start max-md:[.is-aside-collapsed_&]:px-3"
		panelClass := "workspace-aside-panel hidden min-h-0 flex-col gap-4 [.is-active&]:flex [.is-aside-collapsed_.workspace-aside-panels_&]:hidden"
		selected := "false"
		if tab.ID == activeTab {
			tabClass += " is-active bg-[var(--bgColor-default)] text-[var(--fgColor-accent)] shadow-[inset_2px_0_0_var(--borderColor-accent-emphasis)] [.is-aside-collapsed_&]:border-transparent [.is-aside-collapsed_&]:bg-transparent [.is-aside-collapsed_&]:text-[var(--fgColor-muted)] [.is-aside-collapsed_&]:shadow-none"
			panelClass += " is-active"
			selected = "true"
		}
		if strings.TrimSpace(tab.PanelClass) != "" {
			panelClass += " " + strings.TrimSpace(tab.PanelClass)
		}

		countNode := Node(nil)
		if strings.TrimSpace(tab.Count) != "" {
			countNode = Span(Class("workspace-aside-tab-count ml-auto rounded-full bg-[var(--bgColor-muted)] px-2 py-0.5 text-[11px] font-semibold text-[var(--fgColor-muted)] [.is-aside-collapsed_&]:hidden max-md:[.is-aside-collapsed_&]:inline"), Text(tab.Count))
		}

		tabButtons = append(tabButtons, Button(
			Type("button"),
			Class(tabClass),
			ID(tabID),
			Title(tab.Label),
			Attr("aria-label", tab.Label),
			Attr("role", "tab"),
			Attr("aria-selected", selected),
			Attr("aria-controls", panelID),
			Attr("data-workspace-aside-tab", tab.ID),
			Icon(tab.Icon, Class("workspace-aside-tab-icon h-4 w-4 shrink-0")),
			Span(Class("workspace-aside-tab-label truncate [.is-aside-collapsed_&]:hidden max-md:[.is-aside-collapsed_&]:inline"), Text(tab.Label)),
			countNode,
		))

		tabPanels = append(tabPanels, Section(
			Class(panelClass),
			ID(panelID),
			Attr("role", "tabpanel"),
			Attr("aria-labelledby", tabID),
			Attr("data-workspace-aside-panel", tab.ID),
			tab.Content,
		))
	}

	storageAttr := Node(nil)
	if strings.TrimSpace(storageKey) != "" {
		storageAttr = Attr("data-workspace-aside-storage", strings.TrimSpace(storageKey))
	}

	collapseButton := Button(
		Type("button"),
		Class(ClassNames("workspace-aside-toggle shrink-0 [.is-aside-collapsed_&]:hidden", iconButtonClass("small"))),
		Attr("data-workspace-aside-toggle", "true"),
		Attr("aria-label", "Collapse panel"),
		Attr("aria-expanded", "true"),
		Title("Collapse panel"),
		Icon("panel-left-close", Class(IconGlyphClass())),
		Span(Class("sr-only"), Text("Collapse panel")),
	)

	return Aside(
		Class(classes),
		Attr("data-workspace-aside", "true"),
		Attr("data-workspace-aside-default", activeTab),
		storageAttr,
		Div(
			Class("workspace-aside-shell sticky top-0 flex min-h-0 flex-col gap-2 max-md:static"),
			Div(
				Class(headClasses),
				Div(Class("workspace-aside-tabs flex min-w-0 flex-1 flex-col gap-1 [.is-aside-collapsed_&]:items-stretch max-md:overflow-x-auto max-md:[.is-aside-collapsed_&]:flex-row max-md:[.is-aside-collapsed_&]:items-center"), Attr("role", "tablist"), Group(tabButtons)),
				collapseButton,
			),
			Div(Class(panelWrapClasses), Group(tabPanels)),
		),
	)
}

func CatalogExplorerPanel(d CatalogExplorerPanelData) Node {
	catalogNodes := make([]Node, 0, len(d.Catalogs))
	for i := range d.Catalogs {
		catalog := d.Catalogs[i]
		catalogClass := "flex items-center gap-2 rounded-lg px-2.5 py-2 text-sm font-medium text-[var(--fgColor-muted)] no-underline transition-colors hover:bg-[var(--bgColor-default)] hover:text-[var(--fgColor-default)]"
		if catalog.Active {
			catalogClass += " bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]"
		}

		schemaNodes := make([]Node, 0, len(catalog.Schemas))
		for j := range catalog.Schemas {
			schema := catalog.Schemas[j]
			schemaClass := "flex items-center gap-2 rounded-lg px-2.5 py-2 text-sm font-medium text-[var(--fgColor-muted)] no-underline transition-colors hover:bg-[var(--bgColor-default)] hover:text-[var(--fgColor-default)]"
			if schema.Active {
				schemaClass += " bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]"
			}

			openAttr := Node(nil)
			if schema.Open {
				openAttr = Attr("open", "")
			}

			objectNodes := make([]Node, 0, len(schema.Objects))
			for k := range schema.Objects {
				obj := schema.Objects[k]
				leafClass := "flex items-center gap-2 rounded-lg px-2.5 py-2 text-sm text-[var(--fgColor-muted)] no-underline transition-colors hover:bg-[var(--bgColor-default)] hover:text-[var(--fgColor-default)]"
				if obj.Active {
					leafClass += " bg-[var(--bgColor-accent-muted)] font-medium text-[var(--fgColor-accent)]"
				}
				icon := strings.TrimSpace(obj.Icon)
				if icon == "" {
					icon = "table"
				}
				objectNodes = append(objectNodes, Li(A(Href(obj.URL), Class(leafClass), Icon(icon, Class(NavIconClass())), Span(Text(obj.Name)))))
			}

			objectSection := Node(P(Class("m-0 px-2.5 py-2 text-xs text-[var(--fgColor-muted)]"), Text(fallbackString(schema.EmptyText, "No objects in this schema."))))
			if len(objectNodes) > 0 {
				objectSection = Ul(Class("mt-1 grid gap-1 pl-3"), Group(objectNodes))
			}

			schemaFilter := schema.Name + " " + catalogExplorerNames(schema.Objects)
			schemaNodes = append(schemaNodes, Li(
				Class("min-w-0"),
				data.Show(containsExpr(schemaFilter)),
				Details(
					Class(disclosureDetailsClass()),
					openAttr,
					Summary(
						Class(detailsSummaryClass()),
						Div(Class("flex min-w-0 items-center gap-2"),
							Icon("chevron-right", Attr("data-disclosure-icon", "true"), Class(NavIconClass("transition-transform duration-150 ease-out"))),
							A(Href(schema.URL), Class(schemaClass), Icon("folder", Class(NavIconClass())), Span(Text(schema.Name))),
						),
					),
					Div(Class("pt-1"), objectSection),
				),
			))
		}

		childrenNode := Node(P(Class("m-0 px-2.5 py-2 text-xs text-[var(--fgColor-muted)]"), Text(fallbackString(catalog.EmptyText, "No schemas in this catalog."))))
		if len(schemaNodes) > 0 {
			childrenNode = Ul(Class("mt-1 grid gap-2 pl-3"), Group(schemaNodes))
		}

		showValue := catalog.Name + " " + catalogExplorerNamesFromSchemas(catalog.Schemas)
		catalogItem := Node(Li(
			Class("min-w-0"),
			data.Show(containsExpr(showValue)),
			A(Href(catalog.URL), Class(catalogClass), Icon("database", Class(NavIconClass())), Span(Text(catalog.Name))),
		))
		if catalog.Open || len(schemaNodes) > 0 {
			openAttr := Node(nil)
			if catalog.Open {
				openAttr = Attr("open", "")
			}
			catalogItem = Li(
				Class("min-w-0"),
				data.Show(containsExpr(showValue)),
				Details(
					Class(disclosureDetailsClass()),
					openAttr,
					Summary(
						Class(detailsSummaryClass()),
						Div(Class("flex min-w-0 items-center gap-2"),
							Icon("chevron-right", Attr("data-disclosure-icon", "true"), Class(NavIconClass("transition-transform duration-150 ease-out"))),
							A(Href(catalog.URL), Class(catalogClass), Icon("database", Class(NavIconClass())), Span(Text(catalog.Name))),
						),
					),
					childrenNode,
				),
			)
		}
		catalogNodes = append(catalogNodes, catalogItem)
	}

	body := Node(P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(fallbackString(d.EmptyCatalogsText, "No catalogs found."))))
	if len(catalogNodes) > 0 {
		body = Ul(Class("grid gap-2"), Group(catalogNodes))
	}

	newCatalogButton := Node(nil)
	if strings.TrimSpace(d.NewCatalogURL) != "" {
		newCatalogButton = A(Href(d.NewCatalogURL), Class(iconButtonClass("small")), Title("New catalog"), Attr("aria-label", "New catalog"), Icon("plus", Class(IconGlyphClass())), Span(Class("sr-only"), Text("New catalog")))
	}

	filterNode := Node(nil)
	if strings.TrimSpace(d.FilterPlaceholder) != "" {
		filterNode = SearchInput(
			"Filter catalog explorer",
			d.FilterPlaceholder,
			"",
			data.Bind("q"),
			AutoComplete("off"),
		)
	}

	return Div(
		Class("flex min-h-0 flex-col gap-3"),
		Div(Class("flex flex-col gap-3 border-b border-[var(--borderColor-default)] pb-3"),
			Div(Class("flex items-center justify-between gap-2"),
				P(Class("m-0 text-[11px] font-semibold uppercase tracking-[0.08em] text-[var(--fgColor-muted)]"), Text(fallbackString(d.Title, "Catalog Explorer"))),
				newCatalogButton,
			),
			filterNode,
		),
		body,
	)
}

func ExploreNavigatorPanel(d ExploreNavigatorPanelData) Node {
	body := Node(P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(fallbackString(d.EmptyText, "No resources found."))))
	if len(d.Items) > 0 {
		body = Ul(Class("grid gap-2"), Group(renderExploreNavigatorItems(d.Items)))
	}

	filterNode := Node(nil)
	if strings.TrimSpace(d.FilterPlaceholder) != "" {
		filterNode = SearchInput(
			"Filter explorer",
			d.FilterPlaceholder,
			"",
			data.Bind("q"),
			AutoComplete("off"),
		)
	}

	return Div(
		Class("flex min-h-0 flex-col gap-3"),
		Div(Class("flex flex-col gap-3 border-b border-[var(--borderColor-default)] pb-3"),
			P(Class("m-0 text-[11px] font-semibold uppercase tracking-[0.08em] text-[var(--fgColor-muted)]"), Text(fallbackString(d.Title, "Explorer"))),
			filterNode,
		),
		body,
	)
}

func workspaceTabExists(tabs []WorkspaceAsideTab, tabID string) bool {
	for i := range tabs {
		if tabs[i].ID == tabID {
			return true
		}
	}
	return false
}

func catalogExplorerNames(items []CatalogExplorerObjectItem) string {
	names := make([]string, 0, len(items))
	for i := range items {
		names = append(names, items[i].Name)
	}
	return strings.Join(names, " ")
}

func catalogExplorerNamesFromSchemas(items []CatalogExplorerSchemaItem) string {
	names := make([]string, 0, len(items))
	for i := range items {
		names = append(names, items[i].Name)
	}
	return strings.Join(names, " ")
}

func fallbackString(value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func containsExpr(value string) string {
	lower := strings.ToLower(value)
	return "$q === '' || " + strconv.Quote(lower) + ".includes($q.toLowerCase())"
}

func renderExploreNavigatorItems(items []ExploreNavigatorItem) []Node {
	nodes := make([]Node, 0, len(items))
	for i := range items {
		item := items[i]
		nodes = append(nodes, renderExploreNavigatorItem(item))
	}
	return nodes
}

func renderExploreNavigatorItem(item ExploreNavigatorItem) Node {
	filterText := strings.TrimSpace(item.Name + " " + exploreNavigatorNames(item.Children))
	linkClass := "flex items-center gap-2 rounded-lg px-2.5 py-2 text-sm text-[var(--fgColor-muted)] no-underline transition-colors hover:bg-[var(--bgColor-default)] hover:text-[var(--fgColor-default)]"
	if item.Active {
		linkClass += " bg-[var(--bgColor-accent-muted)] font-medium text-[var(--fgColor-accent)]"
	}

	icon := strings.TrimSpace(item.Icon)
	if icon == "" {
		icon = "file-stack"
	}

	if len(item.Children) == 0 {
		return Li(
			Class("min-w-0"),
			data.Show(containsExpr(filterText)),
			A(Href(item.URL), Class(linkClass), Icon(icon, Class(NavIconClass())), Span(Text(item.Name))),
		)
	}

	openAttr := Node(nil)
	if item.Open {
		openAttr = Attr("open", "")
	}

	return Li(
		Class("min-w-0"),
		data.Show(containsExpr(filterText)),
		Details(
			Class(disclosureDetailsClass()),
			openAttr,
			Summary(
				Class(detailsSummaryClass()),
				Div(Class("flex min-w-0 items-center gap-2"),
					Icon("chevron-right", Attr("data-disclosure-icon", "true"), Class(NavIconClass("transition-transform duration-150 ease-out"))),
					A(Href(item.URL), Class(linkClass), Icon(icon, Class(NavIconClass())), Span(Text(item.Name))),
				),
			),
			Ul(Class("mt-1 grid gap-1 pl-3"), Group(renderExploreNavigatorItems(item.Children))),
		),
	)
}

func exploreNavigatorNames(items []ExploreNavigatorItem) string {
	names := make([]string, 0, len(items)*2)
	for i := range items {
		names = append(names, items[i].Name)
		childNames := strings.TrimSpace(exploreNavigatorNames(items[i].Children))
		if childNames != "" {
			names = append(names, childNames)
		}
	}
	return strings.Join(names, " ")
}

func detailsSummaryClass(extra ...string) string {
	return ClassNames("list-none [&::-webkit-details-marker]:hidden", strings.Join(extra, " "))
}

func disclosureDetailsClass(extra ...string) string {
	return ClassNames("[&[open]_summary_[data-disclosure-icon='true']]:rotate-90", strings.Join(extra, " "))
}
