package explore

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

const contentID = "explore-results"

type listRow struct {
	Name         string
	URL          string
	MetaURL      string
	MetaLabel    string
	Kind         string
	Owner        string
	Scope        string
	Folder       string
	Project      string
	Updated      string
	Shared       bool
	ProjectBound bool
}

type breadcrumbItem struct {
	Label   string
	URL     string
	Current bool
}

func listPage(principal domain.ContextPrincipal, rows []listRow, breadcrumbs []breadcrumbItem, selectedFolderID string, selectedKinds []string, selectedOwners []string, searchQuery string, ownerOptions []string, streamID, csrfToken string, page domain.PageRequest, total int64) Node {
	nodes := pageBody(rows, breadcrumbs, selectedFolderID, selectedKinds, selectedOwners, searchQuery, ownerOptions, streamID, csrfToken, page, total)
	nodes = append(nodes, Script(Src(core.UIScriptHref("explore.js"))))
	return core.AppPage("Explore", "explore", principal, nodes...)
}

func mainContent(rows []listRow, breadcrumbs []breadcrumbItem, selectedFolderID string, selectedKinds []string, selectedOwners []string, searchQuery string, ownerOptions []string, streamID, csrfToken string, page domain.PageRequest, total int64) Node {
	return core.MainContentSection("Explore", pageBody(rows, breadcrumbs, selectedFolderID, selectedKinds, selectedOwners, searchQuery, ownerOptions, streamID, csrfToken, page, total)...)
}

func pageBody(rows []listRow, breadcrumbs []breadcrumbItem, selectedFolderID string, selectedKinds []string, selectedOwners []string, searchQuery string, ownerOptions []string, streamID, csrfToken string, page domain.PageRequest, total int64) []Node {
	signalKinds := append([]string{}, selectedKinds...)
	signalOwners := append([]string{}, selectedOwners...)
	return []Node{
		core.PageHeader(
			"Discover",
			"Explore",
			"Browse folders and authored assets in the active project context.",
			core.SecondaryLink("/ui/explore/git-repos", "", core.Icon("git-branch", Class(core.IconGlyphClass())), Span(Text("Git repos"))),
			createMenu(selectedFolderID),
		),
		core.ListPageBody(
			Div(
				data.Signals(map[string]any{
					"streamID":  streamID,
					"csrfToken": strings.TrimSpace(csrfToken),
					"urlParams": map[string]any{
						"folder_id": strings.TrimSpace(selectedFolderID),
						"kind":      signalKinds,
						"owner":     signalOwners,
						"q":         strings.TrimSpace(searchQuery),
					},
					"filterOpen": false,
				}),
				data.Init("@get('/ui/explore/updates/' + $streamID)"),
				content(rows, breadcrumbs, selectedFolderID, selectedKinds, selectedOwners, searchQuery, ownerOptions, page, total),
			),
		),
	}
}

func content(rows []listRow, breadcrumbs []breadcrumbItem, selectedFolderID string, selectedKinds []string, selectedOwners []string, searchQuery string, ownerOptions []string, page domain.PageRequest, total int64) Node {
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		row := rows[i]
		tableRows = append(tableRows, Tr(
			Td(
				Div(Class("flex items-start gap-3"),
					Span(Class(kindIconWrapClass(row.Kind)),
						core.Icon(kindIcon(row.Kind), Class(core.NavIconClass("h-4 w-4"))),
					),
					Div(Class("min-w-0 flex-1"),
						Div(Class("flex flex-wrap items-center gap-2"),
							core.TextLink(row.URL, Text(row.Name)),
							Span(Class("text-[11px] font-semibold uppercase tracking-[0.08em] text-[var(--fgColor-muted)]"), Text(kindLabel(row.Kind))),
						),
						locationText(row),
					),
				),
			),
			Td(contextCell(row)),
			Td(core.Badge(row.Owner, "")),
			Td(Span(Class("text-[var(--fgColor-muted)]"), Text(row.Updated))),
			Td(Class("text-right"), rowActions(row)),
		))
	}

	contentNodes := make([]Node, 0, 4)
	if len(breadcrumbs) > 0 {
		contentNodes = append(contentNodes, breadcrumbsNode(breadcrumbs, hasActiveFilters(selectedKinds, selectedOwners, searchQuery)))
	}
	contentNodes = append(contentNodes, filterBar(page, selectedFolderID, selectedKinds, selectedOwners, searchQuery, ownerOptions))
	if len(tableRows) > 0 {
		contentNodes = append(contentNodes, table([]string{"Name", "Context", "Owner", "Last updated", "Actions"}, tableRows))
		if total > 0 {
			contentNodes = append(contentNodes, pagination(selectedKinds, selectedOwners, searchQuery, selectedFolderID, page, total))
		}
	} else {
		contentNodes = append(contentNodes, emptyState(hasActiveFilters(selectedKinds, selectedOwners, searchQuery)))
	}
	return Div(ID(contentID), Class("grid gap-4"), Group(contentNodes))
}

func breadcrumbsNode(items []breadcrumbItem, hasFilters bool) Node {
	if len(items) == 0 {
		return nil
	}
	nodes := make([]Node, 0, len(items)*2+2)
	for i := range items {
		item := items[i]
		label := Span(Class("truncate"), Text(item.Label))
		if item.Current {
			nodes = append(nodes, Span(Class("inline-flex max-w-full items-center rounded-full bg-[var(--bgColor-muted)] px-3 py-1 text-sm font-medium text-[var(--fgColor-default)]"), label))
		} else {
			nodes = append(nodes, A(Href(item.URL), Class("inline-flex max-w-full items-center rounded-full border border-[var(--borderColor-default)] px-3 py-1 text-sm font-medium text-[var(--fgColor-muted)] no-underline hover:border-[var(--borderColor-accent-muted)] hover:text-[var(--fgColor-accent)]"), label))
		}
		if i < len(items)-1 {
			nodes = append(nodes, Span(Class("text-[var(--fgColor-muted)]"), Text("/")))
		}
	}
	if hasFilters {
		nodes = append(nodes,
			Span(Class("text-[var(--fgColor-muted)]"), Text("/")),
			Span(Class("inline-flex max-w-full items-center gap-2 rounded-full bg-[var(--bgColor-muted)] px-3 py-1 text-sm font-medium text-[var(--fgColor-default)]"),
				core.Icon("filter", Class("h-4 w-4 shrink-0 text-[var(--fgColor-muted)]")),
				Span(Class("truncate"), Text("Filtered results")),
			),
		)
	}
	return Nav(Class("flex flex-wrap items-center gap-2"), Attr("aria-label", "Folder breadcrumbs"), Group(nodes))
}

func hasActiveFilters(selectedKinds []string, selectedOwners []string, searchQuery string) bool {
	return len(selectedKinds) > 0 || len(selectedOwners) > 0 || strings.TrimSpace(searchQuery) != ""
}

func emptyState(hasFilters bool) Node {
	iconName := "folder-search"
	title := "No assets in this folder yet"
	message := "Open another folder or use the New menu to add notebooks, dashboards, pipelines, and other authored work."
	if hasFilters {
		iconName = "search"
		title = "Unable to find any search results"
		message = "Change your keyword or filters, and try again."
	}
	return Div(
		Class("grid min-h-[18rem] place-items-center rounded-2xl border border-dashed border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-6 py-12"),
		Div(Class("grid max-w-xl justify-items-center gap-4 text-center"),
			Div(Class("flex h-20 w-20 items-center justify-center rounded-3xl bg-[var(--bgColor-muted)] text-[var(--fgColor-muted)]"),
				core.Icon(iconName, Class("h-9 w-9 shrink-0")),
			),
			Div(Class("grid gap-2"),
				H2(Class("m-0 text-2xl font-semibold tracking-tight text-[var(--fgColor-default)]"), Text(title)),
				P(Class("m-0 text-sm leading-6 text-[var(--fgColor-muted)]"), Text(message)),
			),
		),
	)
}

func table(headers []string, rows []Node) Node {
	headerNodes := make([]Node, 0, len(headers))
	for i := range headers {
		headerNodes = append(headerNodes, Th(Scope("col"), Text(headers[i])))
	}
	return core.TableContainer("", core.DataTable("", THead(Tr(Group(headerNodes))), TBody(Group(rows))))
}

func createMenu(folderID string) Node {
	return Details(
		Class(core.DetailsClass()),
		Summary(
			Class(core.DetailsSummaryClass("inline-flex min-h-10 items-center justify-center gap-2 rounded-lg border border-[var(--button-primary-borderColor-rest)] bg-[var(--button-primary-bgColor-rest)] px-4 py-2 text-sm font-semibold text-[var(--button-primary-fgColor-rest)] shadow-xs transition-colors duration-100 ease-out hover:bg-[var(--button-primary-bgColor-hover)]")),
			Title("Create new"),
			Attr("aria-label", "Create new"),
			core.Icon("plus", Class(core.IconGlyphClass())),
			Span(Text("New")),
			core.Icon("chevron-down", Class("h-4 w-4")),
		),
		Div(Class(core.DropdownMenuClass("min-w-[13rem]")),
			createMenuLink(folderNewURL(folderID), "folder-plus", "New folder"),
			createMenuLink(notebookNewURL(folderID), "notebook-text", "New notebook"),
			createMenuLink(dashboardNewURL(folderID), "chart-column", "New dashboard"),
			createMenuLink(pipelineNewURL(folderID), "workflow", "New pipeline"),
		),
	)
}

func createMenuLink(href, icon, label string) Node {
	return A(Href(href), Class(core.DropdownItemClass("text-[var(--fgColor-default)]")), core.Icon(icon, Class("h-4 w-4")), Span(Text(label)))
}

func filterBar(page domain.PageRequest, folderID string, selectedKinds []string, selectedOwners []string, searchQuery string, ownerOptions []string) Node {
	groupOptions := []core.FilterMenuGroup{
		{Name: "kind", Label: "Type", Expanded: true, Options: typeFilterOptions(selectedKinds)},
		{Name: "owner", Label: "Owner", Expanded: len(selectedOwners) > 0, Options: ownerFilterOptions(ownerOptions, selectedOwners)},
	}
	hidden := map[string]string{}
	if strings.TrimSpace(folderID) != "" {
		hidden["folder_id"] = folderID
	}
	searchInput := core.InputWithIcon("search", "min-w-[16rem] flex-1 max-w-sm",
		Type("search"),
		Placeholder("Search assets"),
		Value(searchQuery),
		data.On("input", datastarSearchExpr(), data.ModifierDebounce, data.Duration(250*time.Millisecond)),
	)
	return Div(Class("flex w-full flex-wrap items-center justify-end gap-3"),
		searchInput,
		core.FilterMenu(core.FilterMenuConfig{
			Label:             "Filter",
			Action:            "/ui/explore",
			ClearURL:          pageURL(domain.PageRequest{MaxResults: page.Limit()}, nil, nil, "", folderID),
			SelectedCount:     len(selectedKinds) + len(selectedOwners),
			SelectedCountExpr: "$urlParams.kind.length + $urlParams.owner.length",
			HiddenFields:      hidden,
			RootAttrs:         []Node{ID("explore-filter-menu"), data.PreserveAttr("open"), data.On("toggle", "$filterOpen = evt.currentTarget.open")},
			FormAttrs:         []Node{data.On("submit", datastarFilterSubmitExpr(), data.ModifierPrevent)},
			ClearAttrs:        []Node{data.On("click", datastarClearExpr(), data.ModifierPrevent)},
			OptionInputAttrs:  []Node{data.On("change", datastarFilterChangeExpr())},
			HideApply:         true,
			Groups:            groupOptions,
		}),
	)
}

func datastarFilterSubmitExpr() string {
	return "const next = window.DuckUIURLParams.toURL('/ui/explore', $urlParams); @post('/ui/explore/updates/' + $streamID); history.replaceState({}, '', next)"
}
func datastarFilterChangeExpr() string {
	return "const menu = evt.target.closest('details'); if (menu) { menu.open = true; } $filterOpen = true; $urlParams = window.DuckUIURLParams.toggleArrayValue($urlParams, evt.target.name, evt.target.value, evt.target.checked); const next = window.DuckUIURLParams.toURL('/ui/explore', $urlParams); @post('/ui/explore/updates/' + $streamID); history.replaceState({}, '', next)"
}
func datastarClearExpr() string {
	return "const menu = evt.currentTarget.closest('details'); if (menu) { menu.open = true; } $filterOpen = true; $urlParams = window.DuckUIURLParams.clear($urlParams, ['kind', 'owner', 'q']); const next = window.DuckUIURLParams.toURL('/ui/explore', $urlParams); @post('/ui/explore/updates/' + $streamID); history.replaceState({}, '', next)"
}
func datastarSearchExpr() string {
	return "$urlParams = { ...$urlParams, q: evt.target.value?.trim?.() ?? '' }; const next = window.DuckUIURLParams.toURL('/ui/explore', $urlParams); @post('/ui/explore/updates/' + $streamID); history.replaceState({}, '', next)"
}

func typeFilterOptions(selectedKinds []string) []core.FilterMenuOption {
	items := []string{domain.ExploreKindFolder, domain.ExploreKindNotebook, domain.ExploreKindModel, domain.ExploreKindMacro, domain.ExploreKindDashboard, domain.ExploreKindPipeline, domain.ExploreKindSemanticModel}
	options := make([]core.FilterMenuOption, 0, len(items))
	for _, kind := range items {
		options = append(options, core.FilterMenuOption{Label: kindLabel(kind), Value: kind, Icon: kindIcon(kind), Selected: containsString(selectedKinds, kind)})
	}
	return options
}

func ownerFilterOptions(ownerOptions []string, selectedOwners []string) []core.FilterMenuOption {
	options := make([]core.FilterMenuOption, 0, len(ownerOptions))
	for _, owner := range ownerOptions {
		options = append(options, core.FilterMenuOption{Label: owner, Value: owner, Icon: "user", Selected: containsString(selectedOwners, owner)})
	}
	return options
}

func locationText(row listRow) Node {
	location := strings.TrimSpace(row.Folder)
	if location == "" || location == "-" {
		if row.Kind == "folder" {
			location = "Top level"
		}
	}
	if location == "" {
		return nil
	}
	return P(Class("m-0 mt-1 text-xs leading-5 text-[var(--fgColor-muted)]"), Text(location))
}

func contextCell(row listRow) Node {
	badges := make([]Node, 0, 4)
	if row.ProjectBound {
		badges = append(badges, core.Badge("Project-bound", "accent"))
	}
	if row.Shared {
		badges = append(badges, core.Badge("Shared", "success"))
	}
	if strings.TrimSpace(row.Project) != "" {
		badges = append(badges, core.Badge("Project "+strings.TrimSpace(row.Project), ""))
	}
	summary := strings.TrimSpace(row.Scope)
	switch {
	case summary == "", strings.EqualFold(summary, "folder"), strings.EqualFold(summary, strings.TrimSpace(row.Project)), summary == "Project "+strings.TrimSpace(row.Project):
		summary = ""
	}
	if len(badges) == 0 && summary == "" {
		return Span(Class("text-[var(--fgColor-muted)]"), Text("-"))
	}
	nodes := make([]Node, 0, 2)
	if len(badges) > 0 {
		nodes = append(nodes, Div(Class("flex flex-wrap gap-2"), Group(badges)))
	}
	if summary != "" {
		nodes = append(nodes, P(Class("m-0 text-xs leading-5 text-[var(--fgColor-muted)]"), Text(summary)))
	}
	return Div(Class("grid gap-1"), Group(nodes))
}

func rowActions(row listRow) Node {
	items := []Node{core.ActionMenuLink(row.URL, "Open")}
	if strings.TrimSpace(row.MetaURL) != "" && strings.TrimSpace(row.MetaLabel) != "" {
		items = append(items, core.ActionMenuLink(row.MetaURL, row.MetaLabel))
	}
	return core.ActionMenu("Actions", items...)
}

func kindLabel(kind string) string {
	switch kind {
	case "folder":
		return "Folder"
	case domain.ExploreKindNotebook:
		return "Notebook"
	case domain.ExploreKindModel:
		return "Model"
	case domain.ExploreKindMacro:
		return "Macro"
	case domain.ExploreKindDashboard:
		return "Dashboard"
	case domain.ExploreKindPipeline:
		return "Pipeline"
	case domain.ExploreKindSemanticModel:
		return "Semantic Model"
	default:
		return "Asset"
	}
}

func kindIcon(kind string) string {
	switch kind {
	case "folder":
		return "folder"
	case domain.ExploreKindNotebook:
		return "notebook-text"
	case domain.ExploreKindModel:
		return "boxes"
	case domain.ExploreKindMacro:
		return "braces"
	case domain.ExploreKindDashboard:
		return "chart-column"
	case domain.ExploreKindPipeline:
		return "workflow"
	case domain.ExploreKindSemanticModel:
		return "waypoints"
	default:
		return "file-stack"
	}
}

func kindIconWrapClass(kind string) string {
	base := "inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-md"
	switch kind {
	case "folder":
		return base + " bg-[var(--display-gray-scale-0)] text-[var(--display-gray-scale-7)]"
	case domain.ExploreKindNotebook:
		return base + " bg-[var(--display-blue-scale-0)] text-[var(--display-blue-scale-6)]"
	case domain.ExploreKindModel:
		return base + " bg-[var(--display-green-scale-0)] text-[var(--display-green-scale-6)]"
	case domain.ExploreKindMacro:
		return base + " bg-[var(--display-orange-scale-0)] text-[var(--display-orange-scale-6)]"
	case domain.ExploreKindDashboard:
		return base + " bg-[var(--display-plum-scale-0)] text-[var(--display-plum-scale-6)]"
	case domain.ExploreKindPipeline:
		return base + " bg-[var(--display-teal-scale-0)] text-[var(--display-teal-scale-6)]"
	case domain.ExploreKindSemanticModel:
		return base + " bg-[var(--display-indigo-scale-0)] text-[var(--display-indigo-scale-6)]"
	default:
		return base + " bg-[var(--bgColor-muted)] text-[var(--fgColor-muted)]"
	}
}

func pagination(kinds []string, owners []string, searchQuery string, folderID string, page domain.PageRequest, total int64) Node {
	offset := page.Offset()
	limit := page.Limit()
	shownStart := min(total, int64(offset)+1)
	shownEnd := min(total, int64(offset+limit))
	if total == 0 {
		shownStart = 0
	}
	summary := P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(fmt.Sprintf("Showing %d-%d of %d assets", shownStart, shownEnd, total)))
	prevOffset := offset - limit
	if prevOffset < 0 {
		prevOffset = 0
	}
	prevToken := domain.EncodePageToken(prevOffset)
	nextToken := domain.NextPageToken(offset, limit, total)
	prevNode := Node(Span(Class("inline-flex min-h-10 items-center justify-center rounded-l-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 text-sm font-medium text-[var(--fgColor-default)] opacity-60 pointer-events-none"), Attr("aria-disabled", "true"), Text("Previous")))
	if offset > 0 {
		prevNode = A(Href(pageURL(domain.PageRequest{MaxResults: limit, PageToken: prevToken}, kinds, owners, searchQuery, folderID)), Class("inline-flex min-h-10 items-center justify-center rounded-l-lg border border-[var(--button-default-borderColor-rest)] border-r-0 bg-[var(--button-default-bgColor-rest)] px-3 text-sm font-medium text-[var(--button-default-fgColor-rest)] no-underline transition-colors duration-100 ease-out hover:border-[var(--button-default-borderColor-hover)] hover:bg-[var(--button-default-bgColor-hover)] hover:text-[var(--button-default-fgColor-rest)]"), Text("Previous"))
	}
	nextNode := Node(Span(Class("inline-flex min-h-10 items-center justify-center rounded-r-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 text-sm font-medium text-[var(--fgColor-default)] opacity-60 pointer-events-none"), Attr("aria-disabled", "true"), Text("Next")))
	if nextToken != "" {
		nextNode = A(Href(pageURL(domain.PageRequest{MaxResults: limit, PageToken: nextToken}, kinds, owners, searchQuery, folderID)), Class("inline-flex min-h-10 items-center justify-center rounded-r-lg border border-[var(--button-default-borderColor-rest)] bg-[var(--button-default-bgColor-rest)] px-3 text-sm font-medium text-[var(--button-default-fgColor-rest)] no-underline transition-colors duration-100 ease-out hover:border-[var(--button-default-borderColor-hover)] hover:bg-[var(--button-default-bgColor-hover)] hover:text-[var(--button-default-fgColor-rest)]"), Text("Next"))
	}
	return Div(Class("flex items-center justify-between gap-4 border-t border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-6 py-4 max-sm:flex-col max-sm:items-start max-sm:px-4"),
		summary,
		Nav(Attr("aria-label", "Pagination"), Div(Class("inline-flex items-center"), prevNode, nextNode)),
	)
}

func folderNewURL(parentFolderID string) string {
	if parentFolderID == "" {
		return "/ui/explore/folders/new"
	}
	q := url.Values{}
	q.Set("parent_folder_id", parentFolderID)
	return "/ui/explore/folders/new?" + q.Encode()
}
func notebookNewURL(folderID string) string {
	if folderID == "" {
		return "/ui/notebooks/new"
	}
	q := url.Values{}
	q.Set("folder_id", folderID)
	return "/ui/notebooks/new?" + q.Encode()
}
func dashboardNewURL(folderID string) string {
	if folderID == "" {
		return "/ui/dashboards/new"
	}
	q := url.Values{}
	q.Set("folder_id", folderID)
	return "/ui/dashboards/new?" + q.Encode()
}
func pipelineNewURL(folderID string) string {
	if folderID == "" {
		return "/ui/pipelines/new"
	}
	q := url.Values{}
	q.Set("folder_id", folderID)
	return "/ui/pipelines/new?" + q.Encode()
}
