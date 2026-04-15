package projects

import (
	"fmt"
	"strconv"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type projectListRowData struct {
	ID             string
	Name           string
	Kind           string
	WorkspaceName  string
	DefaultBranch  string
	OwnerSummary   string
	ProductSummary string
	CreatedAt      string
	URL            string
}

type projectAssetRowData struct {
	Name   string
	URL    string
	Meta1  string
	Meta2  string
	Detail string
}

type projectEnvironmentRowData struct {
	ID             string
	Name           string
	URL            string
	Description    string
	Kind           string
	TargetLocation string
	Compute        string
	UpdatedAt      string
}

type projectBuildRowData struct {
	ID             string
	URL            string
	State          string
	Environment    string
	EnvironmentURL string
	GitRef         string
	Target         string
	CreatedAt      string
}

type projectHubPageData struct {
	Principal         domain.ContextPrincipal
	Project           domain.Project
	WorkspaceName     string
	OwnerSummary      string
	ProductSummary    string
	ActiveTab         string
	ModelsURL         string
	MacrosURL         string
	NewModelURL       string
	NewMacroURL       string
	NewEnvironmentURL string
	ModelCount        int64
	MacroCount        int64
	EnvironmentCount  int64
	BuildCount        int64
	Models            []projectAssetRowData
	Macros            []projectAssetRowData
	Environments      []projectEnvironmentRowData
	Builds            []projectBuildRowData
}

func projectsListPage(principal domain.ContextPrincipal, rows []projectListRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No projects available yet.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				core.TablePrimaryCell(
					core.IconChip("folder-git-2", "bg-[var(--display-indigo-scale-0)] text-[var(--display-indigo-scale-6)]"),
					A(Href(row.URL), Class("font-semibold text-[var(--fgColor-accent)] no-underline visited:text-[var(--fgColor-accent)] hover:text-[var(--fgColor-accent)] hover:underline active:text-[var(--fgColor-accent)]"), Text(row.Name)),
				),
				Td(core.TableMetaText(row.Kind)),
				Td(core.TableMetaText(row.WorkspaceName)),
				Td(core.TableMetaText(row.DefaultBranch)),
				Td(core.TableMetaText(row.OwnerSummary)),
				Td(core.TableMetaText(row.ProductSummary)),
				Td(core.TableMetaText(row.CreatedAt)),
			))
		}
		table = core.TableContainer("",
			core.DataTable("",
				THead(Tr(
					Th(Text("Project")),
					Th(Text("Kind")),
					Th(Text("Workspace")),
					Th(Text("Branch")),
					Th(Text("Owner")),
					Th(Text("Product")),
					Th(Text("Created")),
				)),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage("Projects", "projects", principal,
		core.ListPageLayout(
			core.SectionSurface(
				Div(Class("flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between"),
					Div(Class("grid gap-3"),
						core.Kicker("Build"),
						H1(Class("m-0 text-3xl font-semibold tracking-tight"), Text("Projects")),
						P(Class("m-0 max-w-3xl text-sm leading-6 text-[var(--fgColor-muted)]"), Text("Projects are the authoring package boundary for models, macros, environments, and builds.")),
					),
				),
			),
			core.MetricsGrid(
				core.MetricCard("Visible projects", strconv.FormatInt(total, 10), "Authoring workspaces you can open"),
			),
			core.ListPageBody(
				table,
				core.ListPagination("/ui/projects", page, total),
			),
		),
	)
}

func projectHubPage(d projectHubPageData) Node {
	content := projectModelsContent(d)
	switch d.ActiveTab {
	case projectTabMacros:
		content = projectMacrosContent(d)
	case projectTabEnvironments:
		content = projectEnvironmentsContent(d)
	case projectTabBuilds:
		content = projectBuildsContent(d)
	}

	return core.AppPage("Project: "+d.Project.Name, "projects", d.Principal,
		Div(Class("grid gap-6"),
			projectHero(d),
			Div(Class("grid gap-5"),
				projectTabs(d),
				content,
			),
		),
	)
}

func projectModelsContent(d projectHubPageData) Node {
	return Div(Class("grid gap-8"),
		projectAssetSection("model", "Models", "Project-scoped transformation models.", d.Models, d.ModelsURL, d.NewModelURL, d.ModelCount, "No models in this project yet.", "New model"),
	)
}

func projectMacrosContent(d projectHubPageData) Node {
	return Div(Class("grid gap-8"),
		projectAssetSection("macro", "Macros", "All macros scoped to this project.", d.Macros, d.MacrosURL, d.NewMacroURL, d.MacroCount, "No macros in this project yet.", "New macro"),
	)
}

func projectEnvironmentsContent(d projectHubPageData) Node {
	return projectEnvironmentSection(d.Environments, d.EnvironmentCount, true, d.NewEnvironmentURL)
}

func projectBuildsContent(d projectHubPageData) Node {
	return projectBuildSection(d.Builds, d.BuildCount, true)
}

func projectAssetSection(kind, title, description string, items []projectAssetRowData, listURL, createURL string, total int64, emptyMessage, emptyActionLabel string) Node {
	rows := items
	if len(rows) > 5 {
		rows = rows[:5]
	}

	if len(items) == 0 {
		return projectPageSection(title, description,
			core.PrimaryLink(createURL, "", Text(emptyActionLabel)),
			core.EmptyState(core.ResourceKindIcon(kind), title+" empty", emptyMessage, core.SecondaryLink(createURL, "", Text(emptyActionLabel))),
		)
	}

	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		item := rows[i]
		tableRows = append(tableRows, Tr(
			core.TablePrimaryCell(core.ResourceIcon(kind), core.TextLink(item.URL, Text(item.Name))),
			Td(core.TableMetaText(valueOrDash(item.Meta1))),
			Td(core.TableMetaText(valueOrDash(item.Meta2))),
			Td(core.TableMetaText(valueOrDash(item.Detail))),
		))
	}

	actions := []Node{core.SecondaryLink(listURL, "", Text("Open page"))}
	if createURL != "" {
		actions = append(actions, core.PrimaryLink(createURL, "", Text(emptyActionLabel)))
	}
	return projectPageSection(title, description,
		Group(actions),
		core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Name")), Th(Text("Primary")), Th(Text("Secondary")), Th(Text("Detail")))),
				TBody(Group(tableRows)),
			),
		),
	)
}

func projectEnvironmentSection(items []projectEnvironmentRowData, total int64, full bool, createURL string) Node {
	rows := items
	if !full && len(rows) > 5 {
		rows = rows[:5]
	}
	if len(items) == 0 {
		return projectPageSection("Environments", "Execution contexts for development and release.",
			core.PrimaryLink(createURL, "", Text("New environment")),
			core.EmptyState("server", "No environments", "Create an environment through the project control plane to bind catalogs, schemas, and compute targets.", core.SecondaryLink(createURL, "", Text("New environment"))),
		)
	}
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		item := rows[i]
		tableRows = append(tableRows, Tr(
			core.TablePrimaryCell(core.ResourceIcon("environment"), core.TextLink(item.URL, Text(item.Name))),
			Td(core.TableMetaText(item.Kind)),
			Td(core.TableMetaText(item.TargetLocation)),
			Td(core.TableMetaText(item.Compute)),
			Td(core.TableMetaText(item.UpdatedAt)),
		))
	}
	title := "Environment previews"
	desc := fmt.Sprintf("%d configured environment(s) for this project.", total)
	if full {
		title = "Environments"
	}
	return projectPageSection(title, desc, core.PrimaryLink(createURL, "", Text("New environment")),
		core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Name")), Th(Text("Kind")), Th(Text("Target")), Th(Text("Compute")), Th(Text("Updated")))),
				TBody(Group(tableRows)),
			),
		),
	)
}

func projectBuildSection(items []projectBuildRowData, total int64, full bool) Node {
	rows := items
	if !full && len(rows) > 5 {
		rows = rows[:5]
	}
	if len(items) == 0 {
		return projectPageSection("Builds", "Immutable project snapshots used for validation and delivery.", nil,
			core.EmptyState("package-open", "No builds", "Builds will appear here once the project has compilation snapshots to review.", nil),
		)
	}
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		item := rows[i]
		tableRows = append(tableRows, Tr(
			core.TablePrimaryCell(core.ResourceIcon("build"), core.TextLink(item.URL, Text(item.ID))),
			Td(core.TableMetaText(item.State)),
			Td(core.TableMetaText(item.Environment)),
			Td(core.TableMetaText(item.GitRef)),
			Td(core.TableMetaText(item.Target)),
			Td(core.TableMetaText(item.CreatedAt)),
		))
	}
	title := "Recent builds"
	desc := fmt.Sprintf("%d build snapshot(s) for this project.", total)
	if full {
		title = "Builds"
	}
	return projectPageSection(title, desc, nil,
		core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Build ID")), Th(Text("State")), Th(Text("Environment")), Th(Text("Git ref")), Th(Text("Target")), Th(Text("Created")))),
				TBody(Group(tableRows)),
			),
		),
	)
}

func projectHero(d projectHubPageData) Node {
	descriptionNode := Node(nil)
	if d.Project.Description != "" {
		descriptionNode = P(
			Class("m-0 max-w-2xl text-sm leading-relaxed text-[var(--fgColor-muted)]"),
			Text(d.Project.Description),
		)
	}

	return Div(
		Class("grid w-full max-w-4xl gap-6 py-2"),
		Div(
			Class("grid gap-1.5"),
			core.DetailTitleRow(
				core.DetailTitle(d.Project.Name),
				core.Badge(projectKindLabel(d.Project.Kind), "accent"),
			),
			descriptionNode,
		),
		Dl(
			Class("grid grid-cols-1 gap-x-8 gap-y-0 border-t border-[var(--borderColor-muted)] pt-6 sm:grid-cols-2 sm:gap-y-5 lg:grid-cols-3 xl:grid-cols-5"),
			core.ViewField("Workspace", core.ViewFieldText(valueOrDash(d.WorkspaceName))),
			core.ViewField("Branch", core.ViewFieldCode(valueOrDash(d.Project.DefaultBranch))),
			core.ViewField("Owner", core.ViewFieldText(valueOrDash(d.OwnerSummary))),
			core.ViewField("Product", projectHeaderProductValue(d.ProductSummary)),
			core.ViewField("Identifier", projectHeaderIDValue(d.Project.ID)),
		),
	)
}

func projectMetaInline(label, value string) Node {
	return Div(
		Class("inline-flex min-w-0 items-baseline gap-2"),
		Span(Class("text-[11px] font-semibold uppercase tracking-[0.08em] text-[var(--fgColor-muted)]"), Text(label)),
		Span(Class("min-w-0 truncate text-sm text-[var(--fgColor-default)]"), Text(value)),
	)
}

func projectHeaderProductValue(value string) Node {
	if value == "Unlinked" {
		return core.ViewFieldMutedText(value)
	}
	return core.ViewFieldText(valueOrDash(value))
}

func projectHeaderIDValue(value string) Node {
	return core.ViewFieldIdentifier(value)
}

func projectPageSection(title, description string, actions Node, nodes ...Node) Node {
	parts := []Node{
		Class("grid gap-3"),
		core.SectionHeader(title, description, actions),
	}
	parts = append(parts, nodes...)
	return Div(parts...)
}

func projectTabs(d projectHubPageData) Node {
	baseURL := projectDetailURL(d.Project.ID)
	tabs := []Node{
		projectTabLink(projectTab(baseURL, projectTabModels), d.ActiveTab == projectTabModels, "model", "Models", d.ModelCount),
		projectTabLink(projectTab(baseURL, projectTabMacros), d.ActiveTab == projectTabMacros, "macro", "Macros", d.MacroCount),
		projectTabLink(projectTab(baseURL, projectTabEnvironments), d.ActiveTab == projectTabEnvironments, "environment", "Environments", d.EnvironmentCount),
		projectTabLink(projectTab(baseURL, projectTabBuilds), d.ActiveTab == projectTabBuilds, "build", "Builds", d.BuildCount),
	}
	return Nav(
		Class("flex flex-wrap items-center gap-2 border-b border-[var(--borderColor-default)] pb-3"),
		Attr("aria-label", "Project sections"),
		Group(tabs),
	)
}

func projectTabLink(href string, active bool, kind, label string, count int64) Node {
	className := "inline-flex min-h-10 items-center gap-2 rounded-full border border-[var(--borderColor-default)] px-3 py-1.5 text-sm font-medium text-[var(--fgColor-muted)] no-underline transition-colors hover:bg-[var(--bgColor-muted)] hover:text-[var(--fgColor-default)]"
	countClass := "inline-flex min-w-5 items-center justify-center rounded-full bg-[var(--bgColor-muted)] px-1.5 py-0.5 text-[11px] font-semibold text-[var(--fgColor-muted)]"
	current := Node(nil)
	if active {
		className = "inline-flex min-h-10 items-center gap-2 rounded-full border border-[var(--borderColor-accent-emphasis)] bg-[var(--bgColor-accent-muted)] px-3 py-1.5 text-sm font-semibold text-[var(--fgColor-accent)] no-underline"
		countClass = "inline-flex min-w-5 items-center justify-center rounded-full bg-[var(--bgColor-default)] px-1.5 py-0.5 text-[11px] font-semibold text-[var(--fgColor-accent)]"
		current = Attr("aria-current", "page")
	}
	return A(
		Href(href),
		Class(className),
		current,
		core.Icon(core.ResourceKindIcon(kind), Class("h-4 w-4 shrink-0 "+core.ResourceKindAccentTextClass(kind)), Attr("style", "stroke-width:1.85")),
		Span(Text(label)),
		Span(Class(countClass), Text(strconv.FormatInt(count, 10))),
	)
}
