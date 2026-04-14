package projects

import (
	"fmt"
	"strconv"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

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
	Name           string
	Kind           string
	TargetLocation string
	Compute        string
	UpdatedAt      string
}

type projectBuildRowData struct {
	ID          string
	State       string
	Environment string
	GitRef      string
	Target      string
	CreatedAt   string
}

type projectHubPageData struct {
	Principal        domain.ContextPrincipal
	Project          domain.Project
	WorkspaceName    string
	OwnerSummary     string
	ProductSummary   string
	ActiveTab        string
	Tabs             []core.SectionTab
	ModelsURL        string
	MacrosURL        string
	SemanticURL      string
	NewModelURL      string
	NewMacroURL      string
	NewSemanticURL   string
	ModelCount       int64
	MacroCount       int64
	SemanticCount    int64
	EnvironmentCount int64
	BuildCount       int64
	Models           []projectAssetRowData
	Macros           []projectAssetRowData
	SemanticModels   []projectAssetRowData
	Environments     []projectEnvironmentRowData
	Builds           []projectBuildRowData
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
						P(Class("m-0 max-w-3xl text-sm leading-6 text-[var(--fgColor-muted)]"), Text("Projects are the authoring package boundary for models, macros, semantic models, environments, and builds.")),
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
	content := projectOverviewContent(d)
	switch d.ActiveTab {
	case projectTabAssets:
		content = projectAssetsContent(d)
	case projectTabEnvironments:
		content = projectEnvironmentsContent(d)
	case projectTabBuilds:
		content = projectBuildsContent(d)
	}

	return core.AppPage("Project: "+d.Project.Name, "projects", d.Principal,
		core.DetailShell(
			core.DetailHero(
				core.DetailHeroCopy(
					core.Kicker("Build"),
					core.DetailTitleRow(
						core.DetailTitle(d.Project.Name),
						core.Badge(projectKindLabel(d.Project.Kind), "accent"),
					),
					core.DetailDescription(valueOrDash(d.Project.Description)),
					core.BadgeRow(
						core.Badge("Workspace "+d.WorkspaceName, ""),
						core.Badge("Branch "+valueOrDash(d.Project.DefaultBranch), ""),
					),
				),
				core.DetailHeroMeta(
					core.MetaItem("Workspace", d.WorkspaceName),
					core.MetaItem("Owner", d.OwnerSummary),
					core.MetaItem("Product", d.ProductSummary),
					core.MetaItem("Project ID", d.Project.ID),
				),
			),
			core.MetricsGrid(
				core.ResourceMetricCard("model", "Models", strconv.FormatInt(d.ModelCount, 10)),
				core.ResourceMetricCard("macro", "Macros", strconv.FormatInt(d.MacroCount, 10)),
				core.ResourceMetricCard("semantic-model", "Semantic", strconv.FormatInt(d.SemanticCount, 10)),
				core.ResourceMetricCard("environment", "Environments", strconv.FormatInt(d.EnvironmentCount, 10)),
				core.ResourceMetricCard("build", "Builds", strconv.FormatInt(d.BuildCount, 10)),
			),
			core.DetailLayout(
				core.DetailMain(
					core.SectionTabs(d.Tabs),
					content,
				),
				core.DetailRail(
					core.DetailRailCard("Open project surfaces", "Use the existing feature pages when you want to go deeper than the hub previews.",
						core.ButtonGroup("",
							core.PrimaryLink(d.ModelsURL, "", Text("Models")),
							core.SecondaryLink(d.MacrosURL, "", Text("Macros")),
							core.SecondaryLink(d.SemanticURL, "", Text("Semantic")),
						),
					),
					core.DetailRailCard("Create inside project", "Project-prefilled create flows keep new authoring work in the same package boundary.",
						core.ButtonGroup("",
							core.SecondaryLink(d.NewModelURL, "", Text("New model")),
							core.SecondaryLink(d.NewMacroURL, "", Text("New macro")),
							core.SecondaryLink(d.NewSemanticURL, "", Text("New semantic model")),
						),
					),
					core.DetailRailCard("Summary", "Project metadata and linkage stay visible while you browse assets and delivery state.",
						core.MetadataSummary([][2]string{
							{"Kind", projectKindLabel(d.Project.Kind)},
							{"Workspace", d.WorkspaceName},
							{"Owner", d.OwnerSummary},
							{"Product", d.ProductSummary},
							{"Default branch", valueOrDash(d.Project.DefaultBranch)},
						}),
					),
				),
			),
		),
	)
}

func projectOverviewContent(d projectHubPageData) Node {
	return Group([]Node{
		projectAssetSection("model", "Models", "Project-scoped transformation models.", d.Models, d.ModelsURL, d.NewModelURL, d.ModelCount, "No models in this project yet.", "New model"),
		projectAssetSection("macro", "Macros", "Reusable helpers owned by this project.", d.Macros, d.MacrosURL, d.NewMacroURL, d.MacroCount, "No macros in this project yet.", "New macro"),
		projectAssetSection("semantic-model", "Semantic models", "Consumer-facing definitions linked to the project’s model layer.", d.SemanticModels, d.SemanticURL, d.NewSemanticURL, d.SemanticCount, "No semantic models in this project yet.", "New semantic model"),
		projectEnvironmentSection(d.Environments, d.EnvironmentCount, false),
		projectBuildSection(d.Builds, d.BuildCount, false),
	})
}

func projectAssetsContent(d projectHubPageData) Node {
	return Group([]Node{
		projectAssetSection("model", "Models", "All models scoped to this project.", d.Models, d.ModelsURL, d.NewModelURL, d.ModelCount, "No models in this project yet.", "New model"),
		projectAssetSection("macro", "Macros", "All macros scoped to this project.", d.Macros, d.MacrosURL, d.NewMacroURL, d.MacroCount, "No macros in this project yet.", "New macro"),
		projectAssetSection("semantic-model", "Semantic models", "All semantic models associated with this project.", d.SemanticModels, d.SemanticURL, d.NewSemanticURL, d.SemanticCount, "No semantic models in this project yet.", "New semantic model"),
	})
}

func projectEnvironmentsContent(d projectHubPageData) Node {
	return projectEnvironmentSection(d.Environments, d.EnvironmentCount, true)
}

func projectBuildsContent(d projectHubPageData) Node {
	return projectBuildSection(d.Builds, d.BuildCount, true)
}

func projectAssetSection(kind, title, description string, items []projectAssetRowData, listURL, createURL string, total int64, emptyMessage, emptyActionLabel string) Node {
	rows := items
	if total > int64(len(rows)) {
		total = int64(len(rows))
	}
	if len(rows) > 5 {
		rows = rows[:5]
	}

	if len(items) == 0 {
		return core.SectionSurface(
			core.SectionHeader(title, description, core.PrimaryLink(createURL, "", Text(emptyActionLabel))),
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

	actions := []Node{core.SecondaryLink(listURL, "", Text("View all"))}
	if createURL != "" {
		actions = append(actions, core.PrimaryLink(createURL, "", Text(emptyActionLabel)))
	}
	return core.SectionSurface(
		core.SectionHeader(title, description, actions...),
		core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Name")), Th(Text("Primary")), Th(Text("Secondary")), Th(Text("Detail")))),
				TBody(Group(tableRows)),
			),
		),
	)
}

func projectEnvironmentSection(items []projectEnvironmentRowData, total int64, full bool) Node {
	rows := items
	if !full && len(rows) > 5 {
		rows = rows[:5]
	}
	if len(items) == 0 {
		return core.SectionSurface(
			core.SectionHeader("Environments", "Execution contexts for development and release."),
			core.EmptyState("server", "No environments", "Create an environment through the project control plane to bind catalogs, schemas, and compute targets.", nil),
		)
	}
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		item := rows[i]
		tableRows = append(tableRows, Tr(
			Td(Text(item.Name)),
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
	return core.SectionSurface(
		core.SectionHeader(title, desc),
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
		return core.SectionSurface(
			core.SectionHeader("Builds", "Immutable project snapshots used for validation and delivery."),
			core.EmptyState("package-open", "No builds", "Builds will appear here once the project has compilation snapshots to review.", nil),
		)
	}
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		item := rows[i]
		tableRows = append(tableRows, Tr(
			Td(core.TableMetaText(item.ID)),
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
	return core.SectionSurface(
		core.SectionHeader(title, desc),
		core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Build ID")), Th(Text("State")), Th(Text("Environment")), Th(Text("Git ref")), Th(Text("Target")), Th(Text("Created")))),
				TBody(Group(tableRows)),
			),
		),
	)
}
