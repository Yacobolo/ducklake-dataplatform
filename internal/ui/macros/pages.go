package macros

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type macrosListRowData struct {
	Name       string
	URL        string
	Project    string
	Type       string
	Visibility string
	Status     string
}

type macroRevisionRowData struct {
	Version   string
	Status    string
	CreatedBy string
	Created   string
}

type macroDetailPageData struct {
	Principal     domain.ContextPrincipal
	Name          string
	Project       string
	Type          string
	Visibility    string
	Status        string
	Owner         string
	EditURL       string
	DiffURL       string
	ImpactURL     string
	DeleteURL     string
	Definition    string
	Revisions     []macroRevisionRowData
	CSRFFieldFunc func() Node
}

type macroRevisionOptionData struct {
	Value string
	Label string
}

type macroDiffPageData struct {
	Principal       domain.ContextPrincipal
	Name            string
	FromVersion     int
	ToVersion       int
	RevisionOptions []macroRevisionOptionData
	Diff            *domain.MacroRevisionDiff
	ImpactAdded     []macroImpactRowData
	ImpactRemoved   []macroImpactRowData
	ImpactUnchanged []macroImpactRowData
}

type macroImpactPageData struct {
	Principal domain.ContextPrincipal
	Name      string
	Rows      []macroImpactRowData
}

func macrosListPage(principal domain.ContextPrincipal, rows []macrosListRowData, page domain.PageRequest, total int64, projectName string) Node {
	table := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No macros yet.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				core.TablePrimaryCell(
					core.IconChip("braces", "bg-[var(--display-orange-scale-0)] text-[var(--display-orange-scale-6)]"),
					A(Href(row.URL), Class("font-mono text-[13px] font-semibold text-[var(--fgColor-accent)] no-underline visited:text-[var(--fgColor-accent)] hover:text-[var(--fgColor-accent)] hover:underline active:text-[var(--fgColor-accent)]"), Text(row.Name)),
				),
				Td(core.TableMetaText(emptyDash(row.Project))),
				Td(statusPill(row.Type, "accent")),
				Td(core.TableMetaText(row.Visibility)),
				Td(statusPill(row.Status, "neutral")),
			))
		}
		table = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Scope("col"), Text("Name")), Th(Scope("col"), Text("Project")), Th(Scope("col"), Text("Type")), Th(Scope("col"), Text("Visibility")), Th(Scope("col"), Text("Status")))),
				TBody(Group(tableRows)),
			),
		)
	}
	title := "Macros"
	description := "Create and manage reusable SQL and transformation helpers."
	basePath := "/ui/macros"
	newHref := "/ui/macros/new"
	if strings.TrimSpace(projectName) != "" {
		title = "Macros: " + projectName
		description = "Project-scoped macros for " + projectName + "."
		basePath = "/ui/macros?project=" + url.QueryEscape(projectName)
		newHref = "/ui/macros/new?project=" + url.QueryEscape(projectName)
	}

	return core.AppPage("Macros", "macros", principal,
		core.ListPageLayout(
			core.ListPageHeader(title, description, core.PrimaryLink(newHref, "", Text("New macro"))),
			core.ListPageBody(
				table,
				core.ListPagination(basePath, page, total),
			),
		),
	)
}

func macroDetailPage(d macroDetailPageData) Node {
	revisions := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No revisions yet.")))
	if len(d.Revisions) > 0 {
		rows := make([]Node, 0, len(d.Revisions))
		for i := range d.Revisions {
			rev := d.Revisions[i]
			rows = append(rows, Tr(
				Td(Text(rev.Version)),
				Td(Text(rev.Status)),
				Td(Text(emptyDash(rev.CreatedBy))),
				Td(Text(rev.Created)),
			))
		}
		revisions = Div(Class("overflow-x-auto"),
			core.TableContainer("",
				core.DataTable("",
					THead(Tr(Th(Text("Version")), Th(Text("Status")), Th(Text("Created by")), Th(Text("Created")))),
					TBody(Group(rows)),
				),
			),
		)
	}

	return core.AppPage("Macro: "+d.Name, "macros", d.Principal,
		core.DetailShell(
			core.DetailHero(
				core.DetailHeroCopy(
					core.Kicker("Build"),
					core.DetailTitle(d.Name),
					core.DetailDescription("Macros now follow the same build-workspace structure as models, with summary in the hero and actions in the rail."),
				),
				core.DetailHeroMeta(
					core.BadgeRow(statusPill(d.Type, "accent"), statusPill(d.Status, "neutral")),
					core.DetailSummaryList([][2]string{
						{"Project", emptyDash(d.Project)},
						{"Visibility", d.Visibility},
						{"Owner", emptyDash(d.Owner)},
					}),
				),
			),
			core.DetailLayout(
				core.DetailMain(
					core.SectionSurface(
						core.SectionHeader("Definition", "Keep the macro body in the primary workspace."),
						Pre(Class("overflow-x-auto rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3 text-sm"), Text(d.Definition)),
					),
					core.SectionSurface(
						core.SectionHeader("Revisions", "Revision history belongs in the main detail stream."),
						revisions,
					),
				),
				core.DetailRail(
					core.DetailRailCard("Summary", "A compact build summary keeps the current macro state easy to scan.",
						core.MetadataSummary([][2]string{
							{"Type", d.Type},
							{"Visibility", d.Visibility},
							{"Status", d.Status},
						}),
					),
					core.DetailRailCard("Actions", "Editing, diffing, and impact analysis stay together in the rail.",
						core.ButtonGroup("",
							core.SecondaryLink(d.EditURL, "", Text("Edit")),
							core.SecondaryLink(d.DiffURL, "", Text("Diff revisions")),
							core.SecondaryLink(d.ImpactURL, "", Text("Impact")),
							Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldFunc(), core.DangerButton("", Type("submit"), Text("Delete"))),
						),
					),
				),
			),
		),
	)
}

func macrosNewPage(principal domain.ContextPrincipal, initialProject string, csrfFieldProvider func() Node) Node {
	return macroFormPage(principal, "New Macro", "/ui/macros", csrfFieldProvider,
		Label(Text("Name")),
		core.InputControl("", Name("name"), Required()),
		Label(Text("Project")),
		core.InputControl("", Name("project_name"), Value(initialProject)),
		Label(Text("Type")),
		core.SelectControl("", Name("macro_type"),
			Option(Value("SCALAR"), Text("SCALAR")),
			Option(Value("TABLE"), Text("TABLE")),
		),
		Label(Text("Visibility")),
		core.SelectControl("", Name("visibility"),
			Option(Value("project"), Text("project")),
			Option(Value("catalog_global"), Text("catalog_global")),
			Option(Value("system"), Text("system")),
		),
		Label(Text("Description")),
		core.TextareaControl("min-h-24", Name("description")),
		Label(Text("Parameters (comma separated)")),
		core.InputControl("", Name("parameters")),
		Label(Text("Body")),
		core.TextareaControl("min-h-40 font-mono text-xs", Name("body"), Required()),
	)
}

func macrosEditPage(principal domain.ContextPrincipal, macroName string, macro *domain.Macro, csrfFieldProvider func() Node) Node {
	return macroFormPage(principal, "Edit Macro", "/ui/macros/"+macroName+"/update", csrfFieldProvider,
		Label(Text("Project")),
		core.InputControl("", Name("project_name"), Value(macro.ProjectName)),
		Label(Text("Description")),
		core.TextareaControl("min-h-24", Name("description"), Text(macro.Description)),
		Label(Text("Visibility")),
		core.SelectControl("", Name("visibility"),
			optionSelected("project", macro.Visibility),
			optionSelected("catalog_global", macro.Visibility),
			optionSelected("system", macro.Visibility),
		),
		Label(Text("Parameters (comma separated)")),
		core.InputControl("", Name("parameters"), Value(csvValues(macro.Parameters))),
		Label(Text("Body")),
		core.TextareaControl("min-h-40 font-mono text-xs", Name("body"), Required(), Text(macro.Body)),
		Label(Text("Status")),
		core.SelectControl("", Name("status"),
			optionSelected("ACTIVE", macro.Status),
			optionSelected("DEPRECATED", macro.Status),
		),
	)
}

func macroFormPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	nodes = append(nodes, Div(Class("mt-4"), core.PrimaryButton("", Type("submit"), Text("Save"))))

	return core.AppPage(title, "macros", principal,
		core.FormPageLayout("Build", title, "Macro authoring now uses the shared single-surface form layout.",
			Form(Class("grid gap-3"), Method("post"), Action(action), Group(nodes)),
		),
	)
}

func macroDiffPage(d macroDiffPageData) Node {
	if d.Diff == nil {
		return core.AppPage("Macro Diff: "+d.Name, "macros", d.Principal,
			core.ResultPageLayout("Build", "Macro diff", "Revision comparison renders as a result workspace instead of a stack of unrelated cards.",
				core.SectionSurface(
					core.SectionHeader("Compare revisions", "At least two revisions are required before a diff can be shown."),
					P(Class("text-xs text-[var(--fgColor-muted)]"), Text("At least two revisions are required to diff a macro.")),
					core.SecondaryLink("/ui/macros/"+d.Name, "", Text("Back to macro")),
				),
			),
		)
	}

	fromOptions := make([]Node, 0, len(d.RevisionOptions))
	toOptions := make([]Node, 0, len(d.RevisionOptions))
	for i := range d.RevisionOptions {
		option := d.RevisionOptions[i]
		fromOptions = append(fromOptions, optionSelected(option.Value, strconv.Itoa(d.FromVersion)))
		toOptions = append(toOptions, optionSelected(option.Value, strconv.Itoa(d.ToVersion)))
	}

	return core.AppPage("Macro Diff: "+d.Name, "macros", d.Principal,
		core.ResultPageLayout("Build", "Macro diff", "Compare revisions in a report-style layout with summary up front and detailed changes below.",
			core.SectionSurface(
				core.SectionHeader("Compare revisions", "Choose the revisions to compare, then inspect the diff sections below."),
				Form(Class("grid gap-3 md:grid-cols-2"), Method("get"), Action("/ui/macros/"+d.Name+"/diff"),
					Div(Label(Text("From revision")), core.SelectControl("", Name("from"), Group(fromOptions))),
					Div(Label(Text("To revision")), core.SelectControl("", Name("to"), Group(toOptions))),
					Div(Class("md:col-span-2"), core.PrimaryButton("", Type("submit"), Text("Compare revisions"))),
				),
				core.MetadataSummary([][2]string{
					{"Changed", boolLabel(d.Diff.Changed)},
					{"Parameters changed", boolLabel(d.Diff.ParametersChanged)},
					{"Body changed", boolLabel(d.Diff.BodyChanged)},
					{"Description changed", boolLabel(d.Diff.DescriptionChanged)},
					{"Status changed", boolLabel(d.Diff.StatusChanged)},
				}),
			),
			core.SectionSurface(
				core.SectionHeader("Parameters", "Review parameter changes separately from body changes."),
				P(Text("From: "+stringsJoin(d.Diff.FromParameters))),
				P(Text("To: "+stringsJoin(d.Diff.ToParameters))),
			),
			core.SectionSurface(
				core.SectionHeader("Description", "Description changes stay separate from the SQL body diff."),
				P(Text("From: "+emptyDash(d.Diff.FromDescription))),
				P(Text("To: "+emptyDash(d.Diff.ToDescription))),
			),
			core.SectionSurface(
				core.SectionHeader("Body", "Read both revisions side by side in the same result workspace."),
				H4(Class("mb-2 text-sm font-semibold uppercase tracking-wide text-[var(--fgColor-muted)]"), Text("From")),
				Pre(Class("overflow-x-auto rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3 text-sm"), Text(d.Diff.FromBody)),
				H4(Class("mb-2 text-sm font-semibold uppercase tracking-wide text-[var(--fgColor-muted)]"), Text("To")),
				Pre(Class("overflow-x-auto rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3 text-sm"), Text(d.Diff.ToBody)),
			),
			macroImpactSection("Impact added", d.ImpactAdded, "No newly impacted models."),
			macroImpactSection("Impact removed", d.ImpactRemoved, "No removed impacted models."),
			macroImpactSection("Impact unchanged", d.ImpactUnchanged, "No unchanged impacted models."),
		),
	)
}

func macroImpactPage(d macroImpactPageData) Node {
	return core.AppPage("Macro Impact: "+d.Name, "macros", d.Principal,
		macroImpactSection("Impacted models", d.Rows, "No impacted models found for this macro."),
	)
}

func macroImpactSection(title string, rowsData []macroImpactRowData, emptyMessage string) Node {
	if len(rowsData) == 0 {
		return core.SectionSurface(
			core.SectionHeader(title, ""),
			P(Class("text-xs text-[var(--fgColor-muted)]"), Text(emptyMessage)),
		)
	}
	rows := make([]Node, 0, len(rowsData))
	for i := range rowsData {
		row := rowsData[i]
		rows = append(rows, Tr(Td(A(Href(row.URL), Class("font-medium text-[var(--fgColor-accent)]"), Text(row.ModelName))), Td(Text(row.LastSeen))))
	}
	return core.SectionSurface(
		core.SectionHeader(title, ""),
		core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Model")), Th(Text("Last seen")))),
				TBody(Group(rows)),
			),
		),
	)
}

func optionSelected(value, current string) Node {
	if value == current {
		return Option(Value(value), Selected(), Text(value))
	}
	return Option(Value(value), Text(value))
}

func metaRow(label, value string) Node {
	return Div(
		Class("rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3"),
		Dt(Class("text-xs font-medium uppercase tracking-wide text-[var(--fgColor-muted)]"), Text(label)),
		Dd(Class("mt-1 ml-0 text-sm text-[var(--fgColor-default)]"), Text(emptyDash(value))),
	)
}

func statusPill(text, tone string) Node {
	className := "inline-flex items-center rounded-full px-2.5 py-1 text-xs font-medium"
	switch tone {
	case "accent":
		className += " bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]"
	default:
		className += " bg-[var(--bgColor-muted)] text-[var(--fgColor-default)]"
	}
	return Span(Class(className), Text(text))
}

func emptyDash(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

func boolLabel(value bool) string {
	if value {
		return "Yes"
	}
	return "No"
}

func stringsJoin(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ", ")
}
