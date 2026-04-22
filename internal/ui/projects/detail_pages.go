package projects

import (
	"sort"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type projectEnvironmentDetailPageData struct {
	Principal         domain.ContextPrincipal
	Project           domain.Project
	Environment       domain.Environment
	WorkspaceName     string
	RelatedBuilds     []projectBuildRowData
	NewEnvironmentURL string
	EditURL           string
	DeleteURL         string
	CSRFFieldProvider func() Node
}

type projectBuildDetailPageData struct {
	Principal       domain.ContextPrincipal
	Project         domain.Project
	Build           domain.Build
	WorkspaceName   string
	EnvironmentName string
	EnvironmentURL  string
}

func projectEnvironmentDetailPage(d projectEnvironmentDetailPageData) Node {
	return core.AppPage("Environment: "+d.Environment.Name, "projects", d.Principal,
		Div(Class("grid gap-6"),
			projectEnvironmentHero(d),
			Div(Class("grid gap-5"),
				projectPageSection("Configuration", "Runtime bindings and execution targets for this environment.", nil,
					projectDetailMetaGrid(
						projectDetailMetaItem("Project", d.Project.Name),
						projectDetailMetaItem("Workspace", d.WorkspaceName),
						projectDetailMetaItem("Target", valueOrDash(d.Environment.TargetCatalog+"."+d.Environment.TargetSchema)),
						projectDetailMetaItem("Compute", valueOrDash(valueOrPtr(d.Environment.ComputeEndpoint))),
						projectDetailMetaItem("Delegation", valueOrDash(valueOrPtr(d.Environment.DeferToEnvironment))),
						projectDetailMetaItem("Updated", formatTime(d.Environment.UpdatedAt)),
					),
				),
				projectPageSection("Description", "Execution intent and notes for this environment.", nil,
					projectBodyTextOrEmpty(d.Environment.Description, "No description", "Add a description through the project control plane to document how this environment should be used."),
				),
				projectStringMapSection("Variables", "Resolved environment variables passed into authoring and runtime operations.", d.Environment.Variables, "No variables", "This environment does not define any variables."),
				projectStringMapSection("Source overrides", "Source-level overrides applied when this environment compiles or runs project assets.", d.Environment.SourceOverrides, "No source overrides", "This environment does not define any source overrides."),
				projectRelatedBuildsSection(d.RelatedBuilds),
			),
		),
	)
}

func projectBuildDetailPage(d projectBuildDetailPageData) Node {
	return core.AppPage("Build: "+d.Build.ID, "projects", d.Principal,
		Div(Class("grid gap-6"),
			projectBuildHero(d),
			Div(Class("grid gap-5"),
				projectPageSection("Configuration", "Snapshot metadata for this immutable build artifact.", nil,
					projectDetailMetaGrid(
						projectDetailMetaItem("Project", d.Project.Name),
						projectDetailMetaItemNode("Environment", projectMaybeTextLink(d.EnvironmentURL, d.EnvironmentName)),
						projectDetailMetaItem("Git ref", valueOrDash(d.Build.GitRef)),
						projectDetailMetaItem("Commit SHA", valueOrDash(valueOrPtr(d.Build.CommitSHA))),
						projectDetailMetaItem("Selector", valueOrDash(d.Build.Selector)),
						projectDetailMetaItem("Target", valueOrDash(d.Build.TargetCatalog+"."+d.Build.TargetSchema)),
						projectDetailMetaItem("Created by", valueOrDash(d.Build.CreatedBy)),
						projectDetailMetaItem("Created", formatTime(d.Build.CreatedAt)),
					),
				),
				projectPageSection("Compile manifest", "Compilation output preserved with this build for inspection and debugging.", nil,
					projectCodeBlock(valueOrDash(d.Build.CompileManifest)),
				),
				projectPageSection("Compile diagnostics", "Diagnostic output captured during build creation.", nil,
					projectCodeBlockOrEmpty(valueOrPtr(d.Build.CompileDiagnostics), "No diagnostics", "This build did not record compile diagnostics."),
				),
			),
		),
	)
}

func projectEnvironmentHero(d projectEnvironmentDetailPageData) Node {
	descriptionNode := Node(nil)
	if strings.TrimSpace(d.Environment.Description) != "" {
		descriptionNode = core.DetailDescription(d.Environment.Description)
	}

	actions := Div(
		Class("flex flex-wrap items-center gap-3"),
		core.SecondaryLink(projectTab(projectDetailURL(d.Project.ID), projectTabEnvironments), "", Text("Back to environments")),
		core.PrimaryLink(d.NewEnvironmentURL, "", Text("New environment")),
		core.SecondaryLink(d.EditURL, "", Text("Edit")),
		Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete"))),
	)

	return Div(
		Class("grid gap-4 rounded-2xl border border-[var(--borderColor-default)] bg-[linear-gradient(135deg,var(--bgColor-muted)_0%,var(--bgColor-default)_65%)] p-5 shadow-sm"),
		Div(
			Class("grid gap-4"),
			actions,
			core.Kicker("Project environment"),
			core.DetailTitleRow(
				core.DetailTitle(d.Environment.Name),
				core.Badge(environmentKindLabel(d.Environment.Kind), "accent"),
			),
			descriptionNode,
			Div(
				Class("flex flex-wrap gap-x-5 gap-y-2 text-sm"),
				projectMetaInline("Project", d.Project.Name),
				projectMetaInline("Workspace", d.WorkspaceName),
				projectMetaInline("Target", valueOrDash(d.Environment.TargetCatalog+"."+d.Environment.TargetSchema)),
				projectMetaInline("Compute", valueOrDash(valueOrPtr(d.Environment.ComputeEndpoint))),
			),
		),
		P(
			Class("m-0 text-xs leading-5 text-[var(--fgColor-muted)]"),
			Text("Environment ID "),
			Span(Class("font-medium text-[var(--fgColor-default)]"), Text(d.Environment.ID)),
		),
	)
}

func projectBuildHero(d projectBuildDetailPageData) Node {
	return Div(
		Class("grid gap-4 rounded-2xl border border-[var(--borderColor-default)] bg-[linear-gradient(135deg,var(--bgColor-muted)_0%,var(--bgColor-default)_65%)] p-5 shadow-sm"),
		Div(
			Class("grid gap-4"),
			core.SecondaryLink(projectTab(projectDetailURL(d.Project.ID), projectTabBuilds), "", Text("Back to builds")),
			core.Kicker("Project build"),
			core.DetailTitleRow(
				core.DetailTitle(d.Build.ID),
				core.Badge(buildStateLabel(d.Build.State), "accent"),
			),
			Div(
				Class("flex flex-wrap gap-x-5 gap-y-2 text-sm"),
				projectMetaInline("Project", d.Project.Name),
				projectMetaInline("Environment", valueOrDash(d.EnvironmentName)),
				projectMetaInline("Git ref", valueOrDash(d.Build.GitRef)),
			),
		),
		P(
			Class("m-0 text-xs leading-5 text-[var(--fgColor-muted)]"),
			Text("Created "),
			Span(Class("font-medium text-[var(--fgColor-default)]"), Text(formatTime(d.Build.CreatedAt))),
			Text(" by "),
			Span(Class("font-medium text-[var(--fgColor-default)]"), Text(valueOrDash(d.Build.CreatedBy))),
		),
	)
}

func projectDetailMetaGrid(items ...Node) Node {
	return Div(Class("grid gap-x-8 gap-y-4 sm:grid-cols-2 xl:grid-cols-3"), Group(items))
}

func projectDetailMetaItem(label, value string) Node {
	return projectDetailMetaItemNode(label, Span(Class("text-sm text-[var(--fgColor-default)]"), Text(value)))
}

func projectDetailMetaItemNode(label string, value Node) Node {
	return Div(
		Class("grid gap-1"),
		Span(Class("text-[11px] font-semibold uppercase tracking-[0.08em] text-[var(--fgColor-muted)]"), Text(label)),
		value,
	)
}

func projectMaybeTextLink(href, label string) Node {
	if strings.TrimSpace(href) == "" {
		return Span(Class("text-sm text-[var(--fgColor-default)]"), Text(valueOrDash(label)))
	}
	return core.TextLink(href, Text(valueOrDash(label)))
}

func projectBodyTextOrEmpty(value, emptyTitle, emptyMessage string) Node {
	if strings.TrimSpace(value) == "" {
		return core.EmptyState("align-left", emptyTitle, emptyMessage, nil)
	}
	return P(Class("m-0 max-w-4xl text-sm leading-6 text-[var(--fgColor-default)]"), Text(value))
}

func projectStringMapSection(title, description string, items map[string]string, emptyTitle, emptyMessage string) Node {
	if len(items) == 0 {
		return projectPageSection(title, description, nil,
			core.EmptyState("list-tree", emptyTitle, emptyMessage, nil),
		)
	}

	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	rows := make([]Node, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, Tr(
			Td(Span(Class("font-medium text-[var(--fgColor-default)]"), Text(key))),
			Td(core.TableMetaText(valueOrDash(items[key]))),
		))
	}

	return projectPageSection(title, description, nil,
		core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Key")), Th(Text("Value")))),
				TBody(Group(rows)),
			),
		),
	)
}

func projectRelatedBuildsSection(items []projectBuildRowData) Node {
	if len(items) == 0 {
		return projectPageSection("Related builds", "Build snapshots produced against this environment.", nil,
			core.EmptyState("package-open", "No related builds", "No builds reference this environment yet.", nil),
		)
	}

	rows := make([]Node, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, Tr(
			core.TablePrimaryCell(core.ResourceIcon("build"), core.TextLink(item.URL, Text(item.ID))),
			Td(core.TableMetaText(item.State)),
			Td(core.TableMetaText(item.GitRef)),
			Td(core.TableMetaText(item.CreatedAt)),
		))
	}

	return projectPageSection("Related builds", "Build snapshots produced against this environment.", nil,
		core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Build")), Th(Text("State")), Th(Text("Git ref")), Th(Text("Created")))),
				TBody(Group(rows)),
			),
		),
	)
}

func projectCodeBlock(value string) Node {
	return Pre(Class("overflow-x-auto rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3 text-sm"), Text(value))
}

func projectCodeBlockOrEmpty(value, emptyTitle, emptyMessage string) Node {
	if strings.TrimSpace(value) == "" {
		return core.EmptyState("file-stack", emptyTitle, emptyMessage, nil)
	}
	return projectCodeBlock(value)
}

func optionSelected(value, current string) Node {
	if value == current {
		return Option(Value(value), Selected(), Text(value))
	}
	return Option(Value(value), Text(value))
}

func environmentKindOptions(projectKind, current string) []Node {
	kinds := []string{domain.EnvironmentKindDevelopment}
	if projectKind != domain.ProjectKindPersonal {
		kinds = append(kinds, domain.EnvironmentKindStaging, domain.EnvironmentKindProduction)
	}

	options := make([]Node, 0, len(kinds))
	for _, kind := range kinds {
		options = append(options, optionSelected(kind, current))
	}
	return options
}

func projectEnvironmentFormPage(principal domain.ContextPrincipal, title, action string, project domain.Project, environment *domain.Environment, csrfFieldProvider func() Node) Node {
	valuesText := ""
	overridesText := ""
	descriptionText := ""
	targetCatalog := ""
	targetSchema := ""
	computeEndpoint := ""
	deferToEnvironment := ""
	kind := domain.EnvironmentKindDevelopment

	if environment != nil {
		valuesText = stringMapEditorValue(environment.Variables)
		overridesText = stringMapEditorValue(environment.SourceOverrides)
		descriptionText = environment.Description
		targetCatalog = environment.TargetCatalog
		targetSchema = environment.TargetSchema
		computeEndpoint = valueOrPtr(environment.ComputeEndpoint)
		deferToEnvironment = valueOrPtr(environment.DeferToEnvironment)
		if strings.TrimSpace(environment.Kind) != "" {
			kind = environment.Kind
		}
	}

	fields := []Node{csrfFieldProvider()}
	if environment == nil {
		fields = append(fields,
			Label(Text("Name")),
			core.InputControl("", Name("name"), Required()),
			Label(Text("Kind")),
			core.SelectControl("", Name("kind"), Group(environmentKindOptions(project.Kind, kind))),
		)
	} else {
		fields = append(fields,
			Label(Text("Name")),
			core.InputControl("", Value(environment.Name), Disabled()),
			Label(Text("Kind")),
			core.InputControl("", Value(environmentKindLabel(environment.Kind)), Disabled()),
		)
	}

	fields = append(fields,
		Label(Text("Description")),
		core.TextareaControl("min-h-24", Name("description"), Text(descriptionText)),
		Label(Text("Target catalog")),
		core.InputControl("", Name("target_catalog"), Value(targetCatalog), Required()),
		Label(Text("Target schema")),
		core.InputControl("", Name("target_schema"), Value(targetSchema), Required()),
		Label(Text("Compute endpoint")),
		core.InputControl("", Name("compute_endpoint"), Value(computeEndpoint)),
		Label(Text("Defer to environment")),
		core.InputControl("", Name("defer_to_environment"), Value(deferToEnvironment)),
		Label(Text("Variables")),
		core.TextareaControl("min-h-32 font-mono text-xs", Name("variables"), Placeholder("KEY=value"), Text(valuesText)),
		P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text("Enter one `KEY=value` pair per line.")),
		Label(Text("Source overrides")),
		core.TextareaControl("min-h-32 font-mono text-xs", Name("source_overrides"), Placeholder("source_name=override_name"), Text(overridesText)),
		P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text("Enter one `source=override` pair per line.")),
		Div(Class("mt-4 flex flex-wrap items-center gap-3"),
			core.PrimaryButton("", Type("submit"), Text("Save")),
			core.SecondaryLink(projectTab(projectDetailURL(project.ID), projectTabEnvironments), "", Text("Cancel")),
		),
	)

	return core.AppPage(title, "projects", principal,
		core.FormPageLayout("Build", title, "Environment authoring stays inside the project workspace and uses the same focused single-surface form layout.",
			Form(Class("grid gap-3"), Method("post"), Action(action), Group(fields)),
		),
	)
}
