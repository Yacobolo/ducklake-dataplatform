package models

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type modelsListRowData struct {
	DetailURL     string
	ModelName     string
	Materialized  string
	Dependencies  int
	UpdatedAtText string
}

type modelTestRowData struct {
	Name      string
	TestType  string
	Column    string
	DeleteURL string
}

type modelDAGNodeData struct {
	Name         string
	Materialized string
	DependsOn    string
	URL          string
}

type modelDAGTierData struct {
	Label string
	Nodes []modelDAGNodeData
}

type modelsDAGPageData struct {
	Principal   domain.ContextPrincipal
	ProjectName *string
	Tiers       []modelDAGTierData
}

type modelRunRowData struct {
	ID            string
	URL           string
	Status        string
	TriggerType   string
	TriggeredBy   string
	Target        string
	Selector      string
	CreatedAtText string
}

type modelRunsListPageData struct {
	Principal      domain.ContextPrincipal
	Rows           []modelRunRowData
	Page           domain.PageRequest
	Total          int64
	SelectedStatus string
}

type modelTestResultRowData struct {
	TestName string
	Status   string
	Message  string
	Executed string
}

type modelRunStepRowData struct {
	ModelName      string
	Status         string
	Tier           string
	RowsAffected   string
	StartedAtText  string
	FinishedAtText string
	ErrorText      string
	TestResults    []modelTestResultRowData
}

type modelRunDetailPageData struct {
	Principal         domain.ContextPrincipal
	RunID             string
	Status            string
	TriggerType       string
	TriggeredBy       string
	TargetCatalog     string
	TargetSchema      string
	Selector          string
	Variables         string
	CompileManifest   string
	ErrorText         string
	CreatedAtText     string
	StartedAtText     string
	FinishedAtText    string
	CancelURL         string
	Steps             []modelRunStepRowData
	CSRFFieldProvider func() Node
}

type modelSourceFreshnessPageData struct {
	Principal         domain.ContextPrincipal
	Result            *domain.SourceFreshnessStatus
	SourceSchema      string
	SourceTable       string
	TimestampColumn   string
	MaxLagSecondsText string
	CSRFFieldProvider func() Node
}

type modelsDetailPageData struct {
	Principal          domain.ContextPrincipal
	ProjectName        string
	ModelName          string
	QualifiedName      string
	Materialization    string
	Owner              string
	DependsOn          string
	ConfigText         string
	EditURL            string
	DeleteURL          string
	NewTestURL         string
	TriggerRunURL      string
	CancelRunURL       string
	RunsURL            string
	DAGURL             string
	FreshnessURL       string
	SourceFreshnessURL string
	DefaultSelector    string
	SQL                string
	Tests              []modelTestRowData
	TriggerProject     string
	TriggerModel       string
	FreshnessStatus    *domain.FreshnessStatus
	CSRFFieldProvider  func() Node
}

func modelsListPage(principal domain.ContextPrincipal, rows []modelsListRowData, page domain.PageRequest, total int64, projectName *string) Node {
	table := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No models available.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				core.TablePrimaryCell(
					core.ResourceIcon("model"),
					A(Href(row.DetailURL), Class("font-mono text-[13px] font-semibold text-[var(--fgColor-accent)] no-underline visited:text-[var(--fgColor-accent)] hover:text-[var(--fgColor-accent)] hover:underline active:text-[var(--fgColor-accent)]"), Text(row.ModelName)),
				),
				Td(core.TableMetaText(row.Materialized)),
				Td(core.TableMetaText(strconv.Itoa(row.Dependencies))),
				Td(core.TableMetaText(row.UpdatedAtText)),
			))
		}
		table = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Scope("col"), Text("Model")), Th(Scope("col"), Text("Materialization")), Th(Scope("col"), Text("Dependencies")), Th(Scope("col"), Text("Updated")))),
				TBody(Group(tableRows)),
			),
		)
	}
	title := "Models"
	description := "Manage dbt-style models and tests."
	basePath := "/ui/models"
	newHref := "/ui/models/new"
	if projectName != nil && strings.TrimSpace(*projectName) != "" {
		title = "Models: " + *projectName
		description = "Project-scoped models for " + *projectName + "."
		basePath = "/ui/models?project=" + url.QueryEscape(*projectName)
		newHref = "/ui/models/new?project=" + url.QueryEscape(*projectName)
	}
	return core.AppPage("Models", "models", principal,
		core.ListPageLayout(
			core.ListPageHeader(title, description, core.SecondaryLink("/ui/macros", "", Text("Open macros")), core.PrimaryLink(newHref, "", Text("New model"))),
			core.ListPageBody(
				table,
				core.ListPagination(basePath, page, total),
			),
		),
	)
}

func modelsDetailPage(d modelsDetailPageData) Node {
	tests := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No tests configured.")))
	if len(d.Tests) > 0 {
		rows := make([]Node, 0, len(d.Tests))
		for i := range d.Tests {
			t := d.Tests[i]
			rows = append(rows, Tr(
				Td(Text(t.Name)),
				Td(Text(t.TestType)),
				Td(Text(valueOrDash(t.Column))),
				core.TableActionCell(core.TableIconActionPost(t.DeleteURL, "Delete test", "x", "danger", d.CSRFFieldProvider)),
			))
		}
		tests = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Column")), core.TableActionHeader())),
				TBody(Group(rows)),
			),
		)
	}

	freshness := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No freshness policy configured.")))
	if d.FreshnessStatus != nil {
		freshness = Div(
			P(Class("m-0 text-sm"), Strong(Text("Fresh: ")), Text(strconv.FormatBool(d.FreshnessStatus.IsFresh))),
			P(Class("m-0 text-sm"), Strong(Text("Last successful run: ")), Text(formatTimePtr(d.FreshnessStatus.LastRunAt))),
			P(Class("m-0 text-sm"), Strong(Text("Max lag seconds: ")), Text(strconv.FormatInt(d.FreshnessStatus.MaxLagSeconds, 10))),
			P(Class("m-0 text-sm"), Strong(Text("Stale since: ")), Text(formatTimePtr(d.FreshnessStatus.StaleSince))),
		)
	}

	return core.AppPage("Model: "+d.QualifiedName, "models", d.Principal,
		core.DetailShell(
			core.DetailHero(
				core.DetailHeroCopy(
					core.Kicker("Build"),
					core.DetailTitle(d.QualifiedName),
					core.DetailDescription("Model detail now prioritizes current state in the main column and keeps run/test actions in a secondary rail."),
				),
				core.DetailHeroMeta(
					core.BadgeRow(core.Badge(d.Materialization, "accent")),
					core.DetailSummaryList([][2]string{
						{"Owner", emptyDash(d.Owner)},
						{"Depends on", emptyDash(d.DependsOn)},
						{"Config", emptyDash(d.ConfigText)},
					}),
				),
			),
			core.DetailLayout(
				core.DetailMain(
					core.SectionSurface(
						core.SectionHeader("Freshness", "Keep SLA and staleness context near the model summary."),
						freshness,
						Form(Method("post"), Action(d.FreshnessURL), d.CSRFFieldProvider(), core.SecondaryButton("", Type("submit"), Text("Refresh freshness status"))),
					),
					core.SectionSurface(
						core.SectionHeader("SQL", "Model SQL remains in the primary content column for review."),
						Pre(Class("overflow-x-auto rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3 text-sm"), Text(d.SQL)),
					),
					core.SectionSurface(
						core.SectionHeader("Tests", "Inspect and remove configured tests from the same workspace."),
						tests,
					),
				),
				core.DetailRail(
					core.DetailRailCard("Actions", "Navigation and mutations live in the rail so the page reads as a detail workspace.",
						core.ButtonGroup("",
							core.SecondaryLink(d.EditURL, "", Text("Edit")),
							core.PrimaryLink(d.NewTestURL, "", Text("New test")),
							core.SecondaryLink(d.RunsURL, "", Text("Runs")),
							core.SecondaryLink(d.DAGURL, "", Text("DAG")),
							core.SecondaryLink(d.SourceFreshnessURL, "", Text("Source freshness")),
							Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete"))),
						),
					),
					core.DetailRailCard("Trigger model run", "Execution controls stay secondary to the model definition.",
						Form(Class("grid gap-3"), Method("post"), Action(d.TriggerRunURL),
							d.CSRFFieldProvider(),
							Input(Type("hidden"), Name("project_name"), Value(d.TriggerProject)),
							Input(Type("hidden"), Name("model_name"), Value(d.TriggerModel)),
							Label(Text("Target catalog")),
							core.InputControl("", Name("target_catalog"), Required()),
							Label(Text("Target schema")),
							core.InputControl("", Name("target_schema"), Required()),
							Label(Text("Selector")),
							core.InputControl("", Name("selector"), Value(d.DefaultSelector)),
							core.PrimaryButton("", Type("submit"), Text("Trigger model run")),
						),
					),
					core.DetailRailCard("Cancel model run", "Keep the interrupt path available without giving it equal weight to the definition.",
						Form(Class("grid gap-3"), Method("post"), Action(d.CancelRunURL),
							d.CSRFFieldProvider(),
							Label(Text("Run ID to cancel")),
							core.InputControl("", Name("run_id")),
							core.SecondaryButton("", Type("submit"), Text("Cancel model run")),
						),
					),
				),
			),
		),
	)
}

func modelsNewPage(principal domain.ContextPrincipal, initialProject string, csrfFieldProvider func() Node) Node {
	return modelFormPage(principal, "New Model", "/ui/models", csrfFieldProvider,
		Label(Text("Project")),
		core.InputControl("", Name("project_name"), Value(initialProject), Required()),
		Label(Text("Name")),
		core.InputControl("", Name("name"), Required()),
		Label(Text("Materialization")),
		core.SelectControl("", Name("materialization"),
			Option(Value("VIEW"), Text("VIEW")),
			Option(Value("TABLE"), Text("TABLE")),
			Option(Value("INCREMENTAL"), Text("INCREMENTAL")),
			Option(Value("EPHEMERAL"), Text("EPHEMERAL")),
		),
		Label(Text("Description")),
		core.TextareaControl("min-h-24", Name("description")),
		Label(Text("Tags (comma separated)")),
		core.InputControl("", Name("tags")),
		Label(Text("SQL")),
		core.TextareaControl("min-h-40 font-mono text-xs", Name("sql"), Required()),
	)
}

func modelsEditPage(principal domain.ContextPrincipal, projectName, modelName string, model *domain.Model, csrfFieldProvider func() Node) Node {
	return modelFormPage(principal, "Edit Model", "/ui/models/"+projectName+"/"+modelName+"/update", csrfFieldProvider,
		Label(Text("Materialization")),
		core.SelectControl("", Name("materialization"),
			optionSelected("VIEW", model.Materialization),
			optionSelected("TABLE", model.Materialization),
			optionSelected("INCREMENTAL", model.Materialization),
			optionSelected("EPHEMERAL", model.Materialization),
		),
		Label(Text("Description")),
		core.TextareaControl("min-h-24", Name("description"), Text(model.Description)),
		Label(Text("Tags (comma separated)")),
		core.InputControl("", Name("tags"), Value(csvValues(model.Tags))),
		Label(Text("SQL")),
		core.TextareaControl("min-h-40 font-mono text-xs", Name("sql"), Required(), Text(model.SQL)),
	)
}

func modelTestsNewPage(principal domain.ContextPrincipal, projectName, modelName string, csrfFieldProvider func() Node) Node {
	return modelFormPage(principal, "New Model Test", "/ui/models/"+projectName+"/"+modelName+"/tests", csrfFieldProvider,
		Label(Text("Name")),
		core.InputControl("", Name("name"), Required()),
		Label(Text("Type")),
		core.SelectControl("", Name("test_type"),
			Option(Value("not_null"), Text("not_null")),
			Option(Value("unique"), Text("unique")),
			Option(Value("accepted_values"), Text("accepted_values")),
			Option(Value("relationships"), Text("relationships")),
			Option(Value("custom_sql"), Text("custom_sql")),
		),
		Label(Text("Column")),
		core.InputControl("", Name("column")),
		Label(Text("Values (accepted_values, comma separated)")),
		core.InputControl("", Name("values")),
		Label(Text("To Model (relationships)")),
		core.InputControl("", Name("to_model")),
		Label(Text("To Column (relationships)")),
		core.InputControl("", Name("to_column")),
		Label(Text("SQL (custom_sql)")),
		core.TextareaControl("min-h-32 font-mono text-xs", Name("test_sql")),
	)
}

func modelsDAGPage(d modelsDAGPageData) Node {
	tierNodes := make([]Node, 0, len(d.Tiers))
	for i := range d.Tiers {
		tier := d.Tiers[i]
		rows := make([]Node, 0, len(tier.Nodes))
		for j := range tier.Nodes {
			node := tier.Nodes[j]
			rows = append(rows, Tr(
				Td(A(Href(node.URL), Class("font-medium text-[var(--fgColor-accent)]"), Text(node.Name))),
				Td(Text(node.Materialized)),
				Td(Text(node.DependsOn)),
			))
		}
		tierNodes = append(tierNodes,
			core.SectionSurface(
				core.SectionHeader(tier.Label, ""),
				core.TableContainer("",
					core.DataTable("",
						THead(Tr(Th(Text("Model")), Th(Text("Materialization")), Th(Text("Depends on")))),
						TBody(Group(rows)),
					),
				),
			),
		)
	}
	if len(tierNodes) == 0 {
		tierNodes = append(tierNodes, core.SectionSurface(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No model DAG available.")), core.SecondaryLink("/ui/models", "", Text("Back to models"))))
	}
	title := "Model DAG"
	if d.ProjectName != nil {
		title += ": " + *d.ProjectName
	}
	content := append([]Node{
		core.PageHeader("Build", title, "Inspect dependency tiers without switching back to a separate hub.", core.SecondaryLink("/ui/models", "", Text("Back to models"))),
	}, tierNodes...)
	return core.AppPage(title, "models", d.Principal, content...)
}

func modelRunsListPage(d modelRunsListPageData) Node {
	table := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No model runs found.")))
	if len(d.Rows) > 0 {
		rows := make([]Node, 0, len(d.Rows))
		for i := range d.Rows {
			row := d.Rows[i]
			rows = append(rows, Tr(
				Td(A(Href(row.URL), Class("font-medium text-[var(--fgColor-accent)]"), Text(row.ID))),
				Td(Text(row.Status)),
				Td(Text(row.TriggerType)),
				Td(Text(row.TriggeredBy)),
				Td(Text(row.Target)),
				Td(Text(row.Selector)),
				Td(Text(row.CreatedAtText)),
			))
		}
		table = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Scope("col"), Text("Run ID")), Th(Scope("col"), Text("Status")), Th(Scope("col"), Text("Trigger")), Th(Scope("col"), Text("By")), Th(Scope("col"), Text("Target")), Th(Scope("col"), Text("Selector")), Th(Scope("col"), Text("Created")))),
				TBody(Group(rows)),
			),
		)
	}
	return core.AppPage("Model Runs", "models", d.Principal,
		core.ListPageLayout(
			core.ListPageHeader("Model runs", "Review execution history separately from model authoring.", core.SecondaryLink("/ui/models/dag", "", Text("View DAG"))),
			core.ListPageBody(
				table,
				core.ListPagination("/ui/models/runs", d.Page, d.Total),
			),
		),
	)
}

func modelRunDetailPage(d modelRunDetailPageData) Node {
	stepRows := make([]Node, 0, len(d.Steps))
	for i := range d.Steps {
		step := d.Steps[i]
		testSummary := "-"
		if len(step.TestResults) > 0 {
			testSummary = step.TestResults[0].Status + " (" + strconv.Itoa(len(step.TestResults)) + " results)"
		}
		stepRows = append(stepRows, Tr(
			Td(Text(step.ModelName)),
			Td(Text(step.Status)),
			Td(Text(step.Tier)),
			Td(Text(step.RowsAffected)),
			Td(Text(step.StartedAtText)),
			Td(Text(step.FinishedAtText)),
			Td(Text(testSummary)),
			Td(Text(step.ErrorText)),
		))
	}
	return core.AppPage("Model Run: "+d.RunID, "models", d.Principal,
		core.ResultPageLayout("Build", "Model run: "+d.RunID, "Execution details are split into a report-style main column and a compact rail for control actions.",
			core.DetailLayout(
				core.DetailMain(
					core.SectionSurface(
						core.SectionHeader("Steps", "Inspect execution and test summaries by model step."),
						core.TableContainer("",
							core.DataTable("",
								THead(Tr(Th(Text("Model")), Th(Text("Status")), Th(Text("Tier")), Th(Text("Rows")), Th(Text("Started")), Th(Text("Finished")), Th(Text("Tests")), Th(Text("Error")))),
								TBody(Group(stepRows)),
							),
						),
					),
					core.SectionSurface(
						core.SectionHeader("Compile manifest", "Manifest output reads as a result report instead of another generic card."),
						Pre(Class("overflow-x-auto rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3 text-sm"), Text(d.CompileManifest)),
					),
				),
				core.DetailRail(
					core.DetailRailCard("Run summary", "Key execution facts stay visible without pushing report content down the page.",
						core.KeyValueGrid([][2]string{
							{"Status", d.Status},
							{"Trigger type", d.TriggerType},
							{"Triggered by", d.TriggeredBy},
							{"Target", d.TargetCatalog + "." + d.TargetSchema},
							{"Selector", d.Selector},
							{"Variables", emptyDash(d.Variables)},
							{"Created", d.CreatedAtText},
							{"Started", d.StartedAtText},
							{"Finished", d.FinishedAtText},
							{"Error", emptyDash(d.ErrorText)},
						}),
					),
					core.DetailRailCard("Actions", "Control actions stay secondary to the run report.",
						Form(Method("post"), Action(d.CancelURL), d.CSRFFieldProvider(), core.SecondaryButton("", Type("submit"), Text("Cancel run"))),
					),
				),
			),
		),
	)
}

func modelSourceFreshnessPage(d modelSourceFreshnessPageData) Node {
	resultNode := Node(nil)
	if d.Result != nil {
		resultNode = core.SectionSurface(
			core.SectionHeader("Result", "Freshness checks render as a report block below the input form."),
			core.KeyValueGrid([][2]string{
				{"Fresh", boolLabel(d.Result.IsFresh)},
				{"Source", d.Result.SourceSchema + "." + d.Result.SourceTable},
				{"Timestamp column", d.Result.TimestampCol},
				{"Last loaded at", formatTimePtr(d.Result.LastLoadedAt)},
				{"Stale since", formatTimePtr(d.Result.StaleSince)},
			}),
		)
	}
	return core.AppPage("Source Freshness", "models", d.Principal,
		core.ResultPageLayout("Build", "Source freshness", "Use a focused input surface, then review the resulting freshness report below.",
			core.SectionSurface(
				core.SectionHeader("Check source freshness", "Run a one-off freshness check for a source table."),
				Form(Class("grid gap-3"), Method("post"), Action("/ui/models/source-freshness"),
					d.CSRFFieldProvider(),
					Label(Text("Source schema")),
					core.InputControl("", Name("source_schema"), Value(d.SourceSchema), Required()),
					Label(Text("Source table")),
					core.InputControl("", Name("source_table"), Value(d.SourceTable), Required()),
					Label(Text("Timestamp column")),
					core.InputControl("", Name("timestamp_column"), Value(d.TimestampColumn)),
					Label(Text("Max lag seconds")),
					core.InputControl("", Name("max_lag_seconds"), Value(defaultString(d.MaxLagSecondsText, "3600")), Required()),
					core.PrimaryButton("", Type("submit"), Text("Check freshness")),
				),
			),
			resultNode,
		),
	)
}

func modelFormPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	nodes = append(nodes, Div(Class("mt-4"), core.PrimaryButton("", Type("submit"), Text("Save"))))
	return core.AppPage(title, "models", principal,
		core.FormPageLayout("Build", title, "Model authoring uses one primary form surface so the page intent is obvious at a glance.",
			Form(Class("grid gap-3"), Method("post"), Action(action), Group(nodes)),
		),
	)
}

func optionSelected(value, current string) Node {
	if value == current {
		return Option(Value(value), Selected(), Text(value))
	}
	return Option(Value(value), Text(value))
}

func emptyDash(v string) string {
	if v == "" {
		return "-"
	}
	return v
}
