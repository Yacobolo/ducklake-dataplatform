package models

import (
	"strconv"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

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

func modelsListPage(principal domain.ContextPrincipal, rows []modelsListRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No models available.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				Td(A(Href(row.DetailURL), Class("font-medium text-[var(--fgColor-accent)]"), Text(row.ModelName))),
				Td(Text(row.Materialized)),
				Td(Text(strconv.Itoa(row.Dependencies))),
				Td(Text(row.UpdatedAtText)),
			))
		}
		table = Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Model")), Th(Text("Materialization")), Th(Text("Dependencies")), Th(Text("Updated")))),
			TBody(Group(tableRows)),
		))
	}
	return core.AppPage("Models", "models", principal,
		sectionHeader("Models", "Manage dbt-style models and tests.", "/ui/models/new", "New model"),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" models. Total: "+strconv.FormatInt(total, 10))),
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
				Td(Class("text-right"), Form(Method("post"), Action(t.DeleteURL), d.CSRFFieldProvider(), core.DangerButton("small", Type("submit"), Text("Delete")))),
			))
		}
		tests = Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Column")), Th(Class("text-right"), Text("Actions")))),
			TBody(Group(rows)),
		))
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
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H2(Class("mt-0 text-lg font-semibold"), Text(d.QualifiedName)),
			P(Class("m-0 text-sm"), Strong(Text("Materialization: ")), Text(d.Materialization)),
			P(Class("m-0 text-sm"), Strong(Text("Owner: ")), Text(d.Owner)),
			P(Class("m-0 text-sm"), Strong(Text("Depends on: ")), Text(d.DependsOn)),
			P(Class("m-0 text-sm"), Strong(Text("Config: ")), Text(d.ConfigText)),
			Div(Class("mt-1 flex flex-wrap items-center gap-2 [&_form]:m-0 [&_form]:inline-flex"),
				core.SecondaryLink(d.EditURL, "", Text("Edit")),
				core.PrimaryLink(d.NewTestURL, "", Text("New test")),
				core.SecondaryLink(d.RunsURL, "", Text("Runs")),
				core.SecondaryLink(d.DAGURL, "", Text("DAG")),
				core.SecondaryLink(d.SourceFreshnessURL, "", Text("Source freshness")),
				Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldProvider(), core.DangerButton("", Type("submit"), Text("Delete"))),
			),
		),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H3(Class("mt-0 text-lg font-semibold"), Text("Freshness")),
			freshness,
			Form(Method("post"), Action(d.FreshnessURL), d.CSRFFieldProvider(), core.SecondaryButton("", Type("submit"), Text("Refresh freshness status"))),
		),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H3(Class("mt-0 text-lg font-semibold"), Text("Trigger model run")),
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
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H3(Class("mt-0 text-lg font-semibold"), Text("Cancel model run")),
			Form(Class("grid gap-3"), Method("post"), Action(d.CancelRunURL),
				d.CSRFFieldProvider(),
				Label(Text("Run ID to cancel")),
				core.InputControl("", Name("run_id")),
				core.SecondaryButton("", Type("submit"), Text("Cancel model run")),
			),
		),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H3(Class("mt-0 text-lg font-semibold"), Text("SQL")),
			Pre(Class("overflow-x-auto rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3 text-sm"), Text(d.SQL)),
		),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H3(Class("mt-0 text-lg font-semibold"), Text("Tests")),
			tests,
		),
	)
}

func modelsNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return modelFormPage(principal, "New Model", "/ui/models", csrfFieldProvider,
		Label(Text("Project")),
		core.InputControl("", Name("project_name"), Required()),
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
			Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
				H2(Class("mt-0 text-lg font-semibold"), Text(tier.Label)),
				Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
					THead(Tr(Th(Text("Model")), Th(Text("Materialization")), Th(Text("Depends on")))),
					TBody(Group(rows)),
				)),
			),
		)
	}
	if len(tierNodes) == 0 {
		tierNodes = append(tierNodes, Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No model DAG available.")), core.SecondaryLink("/ui/models", "", Text("Back to models"))))
	}
	title := "Model DAG"
	if d.ProjectName != nil {
		title += ": " + *d.ProjectName
	}
	content := append([]Node{
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), core.SecondaryLink("/ui/models", "", Text("Back to models"))),
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
		table = Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
			THead(Tr(Th(Text("Run ID")), Th(Text("Status")), Th(Text("Trigger")), Th(Text("By")), Th(Text("Target")), Th(Text("Selector")), Th(Text("Created")))),
			TBody(Group(rows)),
		))
	}
	return core.AppPage("Model Runs", "models", d.Principal,
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), core.SecondaryLink("/ui/models/dag", "", Text("View DAG"))),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(d.Page.MaxResults)+" runs. Total: "+strconv.FormatInt(d.Total, 10))),
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
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			P(Class("m-0 text-sm"), Strong(Text("Status: ")), Text(d.Status)),
			P(Class("m-0 text-sm"), Strong(Text("Trigger type: ")), Text(d.TriggerType)),
			P(Class("m-0 text-sm"), Strong(Text("Triggered by: ")), Text(d.TriggeredBy)),
			P(Class("m-0 text-sm"), Strong(Text("Target: ")), Text(d.TargetCatalog+"."+d.TargetSchema)),
			P(Class("m-0 text-sm"), Strong(Text("Selector: ")), Text(d.Selector)),
			P(Class("m-0 text-sm"), Strong(Text("Variables: ")), Text(d.Variables)),
			P(Class("m-0 text-sm"), Strong(Text("Created: ")), Text(d.CreatedAtText)),
			P(Class("m-0 text-sm"), Strong(Text("Started: ")), Text(d.StartedAtText)),
			P(Class("m-0 text-sm"), Strong(Text("Finished: ")), Text(d.FinishedAtText)),
			P(Class("m-0 text-sm"), Strong(Text("Error: ")), Text(d.ErrorText)),
			Form(Method("post"), Action(d.CancelURL), d.CSRFFieldProvider(), core.SecondaryButton("", Type("submit"), Text("Cancel run"))),
		),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), H2(Class("mt-0 text-lg font-semibold"), Text("Compile manifest")), Pre(Class("overflow-x-auto rounded-lg border border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] p-3 text-sm"), Text(d.CompileManifest))),
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), H2(Class("mt-0 text-lg font-semibold"), Text("Steps")),
			Div(Class("overflow-x-auto"), Table(Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Model")), Th(Text("Status")), Th(Text("Tier")), Th(Text("Rows")), Th(Text("Started")), Th(Text("Finished")), Th(Text("Tests")), Th(Text("Error")))),
				TBody(Group(stepRows)),
			)),
		),
	)
}

func modelSourceFreshnessPage(d modelSourceFreshnessPageData) Node {
	resultNode := Node(nil)
	if d.Result != nil {
		resultNode = Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H2(Class("mt-0 text-lg font-semibold"), Text("Result")),
			P(Text("Fresh: "+boolLabel(d.Result.IsFresh))),
			P(Text("Source: "+d.Result.SourceSchema+"."+d.Result.SourceTable)),
			P(Text("Timestamp column: "+d.Result.TimestampCol)),
			P(Text("Last loaded at: "+formatTimePtr(d.Result.LastLoadedAt))),
			P(Text("Stale since: "+formatTimePtr(d.Result.StaleSince))),
		)
	}
	return core.AppPage("Source Freshness", "models", d.Principal,
		Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
			H2(Class("mt-0 text-lg font-semibold"), Text("Check source freshness")),
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
	)
}

func modelFormPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	nodes = append(nodes, Div(Class("mt-4"), core.PrimaryButton("", Type("submit"), Text("Save"))))
	return core.AppPage(title, "models", principal, Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"), Form(Class("grid gap-3"), Method("post"), Action(action), Group(nodes))))
}

func sectionHeader(title, copy, href, action string) Node {
	return Div(Class("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]"),
		Div(Class("flex flex-wrap items-start justify-between gap-3"),
			Div(H2(Class("m-0 text-xl font-semibold"), Text(title)), P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(copy))),
			core.PrimaryLink(href, "", Text(action)),
		),
	)
}

func optionSelected(value, current string) Node {
	if value == current {
		return Option(Value(value), Selected(), Text(value))
	}
	return Option(Value(value), Text(value))
}
