package legacy

import (
	"duck-demo/internal/domain"
	"strconv"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type modelsListRowData struct {
	FilterValue   string
	DetailURL     string
	ModelName     string
	Materialized  string
	Dependencies  string
	UpdatedAtText string
}

type modelsListPageData struct {
	Principal domain.ContextPrincipal
	Rows      []modelsListRowData
	Page      domain.PageRequest
	Total     int64
}

func modelsListPage(d modelsListPageData) Node {
	rows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		rows = append(rows, Tr(
			data.Show(containsExpr(row.FilterValue)),
			Td(A(Href(row.DetailURL), Text(row.ModelName))),
			Td(statusLabel(row.Materialized, "accent")),
			Td(Text(row.Dependencies)),
			Td(Text(row.UpdatedAtText)),
		))
	}
	tableNode := Node(emptyStateCard("No models available.", "New model", "/ui/models/new"))
	if len(rows) > 0 {
		tableNode = Div(Class(cardClass(tableWrapClass())), Table(Class(dataTableClass()), THead(Tr(Th(Text("Model")), Th(Text("Materialization")), Th(Text("Dependencies")), Th(Text("Updated")))), TBody(Group(rows))))
	}

	return appPage(
		"Models",
		"models",
		d.Principal,
		pageToolbar("/ui/models/new", "New model"),
		quickFilterCard("Filter by project.model or materialization"),
		tableNode,
		paginationCard("/ui/models", d.Page, d.Total),
	)
}

type modelTestRowData struct {
	Name      string
	TestType  string
	Column    string
	DeleteURL string
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

func modelsDetailPage(d modelsDetailPageData) Node {
	testRows := make([]Node, 0, len(d.Tests))
	for i := range d.Tests {
		t := d.Tests[i]
		testRows = append(testRows, Tr(
			Td(Text(t.Name)),
			Td(Text(t.TestType)),
			Td(Text(t.Column)),
			Td(Class("text-right"), actionMenu("Actions", actionMenuPost(t.DeleteURL, "Delete test", d.CSRFFieldProvider, true))),
		))
	}

	freshnessSummary := Node(P(Class(mutedClass()), Text("No freshness policy configured.")))
	if d.FreshnessStatus != nil {
		freshnessSummary = Div(
			P(Text("Freshness: "), statusLabel(boolLabel(d.FreshnessStatus.IsFresh), boolTone(d.FreshnessStatus.IsFresh))),
			P(Text("Last successful run: "+formatTimePtr(d.FreshnessStatus.LastRunAt))),
			P(Text("Max lag seconds: "+int64Text(d.FreshnessStatus.MaxLagSeconds))),
			P(Text("Stale since: "+formatTimePtr(d.FreshnessStatus.StaleSince))),
		)
	}

	return appPage(
		"Model: "+d.QualifiedName,
		"models",
		d.Principal,
		Div(
			Class(cardClass()),
			P(Text("Materialization: "), statusLabel(d.Materialization, "accent")),
			P(Text("Owner: "+d.Owner)),
			P(Text("Depends on: "+d.DependsOn)),
			P(Text("Config: "+d.ConfigText)),
			Div(Class(buttonRowClass()),
				A(Href(d.EditURL), Class(secondaryButtonClass()), Text("Edit")),
				A(Href(d.NewTestURL), Class(primaryButtonClass()), Text("New test")),
				A(Href(d.RunsURL), Class(secondaryButtonClass()), Text("Runs")),
				A(Href(d.DAGURL), Class(secondaryButtonClass()), Text("DAG")),
				A(Href(d.SourceFreshnessURL), Class(secondaryButtonClass()), Text("Source freshness")),
				Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldProvider(), Button(Type("submit"), Class(dangerButtonClass()), Text("Delete"))),
			),
			Div(
				Class(cardClass()),
				H2(Text("Freshness")),
				freshnessSummary,
				Form(
					Method("post"),
					Action(d.FreshnessURL),
					d.CSRFFieldProvider(),
					Button(Type("submit"), Class(secondaryButtonClass()), Text("Refresh freshness status")),
				),
			),
			Form(
				Method("post"),
				Action(d.TriggerRunURL),
				d.CSRFFieldProvider(),
				Input(Type("hidden"), Name("project_name"), Value(d.TriggerProject)),
				Input(Type("hidden"), Name("model_name"), Value(d.TriggerModel)),
				Label(Text("Target catalog")),
				Input(Name("target_catalog"), Class(formControlClass()), Required()),
				Label(Text("Target schema")),
				Input(Name("target_schema"), Class(formControlClass()), Required()),
				Label(Text("Selector")),
				Input(Name("selector"), Class(formControlClass()), Value(d.DefaultSelector)),
				Button(Type("submit"), Class(primaryButtonClass()), Text("Trigger model run")),
			),
			Form(
				Method("post"),
				Action(d.CancelRunURL),
				d.CSRFFieldProvider(),
				Label(Text("Run ID to cancel")),
				Input(Name("run_id"), Class(formControlClass())),
				Button(Type("submit"), Class(secondaryButtonClass()), Text("Cancel model run")),
			),
		),
		Div(Class(cardClass()), H2(Text("SQL")), Pre(Text(d.SQL))),
		Div(Class(cardClass(tableWrapClass())), H2(Text("Tests")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Column")), Th(Class("text-right"), Text("Actions")))), TBody(Group(testRows)))),
	)
}

func modelsNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Model", "models", "/ui/models", csrfFieldProvider,
		Label(Text("Project")),
		Input(Name("project_name"), Required()),
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Materialization")),
		Select(Name("materialization"), Option(Value("VIEW"), Text("VIEW")), Option(Value("TABLE"), Text("TABLE")), Option(Value("INCREMENTAL"), Text("INCREMENTAL")), Option(Value("EPHEMERAL"), Text("EPHEMERAL"))),
		Label(Text("Description")),
		Textarea(Name("description")),
		Label(Text("Tags (comma separated)")),
		Input(Name("tags")),
		Label(Text("SQL")),
		Textarea(Name("sql"), Required()),
	)
}

func modelsEditPage(principal domain.ContextPrincipal, projectName, modelName string, model *domain.Model, csrfFieldProvider func() Node) Node {
	return formPage(principal, "Edit Model", "models", "/ui/models/"+projectName+"/"+modelName+"/update", csrfFieldProvider,
		Label(Text("Materialization")),
		Select(Name("materialization"), optionSelected("VIEW", model.Materialization), optionSelected("TABLE", model.Materialization), optionSelected("INCREMENTAL", model.Materialization), optionSelected("EPHEMERAL", model.Materialization)),
		Label(Text("Description")),
		Textarea(Name("description"), Text(model.Description)),
		Label(Text("Tags (comma separated)")),
		Input(Name("tags"), Value(csvValues(model.Tags))),
		Label(Text("SQL")),
		Textarea(Name("sql"), Text(model.SQL), Required()),
	)
}

func modelTestsNewPage(principal domain.ContextPrincipal, projectName, modelName string, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Model Test", "models", "/ui/models/"+projectName+"/"+modelName+"/tests", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Type")),
		Select(Name("test_type"), Option(Value("not_null"), Text("not_null")), Option(Value("unique"), Text("unique")), Option(Value("accepted_values"), Text("accepted_values")), Option(Value("relationships"), Text("relationships")), Option(Value("custom_sql"), Text("custom_sql"))),
		Label(Text("Column")),
		Input(Name("column")),
		Label(Text("Values (accepted_values, comma separated)")),
		Input(Name("values")),
		Label(Text("To Model (relationships)")),
		Input(Name("to_model")),
		Label(Text("To Column (relationships)")),
		Input(Name("to_column")),
		Label(Text("SQL (custom_sql)")),
		Textarea(Name("test_sql")),
	)
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

func modelsDAGPage(d modelsDAGPageData) Node {
	tierNodes := make([]Node, 0, len(d.Tiers))
	for i := range d.Tiers {
		tier := d.Tiers[i]
		rows := make([]Node, 0, len(tier.Nodes))
		for j := range tier.Nodes {
			node := tier.Nodes[j]
			rows = append(rows, Tr(
				Td(A(Href(node.URL), Text(node.Name))),
				Td(statusLabel(node.Materialized, "accent")),
				Td(Text(node.DependsOn)),
			))
		}
		tierNodes = append(tierNodes,
			Div(
				Class(cardClass(tableWrapClass())),
				H2(Text(tier.Label)),
				Table(Class(dataTableClass()), THead(Tr(Th(Text("Model")), Th(Text("Materialization")), Th(Text("Depends on")))), TBody(Group(rows))),
			),
		)
	}
	if len(tierNodes) == 0 {
		tierNodes = append(tierNodes, emptyStateCard("No model DAG available.", "Back to models", "/ui/models"))
	}
	title := "Model DAG"
	if d.ProjectName != nil {
		title += ": " + *d.ProjectName
	}
	return appPage(title, "models", d.Principal, append([]Node{pageToolbar("/ui/models", "Back to models")}, tierNodes...)...)
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

func modelRunsListPage(d modelRunsListPageData) Node {
	rows := make([]Node, 0, len(d.Rows))
	for i := range d.Rows {
		row := d.Rows[i]
		rows = append(rows, Tr(
			Td(A(Href(row.URL), Text(row.ID))),
			Td(statusLabel(row.Status, modelRunTone(row.Status))),
			Td(Text(row.TriggerType)),
			Td(Text(row.TriggeredBy)),
			Td(Text(row.Target)),
			Td(Text(row.Selector)),
			Td(Text(row.CreatedAtText)),
		))
	}
	tableNode := Node(emptyStateCard("No model runs found.", "View DAG", "/ui/models/dag"))
	if len(rows) > 0 {
		tableNode = Div(Class(cardClass(tableWrapClass())), Table(Class(dataTableClass()), THead(Tr(Th(Text("Run ID")), Th(Text("Status")), Th(Text("Trigger")), Th(Text("By")), Th(Text("Target")), Th(Text("Selector")), Th(Text("Created")))), TBody(Group(rows))))
	}
	return appPage(
		"Model Runs",
		"models",
		d.Principal,
		pageToolbar("/ui/models/dag", "View DAG"),
		tableNode,
		paginationCard("/ui/models/runs", d.Page, d.Total),
	)
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
			Td(statusLabel(step.Status, modelRunTone(step.Status))),
			Td(Text(step.Tier)),
			Td(Text(step.RowsAffected)),
			Td(Text(step.StartedAtText)),
			Td(Text(step.FinishedAtText)),
			Td(Text(testSummary)),
			Td(Text(step.ErrorText)),
		))
	}
	return appPage(
		"Model Run: "+d.RunID,
		"models",
		d.Principal,
		Div(
			Class(cardClass()),
			P(Text("Status: "), statusLabel(d.Status, modelRunTone(d.Status))),
			P(Text("Trigger type: "+d.TriggerType)),
			P(Text("Triggered by: "+d.TriggeredBy)),
			P(Text("Target: "+d.TargetCatalog+"."+d.TargetSchema)),
			P(Text("Selector: "+d.Selector)),
			P(Text("Variables: "+d.Variables)),
			P(Text("Created: "+d.CreatedAtText)),
			P(Text("Started: "+d.StartedAtText)),
			P(Text("Finished: "+d.FinishedAtText)),
			P(Text("Error: "+d.ErrorText)),
			Form(Method("post"), Action(d.CancelURL), d.CSRFFieldProvider(), Button(Type("submit"), Class(secondaryButtonClass()), Text("Cancel run"))),
		),
		Div(Class(cardClass()), H2(Text("Compile manifest")), Pre(Text(d.CompileManifest))),
		Div(Class(cardClass(tableWrapClass())), H2(Text("Steps")), Table(Class(dataTableClass()), THead(Tr(Th(Text("Model")), Th(Text("Status")), Th(Text("Tier")), Th(Text("Rows")), Th(Text("Started")), Th(Text("Finished")), Th(Text("Tests")), Th(Text("Error")))), TBody(Group(stepRows)))),
	)
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

func modelSourceFreshnessPage(d modelSourceFreshnessPageData) Node {
	resultNode := Node(nil)
	if d.Result != nil {
		resultNode = Div(
			Class(cardClass()),
			H2(Text("Result")),
			P(Text("Fresh: "), statusLabel(boolLabel(d.Result.IsFresh), boolTone(d.Result.IsFresh))),
			P(Text("Source: "+d.Result.SourceSchema+"."+d.Result.SourceTable)),
			P(Text("Timestamp column: "+d.Result.TimestampCol)),
			P(Text("Last loaded at: "+formatTimePtr(d.Result.LastLoadedAt))),
			P(Text("Stale since: "+formatTimePtr(d.Result.StaleSince))),
		)
	}
	return appPage(
		"Source Freshness",
		"models",
		d.Principal,
		Div(
			Class(cardClass()),
			H2(Text("Check source freshness")),
			Form(
				Method("post"),
				Action("/ui/models/source-freshness"),
				d.CSRFFieldProvider(),
				Label(Text("Source schema")),
				Input(Name("source_schema"), Value(d.SourceSchema), Required()),
				Label(Text("Source table")),
				Input(Name("source_table"), Value(d.SourceTable), Required()),
				Label(Text("Timestamp column")),
				Input(Name("timestamp_column"), Value(d.TimestampColumn)),
				Label(Text("Max lag seconds")),
				Input(Name("max_lag_seconds"), Value(defaultString(d.MaxLagSecondsText, "3600")), Required()),
				Button(Type("submit"), Class(primaryButtonClass()), Text("Check freshness")),
			),
		),
		resultNode,
	)
}

func modelRunTone(status string) string {
	switch status {
	case domain.ModelRunStatusSuccess:
		return "success"
	case domain.ModelRunStatusFailed, domain.ModelRunStatusCancelled:
		return "danger"
	case domain.ModelRunStatusRunning, domain.ModelRunStatusPending:
		return "attention"
	default:
		return "neutral"
	}
}

func int64Text(v int64) string {
	return strconv.FormatInt(v, 10)
}

func defaultString(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func boolTone(v bool) string {
	if v {
		return "success"
	}
	return "danger"
}
