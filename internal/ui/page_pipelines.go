package ui

import (
	"strconv"

	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type pipelinesListRowData struct {
	Filter   string
	Name     string
	URL      string
	Paused   string
	Schedule string
	Updated  string
}

func pipelinesListPage(principal domain.ContextPrincipal, rows []pipelinesListRowData, page domain.PageRequest, total int64) Node {
	tableRows := make([]Node, 0, len(rows))
	for i := range rows {
		row := rows[i]
		tone := "severe"
		if row.Paused == "false" {
			tone = "success"
		}
		tableRows = append(tableRows, Tr(data.Show(containsExpr(row.Filter)), Td(A(Href(row.URL), Text(row.Name))), Td(statusLabel(row.Paused, tone)), Td(Text(row.Schedule)), Td(Text(row.Updated))))
	}
	tableNode := Node(emptyStateCard("No pipelines yet.", "New pipeline", "/ui/pipelines/new"))
	if len(tableRows) > 0 {
		tableNode = Div(Class(cardClass("table-wrap")), Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("Paused")), Th(Text("Schedule")), Th(Text("Updated")))), TBody(Group(tableRows))))
	}
	return appPage(
		"Pipelines",
		"pipelines",
		principal,
		pageToolbar("/ui/pipelines/new", "New pipeline"),
		quickFilterCard("Filter by pipeline name"),
		tableNode,
		paginationCard("/ui/pipelines", page, total),
	)
}

type pipelineJobRowData struct {
	Name      string
	JobType   string
	Selector  string
	Notebook  string
	DeleteURL string
}

type pipelineRunRowData struct {
	ID        string
	Status    string
	Trigger   string
	Started   string
	Finished  string
	CancelURL string
}

type pipelineDetailPageData struct {
	Principal     domain.ContextPrincipal
	Name          string
	CreatedBy     string
	Concurrency   string
	Schedule      string
	EditURL       string
	DeleteURL     string
	TriggerURL    string
	NewJobURL     string
	Jobs          []pipelineJobRowData
	Runs          []pipelineRunRowData
	CSRFFieldFunc func() Node
}

func pipelineDetailPage(d pipelineDetailPageData) Node {
	jobRows := make([]Node, 0, len(d.Jobs))
	for i := range d.Jobs {
		j := d.Jobs[i]
		jobRows = append(jobRows, Tr(Td(Text(j.Name)), Td(statusLabel(j.JobType, "accent")), Td(Text(j.Selector)), Td(Text(j.Notebook)), Td(Class("text-right"), actionMenu("Actions", actionMenuPost(j.DeleteURL, "Delete job", d.CSRFFieldFunc, true)))))
	}
	runRows := make([]Node, 0, len(d.Runs))
	for i := range d.Runs {
		r := d.Runs[i]
		runRows = append(runRows, Tr(Td(Text(r.ID)), Td(statusLabel(r.Status, "attention")), Td(Text(r.Trigger)), Td(Text(r.Started)), Td(Text(r.Finished)), Td(Class("text-right"), actionMenu("Actions", actionMenuPost(r.CancelURL, "Cancel run", d.CSRFFieldFunc, true)))))
	}
	return appPage(
		"Pipeline: "+d.Name,
		"pipelines",
		d.Principal,
		Div(Class(cardClass()), P(Text("Created by: "+d.CreatedBy)), P(Text("Concurrency: "+d.Concurrency)), P(Text("Schedule: "+d.Schedule)), Div(Class("BtnGroup"), A(Href(d.EditURL), Class(secondaryButtonClass()), Text("Edit")), A(Href(d.NewJobURL), Class(secondaryButtonClass()), Text("New job")), Form(Method("post"), Action(d.TriggerURL), d.CSRFFieldFunc(), Button(Type("submit"), Class(primaryButtonClass()), Text("Trigger run"))), Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldFunc(), Button(Type("submit"), Class("btn btn-danger"), Text("Delete"))))),
		Div(Class(cardClass("table-wrap")), H2(Text("Jobs")), Table(Class("data-table"), THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Selector")), Th(Text("Notebook")), Th(Class("text-right"), Text("Actions")))), TBody(Group(jobRows)))),
		Div(Class(cardClass("table-wrap")), H2(Text("Recent runs")), Table(Class("data-table"), THead(Tr(Th(Text("Run ID")), Th(Text("Status")), Th(Text("Trigger")), Th(Text("Started")), Th(Text("Finished")), Th(Class("text-right"), Text("Actions")))), TBody(Group(runRows)))),
	)
}

func pipelinesNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Pipeline", "pipelines", "/ui/pipelines", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Description")),
		Textarea(Name("description")),
		Label(Text("Schedule Cron")),
		Input(Name("schedule_cron")),
		Label(Text("Concurrency Limit")),
		Input(Name("concurrency_limit"), Value("1")),
		Label(Text("Paused")),
		Input(Type("checkbox"), Name("is_paused")),
	)
}

func pipelinesEditPage(principal domain.ContextPrincipal, pipelineName string, pipeline *domain.Pipeline, csrfFieldProvider func() Node) Node {
	pausedInput := []Node{Type("checkbox"), Name("is_paused")}
	if pipeline.IsPaused {
		pausedInput = append(pausedInput, Checked())
	}

	return formPage(principal, "Edit Pipeline", "pipelines", "/ui/pipelines/"+pipelineName+"/update", csrfFieldProvider,
		Label(Text("Description")),
		Textarea(Name("description"), Text(pipeline.Description)),
		Label(Text("Schedule Cron")),
		Input(Name("schedule_cron"), Value(optionalStringValue(pipeline.ScheduleCron))),
		Label(Text("Concurrency Limit")),
		Input(Name("concurrency_limit"), Value(strconv.Itoa(pipeline.ConcurrencyLimit))),
		Label(Text("Paused")),
		Input(pausedInput...),
	)
}

func pipelineJobsNewPage(principal domain.ContextPrincipal, pipelineName string, csrfFieldProvider func() Node) Node {
	return formPage(principal, "New Pipeline Job", "pipelines", "/ui/pipelines/"+pipelineName+"/jobs", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Required()),
		Label(Text("Type")),
		Select(Name("job_type"), Option(Value("NOTEBOOK"), Text("NOTEBOOK")), Option(Value("MODEL_RUN"), Text("MODEL_RUN"))),
		Label(Text("Notebook ID")),
		Input(Name("notebook_id")),
		Label(Text("Model Selector")),
		Input(Name("model_selector")),
		Label(Text("Depends On (comma separated job names)")),
		Input(Name("depends_on")),
	)
}
