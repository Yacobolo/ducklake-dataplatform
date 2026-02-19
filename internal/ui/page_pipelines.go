package ui

import (
	"duck-demo/internal/domain"

	gomponents "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	html "maragu.dev/gomponents/html"
)

type pipelinesListRowData struct {
	Filter   string
	Name     string
	URL      string
	Paused   string
	Schedule string
	Updated  string
}

func pipelinesListPage(principal domain.ContextPrincipal, rows []pipelinesListRowData, page domain.PageRequest, total int64) gomponents.Node {
	tableRows := make([]gomponents.Node, 0, len(rows))
	for i := range rows {
		row := rows[i]
		tableRows = append(tableRows, html.Tr(data.Show(containsExpr(row.Filter)), html.Td(html.A(html.Href(row.URL), gomponents.Text(row.Name))), html.Td(gomponents.Text(row.Paused)), html.Td(gomponents.Text(row.Schedule)), html.Td(gomponents.Text(row.Updated))))
	}
	return appPage(
		"Pipelines",
		"pipelines",
		principal,
		html.Div(html.Class(cardClass()), html.A(html.Href("/ui/pipelines/new"), gomponents.Text("+ New pipeline"))),
		html.Div(data.Signals(map[string]any{"q": ""}), html.Div(html.Class(cardClass()), html.Label(gomponents.Text("Quick filter")), html.Input(html.Type("text"), data.Bind("q"), html.Placeholder("Filter by pipeline name"))), html.Div(html.Class(cardClass("table-wrap")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Paused")), html.Th(gomponents.Text("Schedule")), html.Th(gomponents.Text("Updated")))), html.TBody(gomponents.Group(tableRows))))),
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
	CSRFFieldFunc func() gomponents.Node
}

func pipelineDetailPage(d pipelineDetailPageData) gomponents.Node {
	jobRows := make([]gomponents.Node, 0, len(d.Jobs))
	for i := range d.Jobs {
		j := d.Jobs[i]
		jobRows = append(jobRows, html.Tr(html.Td(gomponents.Text(j.Name)), html.Td(gomponents.Text(j.JobType)), html.Td(gomponents.Text(j.Selector)), html.Td(gomponents.Text(j.Notebook)), html.Td(html.Form(html.Method("post"), html.Action(j.DeleteURL), d.CSRFFieldFunc(), html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Delete"))))))
	}
	runRows := make([]gomponents.Node, 0, len(d.Runs))
	for i := range d.Runs {
		r := d.Runs[i]
		runRows = append(runRows, html.Tr(html.Td(gomponents.Text(r.ID)), html.Td(gomponents.Text(r.Status)), html.Td(gomponents.Text(r.Trigger)), html.Td(gomponents.Text(r.Started)), html.Td(gomponents.Text(r.Finished)), html.Td(html.Form(html.Method("post"), html.Action(r.CancelURL), d.CSRFFieldFunc(), html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Cancel"))))))
	}
	return appPage(
		"Pipeline: "+d.Name,
		"pipelines",
		d.Principal,
		html.Div(html.Class(cardClass()), html.P(gomponents.Text("Created by: "+d.CreatedBy)), html.P(gomponents.Text("Concurrency: "+d.Concurrency)), html.P(gomponents.Text("Schedule: "+d.Schedule)), html.A(html.Href(d.EditURL), gomponents.Text("Edit pipeline")), html.Form(html.Method("post"), html.Action(d.DeleteURL), d.CSRFFieldFunc(), html.Button(html.Type("submit"), html.Class(secondaryButtonClass()), gomponents.Text("Delete pipeline"))), html.Form(html.Method("post"), html.Action(d.TriggerURL), d.CSRFFieldFunc(), html.Button(html.Type("submit"), html.Class(primaryButtonClass()), gomponents.Text("Trigger run"))), html.A(html.Href(d.NewJobURL), gomponents.Text("+ New job"))),
		html.Div(html.Class(cardClass("table-wrap")), html.H2(gomponents.Text("Jobs")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Type")), html.Th(gomponents.Text("Selector")), html.Th(gomponents.Text("Notebook")), html.Th(gomponents.Text("Actions")))), html.TBody(gomponents.Group(jobRows)))),
		html.Div(html.Class(cardClass("table-wrap")), html.H2(gomponents.Text("Recent runs")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Run ID")), html.Th(gomponents.Text("Status")), html.Th(gomponents.Text("Trigger")), html.Th(gomponents.Text("Started")), html.Th(gomponents.Text("Finished")), html.Th(gomponents.Text("Actions")))), html.TBody(gomponents.Group(runRows)))),
	)
}
