package pipelines

import (
	"strconv"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type pipelinesListRowData struct {
	Name     string
	URL      string
	Paused   bool
	Schedule string
	Updated  string
}

type pipelineJobRowData struct {
	Name      string
	JobType   string
	Selector  string
	Notebook  string
	DeleteURL string
}

type pipelineDetailPageData struct {
	Principal     domain.ContextPrincipal
	Name          string
	CreatedBy     string
	Concurrency   string
	Schedule      string
	EditURL       string
	DeleteURL     string
	NewJobURL     string
	Jobs          []pipelineJobRowData
	CSRFFieldFunc func() Node
}

func pipelinesListPage(principal domain.ContextPrincipal, rows []pipelinesListRowData, page domain.PageRequest, total int64) Node {
	table := Node(P(Class("text-xs text-muted"), Text("No pipelines yet.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				Td(A(Href(row.URL), Class("font-medium text-accent"), Text(row.Name))),
				Td(statusPill(boolLabel(row.Paused), pausedTone(row.Paused))),
				Td(Text(row.Schedule)),
				Td(Text(row.Updated)),
			))
		}
		table = Div(
			Class("overflow-x-auto"),
			Table(
				Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Name")), Th(Text("Paused")), Th(Text("Schedule")), Th(Text("Updated")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage(
		"Pipelines",
		"pipelines",
		principal,
		core.Card(
			Div(Class("mb-4 flex flex-wrap items-center justify-between gap-3"),
				Div(H2(Class("m-0 text-xl font-semibold"), Text("Pipelines")), P(Class("m-0 text-sm text-muted"), Text("Manage orchestrated jobs and schedules."))),
				core.PrimaryLink("/ui/pipelines/new", "", Text("New pipeline")),
			),
			table,
			P(Class("mt-4 text-sm text-muted"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" pipelines. Total: "+strconv.FormatInt(total, 10))),
		),
	)
}

func pipelineDetailPage(d pipelineDetailPageData) Node {
	jobTable := Node(P(Class("text-xs text-muted"), Text("No jobs defined yet.")))
	if len(d.Jobs) > 0 {
		rows := make([]Node, 0, len(d.Jobs))
		for i := range d.Jobs {
			job := d.Jobs[i]
			rows = append(rows, Tr(
				Td(Text(job.Name)),
				Td(statusPill(job.JobType, "accent")),
				Td(Text(emptyDash(job.Selector))),
				Td(Text(emptyDash(job.Notebook))),
				Td(Class("text-right"),
					Form(Method("post"), Action(job.DeleteURL), d.CSRFFieldFunc(),
						core.DangerButton("small", Type("submit"), Text("Delete")),
					),
				),
			))
		}
		jobTable = Div(
			Class("overflow-x-auto"),
			Table(
				Class("min-w-full text-left text-sm"),
				THead(Tr(Th(Text("Name")), Th(Text("Type")), Th(Text("Selector")), Th(Text("Notebook")), Th(Class("text-right"), Text("Actions")))),
				TBody(Group(rows)),
			),
		)
	}

	return core.AppPage(
		"Pipeline: "+d.Name,
		"pipelines",
		d.Principal,
		core.Card(
			Div(Class("flex flex-wrap items-start justify-between gap-3"),
				Div(
					H2(Class("m-0 text-xl font-semibold"), Text(d.Name)),
					P(Class("m-0 text-sm text-muted"), Text("Created by "+emptyDash(d.CreatedBy))),
				),
				core.ButtonGroup("mt-0",
					core.SecondaryLink(d.EditURL, "", Text("Edit")),
					core.SecondaryLink(d.NewJobURL, "", Text("New job")),
					Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldFunc(), core.DangerButton("", Type("submit"), Text("Delete pipeline"))),
				),
			),
			Dl(Class("mt-4 grid gap-3 sm:grid-cols-3"),
				metaRow("Schedule", d.Schedule),
				metaRow("Concurrency", d.Concurrency),
				metaRow("Jobs", strconv.Itoa(len(d.Jobs))),
			),
		),
		core.Card(
			H3(Class("mt-0 text-lg font-semibold"), Text("Jobs")),
			jobTable,
		),
	)
}

func pipelinesNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return pipelineFormPage(principal, "New Pipeline", "/ui/pipelines", csrfFieldProvider,
		Label(Text("Name")),
		core.InputControl("", Name("name"), Required()),
		Label(Text("Description")),
		core.TextareaControl("min-h-28", Name("description")),
		Label(Text("Schedule Cron")),
		core.InputControl("", Name("schedule_cron")),
		Label(Text("Concurrency Limit")),
		core.InputControl("", Name("concurrency_limit"), Value("1")),
		Label(Class("inline-flex items-center gap-2"), Input(Type("checkbox"), Name("is_paused")), Span(Text("Paused"))),
	)
}

func pipelinesEditPage(principal domain.ContextPrincipal, pipelineName string, pipeline *domain.Pipeline, csrfFieldProvider func() Node) Node {
	paused := []Node{Type("checkbox"), Name("is_paused")}
	if pipeline.IsPaused {
		paused = append(paused, Checked())
	}
	paused = append(paused, Class("h-4 w-4"))

	return pipelineFormPage(principal, "Edit Pipeline", "/ui/pipelines/"+pipelineName+"/update", csrfFieldProvider,
		Label(Text("Description")),
		core.TextareaControl("min-h-28", Name("description"), Text(pipeline.Description)),
		Label(Text("Schedule Cron")),
		core.InputControl("", Name("schedule_cron"), Value(optionalStringValue(pipeline.ScheduleCron))),
		Label(Text("Concurrency Limit")),
		core.InputControl("", Name("concurrency_limit"), Value(strconv.Itoa(pipeline.ConcurrencyLimit))),
		Label(Class("inline-flex items-center gap-2"), Input(paused...), Span(Text("Paused"))),
	)
}

func pipelineJobsNewPage(principal domain.ContextPrincipal, pipelineName string, csrfFieldProvider func() Node) Node {
	return pipelineFormPage(principal, "New Pipeline Job", "/ui/pipelines/"+pipelineName+"/jobs", csrfFieldProvider,
		Label(Text("Name")),
		core.InputControl("", Name("name"), Required()),
		Label(Text("Type")),
		core.SelectControl("", Name("job_type"),
			Option(Value("NOTEBOOK"), Text("NOTEBOOK")),
			Option(Value("MODEL_RUN"), Text("MODEL_RUN")),
		),
		Label(Text("Notebook ID")),
		core.InputControl("", Name("notebook_id")),
		Label(Text("Model Selector")),
		core.InputControl("", Name("model_selector")),
		Label(Text("Depends On (comma separated job names)")),
		core.InputControl("", Name("depends_on")),
	)
}

func pipelineFormPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	nodes = append(nodes, Div(Class("mt-4"), core.PrimaryButton("", Type("submit"), Text("Save"))))

	return core.AppPage(
		title,
		"pipelines",
		principal,
		core.Card(
			Form(Class("grid gap-3"), Method("post"), Action(action), Group(nodes)),
		),
	)
}

func metaRow(label, value string) Node {
	return Div(
		Class("rounded-lg border border-border-muted bg-surface-muted p-3"),
		Dt(Class("text-xs font-medium uppercase tracking-wide text-muted"), Text(label)),
		Dd(Class("mt-1 ml-0 text-sm text-foreground"), Text(emptyDash(value))),
	)
}

func statusPill(text, tone string) Node {
	className := "inline-flex items-center rounded-full px-2.5 py-1 text-xs font-medium"
	switch tone {
	case "success":
		className += " bg-success-muted text-success-text"
	case "severe":
		className += " bg-danger-muted text-danger-text"
	default:
		className += " bg-accent-muted text-accent"
	}
	return Span(Class(className), Text(text))
}

func pausedTone(paused bool) string {
	if paused {
		return "severe"
	}
	return "success"
}

func boolLabel(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func emptyDash(v string) string {
	if v == "" {
		return "-"
	}
	return v
}
