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
	table := Node(P(Class(core.MutedClass()), Text("No pipelines yet.")))
	if len(rows) > 0 {
		tableRows := make([]Node, 0, len(rows))
		for i := range rows {
			row := rows[i]
			tableRows = append(tableRows, Tr(
				Td(A(Href(row.URL), Class("font-medium text-[var(--fgColor-accent)]"), Text(row.Name))),
				Td(statusPill(boolLabel(row.Paused), pausedTone(row.Paused))),
				Td(Text(row.Schedule)),
				Td(Text(row.Updated)),
			))
		}
		table = Div(
			Class(core.TableWrapClass()),
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
		Div(
			Class(core.CardClass()),
			Div(Class("mb-4 flex flex-wrap items-center justify-between gap-3"),
				Div(H2(Class("m-0 text-xl font-semibold"), Text("Pipelines")), P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("Manage orchestrated jobs and schedules."))),
				A(Href("/ui/pipelines/new"), Class(core.PrimaryButtonClass()), Text("New pipeline")),
			),
			table,
			P(Class("mt-4 text-sm text-[var(--fgColor-muted)]"), Text("Showing up to "+strconv.Itoa(page.MaxResults)+" pipelines. Total: "+strconv.FormatInt(total, 10))),
		),
	)
}

func pipelineDetailPage(d pipelineDetailPageData) Node {
	jobTable := Node(P(Class(core.MutedClass()), Text("No jobs defined yet.")))
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
						Button(Type("submit"), Class(core.DangerButtonClass("small")), Text("Delete")),
					),
				),
			))
		}
		jobTable = Div(
			Class(core.TableWrapClass()),
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
		Div(
			Class(core.CardClass()),
			Div(Class("flex flex-wrap items-start justify-between gap-3"),
				Div(
					H2(Class("m-0 text-xl font-semibold"), Text(d.Name)),
					P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text("Created by "+emptyDash(d.CreatedBy))),
				),
				Div(Class(core.ButtonRowClass("mt-0")),
					A(Href(d.EditURL), Class(core.SecondaryButtonClass()), Text("Edit")),
					A(Href(d.NewJobURL), Class(core.SecondaryButtonClass()), Text("New job")),
					Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldFunc(), Button(Type("submit"), Class(core.DangerButtonClass()), Text("Delete pipeline"))),
				),
			),
			Dl(Class("mt-4 grid gap-3 sm:grid-cols-3"),
				metaRow("Schedule", d.Schedule),
				metaRow("Concurrency", d.Concurrency),
				metaRow("Jobs", strconv.Itoa(len(d.Jobs))),
			),
		),
		Div(
			Class(core.CardClass()),
			H3(Class("mt-0 text-lg font-semibold"), Text("Jobs")),
			jobTable,
		),
	)
}

func pipelinesNewPage(principal domain.ContextPrincipal, csrfFieldProvider func() Node) Node {
	return pipelineFormPage(principal, "New Pipeline", "/ui/pipelines", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Required(), Class(core.FormControlClass())),
		Label(Text("Description")),
		Textarea(Name("description"), Class(core.FormControlClass("min-h-28"))),
		Label(Text("Schedule Cron")),
		Input(Name("schedule_cron"), Class(core.FormControlClass())),
		Label(Text("Concurrency Limit")),
		Input(Name("concurrency_limit"), Value("1"), Class(core.FormControlClass())),
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
		Textarea(Name("description"), Class(core.FormControlClass("min-h-28")), Text(pipeline.Description)),
		Label(Text("Schedule Cron")),
		Input(Name("schedule_cron"), Value(optionalStringValue(pipeline.ScheduleCron)), Class(core.FormControlClass())),
		Label(Text("Concurrency Limit")),
		Input(Name("concurrency_limit"), Value(strconv.Itoa(pipeline.ConcurrencyLimit)), Class(core.FormControlClass())),
		Label(Class("inline-flex items-center gap-2"), Input(paused...), Span(Text("Paused"))),
	)
}

func pipelineJobsNewPage(principal domain.ContextPrincipal, pipelineName string, csrfFieldProvider func() Node) Node {
	return pipelineFormPage(principal, "New Pipeline Job", "/ui/pipelines/"+pipelineName+"/jobs", csrfFieldProvider,
		Label(Text("Name")),
		Input(Name("name"), Required(), Class(core.FormControlClass())),
		Label(Text("Type")),
		Select(Name("job_type"), Class(core.FormControlClass()),
			Option(Value("NOTEBOOK"), Text("NOTEBOOK")),
			Option(Value("MODEL_RUN"), Text("MODEL_RUN")),
		),
		Label(Text("Notebook ID")),
		Input(Name("notebook_id"), Class(core.FormControlClass())),
		Label(Text("Model Selector")),
		Input(Name("model_selector"), Class(core.FormControlClass())),
		Label(Text("Depends On (comma separated job names)")),
		Input(Name("depends_on"), Class(core.FormControlClass())),
	)
}

func pipelineFormPage(principal domain.ContextPrincipal, title, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	nodes = append(nodes, Div(Class("mt-4"), Button(Type("submit"), Class(core.PrimaryButtonClass()), Text("Save"))))

	return core.AppPage(
		title,
		"pipelines",
		principal,
		Div(
			Class(core.CardClass()),
			Form(Class("grid gap-3"), Method("post"), Action(action), Group(nodes)),
		),
	)
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
	case "success":
		className += " bg-[var(--bgColor-success-muted)] text-[var(--fgColor-success)]"
	case "severe":
		className += " bg-[var(--bgColor-danger-muted)] text-[var(--fgColor-danger)]"
	default:
		className += " bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]"
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
