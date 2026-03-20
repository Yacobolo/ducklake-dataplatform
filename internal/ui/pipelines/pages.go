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
	table := Node(core.WorkspaceEmptyState("workflow", "No pipelines yet.", "Create a pipeline when you are ready to orchestrate notebook or model execution.", core.PrimaryLink("/ui/pipelines/new", "", Text("New pipeline"))))
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
		table = core.TableContainer("",
			core.DataTable("",
				THead(Tr(Th(Text("Name")), Th(Text("Paused")), Th(Text("Schedule")), Th(Text("Updated")))),
				TBody(Group(tableRows)),
			),
		)
	}

	return core.AppPage(
		"Pipelines",
		"pipelines",
		principal,
		core.ListPageLayout(
			core.ListPageHeader("Pipelines", "Manage orchestrated jobs and schedules.", core.PrimaryLink("/ui/pipelines/new", "", Text("New pipeline"))),
			core.ListPageBody(
				table,
				core.ListPagination("/ui/pipelines", page, total),
			),
		),
	)
}

func pipelineDetailPage(d pipelineDetailPageData) Node {
	jobTable := Node(P(Class("text-xs text-[var(--fgColor-muted)]"), Text("No jobs defined yet.")))
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
		core.DetailShell(
			core.DetailHero(
				core.DetailHeroCopy(
					core.Kicker("Operate"),
					core.DetailTitle(d.Name),
					core.DetailDescription("Pipeline detail separates current schedule state from the job-management actions that change it."),
				),
				core.DetailHeroMeta(
					core.DetailSummaryList([][2]string{
						{"Created by", emptyDash(d.CreatedBy)},
						{"Schedule", emptyDash(d.Schedule)},
						{"Concurrency", emptyDash(d.Concurrency)},
					}),
				),
			),
			core.DetailLayout(
				core.DetailMain(
					core.SectionSurface(
						core.SectionHeader("Jobs", "Inspect the pipeline job graph without mixing it into the action controls."),
						jobTable,
					),
				),
				core.DetailRail(
					core.DetailRailCard("Summary", "Keep key schedule metadata pinned in the secondary rail.",
						core.MetadataSummary([][2]string{
							{"Schedule", d.Schedule},
							{"Concurrency", d.Concurrency},
							{"Jobs", strconv.Itoa(len(d.Jobs))},
						}),
					),
					core.DetailRailCard("Actions", "Authoring actions stay secondary to the pipeline state view.",
						core.ButtonGroup("",
							core.SecondaryLink(d.EditURL, "", Text("Edit")),
							core.SecondaryLink(d.NewJobURL, "", Text("New job")),
							Form(Method("post"), Action(d.DeleteURL), d.CSRFFieldFunc(), core.DangerButton("", Type("submit"), Text("Delete pipeline"))),
						),
					),
				),
			),
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
		core.FormPageLayout("Operate", title, "Pipeline authoring follows the shared single-surface form layout.",
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
