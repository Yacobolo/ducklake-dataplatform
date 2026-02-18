package ui

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"

	gomponents "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	html "maragu.dev/gomponents/html"
)

func (h *Handler) Home(w http.ResponseWriter, r *http.Request) {
	p := principalFromContext(r.Context())
	page := appPage(
		"Overview",
		"home",
		p,
		html.Div(
			html.Class("grid"),
			html.Div(html.Class("card"), html.H2(gomponents.Text("Catalogs")), html.P(gomponents.Text("Browse registered catalogs and metastore summary.")), html.A(html.Href("/ui/catalogs"), gomponents.Text("Open catalogs ->"))),
			html.Div(html.Class("card"), html.H2(gomponents.Text("Pipelines")), html.P(gomponents.Text("Inspect pipeline definitions, jobs, and recent runs.")), html.A(html.Href("/ui/pipelines"), gomponents.Text("Open pipelines ->"))),
			html.Div(html.Class("card"), html.H2(gomponents.Text("Notebooks")), html.P(gomponents.Text("Read notebook metadata and cell snapshots.")), html.A(html.Href("/ui/notebooks"), gomponents.Text("Open notebooks ->"))),
			html.Div(html.Class("card"), html.H2(gomponents.Text("Macros")), html.P(gomponents.Text("Inspect macro definitions and revisions.")), html.A(html.Href("/ui/macros"), gomponents.Text("Open macros ->"))),
			html.Div(html.Class("card"), html.H2(gomponents.Text("Models")), html.P(gomponents.Text("Read model SQL, dependencies, and config.")), html.A(html.Href("/ui/models"), gomponents.Text("Open models ->"))),
		),
	)
	renderHTML(w, http.StatusOK, page)
}

func (h *Handler) CatalogsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.CatalogRegistration.List(r.Context(), pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]gomponents.Node, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows,
			html.Tr(
				data.Show(containsExpr(item.Name+" "+string(item.Status))),
				html.Td(html.A(html.Href("/ui/catalogs/"+item.Name), gomponents.Text(item.Name))),
				html.Td(gomponents.Text(string(item.Status))),
				html.Td(gomponents.Text(string(item.MetastoreType))),
				html.Td(gomponents.Text(formatTime(item.UpdatedAt))),
			),
		)
	}

	p := principalFromContext(r.Context())
	renderHTML(w, http.StatusOK, appPage(
		"Catalogs",
		"catalogs",
		p,
		html.Div(
			data.Signals(map[string]any{"q": ""}),
			html.Div(
				html.Class("card"),
				html.Label(gomponents.Text("Quick filter")),
				html.Input(html.Type("text"), data.Bind("q"), html.Placeholder("Filter by catalog name or status")),
			),
			html.Div(
				html.Class("card table-wrap"),
				html.Table(
					html.THead(
						html.Tr(
							html.Th(gomponents.Text("Name")),
							html.Th(gomponents.Text("Status")),
							html.Th(gomponents.Text("Metastore")),
							html.Th(gomponents.Text("Updated")),
						),
					),
					html.TBody(gomponents.Group(rows)),
				),
			),
		),
		paginationCard("/ui/catalogs", pageReq, total),
	))
}

func (h *Handler) CatalogsDetail(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	registration, err := h.CatalogRegistration.Get(r.Context(), catalogName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	summary, _ := h.Catalog.GetMetastoreSummary(r.Context(), catalogName)
	schemas, _, _ := h.Catalog.ListSchemas(r.Context(), catalogName, domain.PageRequest{MaxResults: 20})

	schemaRows := make([]gomponents.Node, 0, len(schemas))
	for i := range schemas {
		s := schemas[i]
		schemaRows = append(schemaRows, html.Tr(html.Td(gomponents.Text(s.Name)), html.Td(gomponents.Text(s.Owner)), html.Td(gomponents.Text(formatTime(s.UpdatedAt)))))
	}

	summaryNode := html.P(gomponents.Text("Metastore summary unavailable"))
	if summary != nil {
		summaryNode = html.Ul(
			html.Li(gomponents.Text("Type: "+summary.MetastoreType)),
			html.Li(gomponents.Text("Storage: "+summary.StorageBackend)),
			html.Li(gomponents.Text("Schemas: "+strconv.FormatInt(summary.SchemaCount, 10))),
			html.Li(gomponents.Text("Tables: "+strconv.FormatInt(summary.TableCount, 10))),
		)
	}

	p := principalFromContext(r.Context())
	renderHTML(w, http.StatusOK, appPage(
		"Catalog: "+registration.Name,
		"catalogs",
		p,
		html.Div(html.Class("card"), html.P(gomponents.Text("Status: "+string(registration.Status))), html.P(gomponents.Text("Data path: "+registration.DataPath)), html.P(gomponents.Text("Default: "+fmt.Sprintf("%t", registration.IsDefault)))),
		html.Div(html.Class("card"), html.H2(gomponents.Text("Metastore")), summaryNode),
		html.Div(
			html.Class("card table-wrap"),
			html.H2(gomponents.Text("Schemas")),
			html.Table(
				html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Owner")), html.Th(gomponents.Text("Updated")))),
				html.TBody(gomponents.Group(schemaRows)),
			),
		),
	))
}

func (h *Handler) PipelinesList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.Pipeline.ListPipelines(r.Context(), pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]gomponents.Node, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, html.Tr(
			data.Show(containsExpr(item.Name)),
			html.Td(html.A(html.Href("/ui/pipelines/"+item.Name), gomponents.Text(item.Name))),
			html.Td(gomponents.Text(fmt.Sprintf("%t", item.IsPaused))),
			html.Td(gomponents.Text(strOrDash(item.ScheduleCron))),
			html.Td(gomponents.Text(formatTime(item.UpdatedAt))),
		))
	}

	p := principalFromContext(r.Context())
	renderHTML(w, http.StatusOK, appPage(
		"Pipelines",
		"pipelines",
		p,
		html.Div(
			data.Signals(map[string]any{"q": ""}),
			html.Div(html.Class("card"), html.Label(gomponents.Text("Quick filter")), html.Input(html.Type("text"), data.Bind("q"), html.Placeholder("Filter by pipeline name"))),
			html.Div(html.Class("card table-wrap"), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Paused")), html.Th(gomponents.Text("Schedule")), html.Th(gomponents.Text("Updated")))), html.TBody(gomponents.Group(rows)))),
		),
		paginationCard("/ui/pipelines", pageReq, total),
	))
}

func (h *Handler) PipelinesDetail(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	pipe, err := h.Pipeline.GetPipeline(r.Context(), name)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	jobs, _ := h.Pipeline.ListJobs(r.Context(), name)
	runs, _, _ := h.Pipeline.ListRuns(r.Context(), name, domain.PipelineRunFilter{Page: domain.PageRequest{MaxResults: 20}})

	jobRows := make([]gomponents.Node, 0, len(jobs))
	for i := range jobs {
		j := jobs[i]
		jobRows = append(jobRows, html.Tr(html.Td(gomponents.Text(j.Name)), html.Td(gomponents.Text(j.JobType)), html.Td(gomponents.Text(j.ModelSelector)), html.Td(gomponents.Text(j.NotebookID))))
	}
	runRows := make([]gomponents.Node, 0, len(runs))
	for i := range runs {
		run := runs[i]
		runRows = append(runRows, html.Tr(html.Td(gomponents.Text(run.ID)), html.Td(gomponents.Text(run.Status)), html.Td(gomponents.Text(run.TriggerType)), html.Td(gomponents.Text(formatTimePtr(run.StartedAt))), html.Td(gomponents.Text(formatTimePtr(run.FinishedAt)))))
	}

	p := principalFromContext(r.Context())
	renderHTML(w, http.StatusOK, appPage(
		"Pipeline: "+pipe.Name,
		"pipelines",
		p,
		html.Div(html.Class("card"), html.P(gomponents.Text("Created by: "+pipe.CreatedBy)), html.P(gomponents.Text("Concurrency: "+strconv.Itoa(pipe.ConcurrencyLimit))), html.P(gomponents.Text("Schedule: "+strOrDash(pipe.ScheduleCron)))),
		html.Div(html.Class("card table-wrap"), html.H2(gomponents.Text("Jobs")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Type")), html.Th(gomponents.Text("Selector")), html.Th(gomponents.Text("Notebook")))), html.TBody(gomponents.Group(jobRows)))),
		html.Div(html.Class("card table-wrap"), html.H2(gomponents.Text("Recent runs")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Run ID")), html.Th(gomponents.Text("Status")), html.Th(gomponents.Text("Trigger")), html.Th(gomponents.Text("Started")), html.Th(gomponents.Text("Finished")))), html.TBody(gomponents.Group(runRows)))),
	))
}

func (h *Handler) NotebooksList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.Notebook.ListNotebooks(r.Context(), nil, pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]gomponents.Node, 0, len(items))
	for i := range items {
		n := items[i]
		rows = append(rows, html.Tr(
			data.Show(containsExpr(n.Name+" "+n.Owner)),
			html.Td(html.A(html.Href("/ui/notebooks/"+n.ID), gomponents.Text(n.Name))),
			html.Td(gomponents.Text(n.Owner)),
			html.Td(gomponents.Text(formatTime(n.UpdatedAt))),
		))
	}

	p := principalFromContext(r.Context())
	renderHTML(w, http.StatusOK, appPage(
		"Notebooks",
		"notebooks",
		p,
		html.Div(
			data.Signals(map[string]any{"q": ""}),
			html.Div(html.Class("card"), html.Label(gomponents.Text("Quick filter")), html.Input(html.Type("text"), data.Bind("q"), html.Placeholder("Filter by notebook or owner"))),
			html.Div(html.Class("card table-wrap"), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Owner")), html.Th(gomponents.Text("Updated")))), html.TBody(gomponents.Group(rows)))),
		),
		paginationCard("/ui/notebooks", pageReq, total),
	))
}

func (h *Handler) NotebooksDetail(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	nb, cells, err := h.Notebook.GetNotebook(r.Context(), id)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	jobs, _, _ := h.SessionManager.ListJobs(r.Context(), id, domain.PageRequest{MaxResults: 20})

	cellNodes := make([]gomponents.Node, 0, len(cells))
	for i := range cells {
		cell := cells[i]
		cellNodes = append(cellNodes, html.Div(html.Class("card"), html.H3(gomponents.Text(fmt.Sprintf("Cell %d (%s)", cell.Position, cell.CellType))), html.Pre(gomponents.Text(cell.Content))))
	}

	jobRows := make([]gomponents.Node, 0, len(jobs))
	for i := range jobs {
		job := jobs[i]
		jobRows = append(jobRows, html.Tr(html.Td(gomponents.Text(job.ID)), html.Td(gomponents.Text(string(job.State))), html.Td(gomponents.Text(formatTime(job.UpdatedAt)))))
	}

	p := principalFromContext(r.Context())
	renderHTML(w, http.StatusOK, appPage(
		"Notebook: "+nb.Name,
		"notebooks",
		p,
		html.Div(html.Class("card"), html.P(gomponents.Text("Owner: "+nb.Owner)), html.P(gomponents.Text("Description: "+stringPtr(nb.Description)))),
		html.Div(html.Class("card table-wrap"), html.H2(gomponents.Text("Recent jobs")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Job ID")), html.Th(gomponents.Text("State")), html.Th(gomponents.Text("Updated")))), html.TBody(gomponents.Group(jobRows)))),
		gomponents.Group(cellNodes),
	))
}

func (h *Handler) MacrosList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.Macro.List(r.Context(), pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]gomponents.Node, 0, len(items))
	for i := range items {
		m := items[i]
		rows = append(rows, html.Tr(
			data.Show(containsExpr(m.Name+" "+m.Visibility)),
			html.Td(html.A(html.Href("/ui/macros/"+m.Name), gomponents.Text(m.Name))),
			html.Td(gomponents.Text(m.MacroType)),
			html.Td(gomponents.Text(m.Visibility)),
			html.Td(gomponents.Text(m.Status)),
		))
	}

	p := principalFromContext(r.Context())
	renderHTML(w, http.StatusOK, appPage(
		"Macros",
		"macros",
		p,
		html.Div(
			data.Signals(map[string]any{"q": ""}),
			html.Div(html.Class("card"), html.Label(gomponents.Text("Quick filter")), html.Input(html.Type("text"), data.Bind("q"), html.Placeholder("Filter by macro name or visibility"))),
			html.Div(html.Class("card table-wrap"), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Type")), html.Th(gomponents.Text("Visibility")), html.Th(gomponents.Text("Status")))), html.TBody(gomponents.Group(rows)))),
		),
		paginationCard("/ui/macros", pageReq, total),
	))
}

func (h *Handler) MacrosDetail(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "macroName")
	m, err := h.Macro.Get(r.Context(), name)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	revisions, _ := h.Macro.ListRevisions(r.Context(), name)

	revRows := make([]gomponents.Node, 0, len(revisions))
	for i := range revisions {
		rev := revisions[i]
		revRows = append(revRows, html.Tr(html.Td(gomponents.Text(strconv.Itoa(rev.Version))), html.Td(gomponents.Text(rev.Status)), html.Td(gomponents.Text(rev.CreatedBy)), html.Td(gomponents.Text(formatTime(rev.CreatedAt)))))
	}

	p := principalFromContext(r.Context())
	renderHTML(w, http.StatusOK, appPage(
		"Macro: "+m.Name,
		"macros",
		p,
		html.Div(html.Class("card"), html.P(gomponents.Text("Type: "+m.MacroType)), html.P(gomponents.Text("Visibility: "+m.Visibility)), html.P(gomponents.Text("Status: "+m.Status)), html.P(gomponents.Text("Owner: "+m.Owner))),
		html.Div(html.Class("card"), html.H2(gomponents.Text("Definition")), html.Pre(gomponents.Text(m.Body))),
		html.Div(html.Class("card table-wrap"), html.H2(gomponents.Text("Revisions")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Version")), html.Th(gomponents.Text("Status")), html.Th(gomponents.Text("Created by")), html.Th(gomponents.Text("Created")))), html.TBody(gomponents.Group(revRows)))),
	))
}

func (h *Handler) ModelsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	var projectName *string
	if p := r.URL.Query().Get("project"); p != "" {
		projectName = &p
	}
	items, total, err := h.Model.ListModels(r.Context(), projectName, pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]gomponents.Node, 0, len(items))
	for i := range items {
		m := items[i]
		detailURL := fmt.Sprintf("/ui/models/%s/%s", m.ProjectName, m.Name)
		rows = append(rows, html.Tr(
			data.Show(containsExpr(m.ProjectName+"."+m.Name+" "+m.Materialization)),
			html.Td(html.A(html.Href(detailURL), gomponents.Text(m.ProjectName+"."+m.Name))),
			html.Td(gomponents.Text(m.Materialization)),
			html.Td(gomponents.Text(strconv.Itoa(len(m.DependsOn)))),
			html.Td(gomponents.Text(formatTime(m.UpdatedAt))),
		))
	}

	p := principalFromContext(r.Context())
	renderHTML(w, http.StatusOK, appPage(
		"Models",
		"models",
		p,
		html.Div(
			data.Signals(map[string]any{"q": ""}),
			html.Div(html.Class("card"), html.Label(gomponents.Text("Quick filter")), html.Input(html.Type("text"), data.Bind("q"), html.Placeholder("Filter by project.model or materialization"))),
			html.Div(html.Class("card table-wrap"), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Model")), html.Th(gomponents.Text("Materialization")), html.Th(gomponents.Text("Dependencies")), html.Th(gomponents.Text("Updated")))), html.TBody(gomponents.Group(rows)))),
		),
		paginationCard("/ui/models", pageReq, total),
	))
}

func (h *Handler) ModelsDetail(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	m, err := h.Model.GetModel(r.Context(), projectName, modelName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	tests, _ := h.Model.ListTests(r.Context(), projectName, modelName)

	testRows := make([]gomponents.Node, 0, len(tests))
	for i := range tests {
		t := tests[i]
		testRows = append(testRows, html.Tr(html.Td(gomponents.Text(t.Name)), html.Td(gomponents.Text(t.TestType)), html.Td(gomponents.Text(t.Column))))
	}

	p := principalFromContext(r.Context())
	renderHTML(w, http.StatusOK, appPage(
		"Model: "+m.ProjectName+"."+m.Name,
		"models",
		p,
		html.Div(html.Class("card"), html.P(gomponents.Text("Materialization: "+m.Materialization)), html.P(gomponents.Text("Owner: "+m.Owner)), html.P(gomponents.Text("Depends on: "+stringsJoin(m.DependsOn))), html.P(gomponents.Text("Config: "+mapJSON(modelConfigMap(m.Config))))),
		html.Div(html.Class("card"), html.H2(gomponents.Text("SQL")), html.Pre(gomponents.Text(m.SQL))),
		html.Div(html.Class("card table-wrap"), html.H2(gomponents.Text("Tests")), html.Table(html.THead(html.Tr(html.Th(gomponents.Text("Name")), html.Th(gomponents.Text("Type")), html.Th(gomponents.Text("Column")))), html.TBody(gomponents.Group(testRows)))),
	))
}

func modelConfigMap(c domain.ModelConfig) map[string]string {
	out := map[string]string{}
	if len(c.UniqueKey) > 0 {
		out["unique_key"] = stringsJoin(c.UniqueKey)
	}
	if c.IncrementalStrategy != "" {
		out["incremental_strategy"] = c.IncrementalStrategy
	}
	if c.OnSchemaChange != "" {
		out["on_schema_change"] = c.OnSchemaChange
	}
	return out
}

func stringsJoin(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	out := values[0]
	for i := 1; i < len(values); i++ {
		out += ", " + values[i]
	}
	return out
}

func strOrDash(v *string) string {
	if v == nil || *v == "" {
		return "-"
	}
	return *v
}

func (h *Handler) renderServiceError(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusInternalServerError
	title := "Unexpected Error"
	message := "An unexpected error occurred while loading this page."

	var notFound *domain.NotFoundError
	var accessDenied *domain.AccessDeniedError
	var validation *domain.ValidationError
	if errors.As(err, &notFound) {
		status = http.StatusNotFound
		title = "Not Found"
		message = notFound.Error()
	} else if errors.As(err, &accessDenied) {
		status = http.StatusForbidden
		title = "Access Denied"
		message = accessDenied.Error()
	} else if errors.As(err, &validation) {
		status = http.StatusBadRequest
		title = "Invalid Request"
		message = validation.Error()
	}

	_ = r
	renderHTML(w, status, errorPage(title, message))
}
