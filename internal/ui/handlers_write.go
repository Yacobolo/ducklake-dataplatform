package ui

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"

	gomponents "maragu.dev/gomponents"
	html "maragu.dev/gomponents/html"
)

func renderFormPage(w http.ResponseWriter, r *http.Request, title, active, action string, fields ...gomponents.Node) {
	p := principalFromContext(r.Context())
	nodes := []gomponents.Node{csrfField(r)}
	nodes = append(nodes, fields...)
	renderHTML(w, http.StatusOK, appPage(
		title,
		active,
		p,
		html.Div(
			html.Class(cardClass()),
			html.Form(
				html.Method("post"),
				html.Action(action),
				gomponents.Group(nodes),
				html.Div(html.StyleAttr("margin-top: 12px"), html.Button(html.Type("submit"), html.Class(primaryButtonClass()), gomponents.Text("Save"))),
			),
		),
	))
}

func (h *Handler) CatalogsNew(w http.ResponseWriter, r *http.Request) {
	renderFormPage(w, r, "New Catalog", "catalogs", "/ui/catalogs",
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Metastore Type")),
		html.Select(html.Name("metastore_type"), html.Option(html.Value("sqlite"), gomponents.Text("sqlite")), html.Option(html.Value("postgres"), gomponents.Text("postgres"))),
		html.Label(gomponents.Text("DSN")),
		html.Input(html.Name("dsn"), html.Required()),
		html.Label(gomponents.Text("Data Path")),
		html.Input(html.Name("data_path"), html.Required()),
		html.Label(gomponents.Text("Comment")),
		html.Textarea(html.Name("comment")),
	)
}

func (h *Handler) CatalogsCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	_, err := h.CatalogRegistration.Register(r.Context(), domain.CreateCatalogRequest{
		Name:          formString(r.Form, "name"),
		MetastoreType: formString(r.Form, "metastore_type"),
		DSN:           formString(r.Form, "dsn"),
		DataPath:      formString(r.Form, "data_path"),
		Comment:       formString(r.Form, "comment"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/catalogs", http.StatusSeeOther)
}

func (h *Handler) CatalogsEdit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "catalogName")
	c, err := h.CatalogRegistration.Get(r.Context(), name)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderFormPage(w, r, "Edit Catalog", "catalogs", "/ui/catalogs/"+name+"/update",
		html.Label(gomponents.Text("Comment")),
		html.Textarea(html.Name("comment"), gomponents.Text(c.Comment)),
		html.Label(gomponents.Text("Data Path")),
		html.Input(html.Name("data_path"), html.Value(c.DataPath)),
		html.Label(gomponents.Text("DSN")),
		html.Input(html.Name("dsn"), html.Value(c.DSN)),
	)
}

func (h *Handler) CatalogsUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "catalogName")
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	_, err := h.CatalogRegistration.Update(r.Context(), name, domain.UpdateCatalogRegistrationRequest{
		Comment:  formOptionalString(r.Form, "comment"),
		DataPath: formOptionalString(r.Form, "data_path"),
		DSN:      formOptionalString(r.Form, "dsn"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/catalogs/"+name, http.StatusSeeOther)
}

func (h *Handler) CatalogsDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "catalogName")
	if err := h.CatalogRegistration.Delete(r.Context(), name); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/catalogs", http.StatusSeeOther)
}

func (h *Handler) CatalogsSetDefault(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "catalogName")
	if _, err := h.CatalogRegistration.SetDefault(r.Context(), name); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/catalogs/"+name, http.StatusSeeOther)
}

func (h *Handler) CatalogSchemasNew(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	renderFormPage(w, r, "New Schema", "catalogs", "/ui/catalogs/"+catalogName+"/schemas",
		html.Label(gomponents.Text("Schema Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Comment")),
		html.Textarea(html.Name("comment")),
		html.Label(gomponents.Text("Location Name")),
		html.Input(html.Name("location_name")),
	)
}

func (h *Handler) CatalogSchemasCreate(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	principal, _ := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	_, err := h.Catalog.CreateSchema(r.Context(), catalogName, principal, domain.CreateSchemaRequest{
		Name:         formString(r.Form, "name"),
		Comment:      formString(r.Form, "comment"),
		LocationName: formString(r.Form, "location_name"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/catalogs/"+catalogName, http.StatusSeeOther)
}

func (h *Handler) CatalogSchemasEdit(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	s, err := h.Catalog.GetSchema(r.Context(), catalogName, schemaName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderFormPage(w, r, "Edit Schema", "catalogs", "/ui/catalogs/"+catalogName+"/schemas/"+schemaName+"/update",
		html.Label(gomponents.Text("Comment")),
		html.Textarea(html.Name("comment"), gomponents.Text(s.Comment)),
	)
}

func (h *Handler) CatalogSchemasUpdate(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	principal, _ := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	_, err := h.Catalog.UpdateSchema(r.Context(), catalogName, principal, schemaName, domain.UpdateSchemaRequest{
		Comment: formOptionalString(r.Form, "comment"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/catalogs/"+catalogName, http.StatusSeeOther)
}

func (h *Handler) CatalogSchemasDelete(w http.ResponseWriter, r *http.Request) {
	catalogName := chi.URLParam(r, "catalogName")
	schemaName := chi.URLParam(r, "schemaName")
	principal, _ := principalLabel(r.Context())
	if err := h.Catalog.DeleteSchema(r.Context(), catalogName, principal, schemaName, true); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/catalogs/"+catalogName, http.StatusSeeOther)
}

func (h *Handler) PipelinesNew(w http.ResponseWriter, r *http.Request) {
	renderFormPage(w, r, "New Pipeline", "pipelines", "/ui/pipelines",
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description")),
		html.Label(gomponents.Text("Schedule Cron")),
		html.Input(html.Name("schedule_cron")),
		html.Label(gomponents.Text("Concurrency Limit")),
		html.Input(html.Name("concurrency_limit"), html.Value("1")),
		html.Label(gomponents.Text("Paused")),
		html.Input(html.Type("checkbox"), html.Name("is_paused")),
	)
}

func (h *Handler) PipelinesCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	concurrency := 1
	if p, err := formOptionalInt(r.Form, "concurrency_limit"); err == nil && p != nil {
		concurrency = *p
	}
	_, err := h.Pipeline.CreatePipeline(r.Context(), principal, domain.CreatePipelineRequest{
		Name:             formString(r.Form, "name"),
		Description:      formString(r.Form, "description"),
		ScheduleCron:     formOptionalString(r.Form, "schedule_cron"),
		IsPaused:         formBool(r.Form, "is_paused"),
		ConcurrencyLimit: concurrency,
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/pipelines", http.StatusSeeOther)
}

func (h *Handler) PipelinesEdit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	p, err := h.Pipeline.GetPipeline(r.Context(), name)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	pausedInput := []gomponents.Node{html.Type("checkbox"), html.Name("is_paused")}
	if p.IsPaused {
		pausedInput = append(pausedInput, html.Checked())
	}
	renderFormPage(w, r, "Edit Pipeline", "pipelines", "/ui/pipelines/"+name+"/update",
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description"), gomponents.Text(p.Description)),
		html.Label(gomponents.Text("Schedule Cron")),
		html.Input(html.Name("schedule_cron"), html.Value(strOrDash(p.ScheduleCron))),
		html.Label(gomponents.Text("Concurrency Limit")),
		html.Input(html.Name("concurrency_limit"), html.Value(strconv.Itoa(p.ConcurrencyLimit))),
		html.Label(gomponents.Text("Paused")),
		html.Input(pausedInput...),
	)
}

func (h *Handler) PipelinesUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	principal, _ := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	concurrency, err := formOptionalInt(r.Form, "concurrency_limit")
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "concurrency_limit must be an integer."))
		return
	}
	schedule := formString(r.Form, "schedule_cron")
	desc := formString(r.Form, "description")
	isPaused := formBool(r.Form, "is_paused")
	_, err = h.Pipeline.UpdatePipeline(r.Context(), principal, name, domain.UpdatePipelineRequest{
		Description:      &desc,
		ScheduleCron:     &schedule,
		IsPaused:         &isPaused,
		ConcurrencyLimit: concurrency,
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/pipelines/"+name, http.StatusSeeOther)
}

func (h *Handler) PipelinesDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	principal, _ := principalLabel(r.Context())
	if err := h.Pipeline.DeletePipeline(r.Context(), principal, name); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/pipelines", http.StatusSeeOther)
}

func (h *Handler) PipelineJobsNew(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	renderFormPage(w, r, "New Pipeline Job", "pipelines", "/ui/pipelines/"+name+"/jobs",
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Type")),
		html.Select(html.Name("job_type"), html.Option(html.Value("NOTEBOOK"), gomponents.Text("NOTEBOOK")), html.Option(html.Value("MODEL_RUN"), gomponents.Text("MODEL_RUN"))),
		html.Label(gomponents.Text("Notebook ID")),
		html.Input(html.Name("notebook_id")),
		html.Label(gomponents.Text("Model Selector")),
		html.Input(html.Name("model_selector")),
		html.Label(gomponents.Text("Depends On (comma separated job names)")),
		html.Input(html.Name("depends_on")),
	)
}

func (h *Handler) PipelineJobsCreate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	principal, _ := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	_, err := h.Pipeline.CreateJob(r.Context(), principal, name, domain.CreatePipelineJobRequest{
		Name:          formString(r.Form, "name"),
		DependsOn:     formCSV(r.Form, "depends_on"),
		NotebookID:    formString(r.Form, "notebook_id"),
		JobType:       formString(r.Form, "job_type"),
		ModelSelector: formString(r.Form, "model_selector"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/pipelines/"+name, http.StatusSeeOther)
}

func (h *Handler) PipelineJobsDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	jobID := chi.URLParam(r, "jobID")
	principal, _ := principalLabel(r.Context())
	if err := h.Pipeline.DeleteJob(r.Context(), principal, name, jobID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/pipelines/"+name, http.StatusSeeOther)
}

func (h *Handler) PipelineRunsTrigger(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	principal, _ := principalLabel(r.Context())
	if _, err := h.Pipeline.TriggerRun(r.Context(), principal, name, nil, domain.TriggerTypeManual); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/pipelines/"+name, http.StatusSeeOther)
}

func (h *Handler) PipelineRunsCancel(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	principal, _ := principalLabel(r.Context())
	if err := h.Pipeline.CancelRun(r.Context(), principal, runID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/pipelines", http.StatusSeeOther)
}

func (h *Handler) NotebooksNew(w http.ResponseWriter, r *http.Request) {
	renderFormPage(w, r, "New Notebook", "notebooks", "/ui/notebooks",
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description")),
		html.Label(gomponents.Text("Initial SQL Source")),
		html.Textarea(html.Name("source")),
	)
}

func (h *Handler) NotebooksCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	_, err := h.Notebook.CreateNotebook(r.Context(), principal, domain.CreateNotebookRequest{
		Name:        formString(r.Form, "name"),
		Description: formOptionalString(r.Form, "description"),
		Source:      formOptionalString(r.Form, "source"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks", http.StatusSeeOther)
}

func (h *Handler) NotebooksEdit(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	nb, _, err := h.Notebook.GetNotebook(r.Context(), id)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderFormPage(w, r, "Edit Notebook", "notebooks", "/ui/notebooks/"+id+"/update",
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Value(nb.Name), html.Required()),
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description"), gomponents.Text(stringPtr(nb.Description))),
	)
}

func (h *Handler) NotebooksUpdate(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	principal, isAdmin := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	_, err := h.Notebook.UpdateNotebook(r.Context(), principal, isAdmin, id, domain.UpdateNotebookRequest{
		Name:        formOptionalString(r.Form, "name"),
		Description: formOptionalString(r.Form, "description"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+id, http.StatusSeeOther)
}

func (h *Handler) NotebooksDelete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	principal, isAdmin := principalLabel(r.Context())
	if err := h.Notebook.DeleteNotebook(r.Context(), principal, isAdmin, id); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks", http.StatusSeeOther)
}

func (h *Handler) NotebookCellsNew(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	renderFormPage(w, r, "New Notebook Cell", "notebooks", "/ui/notebooks/"+id+"/cells",
		html.Label(gomponents.Text("Cell Type")),
		html.Select(html.Name("cell_type"), html.Option(html.Value("sql"), gomponents.Text("sql")), html.Option(html.Value("markdown"), gomponents.Text("markdown"))),
		html.Label(gomponents.Text("Content")),
		html.Textarea(html.Name("content"), html.Required()),
		html.Label(gomponents.Text("Position (optional)")),
		html.Input(html.Name("position")),
	)
}

func (h *Handler) NotebookCellsCreate(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	principal, isAdmin := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	pos, err := formOptionalInt(r.Form, "position")
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "position must be an integer."))
		return
	}
	_, err = h.Notebook.CreateCell(r.Context(), principal, isAdmin, id, domain.CreateCellRequest{
		CellType: domain.CellType(formString(r.Form, "cell_type")),
		Content:  formString(r.Form, "content"),
		Position: pos,
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+id, http.StatusSeeOther)
}

func (h *Handler) NotebookCellsEdit(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	cellID := chi.URLParam(r, "cellID")
	_, cells, err := h.Notebook.GetNotebook(r.Context(), notebookID)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	var found *domain.Cell
	for i := range cells {
		if cells[i].ID == cellID {
			found = &cells[i]
			break
		}
	}
	if found == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Cell not found in notebook."))
		return
	}
	renderFormPage(w, r, "Edit Notebook Cell", "notebooks", "/ui/notebooks/"+notebookID+"/cells/"+cellID+"/update",
		html.Label(gomponents.Text("Content")),
		html.Textarea(html.Name("content"), gomponents.Text(found.Content), html.Required()),
		html.Label(gomponents.Text("Position")),
		html.Input(html.Name("position"), html.Value(strconv.Itoa(found.Position))),
	)
}

func (h *Handler) NotebookCellsUpdate(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	cellID := chi.URLParam(r, "cellID")
	principal, isAdmin := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	pos, err := formOptionalInt(r.Form, "position")
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "position must be an integer."))
		return
	}
	_, err = h.Notebook.UpdateCell(r.Context(), principal, isAdmin, cellID, domain.UpdateCellRequest{
		Content:  formOptionalString(r.Form, "content"),
		Position: pos,
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+notebookID, http.StatusSeeOther)
}

func (h *Handler) NotebookCellsDelete(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	cellID := chi.URLParam(r, "cellID")
	principal, isAdmin := principalLabel(r.Context())
	if err := h.Notebook.DeleteCell(r.Context(), principal, isAdmin, cellID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+notebookID, http.StatusSeeOther)
}

func (h *Handler) NotebookCellsReorder(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	principal, isAdmin := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	cellIDs := r.Form["cell_ids"]
	if len(cellIDs) == 1 {
		cellIDs = formCSV(r.Form, "cell_ids")
	}
	if _, err := h.Notebook.ReorderCells(r.Context(), principal, isAdmin, notebookID, domain.ReorderCellsRequest{CellIDs: cellIDs}); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+notebookID, http.StatusSeeOther)
}

func (h *Handler) MacrosNew(w http.ResponseWriter, r *http.Request) {
	renderFormPage(w, r, "New Macro", "macros", "/ui/macros",
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Type")),
		html.Select(html.Name("macro_type"), html.Option(html.Value("SCALAR"), gomponents.Text("SCALAR")), html.Option(html.Value("TABLE"), gomponents.Text("TABLE"))),
		html.Label(gomponents.Text("Visibility")),
		html.Select(html.Name("visibility"), html.Option(html.Value("project"), gomponents.Text("project")), html.Option(html.Value("catalog_global"), gomponents.Text("catalog_global")), html.Option(html.Value("system"), gomponents.Text("system"))),
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description")),
		html.Label(gomponents.Text("Parameters (comma separated)")),
		html.Input(html.Name("parameters")),
		html.Label(gomponents.Text("Body")),
		html.Textarea(html.Name("body"), html.Required()),
	)
}

func (h *Handler) MacrosCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	_, err := h.Macro.Create(r.Context(), principal, domain.CreateMacroRequest{
		Name:        formString(r.Form, "name"),
		MacroType:   formString(r.Form, "macro_type"),
		Visibility:  formString(r.Form, "visibility"),
		Description: formString(r.Form, "description"),
		Parameters:  formCSV(r.Form, "parameters"),
		Body:        formString(r.Form, "body"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/macros", http.StatusSeeOther)
}

func (h *Handler) MacrosEdit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "macroName")
	m, err := h.Macro.Get(r.Context(), name)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderFormPage(w, r, "Edit Macro", "macros", "/ui/macros/"+name+"/update",
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description"), gomponents.Text(m.Description)),
		html.Label(gomponents.Text("Visibility")),
		html.Select(html.Name("visibility"), optionSelected("project", m.Visibility), optionSelected("catalog_global", m.Visibility), optionSelected("system", m.Visibility)),
		html.Label(gomponents.Text("Parameters (comma separated)")),
		html.Input(html.Name("parameters"), html.Value(stringsJoin(m.Parameters))),
		html.Label(gomponents.Text("Body")),
		html.Textarea(html.Name("body"), gomponents.Text(m.Body), html.Required()),
		html.Label(gomponents.Text("Status")),
		html.Select(html.Name("status"), optionSelected("ACTIVE", m.Status), optionSelected("DEPRECATED", m.Status)),
	)
}

func optionSelected(value, selected string) gomponents.Node {
	if value == selected {
		return html.Option(html.Value(value), html.Selected(), gomponents.Text(value))
	}
	return html.Option(html.Value(value), gomponents.Text(value))
}

func (h *Handler) MacrosUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "macroName")
	principal, _ := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	body := formString(r.Form, "body")
	description := formString(r.Form, "description")
	visibility := formString(r.Form, "visibility")
	status := formString(r.Form, "status")
	_, err := h.Macro.Update(r.Context(), principal, name, domain.UpdateMacroRequest{
		Body:        &body,
		Description: &description,
		Visibility:  &visibility,
		Status:      &status,
		Parameters:  formCSV(r.Form, "parameters"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/macros/"+name, http.StatusSeeOther)
}

func (h *Handler) MacrosDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "macroName")
	principal, _ := principalLabel(r.Context())
	if err := h.Macro.Delete(r.Context(), principal, name); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/macros", http.StatusSeeOther)
}

func (h *Handler) ModelsNew(w http.ResponseWriter, r *http.Request) {
	renderFormPage(w, r, "New Model", "models", "/ui/models",
		html.Label(gomponents.Text("Project")),
		html.Input(html.Name("project_name"), html.Required()),
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Materialization")),
		html.Select(html.Name("materialization"), html.Option(html.Value("VIEW"), gomponents.Text("VIEW")), html.Option(html.Value("TABLE"), gomponents.Text("TABLE")), html.Option(html.Value("INCREMENTAL"), gomponents.Text("INCREMENTAL")), html.Option(html.Value("EPHEMERAL"), gomponents.Text("EPHEMERAL"))),
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description")),
		html.Label(gomponents.Text("Tags (comma separated)")),
		html.Input(html.Name("tags")),
		html.Label(gomponents.Text("SQL")),
		html.Textarea(html.Name("sql"), html.Required()),
	)
}

func (h *Handler) ModelsCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	m, err := h.Model.CreateModel(r.Context(), principal, domain.CreateModelRequest{
		ProjectName:     formString(r.Form, "project_name"),
		Name:            formString(r.Form, "name"),
		Materialization: formString(r.Form, "materialization"),
		Description:     formString(r.Form, "description"),
		Tags:            formCSV(r.Form, "tags"),
		SQL:             formString(r.Form, "sql"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+m.ProjectName+"/"+m.Name, http.StatusSeeOther)
}

func (h *Handler) ModelsEdit(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	m, err := h.Model.GetModel(r.Context(), projectName, modelName)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderFormPage(w, r, "Edit Model", "models", "/ui/models/"+projectName+"/"+modelName+"/update",
		html.Label(gomponents.Text("Materialization")),
		html.Select(html.Name("materialization"), optionSelected("VIEW", m.Materialization), optionSelected("TABLE", m.Materialization), optionSelected("INCREMENTAL", m.Materialization), optionSelected("EPHEMERAL", m.Materialization)),
		html.Label(gomponents.Text("Description")),
		html.Textarea(html.Name("description"), gomponents.Text(m.Description)),
		html.Label(gomponents.Text("Tags (comma separated)")),
		html.Input(html.Name("tags"), html.Value(stringsJoin(m.Tags))),
		html.Label(gomponents.Text("SQL")),
		html.Textarea(html.Name("sql"), gomponents.Text(m.SQL), html.Required()),
	)
}

func (h *Handler) ModelsUpdate(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	principal, _ := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	sql := formString(r.Form, "sql")
	materialization := formString(r.Form, "materialization")
	description := formString(r.Form, "description")
	_, err := h.Model.UpdateModel(r.Context(), principal, projectName, modelName, domain.UpdateModelRequest{
		SQL:             &sql,
		Materialization: &materialization,
		Description:     &description,
		Tags:            formCSV(r.Form, "tags"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+projectName+"/"+modelName, http.StatusSeeOther)
}

func (h *Handler) ModelsDelete(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	principal, _ := principalLabel(r.Context())
	if err := h.Model.DeleteModel(r.Context(), principal, projectName, modelName); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models", http.StatusSeeOther)
}

func (h *Handler) ModelTestsNew(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	renderFormPage(w, r, "New Model Test", "models", "/ui/models/"+projectName+"/"+modelName+"/tests",
		html.Label(gomponents.Text("Name")),
		html.Input(html.Name("name"), html.Required()),
		html.Label(gomponents.Text("Type")),
		html.Select(html.Name("test_type"), html.Option(html.Value("not_null"), gomponents.Text("not_null")), html.Option(html.Value("unique"), gomponents.Text("unique")), html.Option(html.Value("accepted_values"), gomponents.Text("accepted_values")), html.Option(html.Value("relationships"), gomponents.Text("relationships")), html.Option(html.Value("custom_sql"), gomponents.Text("custom_sql"))),
		html.Label(gomponents.Text("Column")),
		html.Input(html.Name("column")),
		html.Label(gomponents.Text("Values (accepted_values, comma separated)")),
		html.Input(html.Name("values")),
		html.Label(gomponents.Text("To Model (relationships)")),
		html.Input(html.Name("to_model")),
		html.Label(gomponents.Text("To Column (relationships)")),
		html.Input(html.Name("to_column")),
		html.Label(gomponents.Text("SQL (custom_sql)")),
		html.Textarea(html.Name("test_sql")),
	)
}

func (h *Handler) ModelTestsCreate(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	principal, _ := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	_, err := h.Model.CreateTest(r.Context(), principal, projectName, modelName, domain.CreateModelTestRequest{
		Name:     formString(r.Form, "name"),
		TestType: formString(r.Form, "test_type"),
		Column:   formString(r.Form, "column"),
		Config: domain.ModelTestConfig{
			Values:   formCSV(r.Form, "values"),
			ToModel:  formString(r.Form, "to_model"),
			ToColumn: formString(r.Form, "to_column"),
			SQL:      formString(r.Form, "test_sql"),
		},
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+projectName+"/"+modelName, http.StatusSeeOther)
}

func (h *Handler) ModelTestsDelete(w http.ResponseWriter, r *http.Request) {
	projectName := chi.URLParam(r, "projectName")
	modelName := chi.URLParam(r, "modelName")
	testID := chi.URLParam(r, "testID")
	principal, _ := principalLabel(r.Context())
	if err := h.Model.DeleteTest(r.Context(), principal, projectName, modelName, testID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models/"+projectName+"/"+modelName, http.StatusSeeOther)
}

func (h *Handler) ModelRunsTrigger(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	req := domain.TriggerModelRunRequest{
		TargetCatalog: formString(r.Form, "target_catalog"),
		TargetSchema:  formString(r.Form, "target_schema"),
		Selector:      formString(r.Form, "selector"),
		TriggerType:   domain.ModelTriggerTypeManual,
		FullRefresh:   formBool(r.Form, "full_refresh"),
	}
	if _, err := h.Model.TriggerRun(r.Context(), principal, req); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	projectName := formString(r.Form, "project_name")
	modelName := formString(r.Form, "model_name")
	if projectName != "" && modelName != "" {
		http.Redirect(w, r, "/ui/models/"+projectName+"/"+modelName, http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/ui/models", http.StatusSeeOther)
}

func (h *Handler) ModelRunsCancel(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	principal, _ := principalLabel(r.Context())
	if err := h.Model.CancelRun(r.Context(), principal, runID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models", http.StatusSeeOther)
}

func (h *Handler) ModelRunsManualCancel(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if err := r.ParseForm(); err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "Unable to parse form."))
		return
	}
	runID := formString(r.Form, "run_id")
	if runID == "" {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "run_id is required."))
		return
	}
	if err := h.Model.CancelRun(r.Context(), principal, runID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/models", http.StatusSeeOther)
}
