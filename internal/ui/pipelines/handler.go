package pipelines

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"
	"duck-demo/internal/ui/legacy"
)

type Handler struct {
	legacy *legacy.Handler
}

func New(h *legacy.Handler) *Handler { return &Handler{legacy: h} }

func (h *Handler) PipelinesList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.legacy.Pipeline.ListPipelines(r.Context(), pageReq)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	rows := make([]pipelinesListRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, pipelinesListRowData{
			Name:     item.Name,
			URL:      "/ui/pipelines/" + item.Name,
			Paused:   item.IsPaused,
			Schedule: strOrDash(item.ScheduleCron),
			Updated:  formatTime(item.UpdatedAt),
		})
	}

	core.RenderHTML(w, http.StatusOK, pipelinesListPage(core.PrincipalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) PipelinesDetail(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	pipe, err := h.legacy.Pipeline.GetPipeline(r.Context(), name)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	jobs, _ := h.legacy.Pipeline.ListJobs(r.Context(), name)

	jobRows := make([]pipelineJobRowData, 0, len(jobs))
	for i := range jobs {
		j := jobs[i]
		jobRows = append(jobRows, pipelineJobRowData{
			Name:      j.Name,
			JobType:   j.JobType,
			Selector:  j.ModelSelector,
			Notebook:  j.NotebookID,
			DeleteURL: "/ui/pipelines/" + name + "/jobs/" + j.ID + "/delete",
		})
	}

	core.RenderHTML(w, http.StatusOK, pipelineDetailPage(pipelineDetailPageData{
		Principal:     core.PrincipalFromContext(r.Context()),
		Name:          pipe.Name,
		CreatedBy:     pipe.CreatedBy,
		Concurrency:   strconv.Itoa(pipe.ConcurrencyLimit),
		Schedule:      strOrDash(pipe.ScheduleCron),
		EditURL:       "/ui/pipelines/" + name + "/edit",
		DeleteURL:     "/ui/pipelines/" + name + "/delete",
		NewJobURL:     "/ui/pipelines/" + name + "/jobs/new",
		Jobs:          jobRows,
		CSRFFieldFunc: h.legacy.CSRFFieldProvider(r),
	}))
}

func (h *Handler) PipelinesNew(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, pipelinesNewPage(core.PrincipalFromContext(r.Context()), h.legacy.CSRFFieldProvider(r)))
}

func (h *Handler) PipelinesCreate(w http.ResponseWriter, r *http.Request) {
	principal := principalName(r)
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	concurrency := 1
	if p, err := formOptionalInt(r.Form, "concurrency_limit"); err == nil && p != nil {
		concurrency = *p
	}
	_, err := h.legacy.Pipeline.CreatePipeline(r.Context(), principal, domain.CreatePipelineRequest{
		Name:             formString(r.Form, "name"),
		Description:      formString(r.Form, "description"),
		ScheduleCron:     formOptionalString(r.Form, "schedule_cron"),
		IsPaused:         formBool(r.Form, "is_paused"),
		ConcurrencyLimit: concurrency,
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/pipelines", http.StatusSeeOther)
}

func (h *Handler) PipelinesEdit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	p, err := h.legacy.Pipeline.GetPipeline(r.Context(), name)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, pipelinesEditPage(core.PrincipalFromContext(r.Context()), name, p, h.legacy.CSRFFieldProvider(r)))
}

func (h *Handler) PipelinesUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	principal := principalName(r)
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	concurrency, err := formOptionalInt(r.Form, "concurrency_limit")
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "concurrency_limit must be an integer."))
		return
	}
	schedule := formString(r.Form, "schedule_cron")
	desc := formString(r.Form, "description")
	isPaused := formBool(r.Form, "is_paused")
	_, err = h.legacy.Pipeline.UpdatePipeline(r.Context(), principal, name, domain.UpdatePipelineRequest{
		Description:      &desc,
		ScheduleCron:     &schedule,
		IsPaused:         &isPaused,
		ConcurrencyLimit: concurrency,
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/pipelines/"+name, http.StatusSeeOther)
}

func (h *Handler) PipelinesDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	if err := h.legacy.Pipeline.DeletePipeline(r.Context(), principalName(r), name); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/pipelines", http.StatusSeeOther)
}

func (h *Handler) PipelineJobsNew(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	core.RenderHTML(w, http.StatusOK, pipelineJobsNewPage(core.PrincipalFromContext(r.Context()), name, h.legacy.CSRFFieldProvider(r)))
}

func (h *Handler) PipelineJobsCreate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.legacy.Pipeline.CreateJob(r.Context(), principalName(r), name, domain.CreatePipelineJobRequest{
		Name:          formString(r.Form, "name"),
		DependsOn:     formCSV(r.Form, "depends_on"),
		NotebookID:    formString(r.Form, "notebook_id"),
		JobType:       formString(r.Form, "job_type"),
		ModelSelector: formString(r.Form, "model_selector"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/pipelines/"+name, http.StatusSeeOther)
}

func (h *Handler) PipelineJobsDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	jobID := chi.URLParam(r, "jobID")
	if err := h.legacy.Pipeline.DeleteJob(r.Context(), principalName(r), name, jobID); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/pipelines/"+name, http.StatusSeeOther)
}
