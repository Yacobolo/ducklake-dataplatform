package ui

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
)

func (h *Handler) PipelinesList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.Pipeline.ListPipelines(r.Context(), pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]pipelinesListRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, pipelinesListRowData{
			Filter:   item.Name,
			Name:     item.Name,
			URL:      "/ui/pipelines/" + item.Name,
			Paused:   fmt.Sprintf("%t", item.IsPaused),
			Schedule: strOrDash(item.ScheduleCron),
			Updated:  formatTime(item.UpdatedAt),
		})
	}

	renderHTML(w, http.StatusOK, pipelinesListPage(principalFromContext(r.Context()), rows, pageReq, total))
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

	jobRows := make([]pipelineJobRowData, 0, len(jobs))
	for i := range jobs {
		j := jobs[i]
		jobRows = append(jobRows, pipelineJobRowData{Name: j.Name, JobType: j.JobType, Selector: j.ModelSelector, Notebook: j.NotebookID, DeleteURL: "/ui/pipelines/" + name + "/jobs/" + j.ID + "/delete"})
	}
	runRows := make([]pipelineRunRowData, 0, len(runs))
	for i := range runs {
		run := runs[i]
		runRows = append(runRows, pipelineRunRowData{ID: run.ID, Status: run.Status, Trigger: run.TriggerType, Started: formatTimePtr(run.StartedAt), Finished: formatTimePtr(run.FinishedAt), CancelURL: "/ui/pipelines/runs/" + run.ID + "/cancel"})
	}

	renderHTML(w, http.StatusOK, pipelineDetailPage(pipelineDetailPageData{
		Principal:     principalFromContext(r.Context()),
		Name:          pipe.Name,
		CreatedBy:     pipe.CreatedBy,
		Concurrency:   strconv.Itoa(pipe.ConcurrencyLimit),
		Schedule:      strOrDash(pipe.ScheduleCron),
		EditURL:       "/ui/pipelines/" + name + "/edit",
		DeleteURL:     "/ui/pipelines/" + name + "/delete",
		TriggerURL:    "/ui/pipelines/" + name + "/runs/trigger",
		NewJobURL:     "/ui/pipelines/" + name + "/jobs/new",
		Jobs:          jobRows,
		Runs:          runRows,
		CSRFFieldFunc: csrfFieldProvider(r),
	}))
}

func (h *Handler) PipelinesNew(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, pipelinesNewPage(principalFromContext(r.Context()), csrfFieldProvider(r)))
}

func (h *Handler) PipelinesCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
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
	renderHTML(w, http.StatusOK, pipelinesEditPage(principalFromContext(r.Context()), name, p, csrfFieldProvider(r)))
}

func (h *Handler) PipelinesUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
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
	renderHTML(w, http.StatusOK, pipelineJobsNewPage(principalFromContext(r.Context()), name, csrfFieldProvider(r)))
}

func (h *Handler) PipelineJobsCreate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "pipelineName")
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
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
