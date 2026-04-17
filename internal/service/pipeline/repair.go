package pipeline

import (
	"context"

	"github.com/Yacobolo/quackstack/internal/domain"
)

// ListRunEvents returns the durable event log for a pipeline run.
func (s *Service) ListRunEvents(ctx context.Context, runID string, page domain.PageRequest) ([]domain.PipelineRunEvent, int64, error) {
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return nil, 0, err
	}
	pipelineDef, err := s.pipelines.GetPipelineByID(ctx, run.PipelineID)
	if err != nil {
		return nil, 0, err
	}
	if err := s.requirePipelineRun(ctx, servicePrincipalName(ctx), pipelineDef); err != nil {
		return nil, 0, err
	}
	return s.runEventsPage(ctx, runID, page)
}

// RepairRun creates a new pipeline run that repairs a previous failed run.
func (s *Service) RepairRun(ctx context.Context, principal string, runID string, req domain.RepairPipelineRunRequest) (*domain.PipelineRun, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	originalRun, err := s.runs.GetRunByID(ctx, runID)
	if err != nil {
		return nil, err
	}
	pipelineDef, err := s.pipelines.GetPipelineByID(ctx, originalRun.PipelineID)
	if err != nil {
		return nil, err
	}
	if err := s.requirePipelineManage(ctx, principal, pipelineDef); err != nil {
		return nil, err
	}
	jobs, err := s.pipelines.ListJobsByPipeline(ctx, pipelineDef.ID)
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return nil, domain.ErrValidation("pipeline has no jobs")
	}
	selectedJobs, err := s.repairSelection(ctx, runID, jobs, req)
	if err != nil {
		return nil, err
	}
	levels, err := ResolveExecutionOrder(jobs)
	if err != nil {
		return nil, err
	}
	queued, err := s.shouldQueueRun(ctx, pipelineDef, domain.TriggerTypeManual)
	if err != nil {
		return nil, err
	}
	run, jobRunsByJobID, err := s.createRunRecords(
		ctx,
		pipelineDef,
		jobs,
		principal,
		originalRun.EffectivePrincipal,
		cloneParams(originalRun.Parameters),
		domain.TriggerTypeManual,
		queued,
		&originalRun.ID,
		selectedJobs,
	)
	if err != nil {
		return nil, err
	}
	if !queued {
		s.dispatchRunExecution(*run, pipelineDef, jobs, levels, jobRunsByJobID)
	}
	return run, nil
}

func (s *Service) repairSelection(ctx context.Context, runID string, jobs []domain.PipelineJob, req domain.RepairPipelineRunRequest) (map[string]bool, error) {
	jobRuns, err := s.runs.ListJobRunsByRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	selected := make(map[string]bool)
	switch req.Mode {
	case domain.PipelineRepairModeFailedOnly:
		for _, jobRun := range jobRuns {
			if jobRun.Status == domain.PipelineJobRunStatusFailed || jobRun.Status == domain.PipelineJobRunStatusSkipped {
				selected[jobRun.JobID] = true
			}
		}
	case domain.PipelineRepairModeFromJob:
		found := false
		for _, job := range jobs {
			if job.ID == *req.FromJobID {
				found = true
				break
			}
		}
		if !found {
			return nil, domain.ErrValidation("from_job_id %q is not part of the pipeline", *req.FromJobID)
		}
		graph := downstreamJobIDs(jobs)
		selected[*req.FromJobID] = true
		for _, downstream := range graph[*req.FromJobID] {
			selected[downstream] = true
		}
	}
	if len(selected) == 0 {
		return nil, domain.ErrValidation("repair selection resolved to zero jobs")
	}
	return selected, nil
}

func downstreamJobIDs(jobs []domain.PipelineJob) map[string][]string {
	byName := make(map[string]string, len(jobs))
	deps := make(map[string][]string)
	for _, job := range jobs {
		byName[job.Name] = job.ID
	}
	for _, job := range jobs {
		for _, depName := range job.DependsOn {
			if upstreamID, ok := byName[depName]; ok {
				deps[upstreamID] = append(deps[upstreamID], job.ID)
			}
		}
	}

	out := make(map[string][]string, len(jobs))
	for _, job := range jobs {
		seen := map[string]bool{}
		queue := append([]string{}, deps[job.ID]...)
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			if seen[current] {
				continue
			}
			seen[current] = true
			out[job.ID] = append(out[job.ID], current)
			queue = append(queue, deps[current]...)
		}
	}
	return out
}
