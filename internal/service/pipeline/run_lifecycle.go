package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
)

func (s *Service) validateComputeEndpoint(ctx context.Context, computeEndpointID *string) error {
	if computeEndpointID == nil || strings.TrimSpace(*computeEndpointID) == "" {
		return nil
	}
	if s.computeRepo == nil {
		return domain.ErrValidation("compute_endpoint_id is not supported: compute endpoint repository is not configured")
	}
	if _, err := s.computeRepo.GetByID(ctx, strings.TrimSpace(*computeEndpointID)); err != nil {
		return fmt.Errorf("validate compute endpoint: %w", err)
	}
	return nil
}

func (s *Service) loadPipelineExecution(ctx context.Context, pipelineName string) (*domain.Pipeline, []domain.PipelineJob, [][]string, error) {
	pipelineDef, err := s.pipelines.GetPipelineByName(ctx, pipelineName)
	if err != nil {
		return nil, nil, nil, err
	}

	jobs, err := s.pipelines.ListJobsByPipeline(ctx, pipelineDef.ID)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(jobs) == 0 {
		return nil, nil, nil, domain.ErrValidation("pipeline has no jobs")
	}

	levels, err := ResolveExecutionOrder(jobs)
	if err != nil {
		return nil, nil, nil, err
	}

	return pipelineDef, jobs, levels, nil
}

func (s *Service) shouldQueueRun(ctx context.Context, pipelineDef *domain.Pipeline, triggerType string) (bool, error) {
	limit := normalizedConcurrencyLimit(pipelineDef)
	activeRuns, err := s.runs.CountActiveRuns(ctx, pipelineDef.ID)
	if err != nil {
		return false, fmt.Errorf("count active runs: %w", err)
	}
	if activeRuns < int64(limit) {
		return false, nil
	}
	if normalizeAdmissionMode(pipelineDef.AdmissionMode) == domain.PipelineAdmissionModeQueue {
		return true, nil
	}

	msg := fmt.Sprintf("pipeline %q already has %d active run(s), concurrency_limit=%d", pipelineDef.Name, activeRuns, limit)
	if triggerType == domain.TriggerTypeScheduled {
		s.logger.Info("skipping scheduled pipeline trigger due to concurrency limit", "pipeline", pipelineDef.Name, "active_runs", activeRuns, "concurrency_limit", limit)
	}
	return false, domain.ErrConflict("%s", msg)
}

func normalizedConcurrencyLimit(pipelineDef *domain.Pipeline) int {
	if pipelineDef == nil || pipelineDef.ConcurrencyLimit <= 0 {
		return 1
	}
	return pipelineDef.ConcurrencyLimit
}

func (s *Service) createRunRecords(
	ctx context.Context,
	pipelineDef *domain.Pipeline,
	jobs []domain.PipelineJob,
	triggeredBy string,
	effectivePrincipal string,
	params map[string]string,
	triggerType string,
	queued bool,
	repairedFromRunID *string,
	selectedJobs map[string]bool,
) (*domain.PipelineRun, map[string]*domain.PipelineJobRun, error) {
	provenance, gitCommitHash, err := s.buildRunProvenance(ctx, pipelineDef, jobs, triggerType, triggeredBy, effectivePrincipal)
	if err != nil {
		return nil, nil, err
	}

	now := time.Now().UTC()
	var queuedAt *time.Time
	var queueStartedAt *time.Time
	if queued {
		queuedAt = &now
	} else {
		queueStartedAt = &now
	}

	run := &domain.PipelineRun{
		ID:                 domain.NewID(),
		PipelineID:         pipelineDef.ID,
		Status:             domain.PipelineRunStatusPending,
		TriggerType:        triggerType,
		TriggeredBy:        triggeredBy,
		EffectivePrincipal: effectivePrincipal,
		Parameters:         cloneParams(params),
		GitCommitHash:      gitCommitHash,
		QueuedAt:           queuedAt,
		QueueStartedAt:     queueStartedAt,
		RepairedFromRunID:  repairedFromRunID,
		Provenance:         provenance,
	}

	createdRun, err := s.runs.CreateRun(ctx, run)
	if err != nil {
		return nil, nil, fmt.Errorf("create pipeline run: %w", err)
	}
	s.logRunEventAndNotify(ctx, createdRun, domain.PipelineRunEventAdmitted, nil, nil, map[string]any{"queued": queued})
	if queued {
		s.logRunEventAndNotify(ctx, createdRun, domain.PipelineRunEventQueued, pipelineMessagePtr("run accepted into queue"), nil, nil)
	}
	if repairedFromRunID != nil {
		s.logRunEventAndNotify(ctx, createdRun, domain.PipelineRunEventRepaired, pipelineMessagePtr("repair run created"), nil, map[string]any{"repaired_from_run_id": *repairedFromRunID})
	}

	jobRunsByJobID := make(map[string]*domain.PipelineJobRun, len(jobs))
	for _, job := range jobs {
		status := domain.PipelineJobRunStatusPending
		var errMessage *string
		if len(selectedJobs) > 0 && !selectedJobs[job.ID] {
			status = domain.PipelineJobRunStatusSkipped
			errMessage = pipelineMessagePtr("reused from repaired run")
		}
		jobRun := &domain.PipelineJobRun{
			ID:           domain.NewID(),
			RunID:        createdRun.ID,
			JobID:        job.ID,
			JobName:      job.Name,
			Status:       status,
			ErrorMessage: errMessage,
		}
		createdJobRun, err := s.runs.CreateJobRun(ctx, jobRun)
		if err != nil {
			msg := err.Error()
			_ = s.runs.UpdateRunFinished(ctx, createdRun.ID, domain.PipelineRunStatusFailed, &msg)
			return nil, nil, fmt.Errorf("create pipeline job run: %w", err)
		}
		jobRunsByJobID[job.ID] = createdJobRun
		if status == domain.PipelineJobRunStatusSkipped {
			s.logJobRunEvent(ctx, createdRun.ID, createdJobRun.ID, domain.PipelineRunEventSkipped, errMessage, nil, map[string]any{"job_id": job.ID})
		}
	}

	return createdRun, jobRunsByJobID, nil
}

func (s *Service) failRun(ctx context.Context, runID, message string) {
	if s.runCancelled(ctx, runID) {
		return
	}
	run, err := s.runs.GetRunByID(ctx, runID)
	if err != nil {
		return
	}
	_ = s.runs.UpdateRunFinished(ctx, runID, domain.PipelineRunStatusFailed, nonEmptyStringPtr(message))
	run.Status = domain.PipelineRunStatusFailed
	run.ErrorMessage = nonEmptyStringPtr(message)
	s.logRunEventAndNotify(ctx, run, domain.PipelineRunEventFailed, nonEmptyStringPtr(message), pipelineMessagePtr("EXECUTION_ERROR"), nil)
	go s.dispatchQueuedRuns(context.Background(), run.PipelineID)
}

func (s *Service) finishFailedRunJobRuns(ctx context.Context, runID, message string) {
	jobRuns, err := s.runs.ListJobRunsByRun(ctx, runID)
	if err != nil {
		return
	}
	for _, jobRun := range jobRuns {
		switch jobRun.Status {
		case domain.PipelineJobRunStatusPending:
			_ = s.runs.UpdateJobRunFinished(ctx, jobRun.ID, domain.PipelineJobRunStatusSkipped, nonEmptyStringPtr(message), nil, jobRun.AttemptCount)
			s.logJobRunEvent(ctx, runID, jobRun.ID, domain.PipelineRunEventSkipped, nonEmptyStringPtr(message), nil, nil)
		case domain.PipelineJobRunStatusRunning:
			_ = s.runs.UpdateJobRunFinished(ctx, jobRun.ID, domain.PipelineJobRunStatusFailed, nonEmptyStringPtr(message), pipelineMessagePtr("EXECUTION_ERROR"), jobRun.AttemptCount)
			s.logJobRunEvent(ctx, runID, jobRun.ID, domain.PipelineRunEventFailed, nonEmptyStringPtr(message), pipelineMessagePtr("EXECUTION_ERROR"), nil)
		}
	}
}

func (s *Service) cancelPendingJobRuns(ctx context.Context, runID string) {
	jobRuns, err := s.runs.ListJobRunsByRun(ctx, runID)
	if err != nil {
		return
	}
	for _, jobRun := range jobRuns {
		if jobRun.Status != domain.PipelineJobRunStatusPending {
			continue
		}
		_ = s.runs.UpdateJobRunFinished(ctx, jobRun.ID, domain.PipelineJobRunStatusCancelled, nil, pipelineMessagePtr("CANCELLED"), jobRun.AttemptCount)
		s.logJobRunEvent(ctx, runID, jobRun.ID, domain.PipelineRunEventCancelled, pipelineMessagePtr("job cancelled"), pipelineMessagePtr("CANCELLED"), nil)
	}
}

func (s *Service) failActiveJobRuns(ctx context.Context, runID, message string) {
	jobRuns, err := s.runs.ListJobRunsByRun(ctx, runID)
	if err != nil {
		return
	}
	for _, jobRun := range jobRuns {
		switch jobRun.Status {
		case domain.PipelineJobRunStatusPending, domain.PipelineJobRunStatusRunning:
			_ = s.runs.UpdateJobRunFinished(ctx, jobRun.ID, domain.PipelineJobRunStatusFailed, nonEmptyStringPtr(message), pipelineMessagePtr("EXECUTION_ERROR"), jobRun.AttemptCount)
			s.logJobRunEvent(ctx, runID, jobRun.ID, domain.PipelineRunEventFailed, nonEmptyStringPtr(message), pipelineMessagePtr("EXECUTION_ERROR"), nil)
		}
	}
}

func (s *Service) runCancelled(ctx context.Context, runID string) bool {
	run, err := s.runs.GetRunByID(ctx, runID)
	if err != nil {
		return false
	}
	return run.Status == domain.PipelineRunStatusCancelled
}

// ReconcileActiveRuns repairs pipeline run state after restarts and resumes queued work.
func (s *Service) ReconcileActiveRuns(ctx context.Context) error {
	if err := s.requireRunsRepo(); err != nil {
		return err
	}

	activeRuns, err := s.listActiveRuns(ctx)
	if err != nil {
		return err
	}

	for i := range activeRuns {
		if err := s.reconcileRun(ctx, &activeRuns[i]); err != nil {
			return err
		}
	}

	if err := s.resumeQueuedRuns(ctx); err != nil {
		return err
	}

	return nil
}

func (s *Service) listActiveRuns(ctx context.Context) ([]domain.PipelineRun, error) {
	statuses := []string{domain.PipelineRunStatusPending, domain.PipelineRunStatusRunning}
	runs := make([]domain.PipelineRun, 0)

	for _, status := range statuses {
		status := status
		page := domain.PageRequest{MaxResults: 200}
		for {
			found, total, err := s.runs.ListRuns(ctx, domain.PipelineRunFilter{
				Status: &status,
				Page:   page,
			})
			if err != nil {
				return nil, fmt.Errorf("list %s pipeline runs: %w", status, err)
			}
			runs = append(runs, found...)

			next := domain.NextPageToken(page.Offset(), page.Limit(), total)
			if next == "" {
				break
			}
			page.PageToken = next
		}
	}

	return runs, nil
}

func (s *Service) reconcileRun(ctx context.Context, run *domain.PipelineRun) error {
	if run == nil {
		return nil
	}

	persistCtx := context.Background()
	recoveryErr := "pipeline run could not be resumed after restart"

	if s.assetRunRepo == nil {
		s.failActiveJobRuns(persistCtx, run.ID, recoveryErr)
		return s.runs.UpdateRunFinished(persistCtx, run.ID, domain.PipelineRunStatusFailed, &recoveryErr)
	}

	assetRuns, _, err := s.assetRunRepo.ListRuns(ctx, domain.AssetRunFilter{
		RunGroupID: &run.ID,
		Page:       domain.PageRequest{MaxResults: 10},
	})
	if err != nil {
		return fmt.Errorf("list correlated asset runs for pipeline run %s: %w", run.ID, err)
	}
	if len(assetRuns) == 0 {
		s.failActiveJobRuns(persistCtx, run.ID, recoveryErr)
		return s.runs.UpdateRunFinished(persistCtx, run.ID, domain.PipelineRunStatusFailed, &recoveryErr)
	}

	assetRun := assetRuns[0]
	switch assetRun.Status {
	case domain.AssetRunStatusSuccess:
		if err := s.runs.UpdateRunFinished(persistCtx, run.ID, domain.PipelineRunStatusSuccess, nil); err != nil {
			return err
		}
		run.Status = domain.PipelineRunStatusSuccess
		s.logRunEventAndNotify(persistCtx, run, domain.PipelineRunEventSucceeded, nil, nil, nil)
		go s.dispatchQueuedRuns(context.Background(), run.PipelineID)
		return nil
	case domain.AssetRunStatusCancelled:
		s.cancelPendingJobRuns(persistCtx, run.ID)
		msg := "pipeline run cancelled during previous process"
		if err := s.runs.UpdateRunFinished(persistCtx, run.ID, domain.PipelineRunStatusCancelled, &msg); err != nil {
			return err
		}
		run.Status = domain.PipelineRunStatusCancelled
		run.ErrorMessage = &msg
		s.logRunEventAndNotify(persistCtx, run, domain.PipelineRunEventCancelled, &msg, pipelineMessagePtr("CANCELLED"), nil)
		go s.dispatchQueuedRuns(context.Background(), run.PipelineID)
		return nil
	case domain.AssetRunStatusFailed:
		msg := recoveryErr
		if assetRun.ErrorMessage != nil && strings.TrimSpace(*assetRun.ErrorMessage) != "" {
			msg = *assetRun.ErrorMessage
		}
		s.finishFailedRunJobRuns(persistCtx, run.ID, msg)
		if err := s.runs.UpdateRunFinished(persistCtx, run.ID, domain.PipelineRunStatusFailed, &msg); err != nil {
			return err
		}
		run.Status = domain.PipelineRunStatusFailed
		run.ErrorMessage = &msg
		s.logRunEventAndNotify(persistCtx, run, domain.PipelineRunEventFailed, &msg, pipelineMessagePtr("EXECUTION_ERROR"), nil)
		go s.dispatchQueuedRuns(context.Background(), run.PipelineID)
		return nil
	default:
		s.failActiveJobRuns(persistCtx, run.ID, recoveryErr)
		updateErr := s.runs.UpdateRunFinished(persistCtx, run.ID, domain.PipelineRunStatusFailed, &recoveryErr)
		cancelErr := s.assetRunRepo.UpdateRunFinished(persistCtx, assetRun.ID, domain.AssetRunStatusFailed, &recoveryErr)
		if updateErr != nil {
			return updateErr
		}
		if cancelErr != nil && !errors.As(cancelErr, new(*domain.NotFoundError)) {
			return cancelErr
		}
		return nil
	}
}

func nonEmptyStringPtr(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return &value
}

func (s *Service) resumeQueuedRuns(ctx context.Context) error {
	if err := s.requirePipelinesRepo(); err != nil {
		return err
	}
	page := domain.PageRequest{MaxResults: 200}
	for {
		pipelines, total, err := s.pipelines.ListPipelines(ctx, page)
		if err != nil {
			return fmt.Errorf("list pipelines for queue resume: %w", err)
		}
		for i := range pipelines {
			go s.dispatchQueuedRuns(context.Background(), pipelines[i].ID)
		}
		next := domain.NextPageToken(page.Offset(), page.Limit(), total)
		if next == "" {
			return nil
		}
		page.PageToken = next
	}
}
