package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

func (s *Service) enforceConcurrencyLimit(ctx context.Context, pipelineDef *domain.Pipeline, triggerType string) error {
	limit := normalizedConcurrencyLimit(pipelineDef)
	activeRuns, err := s.runs.CountActiveRuns(ctx, pipelineDef.ID)
	if err != nil {
		return fmt.Errorf("count active runs: %w", err)
	}
	if activeRuns < int64(limit) {
		return nil
	}

	msg := fmt.Sprintf("pipeline %q already has %d active run(s), concurrency_limit=%d", pipelineDef.Name, activeRuns, limit)
	if triggerType == domain.TriggerTypeScheduled {
		s.logger.Info("skipping scheduled pipeline trigger due to concurrency limit", "pipeline", pipelineDef.Name, "active_runs", activeRuns, "concurrency_limit", limit)
	}
	return domain.ErrConflict("%s", msg)
}

func normalizedConcurrencyLimit(pipelineDef *domain.Pipeline) int {
	if pipelineDef == nil || pipelineDef.ConcurrencyLimit <= 0 {
		return 1
	}
	return pipelineDef.ConcurrencyLimit
}

func (s *Service) createRunRecords(ctx context.Context, pipelineDef *domain.Pipeline, jobs []domain.PipelineJob, principal string,
	params map[string]string, triggerType string) (*domain.PipelineRun, map[string]*domain.PipelineJobRun, error) {

	run := &domain.PipelineRun{
		ID:          domain.NewID(),
		PipelineID:  pipelineDef.ID,
		Status:      domain.PipelineRunStatusPending,
		TriggerType: triggerType,
		TriggeredBy: principal,
		Parameters:  cloneParams(params),
	}

	createdRun, err := s.runs.CreateRun(ctx, run)
	if err != nil {
		return nil, nil, fmt.Errorf("create pipeline run: %w", err)
	}

	jobRunsByJobID := make(map[string]*domain.PipelineJobRun, len(jobs))
	for _, job := range jobs {
		jobRun := &domain.PipelineJobRun{
			ID:      domain.NewID(),
			RunID:   createdRun.ID,
			JobID:   job.ID,
			JobName: job.Name,
			Status:  domain.PipelineJobRunStatusPending,
		}
		createdJobRun, err := s.runs.CreateJobRun(ctx, jobRun)
		if err != nil {
			msg := err.Error()
			_ = s.runs.UpdateRunFinished(ctx, createdRun.ID, domain.PipelineRunStatusFailed, &msg)
			return nil, nil, fmt.Errorf("create pipeline job run: %w", err)
		}
		jobRunsByJobID[job.ID] = createdJobRun
	}

	return createdRun, jobRunsByJobID, nil
}

func (s *Service) failRun(ctx context.Context, runID, message string) {
	if s.runCancelled(ctx, runID) {
		return
	}
	_ = s.runs.UpdateRunFinished(ctx, runID, domain.PipelineRunStatusFailed, nonEmptyStringPtr(message))
}

func (s *Service) skipPendingJobRuns(ctx context.Context, runID, message string) {
	jobRuns, err := s.runs.ListJobRunsByRun(ctx, runID)
	if err != nil {
		return
	}
	for _, jobRun := range jobRuns {
		if jobRun.Status != domain.PipelineJobRunStatusPending {
			continue
		}
		_ = s.runs.UpdateJobRunFinished(ctx, jobRun.ID, domain.PipelineJobRunStatusSkipped, nonEmptyStringPtr(message))
	}
}

func (s *Service) finishFailedRunJobRuns(ctx context.Context, runID, message string) {
	jobRuns, err := s.runs.ListJobRunsByRun(ctx, runID)
	if err != nil {
		return
	}
	for _, jobRun := range jobRuns {
		switch jobRun.Status {
		case domain.PipelineJobRunStatusPending:
			_ = s.runs.UpdateJobRunFinished(ctx, jobRun.ID, domain.PipelineJobRunStatusSkipped, nonEmptyStringPtr(message))
		case domain.PipelineJobRunStatusRunning:
			_ = s.runs.UpdateJobRunFinished(ctx, jobRun.ID, domain.PipelineJobRunStatusFailed, nonEmptyStringPtr(message))
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
		_ = s.runs.UpdateJobRunFinished(ctx, jobRun.ID, domain.PipelineJobRunStatusCancelled, nil)
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
			_ = s.runs.UpdateJobRunFinished(ctx, jobRun.ID, domain.PipelineJobRunStatusFailed, nonEmptyStringPtr(message))
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
		return s.runs.UpdateRunFinished(persistCtx, run.ID, domain.PipelineRunStatusSuccess, nil)
	case domain.AssetRunStatusCancelled:
		s.cancelPendingJobRuns(persistCtx, run.ID)
		msg := "pipeline run cancelled during previous process"
		return s.runs.UpdateRunFinished(persistCtx, run.ID, domain.PipelineRunStatusCancelled, &msg)
	case domain.AssetRunStatusFailed:
		msg := recoveryErr
		if assetRun.ErrorMessage != nil && strings.TrimSpace(*assetRun.ErrorMessage) != "" {
			msg = *assetRun.ErrorMessage
		}
		s.finishFailedRunJobRuns(persistCtx, run.ID, msg)
		return s.runs.UpdateRunFinished(persistCtx, run.ID, domain.PipelineRunStatusFailed, &msg)
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
