package pipeline

import (
	"context"
	"strings"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
)

func (s *Service) dispatchQueuedRuns(ctx context.Context, pipelineID string) {
	if strings.TrimSpace(pipelineID) == "" {
		return
	}
	if _, loaded := s.dispatching.LoadOrStore(pipelineID, struct{}{}); loaded {
		return
	}
	defer s.dispatching.Delete(pipelineID)

	if err := s.requirePipelinesRepo(); err != nil || s.runs == nil {
		return
	}
	pipelineDef, err := s.pipelines.GetPipelineByID(ctx, pipelineID)
	if err != nil {
		return
	}
	jobs, err := s.pipelines.ListJobsByPipeline(ctx, pipelineID)
	if err != nil || len(jobs) == 0 {
		return
	}
	levels, err := ResolveExecutionOrder(jobs)
	if err != nil {
		return
	}
	limit := normalizedConcurrencyLimit(pipelineDef)

	for {
		activeRuns, err := s.runs.CountActiveRuns(ctx, pipelineID)
		if err != nil || activeRuns >= int64(limit) {
			return
		}
		queuedRuns, err := s.runs.ListQueuedRuns(ctx, pipelineID, 1)
		if err != nil || len(queuedRuns) == 0 {
			return
		}
		run := queuedRuns[0]
		if err := s.runs.UpdateRunQueueStarted(ctx, run.ID); err != nil {
			return
		}
		run.QueueStartedAt = timePtr(time.Now().UTC())
		jobRunsByJobID, err := s.jobRunMap(ctx, run.ID)
		if err != nil {
			return
		}
		s.dispatchRunExecution(run, pipelineDef, jobs, levels, jobRunsByJobID)
	}
}

func (s *Service) dispatchRunExecution(run domain.PipelineRun, pipelineDef *domain.Pipeline, jobs []domain.PipelineJob, levels [][]string, jobRunsByJobID map[string]*domain.PipelineJobRun) {
	baseCtx, baseCancel := context.WithCancel(context.Background())
	execCtx := baseCtx
	cancel := context.CancelFunc(baseCancel)
	if pipelineDef.MaxRunDurationSeconds != nil && *pipelineDef.MaxRunDurationSeconds > 0 {
		timeoutCtx, timeoutCancel := context.WithTimeout(baseCtx, time.Duration(*pipelineDef.MaxRunDurationSeconds)*time.Second)
		execCtx = timeoutCtx
		cancel = func() {
			timeoutCancel()
			baseCancel()
		}
	}
	s.runCancels.Store(run.ID, cancel)
	go func() {
		defer s.runCancels.LoadAndDelete(run.ID)
		defer cancel()
		s.executeRunViaAssets(execCtx, &run, pipelineDef, jobs, levels, cloneParams(run.Parameters), run.EffectivePrincipal, jobRunsByJobID)
	}()
}

func (s *Service) jobRunMap(ctx context.Context, runID string) (map[string]*domain.PipelineJobRun, error) {
	jobRuns, err := s.runs.ListJobRunsByRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*domain.PipelineJobRun, len(jobRuns))
	for i := range jobRuns {
		jobRun := jobRuns[i]
		out[jobRun.JobID] = &jobRun
	}
	return out, nil
}

func timePtr(v time.Time) *time.Time {
	return &v
}
