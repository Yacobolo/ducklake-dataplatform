package pipeline

import (
	"context"
	"log/slog"
	"sync"

	"github.com/robfig/cron/v3"

	"duck-demo/internal/domain"
)

// Scheduler manages cron-based pipeline execution.
type Scheduler struct {
	cron      *cron.Cron
	svc       *Service
	pipelines domain.PipelineRepository
	logger    *slog.Logger
	mu        sync.Mutex
	entries   map[string]cron.EntryID // pipeline ID → cron entry
}

// NewScheduler creates a new pipeline scheduler.
func NewScheduler(svc *Service, pipelines domain.PipelineRepository, logger *slog.Logger) *Scheduler {
	return &Scheduler{
		cron:      cron.New(),
		svc:       svc,
		pipelines: pipelines,
		logger:    logger,
		entries:   make(map[string]cron.EntryID),
	}
}

// Start loads all scheduled pipelines and starts the cron scheduler.
func (s *Scheduler) Start(ctx context.Context) error {
	if err := s.loadSchedules(ctx); err != nil {
		return err
	}
	s.cron.Start()
	s.logger.Info("pipeline scheduler started")
	return nil
}

// Stop gracefully stops the cron scheduler.
func (s *Scheduler) Stop() {
	s.cron.Stop()
	s.logger.Info("pipeline scheduler stopped")
}

// Reload clears all cron entries and reloads from the database.
// Implements the ScheduleReloader interface.
func (s *Scheduler) Reload(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove all existing entries.
	for _, entryID := range s.entries {
		s.cron.Remove(entryID)
	}
	s.entries = make(map[string]cron.EntryID)

	return s.loadSchedules(ctx)
}

// loadSchedules queries for active scheduled pipelines and adds them to cron.
func (s *Scheduler) loadSchedules(ctx context.Context) error {
	pipelines, err := s.pipelines.ListScheduledPipelines(ctx)
	if err != nil {
		return err
	}

	for _, p := range pipelines {
		if p.ScheduleCron == nil {
			continue
		}
		schedule := *p.ScheduleCron
		pipelineName := p.Name
		createdBy := p.CreatedBy

		entryID, err := s.cron.AddFunc(schedule, func() {
			ctx := context.Background()
			_, triggerErr := s.svc.TriggerRun(ctx, createdBy, pipelineName, nil, domain.TriggerTypeScheduled)
			if triggerErr != nil {
				s.logger.Warn("scheduled trigger failed",
					"pipeline", pipelineName,
					"error", triggerErr,
				)
			}
		})
		if err != nil {
			s.logger.Warn("invalid cron schedule",
				"pipeline", pipelineName,
				"schedule", schedule,
				"error", err,
			)
			continue
		}

		s.entries[p.ID] = entryID
		s.logger.Info("scheduled pipeline", "pipeline", pipelineName, "schedule", schedule)
	}

	return nil
}

// Compile-time check that Scheduler implements ScheduleReloader.
var _ ScheduleReloader = (*Scheduler)(nil)
