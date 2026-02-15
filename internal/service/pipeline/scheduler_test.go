package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"
)

// discardLogger returns a logger that discards all output.
func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestScheduler_Start(t *testing.T) {
	t.Parallel()

	cron5min := "*/5 * * * *"

	tests := []struct {
		name      string
		pipelines []domain.Pipeline
		repoErr   error
		wantErr   bool
		wantCount int // expected number of entries in the scheduler
	}{
		{
			name: "loads schedules from repo",
			pipelines: []domain.Pipeline{
				{ID: "p1", Name: "etl-daily", ScheduleCron: &cron5min, CreatedBy: "alice"},
			},
			wantErr:   false,
			wantCount: 1,
		},
		{
			name:      "empty pipelines succeeds",
			pipelines: []domain.Pipeline{},
			wantErr:   false,
			wantCount: 0,
		},
		{
			name:    "repo error propagates",
			repoErr: fmt.Errorf("connection refused"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := &testutil.MockPipelineRepo{
				ListScheduledPipelinesFn: func(_ context.Context) ([]domain.Pipeline, error) {
					if tt.repoErr != nil {
						return nil, tt.repoErr
					}
					return tt.pipelines, nil
				},
			}

			scheduler := NewScheduler(nil, repo, discardLogger())
			t.Cleanup(func() { scheduler.Stop() })

			err := scheduler.Start(context.Background())

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Len(t, scheduler.entries, tt.wantCount)
			}
		})
	}
}

func TestScheduler_Reload(t *testing.T) {
	t.Parallel()

	cron1min := "* * * * *"
	cron5min := "*/5 * * * *"

	callCount := 0
	repo := &testutil.MockPipelineRepo{
		ListScheduledPipelinesFn: func(_ context.Context) ([]domain.Pipeline, error) {
			callCount++
			if callCount == 1 {
				return []domain.Pipeline{
					{ID: "p1", Name: "etl-a", ScheduleCron: &cron1min, CreatedBy: "alice"},
				}, nil
			}
			// Second call returns different pipelines
			return []domain.Pipeline{
				{ID: "p2", Name: "etl-b", ScheduleCron: &cron5min, CreatedBy: "bob"},
				{ID: "p3", Name: "etl-c", ScheduleCron: &cron1min, CreatedBy: "carol"},
			}, nil
		},
	}

	scheduler := NewScheduler(nil, repo, discardLogger())
	t.Cleanup(func() { scheduler.Stop() })

	err := scheduler.Start(context.Background())
	require.NoError(t, err)
	assert.Len(t, scheduler.entries, 1)

	// Reload should clear old entries and load new ones
	err = scheduler.Reload(context.Background())
	require.NoError(t, err)
	assert.Len(t, scheduler.entries, 2)

	// Verify old entry is gone
	_, hasP1 := scheduler.entries["p1"]
	assert.False(t, hasP1, "old entry should be removed after reload")

	// Verify new entries exist
	_, hasP2 := scheduler.entries["p2"]
	assert.True(t, hasP2, "p2 should be present after reload")
	_, hasP3 := scheduler.entries["p3"]
	assert.True(t, hasP3, "p3 should be present after reload")
}

func TestScheduler_Stop(t *testing.T) {
	t.Parallel()

	repo := &testutil.MockPipelineRepo{
		ListScheduledPipelinesFn: func(_ context.Context) ([]domain.Pipeline, error) {
			return nil, nil
		},
	}

	scheduler := NewScheduler(nil, repo, discardLogger())
	err := scheduler.Start(context.Background())
	require.NoError(t, err)

	// Stop should not panic
	assert.NotPanics(t, func() {
		scheduler.Stop()
	})
}

func TestScheduler_InvalidCronExpression(t *testing.T) {
	t.Parallel()

	badCron := "not a cron"
	goodCron := "*/5 * * * *"

	repo := &testutil.MockPipelineRepo{
		ListScheduledPipelinesFn: func(_ context.Context) ([]domain.Pipeline, error) {
			return []domain.Pipeline{
				{ID: "bad", Name: "bad-cron", ScheduleCron: &badCron, CreatedBy: "alice"},
				{ID: "good", Name: "good-cron", ScheduleCron: &goodCron, CreatedBy: "bob"},
			}, nil
		},
	}

	scheduler := NewScheduler(nil, repo, discardLogger())
	t.Cleanup(func() { scheduler.Stop() })

	err := scheduler.Start(context.Background())
	require.NoError(t, err)

	// Only the valid cron should be registered
	assert.Len(t, scheduler.entries, 1)
	_, hasGood := scheduler.entries["good"]
	assert.True(t, hasGood, "valid cron pipeline should be registered")
	_, hasBad := scheduler.entries["bad"]
	assert.False(t, hasBad, "invalid cron pipeline should be skipped")
}

func TestScheduler_PipelineWithoutScheduleCron(t *testing.T) {
	t.Parallel()

	goodCron := "*/5 * * * *"

	repo := &testutil.MockPipelineRepo{
		ListScheduledPipelinesFn: func(_ context.Context) ([]domain.Pipeline, error) {
			return []domain.Pipeline{
				{ID: "no-cron", Name: "manual-only", ScheduleCron: nil, CreatedBy: "alice"},
				{ID: "with-cron", Name: "scheduled", ScheduleCron: &goodCron, CreatedBy: "bob"},
			}, nil
		},
	}

	scheduler := NewScheduler(nil, repo, discardLogger())
	t.Cleanup(func() { scheduler.Stop() })

	err := scheduler.Start(context.Background())
	require.NoError(t, err)

	// Only the pipeline with a cron schedule should be registered
	assert.Len(t, scheduler.entries, 1)
	_, hasCron := scheduler.entries["with-cron"]
	assert.True(t, hasCron, "pipeline with cron should be registered")
	_, hasNoCron := scheduler.entries["no-cron"]
	assert.False(t, hasNoCron, "pipeline without cron should be skipped")
}
