package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

// setupPipelineRunRepos creates both PipelineRepo and PipelineRunRepo backed by
// the same test SQLite database, plus a helper pipeline and two jobs for FK use.
func setupPipelineRunRepos(t *testing.T) (
	pipelineRepo *PipelineRepo,
	runRepo *PipelineRunRepo,
	pipeline *domain.Pipeline,
	jobs []*domain.PipelineJob,
) {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	pipelineRepo = NewPipelineRepo(writeDB)
	runRepo = NewPipelineRunRepo(writeDB)
	ctx := context.Background()

	pipeline, err := pipelineRepo.CreatePipeline(ctx, &domain.Pipeline{
		Name:      "run-test-pipeline",
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	job1, err := pipelineRepo.CreateJob(ctx, &domain.PipelineJob{
		PipelineID: pipeline.ID,
		Name:       "step-1",
		NotebookID: "nb-1",
		DependsOn:  []string{},
		JobOrder:   0,
	})
	require.NoError(t, err)

	job2, err := pipelineRepo.CreateJob(ctx, &domain.PipelineJob{
		PipelineID: pipeline.ID,
		Name:       "step-2",
		NotebookID: "nb-2",
		DependsOn:  []string{"step-1"},
		JobOrder:   1,
	})
	require.NoError(t, err)

	return pipelineRepo, runRepo, pipeline, []*domain.PipelineJob{job1, job2}
}

func TestPipelineRun_CreateAndGet(t *testing.T) {
	_, runRepo, pipeline, _ := setupPipelineRunRepos(t)
	ctx := context.Background()

	run, err := runRepo.CreateRun(ctx, &domain.PipelineRun{
		PipelineID:  pipeline.ID,
		Status:      domain.PipelineRunStatusPending,
		TriggerType: domain.TriggerTypeManual,
		TriggeredBy: "admin",
		Parameters:  map[string]string{"env": "prod", "batch": "42"},
	})
	require.NoError(t, err)
	require.NotNil(t, run)

	assert.NotEmpty(t, run.ID)
	assert.Equal(t, pipeline.ID, run.PipelineID)
	assert.Equal(t, domain.PipelineRunStatusPending, run.Status)
	assert.Equal(t, domain.TriggerTypeManual, run.TriggerType)
	assert.Equal(t, "admin", run.TriggeredBy)
	assert.False(t, run.CreatedAt.IsZero())
	assert.Nil(t, run.StartedAt)
	assert.Nil(t, run.FinishedAt)
	assert.Nil(t, run.ErrorMessage)

	// Verify Parameters map deserialized correctly
	require.NotNil(t, run.Parameters)
	assert.Equal(t, "prod", run.Parameters["env"])
	assert.Equal(t, "42", run.Parameters["batch"])

	t.Run("get_by_id", func(t *testing.T) {
		got, err := runRepo.GetRunByID(ctx, run.ID)
		require.NoError(t, err)
		assert.Equal(t, run.ID, got.ID)
		assert.Equal(t, "prod", got.Parameters["env"])
		assert.Equal(t, "42", got.Parameters["batch"])
	})

	t.Run("get_nonexistent", func(t *testing.T) {
		_, err := runRepo.GetRunByID(ctx, "nonexistent")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

func TestPipelineRun_ListWithFilter(t *testing.T) {
	_, runRepo, pipeline, _ := setupPipelineRunRepos(t)
	ctx := context.Background()

	// Create 2 PENDING + 1 SUCCESS
	for i := 0; i < 2; i++ {
		_, err := runRepo.CreateRun(ctx, &domain.PipelineRun{
			PipelineID:  pipeline.ID,
			Status:      domain.PipelineRunStatusPending,
			TriggerType: domain.TriggerTypeManual,
			TriggeredBy: "admin",
			Parameters:  map[string]string{},
		})
		require.NoError(t, err)
	}

	successRun, err := runRepo.CreateRun(ctx, &domain.PipelineRun{
		PipelineID:  pipeline.ID,
		Status:      domain.PipelineRunStatusPending,
		TriggerType: domain.TriggerTypeManual,
		TriggeredBy: "admin",
		Parameters:  map[string]string{},
	})
	require.NoError(t, err)
	// Mark it as finished with SUCCESS
	err = runRepo.UpdateRunFinished(ctx, successRun.ID, domain.PipelineRunStatusSuccess, nil)
	require.NoError(t, err)

	t.Run("filter_by_pending_status", func(t *testing.T) {
		status := domain.PipelineRunStatusPending
		runs, total, err := runRepo.ListRuns(ctx, domain.PipelineRunFilter{
			PipelineID: &pipeline.ID,
			Status:     &status,
		})
		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		assert.Len(t, runs, 2)
		for _, r := range runs {
			assert.Equal(t, domain.PipelineRunStatusPending, r.Status)
		}
	})

	t.Run("filter_by_success_status", func(t *testing.T) {
		status := domain.PipelineRunStatusSuccess
		runs, total, err := runRepo.ListRuns(ctx, domain.PipelineRunFilter{
			PipelineID: &pipeline.ID,
			Status:     &status,
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Len(t, runs, 1)
		assert.Equal(t, domain.PipelineRunStatusSuccess, runs[0].Status)
	})

	t.Run("all_statuses_sum_to_total", func(t *testing.T) {
		// Verify that filtering by each status covers all runs
		pendingStatus := domain.PipelineRunStatusPending
		pendingRuns, pendingTotal, err := runRepo.ListRuns(ctx, domain.PipelineRunFilter{
			PipelineID: &pipeline.ID,
			Status:     &pendingStatus,
		})
		require.NoError(t, err)

		successStatus := domain.PipelineRunStatusSuccess
		successRuns, successTotal, err := runRepo.ListRuns(ctx, domain.PipelineRunFilter{
			PipelineID: &pipeline.ID,
			Status:     &successStatus,
		})
		require.NoError(t, err)

		assert.Equal(t, int64(3), pendingTotal+successTotal)
		assert.Len(t, append(pendingRuns, successRuns...), 3)
	})
}

func TestPipelineRun_ListWithoutFilters(t *testing.T) {
	_, runRepo, pipeline, _ := setupPipelineRunRepos(t)
	ctx := context.Background()

	// Create runs with different statuses for the same pipeline.
	statuses := []string{
		domain.PipelineRunStatusPending,
		domain.PipelineRunStatusPending,
		domain.PipelineRunStatusPending, // will be marked SUCCESS below
	}
	var runIDs []string
	for _, s := range statuses {
		r, err := runRepo.CreateRun(ctx, &domain.PipelineRun{
			PipelineID:  pipeline.ID,
			Status:      s,
			TriggerType: domain.TriggerTypeManual,
			TriggeredBy: "admin",
			Parameters:  map[string]string{},
		})
		require.NoError(t, err)
		runIDs = append(runIDs, r.ID)
	}
	// Mark last run as SUCCESS so we have mixed statuses.
	err := runRepo.UpdateRunFinished(ctx, runIDs[2], domain.PipelineRunStatusSuccess, nil)
	require.NoError(t, err)

	t.Run("pipeline_id_only_no_status_filter", func(t *testing.T) {
		runs, total, err := runRepo.ListRuns(ctx, domain.PipelineRunFilter{
			PipelineID: &pipeline.ID,
			Status:     nil,
		})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, runs, 3)
	})

	t.Run("no_filters_at_all", func(t *testing.T) {
		runs, total, err := runRepo.ListRuns(ctx, domain.PipelineRunFilter{
			PipelineID: nil,
			Status:     nil,
		})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, runs, 3)
	})
}

func TestPipelineRun_Lifecycle(t *testing.T) {
	_, runRepo, pipeline, _ := setupPipelineRunRepos(t)
	ctx := context.Background()

	run, err := runRepo.CreateRun(ctx, &domain.PipelineRun{
		PipelineID:  pipeline.ID,
		Status:      domain.PipelineRunStatusPending,
		TriggerType: domain.TriggerTypeScheduled,
		TriggeredBy: "scheduler",
		Parameters:  map[string]string{},
	})
	require.NoError(t, err)
	assert.Nil(t, run.StartedAt)
	assert.Nil(t, run.FinishedAt)

	// Mark as started
	err = runRepo.UpdateRunStarted(ctx, run.ID)
	require.NoError(t, err)

	started, err := runRepo.GetRunByID(ctx, run.ID)
	require.NoError(t, err)
	require.NotNil(t, started.StartedAt)
	assert.False(t, started.StartedAt.IsZero())
	assert.Nil(t, started.FinishedAt)

	// Mark as finished
	err = runRepo.UpdateRunFinished(ctx, run.ID, domain.PipelineRunStatusSuccess, nil)
	require.NoError(t, err)

	finished, err := runRepo.GetRunByID(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.PipelineRunStatusSuccess, finished.Status)
	require.NotNil(t, finished.StartedAt)
	require.NotNil(t, finished.FinishedAt)
	assert.False(t, finished.FinishedAt.IsZero())
	assert.Nil(t, finished.ErrorMessage)
}

func TestPipelineRun_CountActive(t *testing.T) {
	_, runRepo, pipeline, _ := setupPipelineRunRepos(t)
	ctx := context.Background()

	// Create 2 PENDING
	for i := 0; i < 2; i++ {
		_, err := runRepo.CreateRun(ctx, &domain.PipelineRun{
			PipelineID:  pipeline.ID,
			Status:      domain.PipelineRunStatusPending,
			TriggerType: domain.TriggerTypeManual,
			TriggeredBy: "admin",
			Parameters:  map[string]string{},
		})
		require.NoError(t, err)
	}

	// Create 1 and finish it as SUCCESS
	done, err := runRepo.CreateRun(ctx, &domain.PipelineRun{
		PipelineID:  pipeline.ID,
		Status:      domain.PipelineRunStatusPending,
		TriggerType: domain.TriggerTypeManual,
		TriggeredBy: "admin",
		Parameters:  map[string]string{},
	})
	require.NoError(t, err)
	err = runRepo.UpdateRunFinished(ctx, done.ID, domain.PipelineRunStatusSuccess, nil)
	require.NoError(t, err)

	count, err := runRepo.CountActiveRuns(ctx, pipeline.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

func TestPipelineJobRun_CRUD(t *testing.T) {
	_, runRepo, pipeline, jobs := setupPipelineRunRepos(t)
	ctx := context.Background()

	run, err := runRepo.CreateRun(ctx, &domain.PipelineRun{
		PipelineID:  pipeline.ID,
		Status:      domain.PipelineRunStatusPending,
		TriggerType: domain.TriggerTypeManual,
		TriggeredBy: "admin",
		Parameters:  map[string]string{},
	})
	require.NoError(t, err)

	jr, err := runRepo.CreateJobRun(ctx, &domain.PipelineJobRun{
		RunID:        run.ID,
		JobID:        jobs[0].ID,
		JobName:      "step-1",
		Status:       domain.PipelineJobRunStatusPending,
		RetryAttempt: 0,
	})
	require.NoError(t, err)
	require.NotNil(t, jr)
	assert.NotEmpty(t, jr.ID)
	assert.Equal(t, run.ID, jr.RunID)
	assert.Equal(t, jobs[0].ID, jr.JobID)
	assert.Equal(t, "step-1", jr.JobName)
	assert.Equal(t, domain.PipelineJobRunStatusPending, jr.Status)
	assert.Equal(t, 0, jr.RetryAttempt)
	assert.Nil(t, jr.StartedAt)
	assert.Nil(t, jr.FinishedAt)
	assert.False(t, jr.CreatedAt.IsZero())

	t.Run("get_job_run_by_id", func(t *testing.T) {
		got, err := runRepo.GetJobRunByID(ctx, jr.ID)
		require.NoError(t, err)
		assert.Equal(t, jr.ID, got.ID)
		assert.Equal(t, "step-1", got.JobName)
	})

	t.Run("list_job_runs_by_run", func(t *testing.T) {
		// Add a second job run
		_, err := runRepo.CreateJobRun(ctx, &domain.PipelineJobRun{
			RunID:        run.ID,
			JobID:        jobs[1].ID,
			JobName:      "step-2",
			Status:       domain.PipelineJobRunStatusPending,
			RetryAttempt: 0,
		})
		require.NoError(t, err)

		jobRuns, err := runRepo.ListJobRunsByRun(ctx, run.ID)
		require.NoError(t, err)
		assert.Len(t, jobRuns, 2)
	})

	t.Run("job_run_lifecycle", func(t *testing.T) {
		err := runRepo.UpdateJobRunStarted(ctx, jr.ID)
		require.NoError(t, err)

		started, err := runRepo.GetJobRunByID(ctx, jr.ID)
		require.NoError(t, err)
		require.NotNil(t, started.StartedAt)

		err = runRepo.UpdateJobRunFinished(ctx, jr.ID, domain.PipelineJobRunStatusSuccess, nil)
		require.NoError(t, err)

		finished, err := runRepo.GetJobRunByID(ctx, jr.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.PipelineJobRunStatusSuccess, finished.Status)
		require.NotNil(t, finished.FinishedAt)
	})
}

func TestPipelineRunRepo_UpdateRunStatus(t *testing.T) {
	_, runRepo, pipeline, _ := setupPipelineRunRepos(t)
	ctx := context.Background()

	run, err := runRepo.CreateRun(ctx, &domain.PipelineRun{
		PipelineID:  pipeline.ID,
		Status:      domain.PipelineRunStatusPending,
		TriggerType: domain.TriggerTypeManual,
		TriggeredBy: "admin",
		Parameters:  map[string]string{},
	})
	require.NoError(t, err)
	assert.Equal(t, domain.PipelineRunStatusPending, run.Status)

	errMsg := "something went wrong"
	err = runRepo.UpdateRunStatus(ctx, run.ID, domain.PipelineRunStatusFailed, &errMsg)
	require.NoError(t, err)

	got, err := runRepo.GetRunByID(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.PipelineRunStatusFailed, got.Status)
	require.NotNil(t, got.ErrorMessage)
	assert.Equal(t, "something went wrong", *got.ErrorMessage)
}

func TestPipelineRunRepo_CancelPendingRuns(t *testing.T) {
	_, runRepo, pipeline, _ := setupPipelineRunRepos(t)
	ctx := context.Background()

	// Create a PENDING run
	pendingRun, err := runRepo.CreateRun(ctx, &domain.PipelineRun{
		PipelineID:  pipeline.ID,
		Status:      domain.PipelineRunStatusPending,
		TriggerType: domain.TriggerTypeManual,
		TriggeredBy: "admin",
		Parameters:  map[string]string{},
	})
	require.NoError(t, err)

	// Create a RUNNING run (start as PENDING then update status)
	runningRun, err := runRepo.CreateRun(ctx, &domain.PipelineRun{
		PipelineID:  pipeline.ID,
		Status:      domain.PipelineRunStatusPending,
		TriggerType: domain.TriggerTypeManual,
		TriggeredBy: "admin",
		Parameters:  map[string]string{},
	})
	require.NoError(t, err)
	err = runRepo.UpdateRunStatus(ctx, runningRun.ID, domain.PipelineRunStatusRunning, nil)
	require.NoError(t, err)

	// Create a SUCCESS run
	successRun, err := runRepo.CreateRun(ctx, &domain.PipelineRun{
		PipelineID:  pipeline.ID,
		Status:      domain.PipelineRunStatusPending,
		TriggerType: domain.TriggerTypeManual,
		TriggeredBy: "admin",
		Parameters:  map[string]string{},
	})
	require.NoError(t, err)
	err = runRepo.UpdateRunFinished(ctx, successRun.ID, domain.PipelineRunStatusSuccess, nil)
	require.NoError(t, err)

	// Cancel all pending runs
	_, err = runRepo.CancelPendingRuns(ctx, pipeline.ID)
	require.NoError(t, err)

	// Verify PENDING run is now cancelled
	gotPending, err := runRepo.GetRunByID(ctx, pendingRun.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.PipelineRunStatusCancelled, gotPending.Status)

	// Verify RUNNING run is unchanged
	gotRunning, err := runRepo.GetRunByID(ctx, runningRun.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.PipelineRunStatusRunning, gotRunning.Status)

	// Verify SUCCESS run is unchanged
	gotSuccess, err := runRepo.GetRunByID(ctx, successRun.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.PipelineRunStatusSuccess, gotSuccess.Status)
}

func TestPipelineRunRepo_UpdateJobRunStatus(t *testing.T) {
	_, runRepo, pipeline, jobs := setupPipelineRunRepos(t)
	ctx := context.Background()

	run, err := runRepo.CreateRun(ctx, &domain.PipelineRun{
		PipelineID:  pipeline.ID,
		Status:      domain.PipelineRunStatusPending,
		TriggerType: domain.TriggerTypeManual,
		TriggeredBy: "admin",
		Parameters:  map[string]string{},
	})
	require.NoError(t, err)

	jr, err := runRepo.CreateJobRun(ctx, &domain.PipelineJobRun{
		RunID:        run.ID,
		JobID:        jobs[0].ID,
		JobName:      "step-1",
		Status:       domain.PipelineJobRunStatusPending,
		RetryAttempt: 0,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.PipelineJobRunStatusPending, jr.Status)

	err = runRepo.UpdateJobRunStatus(ctx, jr.ID, domain.PipelineJobRunStatusRunning, nil)
	require.NoError(t, err)

	got, err := runRepo.GetJobRunByID(ctx, jr.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.PipelineJobRunStatusRunning, got.Status)
	assert.Nil(t, got.ErrorMessage)
}

func TestPipelineRunRepo_UpdateRunFinished_withError(t *testing.T) {
	_, runRepo, pipeline, _ := setupPipelineRunRepos(t)
	ctx := context.Background()

	run, err := runRepo.CreateRun(ctx, &domain.PipelineRun{
		PipelineID:  pipeline.ID,
		Status:      domain.PipelineRunStatusPending,
		TriggerType: domain.TriggerTypeManual,
		TriggeredBy: "admin",
		Parameters:  map[string]string{},
	})
	require.NoError(t, err)

	// Start the run first
	err = runRepo.UpdateRunStarted(ctx, run.ID)
	require.NoError(t, err)

	// Finish with FAILED status and an error message
	errMsg := "job step-1 timed out after 300s"
	err = runRepo.UpdateRunFinished(ctx, run.ID, domain.PipelineRunStatusFailed, &errMsg)
	require.NoError(t, err)

	got, err := runRepo.GetRunByID(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.PipelineRunStatusFailed, got.Status)
	require.NotNil(t, got.FinishedAt)
	assert.False(t, got.FinishedAt.IsZero())
	require.NotNil(t, got.ErrorMessage)
	assert.Equal(t, "job step-1 timed out after 300s", *got.ErrorMessage)
}
