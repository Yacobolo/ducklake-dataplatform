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

func setupPipelineRepo(t *testing.T) *PipelineRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewPipelineRepo(writeDB)
}

func TestPipeline_CreateAndGet(t *testing.T) {
	repo := setupPipelineRepo(t)
	ctx := context.Background()

	cron := "0 * * * *"
	p := &domain.Pipeline{
		Name:             "etl-daily",
		Description:      "Daily ETL pipeline",
		ScheduleCron:     &cron,
		IsPaused:         false,
		ConcurrencyLimit: 2,
		CreatedBy:        "admin",
	}

	created, err := repo.CreatePipeline(ctx, p)
	require.NoError(t, err)
	require.NotNil(t, created)

	assert.NotEmpty(t, created.ID)
	assert.Equal(t, "etl-daily", created.Name)
	assert.Equal(t, "Daily ETL pipeline", created.Description)
	require.NotNil(t, created.ScheduleCron)
	assert.Equal(t, "0 * * * *", *created.ScheduleCron)
	assert.False(t, created.IsPaused)
	assert.Equal(t, 2, created.ConcurrencyLimit)
	assert.Equal(t, "admin", created.CreatedBy)
	assert.False(t, created.CreatedAt.IsZero())
	assert.False(t, created.UpdatedAt.IsZero())

	t.Run("get_by_id", func(t *testing.T) {
		got, err := repo.GetPipelineByID(ctx, created.ID)
		require.NoError(t, err)
		assert.Equal(t, created.ID, got.ID)
		assert.Equal(t, "etl-daily", got.Name)
		assert.Equal(t, "Daily ETL pipeline", got.Description)
		require.NotNil(t, got.ScheduleCron)
		assert.Equal(t, "0 * * * *", *got.ScheduleCron)
	})

	t.Run("get_by_name", func(t *testing.T) {
		got, err := repo.GetPipelineByName(ctx, "etl-daily")
		require.NoError(t, err)
		assert.Equal(t, created.ID, got.ID)
		assert.Equal(t, "etl-daily", got.Name)
	})

	t.Run("get_nonexistent_id", func(t *testing.T) {
		_, err := repo.GetPipelineByID(ctx, "nonexistent")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("get_nonexistent_name", func(t *testing.T) {
		_, err := repo.GetPipelineByName(ctx, "nonexistent")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

func TestPipeline_DuplicateName(t *testing.T) {
	repo := setupPipelineRepo(t)
	ctx := context.Background()

	p := &domain.Pipeline{
		Name:      "unique-pipeline",
		CreatedBy: "admin",
	}
	_, err := repo.CreatePipeline(ctx, p)
	require.NoError(t, err)

	_, err = repo.CreatePipeline(ctx, &domain.Pipeline{
		Name:      "unique-pipeline",
		CreatedBy: "other",
	})
	require.Error(t, err)
	var conflict *domain.ConflictError
	assert.ErrorAs(t, err, &conflict)
}

func TestPipeline_ListPaginated(t *testing.T) {
	repo := setupPipelineRepo(t)
	ctx := context.Background()

	for _, name := range []string{"alpha", "beta", "gamma"} {
		_, err := repo.CreatePipeline(ctx, &domain.Pipeline{
			Name:      name,
			CreatedBy: "admin",
		})
		require.NoError(t, err)
	}

	t.Run("page_size_2", func(t *testing.T) {
		page := domain.PageRequest{MaxResults: 2}
		pipelines, total, err := repo.ListPipelines(ctx, page)
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, pipelines, 2)
	})

	t.Run("list_all", func(t *testing.T) {
		pipelines, total, err := repo.ListPipelines(ctx, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(3), total)
		assert.Len(t, pipelines, 3)
	})
}

func TestPipeline_Update(t *testing.T) {
	repo := setupPipelineRepo(t)
	ctx := context.Background()

	p, err := repo.CreatePipeline(ctx, &domain.Pipeline{
		Name:        "to-update",
		Description: "original",
		CreatedBy:   "admin",
	})
	require.NoError(t, err)

	t.Run("update_description_and_pause", func(t *testing.T) {
		newDesc := "updated description"
		paused := true
		updated, err := repo.UpdatePipeline(ctx, p.ID, domain.UpdatePipelineRequest{
			Description: &newDesc,
			IsPaused:    &paused,
		})
		require.NoError(t, err)
		assert.Equal(t, "updated description", updated.Description)
		assert.True(t, updated.IsPaused)
		assert.Equal(t, "to-update", updated.Name) // unchanged
	})

	t.Run("update_nonexistent", func(t *testing.T) {
		desc := "x"
		_, err := repo.UpdatePipeline(ctx, "nonexistent", domain.UpdatePipelineRequest{Description: &desc})
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

func TestPipeline_Delete(t *testing.T) {
	repo := setupPipelineRepo(t)
	ctx := context.Background()

	p, err := repo.CreatePipeline(ctx, &domain.Pipeline{
		Name:      "to-delete",
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	err = repo.DeletePipeline(ctx, p.ID)
	require.NoError(t, err)

	_, err = repo.GetPipelineByID(ctx, p.ID)
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestPipelineJob_CRUD(t *testing.T) {
	repo := setupPipelineRepo(t)
	ctx := context.Background()

	// Create a pipeline first (FK constraint)
	p, err := repo.CreatePipeline(ctx, &domain.Pipeline{
		Name:      "job-test-pipeline",
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	timeout := int64(300)
	job1, err := repo.CreateJob(ctx, &domain.PipelineJob{
		PipelineID:     p.ID,
		Name:           "extract",
		DependsOn:      []string{},
		NotebookID:     "nb-1",
		TimeoutSeconds: &timeout,
		RetryCount:     2,
		JobOrder:       0,
	})
	require.NoError(t, err)
	require.NotNil(t, job1)
	assert.NotEmpty(t, job1.ID)
	assert.Equal(t, "extract", job1.Name)
	assert.Equal(t, p.ID, job1.PipelineID)
	assert.Empty(t, job1.DependsOn)
	require.NotNil(t, job1.TimeoutSeconds)
	assert.Equal(t, int64(300), *job1.TimeoutSeconds)
	assert.Equal(t, 2, job1.RetryCount)
	assert.Equal(t, 0, job1.JobOrder)

	job2, err := repo.CreateJob(ctx, &domain.PipelineJob{
		PipelineID: p.ID,
		Name:       "transform",
		DependsOn:  []string{"extract"},
		NotebookID: "nb-2",
		RetryCount: 0,
		JobOrder:   1,
	})
	require.NoError(t, err)
	require.NotNil(t, job2)
	assert.Equal(t, []string{"extract"}, job2.DependsOn)

	t.Run("get_job_by_id", func(t *testing.T) {
		got, err := repo.GetJobByID(ctx, job2.ID)
		require.NoError(t, err)
		assert.Equal(t, "transform", got.Name)
		assert.Equal(t, []string{"extract"}, got.DependsOn)
	})

	t.Run("list_jobs_ordered", func(t *testing.T) {
		jobs, err := repo.ListJobsByPipeline(ctx, p.ID)
		require.NoError(t, err)
		require.Len(t, jobs, 2)
		assert.Equal(t, "extract", jobs[0].Name)
		assert.Equal(t, "transform", jobs[1].Name)
		assert.Equal(t, 0, jobs[0].JobOrder)
		assert.Equal(t, 1, jobs[1].JobOrder)
	})

	t.Run("delete_job", func(t *testing.T) {
		err := repo.DeleteJob(ctx, job1.ID)
		require.NoError(t, err)

		_, err = repo.GetJobByID(ctx, job1.ID)
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("delete_jobs_by_pipeline", func(t *testing.T) {
		err := repo.DeleteJobsByPipeline(ctx, p.ID)
		require.NoError(t, err)

		jobs, err := repo.ListJobsByPipeline(ctx, p.ID)
		require.NoError(t, err)
		assert.Empty(t, jobs)
	})
}

func TestPipelineJob_UniqueNamePerPipeline(t *testing.T) {
	repo := setupPipelineRepo(t)
	ctx := context.Background()

	p, err := repo.CreatePipeline(ctx, &domain.Pipeline{
		Name:      "dup-job-pipeline",
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	_, err = repo.CreateJob(ctx, &domain.PipelineJob{
		PipelineID: p.ID,
		Name:       "same-name",
		NotebookID: "nb-1",
		DependsOn:  []string{},
		JobOrder:   0,
	})
	require.NoError(t, err)

	_, err = repo.CreateJob(ctx, &domain.PipelineJob{
		PipelineID: p.ID,
		Name:       "same-name",
		NotebookID: "nb-2",
		DependsOn:  []string{},
		JobOrder:   1,
	})
	require.Error(t, err)
	var conflict *domain.ConflictError
	assert.ErrorAs(t, err, &conflict)
}

func TestPipelineRepo_ListScheduledPipelines(t *testing.T) {
	repo := setupPipelineRepo(t)
	ctx := context.Background()

	cron := "*/5 * * * *"

	// Pipeline with cron + active (not paused)
	_, err := repo.CreatePipeline(ctx, &domain.Pipeline{
		Name:         "scheduled-active",
		ScheduleCron: &cron,
		IsPaused:     false,
		CreatedBy:    "admin",
	})
	require.NoError(t, err)

	// Pipeline with cron + paused
	pausedPipeline, err := repo.CreatePipeline(ctx, &domain.Pipeline{
		Name:         "scheduled-paused",
		ScheduleCron: &cron,
		IsPaused:     false,
		CreatedBy:    "admin",
	})
	require.NoError(t, err)
	// Pause it via update
	paused := true
	_, err = repo.UpdatePipeline(ctx, pausedPipeline.ID, domain.UpdatePipelineRequest{
		IsPaused: &paused,
	})
	require.NoError(t, err)

	// Pipeline without cron (nil schedule → stored as NULL)
	_, err = repo.CreatePipeline(ctx, &domain.Pipeline{
		Name:      "no-schedule",
		IsPaused:  false,
		CreatedBy: "admin",
	})
	require.NoError(t, err)

	scheduled, err := repo.ListScheduledPipelines(ctx)
	require.NoError(t, err)
	require.Len(t, scheduled, 1)
	assert.Equal(t, "scheduled-active", scheduled[0].Name)
	require.NotNil(t, scheduled[0].ScheduleCron)
	assert.Equal(t, "*/5 * * * *", *scheduled[0].ScheduleCron)
	assert.False(t, scheduled[0].IsPaused)
}

func TestPipelineRepo_UpdatePipeline_schedule(t *testing.T) {
	repo := setupPipelineRepo(t)
	ctx := context.Background()

	p, err := repo.CreatePipeline(ctx, &domain.Pipeline{
		Name:      "schedule-update-test",
		CreatedBy: "admin",
	})
	require.NoError(t, err)
	assert.Nil(t, p.ScheduleCron)

	t.Run("set_cron_schedule", func(t *testing.T) {
		cron := "0 3 * * *"
		updated, err := repo.UpdatePipeline(ctx, p.ID, domain.UpdatePipelineRequest{
			ScheduleCron: &cron,
		})
		require.NoError(t, err)
		require.NotNil(t, updated.ScheduleCron)
		assert.Equal(t, "0 3 * * *", *updated.ScheduleCron)
	})

	t.Run("change_cron_schedule", func(t *testing.T) {
		newCron := "0 6 * * MON"
		updated, err := repo.UpdatePipeline(ctx, p.ID, domain.UpdatePipelineRequest{
			ScheduleCron: &newCron,
		})
		require.NoError(t, err)
		require.NotNil(t, updated.ScheduleCron)
		assert.Equal(t, "0 6 * * MON", *updated.ScheduleCron)
	})

	t.Run("verify_scheduled_pipeline_listed", func(t *testing.T) {
		scheduled, err := repo.ListScheduledPipelines(ctx)
		require.NoError(t, err)
		var found bool
		for _, s := range scheduled {
			if s.ID == p.ID {
				found = true
				assert.Equal(t, "0 6 * * MON", *s.ScheduleCron)
			}
		}
		assert.True(t, found, "pipeline with schedule should appear in scheduled list")
	})
}
