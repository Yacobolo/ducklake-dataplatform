package repository

import (
	"context"
	"testing"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupNotebookJobRepo(t *testing.T) (*NotebookJobRepo, *NotebookRepo) {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewNotebookJobRepo(writeDB), NewNotebookRepo(writeDB)
}

func TestNotebookJobRepo_CreateAndGet(t *testing.T) {
	jobRepo, nbRepo := setupNotebookJobRepo(t)
	ctx := context.Background()

	// Create a notebook first (foreign key)
	nb, err := nbRepo.CreateNotebook(ctx, &domain.Notebook{Name: "JobNB", Owner: "alice"})
	require.NoError(t, err)

	job := &domain.NotebookJob{
		NotebookID: nb.ID,
		SessionID:  "session-123",
		State:      domain.JobStatePending,
	}

	created, err := jobRepo.CreateJob(ctx, job)
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.NotEmpty(t, created.ID)
	assert.Equal(t, nb.ID, created.NotebookID)
	assert.Equal(t, "session-123", created.SessionID)
	assert.Equal(t, domain.JobStatePending, created.State)

	got, err := jobRepo.GetJob(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)
	assert.Equal(t, domain.JobStatePending, got.State)
}

func TestNotebookJobRepo_GetJob_NotFound(t *testing.T) {
	jobRepo, _ := setupNotebookJobRepo(t)
	ctx := context.Background()

	_, err := jobRepo.GetJob(ctx, "nonexistent")
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestNotebookJobRepo_ListJobs(t *testing.T) {
	jobRepo, nbRepo := setupNotebookJobRepo(t)
	ctx := context.Background()

	nb, err := nbRepo.CreateNotebook(ctx, &domain.Notebook{Name: "ListJobsNB", Owner: "alice"})
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err := jobRepo.CreateJob(ctx, &domain.NotebookJob{
			NotebookID: nb.ID,
			SessionID:  "session-abc",
			State:      domain.JobStatePending,
		})
		require.NoError(t, err)
	}

	jobs, total, err := jobRepo.ListJobs(ctx, nb.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)
	assert.Len(t, jobs, 3)
}

func TestNotebookJobRepo_UpdateJobState(t *testing.T) {
	jobRepo, nbRepo := setupNotebookJobRepo(t)
	ctx := context.Background()

	nb, err := nbRepo.CreateNotebook(ctx, &domain.Notebook{Name: "UpdateStateNB", Owner: "alice"})
	require.NoError(t, err)

	job, err := jobRepo.CreateJob(ctx, &domain.NotebookJob{
		NotebookID: nb.ID,
		SessionID:  "session-xyz",
		State:      domain.JobStatePending,
	})
	require.NoError(t, err)

	t.Run("transition to running", func(t *testing.T) {
		err := jobRepo.UpdateJobState(ctx, job.ID, domain.JobStateRunning, nil, nil)
		require.NoError(t, err)

		got, err := jobRepo.GetJob(ctx, job.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.JobStateRunning, got.State)
	})

	t.Run("transition to complete with result", func(t *testing.T) {
		result := `{"total_duration":"1s"}`
		err := jobRepo.UpdateJobState(ctx, job.ID, domain.JobStateComplete, &result, nil)
		require.NoError(t, err)

		got, err := jobRepo.GetJob(ctx, job.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.JobStateComplete, got.State)
		require.NotNil(t, got.Result)
		assert.Equal(t, result, *got.Result)
	})

	t.Run("transition to failed with error", func(t *testing.T) {
		job2, err := jobRepo.CreateJob(ctx, &domain.NotebookJob{
			NotebookID: nb.ID,
			SessionID:  "session-xyz",
			State:      domain.JobStatePending,
		})
		require.NoError(t, err)

		errMsg := "something went wrong"
		err = jobRepo.UpdateJobState(ctx, job2.ID, domain.JobStateFailed, nil, &errMsg)
		require.NoError(t, err)

		got, err := jobRepo.GetJob(ctx, job2.ID)
		require.NoError(t, err)
		assert.Equal(t, domain.JobStateFailed, got.State)
		require.NotNil(t, got.Error)
		assert.Equal(t, errMsg, *got.Error)
	})
}
