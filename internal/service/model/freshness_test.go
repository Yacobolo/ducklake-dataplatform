package model

import (
	"context"
	"fmt"
	"testing"
	"time"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type freshnessModelRepoStub struct {
	model domain.Model
}

func (s freshnessModelRepoStub) Create(context.Context, *domain.Model) (*domain.Model, error) {
	panic("unexpected call")
}

func (s freshnessModelRepoStub) GetByID(context.Context, string) (*domain.Model, error) {
	panic("unexpected call")
}

func (s freshnessModelRepoStub) GetByName(_ context.Context, _, _ string) (*domain.Model, error) {
	m := s.model
	return &m, nil
}

func (s freshnessModelRepoStub) List(context.Context, *string, domain.PageRequest) ([]domain.Model, int64, error) {
	panic("unexpected call")
}

func (s freshnessModelRepoStub) Update(context.Context, string, domain.UpdateModelRequest) (*domain.Model, error) {
	panic("unexpected call")
}

func (s freshnessModelRepoStub) Delete(context.Context, string) error {
	panic("unexpected call")
}

func (s freshnessModelRepoStub) ListAll(context.Context) ([]domain.Model, error) {
	panic("unexpected call")
}

func (s freshnessModelRepoStub) UpdateDependencies(context.Context, string, []string) error {
	panic("unexpected call")
}

type freshnessRunRepoStub struct {
	runs  []domain.ModelRun
	steps map[string][]domain.ModelRunStep
}

func (s freshnessRunRepoStub) CreateRun(context.Context, *domain.ModelRun) (*domain.ModelRun, error) {
	panic("unexpected call")
}

func (s freshnessRunRepoStub) GetRunByID(context.Context, string) (*domain.ModelRun, error) {
	panic("unexpected call")
}

func (s freshnessRunRepoStub) ListRuns(_ context.Context, _ domain.ModelRunFilter) ([]domain.ModelRun, int64, error) {
	return s.runs, int64(len(s.runs)), nil
}

func (s freshnessRunRepoStub) UpdateRunStarted(context.Context, string) error {
	panic("unexpected call")
}

func (s freshnessRunRepoStub) UpdateRunFinished(context.Context, string, string, *string) error {
	panic("unexpected call")
}

func (s freshnessRunRepoStub) CreateStep(context.Context, *domain.ModelRunStep) (*domain.ModelRunStep, error) {
	panic("unexpected call")
}

func (s freshnessRunRepoStub) ListStepsByRun(_ context.Context, runID string) ([]domain.ModelRunStep, error) {
	return s.steps[runID], nil
}

func (s freshnessRunRepoStub) UpdateStepStarted(context.Context, string) error {
	panic("unexpected call")
}

func (s freshnessRunRepoStub) UpdateStepFinished(context.Context, string, string, *int64, *string) error {
	panic("unexpected call")
}

func TestCheckFreshness_UsesModelSpecificLineage(t *testing.T) {
	now := time.Now().UTC()
	older := now.Add(-10 * time.Minute)
	newer := now.Add(-1 * time.Minute)

	svc := &Service{
		models: freshnessModelRepoStub{model: domain.Model{
			ProjectName: "analytics",
			Name:        "fct_orders",
			Freshness:   &domain.FreshnessPolicy{MaxLagSeconds: 3600},
		}},
		runs: freshnessRunRepoStub{
			runs: []domain.ModelRun{
				{ID: "run-new", FinishedAt: &newer},
				{ID: "run-old", FinishedAt: &older},
			},
			steps: map[string][]domain.ModelRunStep{
				"run-new": {{ModelName: "analytics.other_model", Status: domain.ModelRunStatusSuccess}},
				"run-old": {{ModelName: "analytics.fct_orders", Status: domain.ModelRunStatusSuccess}},
			},
		},
	}

	status, err := svc.CheckFreshness(context.Background(), "analytics", "fct_orders")
	require.NoError(t, err)
	assert.True(t, status.IsFresh)
	require.NotNil(t, status.LastRunAt)
	assert.Equal(t, older.Unix(), status.LastRunAt.Unix())
}

func TestCheckFreshness_NoSuccessfulStepForModelIsStale(t *testing.T) {
	now := time.Now().UTC().Add(-5 * time.Minute)

	svc := &Service{
		models: freshnessModelRepoStub{model: domain.Model{
			ProjectName: "analytics",
			Name:        "fct_orders",
			Freshness:   &domain.FreshnessPolicy{MaxLagSeconds: 3600},
		}},
		runs: freshnessRunRepoStub{
			runs: []domain.ModelRun{{ID: "run-1", FinishedAt: &now}},
			steps: map[string][]domain.ModelRunStep{
				"run-1": {{ModelName: "analytics.other_model", Status: domain.ModelRunStatusSuccess}},
			},
		},
	}

	status, err := svc.CheckFreshness(context.Background(), "analytics", "fct_orders")
	require.NoError(t, err)
	assert.False(t, status.IsFresh)
	assert.Nil(t, status.LastRunAt)
}

func TestCheckFreshness_StaleWhenPastLag(t *testing.T) {
	old := time.Now().UTC().Add(-2 * time.Hour)

	svc := &Service{
		models: freshnessModelRepoStub{model: domain.Model{
			ProjectName: "analytics",
			Name:        "fct_orders",
			Freshness:   &domain.FreshnessPolicy{MaxLagSeconds: 60},
		}},
		runs: freshnessRunRepoStub{
			runs: []domain.ModelRun{{ID: "run-1", FinishedAt: &old}},
			steps: map[string][]domain.ModelRunStep{
				"run-1": {{ModelName: "analytics.fct_orders", Status: domain.ModelRunStatusSuccess}},
			},
		},
	}

	status, err := svc.CheckFreshness(context.Background(), "analytics", "fct_orders")
	require.NoError(t, err)
	assert.False(t, status.IsFresh)
	require.NotNil(t, status.StaleSince)
	require.NotNil(t, status.LastRunAt)
	assert.True(t, status.StaleSince.After(*status.LastRunAt))
}

func TestCheckSourceFreshness(t *testing.T) {
	t.Run("auto-detect timestamp column and mark fresh", func(t *testing.T) {
		svc, db := newDuckDBServiceForTest(t)
		_, err := db.Exec(`CREATE TABLE analytics.raw_orders (id INTEGER, updated_at TIMESTAMP)`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO analytics.raw_orders VALUES (1, CURRENT_TIMESTAMP)`)
		require.NoError(t, err)

		status, err := svc.CheckSourceFreshness(context.Background(), "admin", "analytics", "raw_orders", "", 3600)
		require.NoError(t, err)
		assert.True(t, status.IsFresh)
		assert.Equal(t, "updated_at", status.TimestampCol)
		require.NotNil(t, status.LastLoadedAt)
	})

	t.Run("stale when max timestamp is old", func(t *testing.T) {
		svc, db := newDuckDBServiceForTest(t)
		_, err := db.Exec(`CREATE TABLE analytics.raw_orders (id INTEGER, updated_at TIMESTAMP)`)
		require.NoError(t, err)
		old := time.Now().UTC().Add(-2 * time.Hour).Format("2006-01-02 15:04:05")
		_, err = db.Exec(fmt.Sprintf(`INSERT INTO analytics.raw_orders VALUES (1, TIMESTAMP '%s')`, old))
		require.NoError(t, err)

		status, err := svc.CheckSourceFreshness(context.Background(), "admin", "analytics", "raw_orders", "updated_at", 60)
		require.NoError(t, err)
		assert.False(t, status.IsFresh)
		require.NotNil(t, status.StaleSince)
	})

	t.Run("fails without usable timestamp column", func(t *testing.T) {
		svc, db := newDuckDBServiceForTest(t)
		_, err := db.Exec(`CREATE TABLE analytics.raw_orders (id INTEGER, amount INTEGER)`)
		require.NoError(t, err)

		_, err = svc.CheckSourceFreshness(context.Background(), "admin", "analytics", "raw_orders", "", 60)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "source freshness requires timestamp column")
	})
}
