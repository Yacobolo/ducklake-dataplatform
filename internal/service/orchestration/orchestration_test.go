package orchestration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestAssetRunStateMachine_CanTransition(t *testing.T) {
	sm := NewAssetRunStateMachine()
	assert.True(t, sm.CanTransition(domain.AssetRunStatusQueued, domain.AssetRunStatusRunning))
	assert.True(t, sm.CanTransition(domain.AssetRunStatusRunning, domain.AssetRunStatusSuccess))
	assert.False(t, sm.CanTransition(domain.AssetRunStatusSuccess, domain.AssetRunStatusRunning))
}

func TestAssetScheduler_BuildPlan(t *testing.T) {
	assets := &fakeAssets{items: map[string]*domain.DataAsset{
		"a": {ID: "a"},
		"b": {ID: "b"},
		"c": {ID: "c"},
	}}
	deps := &fakeDeps{downstream: map[string][]domain.AssetDependency{
		"a": {{AssetID: "b", UpstreamAssetID: "a"}, {AssetID: "c", UpstreamAssetID: "a"}},
	}}

	s := NewAssetScheduler(assets, deps, nil)
	plan, err := s.BuildPlan(context.Background(), "a")
	require.NoError(t, err)
	require.Len(t, plan.Levels, 2)
	assert.Equal(t, []string{"a"}, plan.Levels[0])
	assert.ElementsMatch(t, []string{"b", "c"}, plan.Levels[1])
}

func TestAssetExecutor_ExecutePlan(t *testing.T) {
	runs := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	state := NewAssetRunStateMachine()
	io := NewInMemoryIOManager()
	limiter := NewConcurrencyLimiter(4, 2)
	stepper := &fakeStepper{}

	executor := NewAssetExecutor(runs, state, io, limiter, stepper)
	runID := "run-1"
	runs.runs[runID] = &domain.AssetRun{ID: runID, Status: domain.AssetRunStatusQueued}

	err := executor.ExecutePlan(context.Background(), runID, domain.AssetRunStatusQueued, &AssetRunPlan{
		RootAssetID: "a",
		Levels:      [][]string{{"a"}, {"b"}},
	})
	require.NoError(t, err)
	assert.Equal(t, domain.AssetRunStatusSuccess, runs.runs[runID].Status)
	assert.Len(t, runs.events, 2)
}

func TestAssetExecutor_ExecutePlanFailure(t *testing.T) {
	runs := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	state := NewAssetRunStateMachine()
	io := NewInMemoryIOManager()
	limiter := NewConcurrencyLimiter(2, 1)
	stepper := &fakeStepper{failAsset: "b"}

	executor := NewAssetExecutor(runs, state, io, limiter, stepper)
	runID := "run-2"
	runs.runs[runID] = &domain.AssetRun{ID: runID, Status: domain.AssetRunStatusQueued}

	err := executor.ExecutePlan(context.Background(), runID, domain.AssetRunStatusQueued, &AssetRunPlan{
		RootAssetID: "a",
		Levels:      [][]string{{"a"}, {"b"}},
	})
	require.Error(t, err)
	assert.Equal(t, domain.AssetRunStatusFailed, runs.runs[runID].Status)
}

type fakeAssets struct {
	items map[string]*domain.DataAsset
}

func (f *fakeAssets) Create(context.Context, *domain.DataAsset) (*domain.DataAsset, error) {
	panic("not implemented")
}
func (f *fakeAssets) GetByID(_ context.Context, id string) (*domain.DataAsset, error) {
	v, ok := f.items[id]
	if !ok {
		return nil, domain.ErrNotFound("asset not found")
	}
	return v, nil
}
func (f *fakeAssets) GetByKey(context.Context, string) (*domain.DataAsset, error) {
	panic("not implemented")
}
func (f *fakeAssets) List(context.Context, domain.AssetFilter) ([]domain.DataAsset, int64, error) {
	panic("not implemented")
}
func (f *fakeAssets) Update(context.Context, string, *domain.DataAsset) (*domain.DataAsset, error) {
	panic("not implemented")
}
func (f *fakeAssets) Delete(context.Context, string) error { panic("not implemented") }

type fakeDeps struct {
	downstream map[string][]domain.AssetDependency
}

func (f *fakeDeps) Create(context.Context, *domain.AssetDependency) (*domain.AssetDependency, error) {
	panic("not implemented")
}
func (f *fakeDeps) ListUpstream(context.Context, string) ([]domain.AssetDependency, error) {
	panic("not implemented")
}
func (f *fakeDeps) ListDownstream(_ context.Context, upstreamAssetID string) ([]domain.AssetDependency, error) {
	return f.downstream[upstreamAssetID], nil
}
func (f *fakeDeps) Delete(context.Context, string) error        { panic("not implemented") }
func (f *fakeDeps) DeleteByAsset(context.Context, string) error { panic("not implemented") }

type fakeRunRepo struct {
	runs   map[string]*domain.AssetRun
	events []domain.AssetRunEvent
}

func (f *fakeRunRepo) CreateRun(context.Context, *domain.AssetRun) (*domain.AssetRun, error) {
	panic("not implemented")
}
func (f *fakeRunRepo) GetRunByID(_ context.Context, id string) (*domain.AssetRun, error) {
	return f.runs[id], nil
}
func (f *fakeRunRepo) ListRuns(context.Context, domain.AssetRunFilter) ([]domain.AssetRun, int64, error) {
	panic("not implemented")
}
func (f *fakeRunRepo) UpdateRunStarted(_ context.Context, id string) error {
	f.runs[id].Status = domain.AssetRunStatusRunning
	now := time.Now().UTC()
	f.runs[id].StartedAt = &now
	return nil
}
func (f *fakeRunRepo) UpdateRunFinished(_ context.Context, id string, status string, errMsg *string) error {
	f.runs[id].Status = status
	f.runs[id].ErrorMessage = errMsg
	now := time.Now().UTC()
	f.runs[id].FinishedAt = &now
	return nil
}
func (f *fakeRunRepo) UpdateRunRetrying(_ context.Context, id string, attempt int, errMsg *string) error {
	f.runs[id].Status = domain.AssetRunStatusRetrying
	f.runs[id].AttemptCount = attempt
	f.runs[id].ErrorMessage = errMsg
	return nil
}
func (f *fakeRunRepo) CreateRunEvent(_ context.Context, event *domain.AssetRunEvent) (*domain.AssetRunEvent, error) {
	f.events = append(f.events, *event)
	return event, nil
}
func (f *fakeRunRepo) ListRunEvents(context.Context, string, domain.PageRequest) ([]domain.AssetRunEvent, int64, error) {
	panic("not implemented")
}
func (f *fakeRunRepo) CreateMaterialization(context.Context, *domain.AssetMaterialization) (*domain.AssetMaterialization, error) {
	panic("not implemented")
}
func (f *fakeRunRepo) ListMaterializationsByAsset(context.Context, string, domain.PageRequest) ([]domain.AssetMaterialization, int64, error) {
	panic("not implemented")
}

type fakeStepper struct {
	failAsset string
}

func (f *fakeStepper) Execute(_ context.Context, assetID string, _ IOManager) (map[string]any, error) {
	if assetID == f.failAsset {
		return nil, errors.New("boom")
	}
	return map[string]any{"asset": assetID, "row_count": 1}, nil
}
