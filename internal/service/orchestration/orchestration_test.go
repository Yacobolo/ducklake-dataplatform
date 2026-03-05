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
	t.Run("succeeds after retry", func(t *testing.T) {
		runs := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
		state := NewAssetRunStateMachine()
		io := NewInMemoryIOManager()
		limiter := NewConcurrencyLimiter(4, 2)
		stepper := &fakeStepper{failuresRemaining: map[string]int{"b": 1}}

		executor := NewAssetExecutor(runs, state, io, limiter, stepper)
		runID := "run-1"
		runs.runs[runID] = &domain.AssetRun{ID: runID, Status: domain.AssetRunStatusQueued, MaxAttempts: 2}

		err := executor.ExecutePlan(context.Background(), runID, domain.AssetRunStatusQueued, &AssetRunPlan{
			RootAssetID: "a",
			Levels:      [][]string{{"a"}, {"b"}},
		})
		require.NoError(t, err)
		assert.Equal(t, domain.AssetRunStatusSuccess, runs.runs[runID].Status)
		assert.Equal(t, 1, runs.runs[runID].AttemptCount)
		assert.Equal(t, []int{1}, runs.retryAttempts)
		assert.Equal(t, 2, runs.startCalls)
		assert.Equal(t, 2, stepper.executeCalls["b"])
		require.Len(t, runs.events, 3)
		assert.Equal(t, "ASSET_EXECUTION_RETRY", runs.events[1].EventType)
		assert.Equal(t, "b", runs.events[1].MetadataJSON["asset_id"])
		assert.Equal(t, 1, runs.events[1].MetadataJSON["attempt"])
		assert.Equal(t, 2, runs.events[1].MetadataJSON["max_attempts"])
	})

	t.Run("fails after max attempts", func(t *testing.T) {
		runs := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
		state := NewAssetRunStateMachine()
		io := NewInMemoryIOManager()
		limiter := NewConcurrencyLimiter(2, 1)
		stepper := &fakeStepper{failuresRemaining: map[string]int{"b": 2}}

		executor := NewAssetExecutor(runs, state, io, limiter, stepper)
		runID := "run-2"
		runs.runs[runID] = &domain.AssetRun{ID: runID, Status: domain.AssetRunStatusQueued, MaxAttempts: 2}

		err := executor.ExecutePlan(context.Background(), runID, domain.AssetRunStatusQueued, &AssetRunPlan{
			RootAssetID: "a",
			Levels:      [][]string{{"a"}, {"b"}},
		})
		require.Error(t, err)
		assert.Equal(t, domain.AssetRunStatusFailed, runs.runs[runID].Status)
		assert.Equal(t, []int{1}, runs.retryAttempts)
		require.Len(t, runs.events, 3)
		assert.Equal(t, "ASSET_EXECUTION_FAILED", runs.events[2].EventType)
		assert.Equal(t, "b", runs.events[2].MetadataJSON["asset_id"])
		assert.Equal(t, 2, runs.events[2].MetadataJSON["attempt"])
	})

	t.Run("retry transitions reflected in repo state", func(t *testing.T) {
		runs := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
		state := NewAssetRunStateMachine()
		io := NewInMemoryIOManager()
		limiter := NewConcurrencyLimiter(1, 1)
		stepper := &fakeStepper{failuresRemaining: map[string]int{"a": 1}}

		executor := NewAssetExecutor(runs, state, io, limiter, stepper)
		runID := "run-3"
		runs.runs[runID] = &domain.AssetRun{ID: runID, Status: domain.AssetRunStatusQueued, MaxAttempts: 3}

		err := executor.ExecutePlan(context.Background(), runID, domain.AssetRunStatusQueued, &AssetRunPlan{
			RootAssetID: "a",
			Levels:      [][]string{{"a"}},
		})
		require.NoError(t, err)
		assert.Equal(t, domain.AssetRunStatusSuccess, runs.runs[runID].Status)
		assert.Equal(t, 1, runs.runs[runID].AttemptCount)
		assert.Equal(t, []int{1}, runs.retryAttempts)
		assert.Equal(t, 2, runs.startCalls)
		require.Len(t, runs.events, 2)
		assert.Equal(t, "ASSET_EXECUTION_RETRY", runs.events[0].EventType)
		assert.Equal(t, "ASSET_EXECUTED", runs.events[1].EventType)
	})
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
	upstream   map[string][]domain.AssetDependency
	downstream map[string][]domain.AssetDependency
}

func (f *fakeDeps) Create(context.Context, *domain.AssetDependency) (*domain.AssetDependency, error) {
	panic("not implemented")
}
func (f *fakeDeps) ListUpstream(_ context.Context, assetID string) ([]domain.AssetDependency, error) {
	return f.upstream[assetID], nil
}
func (f *fakeDeps) ListDownstream(_ context.Context, upstreamAssetID string) ([]domain.AssetDependency, error) {
	return f.downstream[upstreamAssetID], nil
}
func (f *fakeDeps) Delete(context.Context, string) error        { panic("not implemented") }
func (f *fakeDeps) DeleteByAsset(context.Context, string) error { panic("not implemented") }

type fakeRunRepo struct {
	runs          map[string]*domain.AssetRun
	events        []domain.AssetRunEvent
	materialize   map[string][]domain.AssetMaterialization
	createOrder   []string
	startCalls    int
	retryAttempts []int
}

func (f *fakeRunRepo) CreateRun(_ context.Context, run *domain.AssetRun) (*domain.AssetRun, error) {
	if f.runs == nil {
		f.runs = map[string]*domain.AssetRun{}
	}
	runCopy := *run
	if runCopy.ID == "" {
		runCopy.ID = domain.NewID()
	}
	now := time.Now().UTC()
	if runCopy.CreatedAt.IsZero() {
		runCopy.CreatedAt = now
	}
	if runCopy.UpdatedAt.IsZero() {
		runCopy.UpdatedAt = now
	}
	f.runs[runCopy.ID] = &runCopy
	if runCopy.PartitionKey != nil {
		f.createOrder = append(f.createOrder, *runCopy.PartitionKey)
	}
	return &runCopy, nil
}
func (f *fakeRunRepo) GetRunByID(_ context.Context, id string) (*domain.AssetRun, error) {
	return f.runs[id], nil
}
func (f *fakeRunRepo) ListRuns(_ context.Context, filter domain.AssetRunFilter) ([]domain.AssetRun, int64, error) {
	out := make([]domain.AssetRun, 0, len(f.runs))
	for _, run := range f.runs {
		if filter.AssetID != nil && run.AssetID != *filter.AssetID {
			continue
		}
		if filter.Status != nil && run.Status != *filter.Status {
			continue
		}
		out = append(out, *run)
	}
	return out, int64(len(out)), nil
}
func (f *fakeRunRepo) UpdateRunStarted(_ context.Context, id string) error {
	f.runs[id].Status = domain.AssetRunStatusRunning
	now := time.Now().UTC()
	f.runs[id].StartedAt = &now
	f.startCalls++
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
	f.retryAttempts = append(f.retryAttempts, attempt)
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
func (f *fakeRunRepo) ListMaterializationsByAsset(_ context.Context, assetID string, page domain.PageRequest) ([]domain.AssetMaterialization, int64, error) {
	materializations := f.materialize[assetID]
	if len(materializations) == 0 {
		return nil, 0, nil
	}
	offset := page.Offset()
	if offset >= len(materializations) {
		return nil, int64(len(materializations)), nil
	}
	limit := page.Limit()
	end := offset + limit
	if end > len(materializations) {
		end = len(materializations)
	}
	out := make([]domain.AssetMaterialization, 0, end-offset)
	out = append(out, materializations[offset:end]...)
	return out, int64(len(materializations)), nil
}

type fakeStepper struct {
	failuresRemaining map[string]int
	executeCalls      map[string]int
}

func (f *fakeStepper) Execute(_ context.Context, assetID string, _ IOManager) (map[string]any, error) {
	if f.executeCalls == nil {
		f.executeCalls = map[string]int{}
	}
	f.executeCalls[assetID]++

	if f.failuresRemaining[assetID] > 0 {
		f.failuresRemaining[assetID]--
		return nil, errors.New("boom")
	}
	return map[string]any{"asset": assetID, "row_count": 1}, nil
}
