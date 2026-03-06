package orchestration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestBackfillRunner_RunRequestLifecycleAndOrdering(t *testing.T) {
	ctx := context.Background()
	repo := newBackfillRepoForRunnerTest()
	repo.requests["req-1"] = domain.BackfillRequest{
		ID:            "req-1",
		AssetID:       "asset-1",
		PartitionFrom: "2026-03-01",
		PartitionTo:   "2026-03-03",
		Status:        domain.BackfillStatusPending,
		RequestedBy:   "admin",
	}
	repo.slices["s-1"] = domain.BackfillSlice{ID: "s-1", RequestID: "req-1", AssetID: "asset-1", PartitionKey: "2026-03-03", Status: domain.BackfillStatusPending}
	repo.slices["s-2"] = domain.BackfillSlice{ID: "s-2", RequestID: "req-1", AssetID: "asset-1", PartitionKey: "2026-03-01", Status: domain.BackfillStatusPending}
	repo.slices["s-3"] = domain.BackfillSlice{ID: "s-3", RequestID: "req-1", AssetID: "asset-1", PartitionKey: "2026-03-02", Status: domain.BackfillStatusPending}

	assets := &fakeAssets{items: map[string]*domain.DataAsset{"asset-1": {ID: "asset-1"}}}
	deps := &fakeDeps{downstream: map[string][]domain.AssetDependency{}, upstream: map[string][]domain.AssetDependency{}}
	runs := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	scheduler := NewAssetScheduler(assets, deps, runs)
	executor := NewAssetExecutor(runs, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})
	runner := NewBackfillRunner(repo, deps, runs, scheduler, executor)

	err := runner.RunRequest(ctx, "req-1")
	require.NoError(t, err)

	assert.Equal(t, domain.BackfillStatusSuccess, repo.requests["req-1"].Status)
	assert.Equal(t, []string{"2026-03-01", "2026-03-02", "2026-03-03"}, runs.createOrder)
	for _, id := range []string{"s-1", "s-2", "s-3"} {
		s := repo.slices[id]
		assert.Equal(t, domain.BackfillStatusSuccess, s.Status)
		require.NotNil(t, s.RunID)
		assert.NotEmpty(t, *s.RunID)
		run, ok := runs.runs[*s.RunID]
		require.True(t, ok)
		require.NotNil(t, run.PartitionFrom)
		require.NotNil(t, run.PartitionTo)
		assert.Equal(t, "2026-03-01", *run.PartitionFrom)
		assert.Equal(t, "2026-03-03", *run.PartitionTo)
	}
}

func TestBackfillRunner_RunRequestMarksDeferredWhenBlocked(t *testing.T) {
	ctx := context.Background()
	repo := newBackfillRepoForRunnerTest()
	repo.requests["req-2"] = domain.BackfillRequest{
		ID:            "req-2",
		AssetID:       "asset-target",
		PartitionFrom: "2026-03-01",
		PartitionTo:   "2026-03-01",
		Status:        domain.BackfillStatusPending,
		RequestedBy:   "admin",
	}
	repo.slices["slice-a"] = domain.BackfillSlice{ID: "slice-a", RequestID: "req-2", AssetID: "asset-target", PartitionKey: "2026-03-01", Status: domain.BackfillStatusPending}

	assets := &fakeAssets{items: map[string]*domain.DataAsset{
		"asset-target": {ID: "asset-target"},
		"upstream-1":   {ID: "upstream-1"},
	}}
	deps := &fakeDeps{
		downstream: map[string][]domain.AssetDependency{},
		upstream: map[string][]domain.AssetDependency{
			"asset-target": {{AssetID: "asset-target", UpstreamAssetID: "upstream-1", DependencyType: domain.DependencyTypeHard}},
		},
	}
	runs := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	scheduler := NewAssetScheduler(assets, deps, runs)
	executor := NewAssetExecutor(runs, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})
	runner := NewBackfillRunner(repo, deps, runs, scheduler, executor)

	err := runner.RunRequest(ctx, "req-2")
	require.Error(t, err)

	assert.Equal(t, domain.BackfillStatusFailed, repo.requests["req-2"].Status)
	slice := repo.slices["slice-a"]
	assert.Equal(t, domain.BackfillStatusFailed, slice.Status)
	require.NotNil(t, slice.ErrorMessage)
	assert.Contains(t, *slice.ErrorMessage, "deferred")
	assert.Contains(t, *slice.ErrorMessage, "upstream")
	assert.Empty(t, runs.createOrder)
}

func TestBackfillRunner_RunRequestHonorsMaxParallelism(t *testing.T) {
	ctx := context.Background()
	repo := newBackfillRepoForRunnerTest()
	repo.requests["req-par"] = domain.BackfillRequest{
		ID:             "req-par",
		AssetID:        "asset-1",
		PartitionFrom:  "2026-03-01",
		PartitionTo:    "2026-03-03",
		Status:         domain.BackfillStatusPending,
		RequestedBy:    "admin",
		MaxParallelism: 2,
	}
	repo.slices["s-1"] = domain.BackfillSlice{ID: "s-1", RequestID: "req-par", AssetID: "asset-1", PartitionKey: "2026-03-01", Status: domain.BackfillStatusPending}
	repo.slices["s-2"] = domain.BackfillSlice{ID: "s-2", RequestID: "req-par", AssetID: "asset-1", PartitionKey: "2026-03-02", Status: domain.BackfillStatusPending}
	repo.slices["s-3"] = domain.BackfillSlice{ID: "s-3", RequestID: "req-par", AssetID: "asset-1", PartitionKey: "2026-03-03", Status: domain.BackfillStatusPending}

	assets := &fakeAssets{items: map[string]*domain.DataAsset{"asset-1": {ID: "asset-1"}}}
	deps := &fakeDeps{downstream: map[string][]domain.AssetDependency{}, upstream: map[string][]domain.AssetDependency{}}
	runs := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	stepper := newGateStepper()
	scheduler := NewAssetScheduler(assets, deps, runs)
	executor := NewAssetExecutor(runs, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(10, 10), stepper)
	runner := NewBackfillRunner(repo, deps, runs, scheduler, executor)

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.RunRequest(ctx, "req-par")
	}()

	stepper.waitForStarted(t)
	stepper.waitForStarted(t)
	stepper.assertNoAdditionalStart(t)

	stepper.releaseOne()
	stepper.waitForStarted(t)

	stepper.releaseOne()
	stepper.releaseOne()

	require.NoError(t, <-errCh)
	assert.Equal(t, 2, stepper.maxInFlight())
	assert.Equal(t, domain.BackfillStatusSuccess, repo.requests["req-par"].Status)
}

func TestBackfillRunner_RunRequestNormalizesMaxParallelismToOne(t *testing.T) {
	ctx := context.Background()
	repo := newBackfillRepoForRunnerTest()
	repo.requests["req-norm"] = domain.BackfillRequest{
		ID:            "req-norm",
		AssetID:       "asset-1",
		PartitionFrom: "2026-03-01",
		PartitionTo:   "2026-03-02",
		Status:        domain.BackfillStatusPending,
		RequestedBy:   "admin",
	}
	repo.slices["s-1"] = domain.BackfillSlice{ID: "s-1", RequestID: "req-norm", AssetID: "asset-1", PartitionKey: "2026-03-01", Status: domain.BackfillStatusPending}
	repo.slices["s-2"] = domain.BackfillSlice{ID: "s-2", RequestID: "req-norm", AssetID: "asset-1", PartitionKey: "2026-03-02", Status: domain.BackfillStatusPending}

	assets := &fakeAssets{items: map[string]*domain.DataAsset{"asset-1": {ID: "asset-1"}}}
	deps := &fakeDeps{downstream: map[string][]domain.AssetDependency{}, upstream: map[string][]domain.AssetDependency{}}
	runs := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	stepper := newGateStepper()
	scheduler := NewAssetScheduler(assets, deps, runs)
	executor := NewAssetExecutor(runs, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(10, 10), stepper)
	runner := NewBackfillRunner(repo, deps, runs, scheduler, executor)

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.RunRequest(ctx, "req-norm")
	}()

	stepper.waitForStarted(t)
	stepper.assertNoAdditionalStart(t)
	stepper.releaseOne()
	stepper.waitForStarted(t)
	stepper.releaseOne()

	require.NoError(t, <-errCh)
	assert.Equal(t, 1, stepper.maxInFlight())
	assert.Equal(t, domain.BackfillStatusSuccess, repo.requests["req-norm"].Status)
}

type backfillRepoForRunnerTest struct {
	mu       sync.Mutex
	requests map[string]domain.BackfillRequest
	slices   map[string]domain.BackfillSlice
}

func newBackfillRepoForRunnerTest() *backfillRepoForRunnerTest {
	return &backfillRepoForRunnerTest{
		requests: map[string]domain.BackfillRequest{},
		slices:   map[string]domain.BackfillSlice{},
	}
}

func (m *backfillRepoForRunnerTest) CreateRequest(context.Context, *domain.BackfillRequest) (*domain.BackfillRequest, error) {
	panic("not implemented")
}

func (m *backfillRepoForRunnerTest) GetRequestByID(_ context.Context, id string) (*domain.BackfillRequest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	req, ok := m.requests[id]
	if !ok {
		return nil, domain.ErrNotFound("backfill request not found")
	}
	copyReq := req
	return &copyReq, nil
}

func (m *backfillRepoForRunnerTest) ListRequests(context.Context, domain.BackfillFilter) ([]domain.BackfillRequest, int64, error) {
	panic("not implemented")
}

func (m *backfillRepoForRunnerTest) UpdateRequestStatus(_ context.Context, id string, status string, errMsg *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	req := m.requests[id]
	req.Status = status
	req.ErrorMessage = errMsg
	m.requests[id] = req
	return nil
}

func (m *backfillRepoForRunnerTest) CreateSlice(context.Context, *domain.BackfillSlice) (*domain.BackfillSlice, error) {
	panic("not implemented")
}

func (m *backfillRepoForRunnerTest) ListSlicesByRequest(_ context.Context, requestID string) ([]domain.BackfillSlice, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]domain.BackfillSlice, 0)
	for _, s := range m.slices {
		if s.RequestID == requestID {
			out = append(out, s)
		}
	}
	return out, nil
}

func (m *backfillRepoForRunnerTest) UpdateSliceStatus(_ context.Context, id string, status string, runID *string, errMsg *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s := m.slices[id]
	s.Status = status
	if runID != nil {
		v := *runID
		s.RunID = &v
	}
	s.ErrorMessage = errMsg
	m.slices[id] = s
	return nil
}

type gateStepper struct {
	started chan struct{}
	release chan struct{}

	mu          sync.Mutex
	inFlight    int
	maxObserved int
}

func newGateStepper() *gateStepper {
	return &gateStepper{
		started: make(chan struct{}, 16),
		release: make(chan struct{}, 16),
	}
}

func (g *gateStepper) Execute(_ context.Context, _ string, _ IOManager) (map[string]any, error) {
	g.mu.Lock()
	g.inFlight++
	if g.inFlight > g.maxObserved {
		g.maxObserved = g.inFlight
	}
	g.mu.Unlock()

	g.started <- struct{}{}
	<-g.release

	g.mu.Lock()
	g.inFlight--
	g.mu.Unlock()

	return map[string]any{"rows": 1}, nil
}

func (g *gateStepper) waitForStarted(t *testing.T) {
	t.Helper()
	select {
	case <-g.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for execution start")
	}
}

func (g *gateStepper) assertNoAdditionalStart(t *testing.T) {
	t.Helper()
	select {
	case <-g.started:
		t.Fatal("unexpected additional execution start")
	case <-time.After(150 * time.Millisecond):
	}
}

func (g *gateStepper) releaseOne() {
	g.release <- struct{}{}
}

func (g *gateStepper) maxInFlight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.maxObserved
}
