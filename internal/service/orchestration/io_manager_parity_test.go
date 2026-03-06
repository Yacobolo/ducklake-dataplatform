package orchestration

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestAssetExecutor_IOManagerParity(t *testing.T) {
	type factory func(t *testing.T) IOManager

	tests := []struct {
		name    string
		factory factory
	}{
		{
			name: "in-memory",
			factory: func(_ *testing.T) IOManager {
				return NewInMemoryIOManager()
			},
		},
		{
			name: "filesystem",
			factory: func(t *testing.T) IOManager {
				manager, err := NewFilesystemIOManager(t.TempDir())
				require.NoError(t, err)
				return manager
			},
		},
	}

	results := make(map[string]parityResult, len(tests))
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			results[tc.name] = runExecutorWithIOManager(t, tc.factory(t))
		})
	}

	inMemory := results["in-memory"]
	filesystem := results["filesystem"]

	assert.Equal(t, inMemory.status, filesystem.status)
	assert.Equal(t, inMemory.attemptCount, filesystem.attemptCount)
	assert.Equal(t, inMemory.retryAttempts, filesystem.retryAttempts)
	assert.Equal(t, inMemory.eventTypes, filesystem.eventTypes)
	assert.Equal(t, inMemory.retryMetadata, filesystem.retryMetadata)
}

type parityResult struct {
	status        string
	attemptCount  int
	retryAttempts []int
	eventTypes    []string
	retryMetadata map[string]any
}

func runExecutorWithIOManager(t *testing.T, io IOManager) parityResult {
	t.Helper()

	runs := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	executor := NewAssetExecutor(
		runs,
		NewAssetRunStateMachine(),
		io,
		NewConcurrencyLimiter(4, 1),
		&ioParityStepper{failFirstForAsset: "b", failuresRemaining: 1},
	)

	runID := "io-parity-run"
	runs.runs[runID] = &domain.AssetRun{ID: runID, Status: domain.AssetRunStatusQueued, MaxAttempts: 2}

	err := executor.ExecutePlan(context.Background(), runID, domain.AssetRunStatusQueued, &AssetRunPlan{
		RootAssetID: "a",
		Levels:      [][]string{{"a"}, {"b"}},
	})
	require.NoError(t, err)

	require.Len(t, runs.events, 3)
	retryEvent := runs.events[1]
	assert.Equal(t, "ASSET_EXECUTION_RETRY", retryEvent.EventType)

	eventTypes := make([]string, 0, len(runs.events))
	for _, event := range runs.events {
		eventTypes = append(eventTypes, event.EventType)
	}

	return parityResult{
		status:        runs.runs[runID].Status,
		attemptCount:  runs.runs[runID].AttemptCount,
		retryAttempts: runs.retryAttempts,
		eventTypes:    eventTypes,
		retryMetadata: retryEvent.MetadataJSON,
	}
}

type ioParityStepper struct {
	failFirstForAsset string
	failuresRemaining int
}

func (s *ioParityStepper) Execute(ctx context.Context, assetID string, io IOManager) (map[string]any, error) {
	if assetID == s.failFirstForAsset && s.failuresRemaining > 0 {
		s.failuresRemaining--
		return nil, fmt.Errorf("transient failure")
	}

	if assetID == "a" {
		return map[string]any{"value": 1}, nil
	}

	upstream, err := io.LoadInput(ctx, "a")
	if err != nil {
		return nil, err
	}

	value, err := numberToInt(upstream["value"])
	if err != nil {
		return nil, err
	}

	return map[string]any{"value_plus_one": value + 1}, nil
}

func numberToInt(v any) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case float64:
		return int(n), nil
	default:
		return 0, fmt.Errorf("unsupported value type %T", v)
	}
}
