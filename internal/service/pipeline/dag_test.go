package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestResolveExecutionOrder(t *testing.T) {
	// levelIDs is a helper that extracts the set of IDs in a level.
	levelIDs := func(t *testing.T, level []string) map[string]struct{} {
		t.Helper()
		ids := make(map[string]struct{}, len(level))
		for _, id := range level {
			ids[id] = struct{}{}
		}
		return ids
	}

	tests := []struct {
		name       string
		jobs       []domain.PipelineJob
		wantLevels int
		wantIDs    []map[string]struct{} // expected IDs per level (nil if expecting error)
		wantErr    bool
		errType    any // expected error type target for assert.ErrorAs
	}{
		{
			name: "single_job_no_deps",
			jobs: []domain.PipelineJob{
				{ID: "j1", Name: "extract"},
			},
			wantLevels: 1,
			wantIDs: []map[string]struct{}{
				{"j1": {}},
			},
		},
		{
			name: "linear_chain",
			jobs: []domain.PipelineJob{
				{ID: "j1", Name: "A"},
				{ID: "j2", Name: "B", DependsOn: []string{"A"}},
				{ID: "j3", Name: "C", DependsOn: []string{"B"}},
			},
			wantLevels: 3,
			wantIDs: []map[string]struct{}{
				{"j1": {}},
				{"j2": {}},
				{"j3": {}},
			},
		},
		{
			name: "diamond_dependency",
			jobs: []domain.PipelineJob{
				{ID: "j1", Name: "extract"},
				{ID: "j2", Name: "transform-a", DependsOn: []string{"extract"}},
				{ID: "j3", Name: "transform-b", DependsOn: []string{"extract"}},
				{ID: "j4", Name: "load", DependsOn: []string{"transform-a", "transform-b"}},
			},
			wantLevels: 3,
			wantIDs: []map[string]struct{}{
				{"j1": {}},
				{"j2": {}, "j3": {}},
				{"j4": {}},
			},
		},
		{
			name: "parallel_independent_jobs",
			jobs: []domain.PipelineJob{
				{ID: "j1", Name: "ingest-a"},
				{ID: "j2", Name: "ingest-b"},
				{ID: "j3", Name: "ingest-c"},
			},
			wantLevels: 1,
			wantIDs: []map[string]struct{}{
				{"j1": {}, "j2": {}, "j3": {}},
			},
		},
		{
			name: "cycle_detected",
			jobs: []domain.PipelineJob{
				{ID: "j1", Name: "A", DependsOn: []string{"B"}},
				{ID: "j2", Name: "B", DependsOn: []string{"A"}},
			},
			wantErr: true,
			errType: new(*domain.ValidationError),
		},
		{
			name: "unknown_dependency",
			jobs: []domain.PipelineJob{
				{ID: "j1", Name: "A", DependsOn: []string{"nonexistent"}},
			},
			wantErr: true,
			errType: new(*domain.ValidationError),
		},
		{
			name:    "empty_jobs",
			jobs:    nil,
			wantErr: false,
		},
		{
			name: "self_dependency",
			jobs: []domain.PipelineJob{
				{ID: "j1", Name: "A", DependsOn: []string{"A"}},
			},
			wantErr: true,
			errType: new(*domain.ValidationError),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			levels, err := ResolveExecutionOrder(tt.jobs)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorAs(t, err, tt.errType)
				return
			}

			require.NoError(t, err)

			if tt.jobs == nil {
				assert.Nil(t, levels)
				return
			}

			require.Len(t, levels, tt.wantLevels)

			for i, wantSet := range tt.wantIDs {
				gotSet := levelIDs(t, levels[i])
				assert.Equal(t, wantSet, gotSet, "level %d mismatch", i)
			}
		})
	}
}
